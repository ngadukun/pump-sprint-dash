"""Pump Sprint paper-trading bot (agent-fun/replay/v2).

Reads the harness stream on stdin: one hello message, then a batch per
step (each batch carries fills, the account snapshot, curve marks and the
slot's events), then done. For every batch it writes exactly one reply
line holding its orders for that slot.

Strategy, in one paragraph: follow short bursts of one-sided buying on
young curves, but only where the real reserve is deep enough that the
size cap still allows a meaningful stake; size each stake as a small,
fixed slice of the starting capital; leave on a take-profit, a stop-loss,
a trailing give-back, or a maximum hold measured in slots; and stop
opening new positions once the equity drawdown from its peak gets close
to the guardrail. The guardrails ask for round trips of at least 0.01 SOL
on at least 20 distinct coins, so while the run is young the bot prefers
coins it has never traded, which spreads the required round trips out
instead of piling them onto a few names.

Everything is deterministic: no clock, no randomness, no network, and
every iteration order is either insertion order or an explicit sort, so
two runs over the same tape produce the same action log byte for byte.
"""

import json
import sys
from collections import deque

BPS = 10_000
LAMPORTS_PER_SOL = 1_000_000_000

# --- strategy constants -------------------------------------------------
STAKE_LAMPORTS = LAMPORTS_PER_SOL // 4          # 0.25 SOL target stake
MIN_STAKE_LAMPORTS = LAMPORTS_PER_SOL // 100    # 0.01 SOL guardrail floor
FLOW_WINDOW = 24                                # trades kept per coin
MIN_TRADES_TO_ENTER = 6                         # flow sample before entry
MIN_BUY_SHARE = 0.68                            # buy share of flow volume
MIN_PRICE_GAIN = 0.04                           # price rise over the window
MIN_REAL_RESERVE = 8 * LAMPORTS_PER_SOL         # depth needed for the cap
TAKE_PROFIT = 0.30
STOP_LOSS = -0.16
TRAIL_ARM = 0.18                                # gain that arms the trail
TRAIL_GIVEBACK = 0.10                           # fall from peak that exits
MAX_HOLD_SLOTS = 9_000                          # ~1 hour at 0.4 s slots
DRAWDOWN_HALT = 0.35                            # stop buying past this
SLIPPAGE_BPS = 1_000


class Coin:
    """Rolling per-coin view built from marks and trade events."""

    __slots__ = (
        "mint", "created_slot", "complete", "reserves_slot",
        "virtual_sol", "virtual_token", "real_sol", "real_token",
        "flow", "first_price", "last_price", "trades",
    )

    def __init__(self, mint, created_slot, slot):
        self.mint = mint
        self.created_slot = created_slot
        self.complete = False
        self.reserves_slot = slot
        self.virtual_sol = 0
        self.virtual_token = 0
        self.real_sol = 0
        self.real_token = 0
        self.flow = deque(maxlen=FLOW_WINDOW)
        self.first_price = 0.0
        self.last_price = 0.0
        self.trades = 0

    def set_reserves(self, vsol, vtok, rsol, rtok, slot):
        self.virtual_sol = vsol
        self.virtual_token = vtok
        self.real_sol = rsol
        self.real_token = rtok
        self.reserves_slot = slot
        if vtok > 0:
            self.last_price = vsol / vtok

    def note_trade(self, is_buy, sol_amount):
        self.flow.append((1 if is_buy else -1, sol_amount, self.last_price))
        self.trades += 1

    def buy_share(self):
        buy = sum(a for s, a, _ in self.flow if s > 0)
        total = sum(a for _, a, _ in self.flow)
        return buy / total if total > 0 else 0.0

    def window_gain(self):
        if not self.flow or self.last_price <= 0.0:
            return 0.0
        start = self.flow[0][2]
        if start <= 0.0:
            return 0.0
        return self.last_price / start - 1.0


class Bot:
    def __init__(self, hello):
        self.hello = hello
        self.caps = hello["caps"]
        self.cash = int(hello["capital"]["lamports"])
        self.start_cash = self.cash
        fees = hello["fees"]
        self.tx_fee = int(fees["base_fee_lamports"]) + int(fees["priority_fee_lamports"])
        self.coins = {}
        self.positions = {}          # mint -> {"tokens": int, "cost": int}
        self.pending = {}            # client_id -> order dict
        self.entered_slot = {}       # mint -> slot the position was opened
        self.peak_value = {}         # mint -> best mark-to-market gain seen
        self.traded = set()          # mints with at least one buy sent
        self.decided = set()         # mints ruled out for this run
        self.next_id = 1
        self.slot = -1
        self.equity_peak = float(self.cash)

    # -- state folding ----------------------------------------------------
    def coin(self, mint, created_slot, slot):
        c = self.coins.get(mint)
        if c is None:
            c = Coin(mint, created_slot, slot)
            self.coins[mint] = c
        return c

    def apply_batch(self, batch):
        self.slot = batch["slot"]
        for f in batch.get("fills", []):
            self.pending.pop(f["client_id"], None)
        account = batch["account"]
        self.cash = int(account["cash_lamports"])
        self.positions = {}
        for p in account["positions"]:
            tokens = int(p["token_amount"])
            if tokens > 0:
                self.positions[p["mint"]] = {
                    "tokens": tokens,
                    "cost": int(p["cost_lamports"]),
                }
        for m in batch.get("marks", []):
            c = self.coins.get(m["mint"])
            if c is not None and m["slot"] < c.reserves_slot:
                continue
            if c is None:
                c = self.coin(m["mint"], None, m["slot"])
            c.set_reserves(
                int(m["virtual_sol_reserves"]), int(m["virtual_token_reserves"]),
                int(m["real_sol_reserves"]), int(m["real_token_reserves"]),
                m["slot"],
            )
        for e in batch.get("events", []):
            kind = e["event"]
            mint = e["mint"]
            if kind == "CreateEvent":
                c = self.coin(mint, e["slot"], e["slot"])
                c.set_reserves(
                    int(e["virtual_sol_reserves"]), int(e["virtual_token_reserves"]),
                    int(e["real_sol_reserves"]), 0, e["slot"],
                )
                c.real_sol = int(e["real_sol_reserves"])
            elif kind == "TradeEvent":
                c = self.coins.get(mint) or self.coin(mint, None, e["slot"])
                c.set_reserves(
                    int(e["virtual_sol_reserves"]), int(e["virtual_token_reserves"]),
                    int(e["real_sol_reserves"]), int(e["real_token_reserves"]),
                    e["slot"],
                )
                c.note_trade(bool(e["is_buy"]), int(e["sol_amount"]))
            else:  # CompleteEvent
                c = self.coins.get(mint) or self.coin(mint, None, e["slot"])
                c.complete = True

    # -- accounting helpers -------------------------------------------------
    def free_cash(self):
        free = self.cash
        for o in self.pending.values():
            free -= self.tx_fee
            if o["side"] == "buy":
                free -= int(o["quote_lamports"])
        return free

    def committed_count(self):
        mints = set(self.positions)
        for o in self.pending.values():
            if o["side"] == "buy":
                mints.add(o["mint"])
        return len(mints)

    def max_buy(self, coin):
        if coin.complete:
            return 0
        return coin.real_sol * self.caps["max_order_bps_of_real_reserve"] // BPS

    def equity(self):
        total = float(self.cash)
        for mint, pos in self.positions.items():
            c = self.coins.get(mint)
            if c is not None and c.virtual_token > 0:
                total += pos["tokens"] * c.virtual_sol / c.virtual_token
            else:
                total += pos["cost"]
        return total

    def new_id(self):
        cid = "o%d" % self.next_id
        self.next_id += 1
        return cid

    # -- decisions ------------------------------------------------------------
    def decide(self, batch):
        orders = []
        limit = self.caps["max_orders_per_batch"]
        free = self.free_cash()
        slots_open = self.caps["max_open_positions"] - self.committed_count()
        equity = self.equity()
        if equity > self.equity_peak:
            self.equity_peak = equity
        drawdown = 1.0 - equity / self.equity_peak if self.equity_peak > 0 else 0.0

        # Exits first: they free both cash and position slots.
        selling = {o["mint"] for o in self.pending.values() if o["side"] == "sell"}
        for mint in sorted(self.positions):
            if len(orders) >= limit:
                break
            if mint in selling:
                continue
            pos = self.positions[mint]
            c = self.coins.get(mint)
            if c is None or c.virtual_token <= 0:
                continue
            value = pos["tokens"] * c.virtual_sol / c.virtual_token
            gain = value / pos["cost"] - 1.0 if pos["cost"] > 0 else 0.0
            peak = max(self.peak_value.get(mint, gain), gain)
            self.peak_value[mint] = peak
            held = self.slot - self.entered_slot.get(mint, self.slot)
            exit_now = (
                gain >= TAKE_PROFIT
                or gain <= STOP_LOSS
                or (peak >= TRAIL_ARM and gain <= peak - TRAIL_GIVEBACK)
                or held >= MAX_HOLD_SLOTS
                or c.complete
            )
            if exit_now:
                orders.append({
                    "side": "sell",
                    "client_id": self.new_id(),
                    "mint": mint,
                    "token_amount": str(pos["tokens"]),
                    "max_slippage_bps": SLIPPAGE_BPS,
                })

        # Entries: strongest fresh flow first; untraded coins win ties so the
        # 20-coin round-trip guardrail is met early rather than late.
        if drawdown < DRAWDOWN_HALT:
            ranked = []
            for mint, c in self.coins.items():
                if mint in self.positions or mint in self.decided or c.complete:
                    continue
                if any(o["side"] == "buy" and o["mint"] == mint for o in self.pending.values()):
                    continue
                if len(c.flow) < MIN_TRADES_TO_ENTER:
                    continue
                share = c.buy_share()
                gain = c.window_gain()
                if share < MIN_BUY_SHARE or gain < MIN_PRICE_GAIN:
                    continue
                if c.real_sol < MIN_REAL_RESERVE:
                    continue
                score = share * 2.0 + gain
                ranked.append((0 if mint not in self.traded else 1, -score, mint))
            ranked.sort()
            for _, _, mint in ranked:
                if len(orders) >= limit or slots_open <= 0:
                    break
                c = self.coins[mint]
                amount = min(STAKE_LAMPORTS, self.max_buy(c))
                if amount < MIN_STAKE_LAMPORTS:
                    self.decided.add(mint)
                    continue
                if free < amount + self.tx_fee:
                    break
                orders.append({
                    "side": "buy",
                    "client_id": self.new_id(),
                    "mint": mint,
                    "quote_lamports": str(amount),
                    "max_slippage_bps": SLIPPAGE_BPS,
                })
                free -= amount + self.tx_fee
                slots_open -= 1
                self.traded.add(mint)
                self.entered_slot[mint] = self.slot

        for o in orders:
            self.pending[o["client_id"]] = o
        return orders


def main():
    out = sys.stdout
    bot = None
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        msg = json.loads(line)
        kind = msg.get("type")
        if kind == "hello":
            bot = Bot(msg)
        elif kind == "batch":
            bot.apply_batch(msg)
            orders = bot.decide(msg)
            reply = {"type": "reply", "seq": msg["seq"], "orders": orders}
            out.write(json.dumps(reply, separators=(",", ":")) + "\n")
            out.flush()
        elif kind == "done":
            break


if __name__ == "__main__":
    main()
