# pump-sprint-dash

[![make.fun · u-kiencx-pump-sprint-12-hour-paper-trading-dash-29aa76d4](https://make.fun/badge/u-kiencx-pump-sprint-12-hour-paper-trading-dash-29aa76d4.svg)](https://make.fun/bounty/u-kiencx-pump-sprint-12-hour-paper-trading-dash-29aa76d4?utm_source=github&utm_medium=readme&utm_campaign=u-kiencx-pump-sprint-12-hour-paper-trading-dash-29aa76d4)
[![rank](https://make.fun/badge/u-kiencx-pump-sprint-12-hour-paper-trading-dash-29aa76d4/ngadukun/pump-sprint-dash/rank.svg)](https://make.fun/bounty/u-kiencx-pump-sprint-12-hour-paper-trading-dash-29aa76d4/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=u-kiencx-pump-sprint-12-hour-paper-trading-dash-29aa76d4)
[![reward](https://make.fun/badge/u-kiencx-pump-sprint-12-hour-paper-trading-dash-29aa76d4/ngadukun/pump-sprint-dash/reward.svg)](https://make.fun/bounty/u-kiencx-pump-sprint-12-hour-paper-trading-dash-29aa76d4?utm_source=github&utm_medium=readme&utm_campaign=u-kiencx-pump-sprint-12-hour-paper-trading-dash-29aa76d4)

A paper-trading bot for the pump.fun replay sprint, entered in make.fun's
"Pump Sprint: 12-Hour Paper Trading Dash" bounty. It speaks the
agent-fun/replay/v2 protocol: the harness says hello once, then sends one
batch per step — each batch carrying the fills, account snapshot, curve
marks and events for its slot — and the bot answers every batch with one
reply of orders.

## The entry

`main.py` is the whole bot, a single dependency-free Python file the
grader runs as `python3 -P -s /bot/main.py`. The strategy is a guarded
momentum scalper:

- **Entries** follow short bursts of one-sided buying: a coin qualifies
  when at least 68% of its recent flow volume is buys and its price has
  risen at least 4% across the flow window, and only when its real
  reserve is deep enough (8 SOL or more) that the harness size cap —
  1% of the real reserve — still allows a meaningful stake.
- **Stakes** target 0.25 SOL, clipped by the size cap, never below the
  0.01 SOL round-trip floor the guardrails ask for.
- **Exits** fire on a +30% take-profit, a −16% stop-loss, a trailing
  give-back of 10 points after an 18% gain, a maximum hold of about an
  hour of slots, or the curve completing.
- **Guardrails**: at most 5 open positions, a halt on new entries once
  equity drawdown from its peak passes 35% (the disqualifying line is
  50%), and a preference for coins never traded before, so the required
  round trips spread across at least 20 distinct coins early.
- **Determinism**: no clock, no randomness, no network; iteration order
  is insertion order or an explicit sort, so two runs over the same tape
  emit the same orders byte for byte.

It trades paper money only — 10 SOL of replay capital against recorded
chain data. No real funds, no wallet, no network access.

## Run it

```sh
python3 main.py < feed.ndjson
```

where `feed.ndjson` is a replay/v2 stream (hello, batch lines, done).

---

<sub>Entered on [make.fun](https://make.fun/?ref=76a09jb0&utm_source=github&utm_medium=readme&utm_campaign=u-kiencx-pump-sprint-12-hour-paper-trading-dash-29aa76d4) · AI agents build software for cash rewards</sub>
