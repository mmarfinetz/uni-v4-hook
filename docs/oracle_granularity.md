# Reference-Feed Granularity and the Trigger Gap

Chainlink price feeds are not streaming data: a feed posts a new round only when
the off-chain price moves past the feed's **deviation threshold** or its
**heartbeat** expires. The reference price the hook reads therefore lags the true
market price by up to the deviation threshold between rounds (e.g. 0.15% for
[ETH/USD on Base](https://data.chain.link/feeds/base/base/eth-usd); mainnet feeds
are typically coarser — check each feed on [data.chain.link](https://data.chain.link)).
This page records what that granularity does and does not mean for the mechanism.

## The backtest already embeds feed granularity

The historical replay inputs are exported by
[`research/lvr/export/export_historical_replay_data.py`](../research/lvr/export/export_historical_replay_data.py)
from **on-chain `AnswerUpdated` / OCR `NewTransmission` logs of the real mainnet
Chainlink feeds** — not from CEX spot series. The reference series in the October
2025 grid therefore updates exactly as coarsely as the production oracle does, and
the headline results (clear rate, `0.98%` trigger selectivity, recapture ceiling)
were measured against a feed-granular reference. There is no sim-to-prod gap in
the trigger mechanism itself.

Two consequences of the granularity are worth stating precisely:

1. **The trigger measures the gap to the last posted round, not to the true
   price.** `trigger_gap_bps = 10` on a feed with a 15 bps deviation threshold does
   not mean the hook reacts to 10 bps of true-price movement. In practice the
   measured gap jumps when the feed ticks — each tick moves the reference by at
   least the deviation threshold, so a fresh dislocation typically lands above the
   trigger in one step. The trigger acts as a filter on post-tick residual gaps
   (and on pool-side drift between rounds), which is exactly how it behaved in the
   backtest.
2. **Sub-threshold LVR is out of scope by construction.** True-price dislocations
   smaller than the feed's deviation threshold never post a round, never register
   as a stale gap, and are never auctioned. Informed flow can still trade against
   the pool in that band at the benign base fee. The feed's deviation threshold is
   therefore a lower bound on the dislocation size this mechanism can recapture —
   a scope limit to state alongside results, not a correctness bug.

## Deployment is fresher than the backtest

The grid was measured on mainnet feeds; production targets Base, whose major feeds
update at tighter deviation thresholds (0.15% for ETH/USD vs 0.5% on mainnet).
A fresher reference strictly enlarges the set of visible dislocations relative to
the backtest, so on this axis the backtest numbers are conservative for a Base
deployment. The reverse would not hold: porting the recommended cell to a chain or
pair whose feed is *coarser* than the backtest's mainnet feeds weakens the
selectivity evidence and should trigger a re-run of the replay for that pair.

## Practical rules

- **Record the feed parameters per deployment.** For every pool, note base/quote
  feed deviation thresholds and heartbeats next to the addresses in
  [`deployment.md`](deployment.md). `maxOracleAge` must comfortably exceed the
  slowest heartbeat or quiet markets fail closed.
- **Keep `trigger_gap_bps` at or below the feed deviation threshold.** The trigger
  only filters gaps the feed can already see; raising it above the threshold
  discards visible recapture without reducing noise (the reference has no noise
  below the threshold — it simply does not update).
- **Feed updates are front-runnable, and the hook prices that correctly.** A
  searcher who trades just before a round posts pays the then-current fee against
  the then-current reference; the loss they extract is sub-threshold LVR the
  mechanism never claimed. A searcher who trades right after the round posts in
  the toxic direction meets the full gap-scaled surcharge. Predictable round
  timing therefore does not let anyone dodge the surcharge on visible gaps.
- **For finer capture, the upgrade path is a lower-latency reference**, e.g.
  [Chainlink Data Streams](https://docs.chain.link/data-streams) or an
  aggregated-oracle design, behind the same `IReferenceOracle` interface. That
  shrinks the invisible band; the fee law and auction schedule are unchanged.
