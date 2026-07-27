# EURC/USDC and PAXG/USDC: Market-Hours Feeds and Staleness Tolerance

Two new pairs run through the standard pipeline (recommended cell, 24 stratified
daily windows each across Jan–Jun 2026, mainnet Chainlink feeds). Both are
**market-hours** assets whose reference feeds freeze when the underlying market
closes, so windows are stratified **2 weekend + 2 weekday per month** rather than
evenly, to isolate closure staleness instead of averaging it away.

| Pair | Pool | Feeds (base/quote) | Feed params |
| --- | --- | --- | --- |
| EURC/USDC | `0x95dbb3c7…5bd73d6` (v3 0.05%, ~$56M) | USDC/USD ÷ EURC/USD | 0.3% dev / 24h |
| PAXG/USDC | `0x5ae13baa…7adb4082` (v3 0.05%, ~$2.2M) | USDC/USD ÷ XAU/USD | 0.3% dev / 24h |

Reproduce: manifests in `exports/study_{eurc,rwa}/manifests_strat/`, runners in
`exports/rerun_matched_age.sh`.

## Finding 1: staleness tolerance must match the feed heartbeat

The batch default `--max-oracle-age-seconds` is **3600 (1 hour)**. Applied to a
feed with a **24-hour heartbeat**, this makes the hook fail closed almost always.
Re-running with a matched 25-hour tolerance:

| Pair | config | stale rejections | volume loss |
| --- | --- | ---: | ---: |
| EURC/USDC | maxAge 1h | 71.8% | 85.4% |
| EURC/USDC | **maxAge 25h** | **0.0%** | **11.2%** |
| PAXG/USDC | maxAge 1h | 37.9% | 77.1% |
| PAXG/USDC | **maxAge 25h** | **0.0%** | **56.5%** |

Misconfiguring staleness tolerance destroyed **74 percentage points** of EURC
volume. `maxOracleAge` is not a one-size default: it must be set per pool from
the slowest feed's heartbeat. This is a deployment lesson, not an asset property.

## Finding 2: EURC/USDC works, and the weekend effect is real

At matched tolerance, EURC/USDC is healthy — no stale rejections, **no fee-cap
rejections**, toxic clip rate 0.000:

| split | trigger rate | windows with fills | LP vs fixed-fee |
| --- | ---: | ---: | ---: |
| weekday | 12.34% | 12 / 12 | +$15,350 |
| weekend | 5.37% | 7 / 12 | +$4,475 |
| **all** | **8.85%** | **19 / 24** | **+$19,825** |

The auction adds **+$3,166** over the hook-only counterfactual, so the auction
layer earns its keep on this pair rather than being redundant.

The weekend split is large: trigger rate more than halves and fills drop from
12/12 windows to 7/12. Honest reading — this is most likely *less real LVR*
(EUR/USD genuinely barely moves while forex is closed) rather than *hidden* LVR,
so weekends look quiet for the mechanism rather than dangerous. That is a
weaker claim than "closure creates recapturable LVR" and should be stated that way.

**Base is materially better than this backtest.** These are mainnet feeds at
0.3%/24h. EURC/USD on **Base is 0.1% / 1 hour** — roughly 3× tighter deviation
and 24× tighter heartbeat — so a Base deployment sees far more of the true gap
and can run a much shorter `maxOracleAge`. The mainnet numbers here are a floor.

## Finding 3: gold's gaps are large — but the gold result is unresolved

Observed pool-vs-oracle gaps on PAXG/USDC are much wider than crypto pairs:
**p10 51 bps, median 75 bps, p90 92 bps** in a representative window. That is the
coarse-feed thesis showing up directly in the data.

However, **the gold run is invalid and must not be cited.** The simulated pool
diverges catastrophically: within a single 24-hour window it walks from the
reference price `0.000231` to `0.269` — a **1000× divergence** — driven by
implausible price impact (a ~$37 swap moving the pool >10%). Once the gap
exceeds ~6,400 bps the exact fee law correctly computes a fee above the 2500 bps
cap, so the hook starts refusing swaps; that is a *downstream symptom*, not the
cause. Invalid outputs: trigger rate 0.00% in all 24 windows, auction adds
$0.00 over hook-only, LP vs fixed-fee +$2,274, 56.5% volume loss.

### Trace so far (root cause not yet isolated)

Ruled out:

- **Reference orientation.** Both code paths agree: `dutch_auction_swaps.csv`
  and the strategy `series.csv` report the same gaps (median 75, p90 92 bps).
- **Reserve-scale formula.** `virtual_reserves` returns `x = scale/√P`,
  `y = scale·√P`, and `y/x == P` exactly for both PAXG/USDC and WETH/USDC.
  Virtual reserves run 8–58× actual balances, which is normal for v3
  concentrated liquidity. (An earlier note claiming a 138,194× reserve error was
  wrong — it mis-mapped `x` to token0; `y` is the token0 leg.)
- **Missing input fields.** `lvr_historical_replay.py:1221` only normalises raw
  amounts when `token0_decimals` is present, and `reserve_scale` silently
  returns `1.0` when `liquidity` is missing — but both columns are 100%
  populated on every PAXG and WETH row, so neither fallback fires.

Still open: what makes a ~$37 swap move a pool with ~59.9k PAXG / ~259M USDC of
virtual reserves by >10%. The distinguishing feature of this pool versus every
previously studied pair is the decimals direction — token0 has **more** decimals
than token1 (18 vs 6, i.e. −12 rather than WETH/USDC's +12) with a small raw
price (~2.3e-4).

Next step: instrument the pool-update path directly — call it with a single real
PAXG swap sample and compare the returned `pool_price_after` against a hand-
computed constant-product result, which localises the mis-scaling to one
expression.

## Correction to earlier RWA framing

An earlier working note argued tokenized RWAs might be a *better* fit for this
mechanism than crypto pairs. **The data does not support that.** At current RWA
feed granularity (0.3% deviation / 24h heartbeat), the reference is far too slow:
gaps run 50–100 bps before the oracle even sees them, and the pool must tolerate
a full day of staleness to transact at all. The mechanism wants *fast* feeds.
EURC/USDC — a market-hours pair with an unusually tight Base feed — is the
defensible RWA-adjacent target; tokenized equities and funds on 24h NAV feeds
are not, until low-latency reference infrastructure exists for them.
