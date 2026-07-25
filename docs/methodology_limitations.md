# Methodology Limitations and Corrections

This document records the known limitations of the LVR-hook backtest and the
corrections applied to each. It exists so the headline results are read with the
right caveats, and so external reviewers see the assumptions stated plainly
rather than discovered later. Corrections implemented as of 2026-07-24 are marked
**[fixed]**; framing caveats that cannot be removed by re-running are marked
**[caveat]**.

## 1. Oracle-vs-truth conflation — the recapture ceiling **[fixed]**

**The problem.** The main study measured LVR against the *same* Chainlink series
the hook uses as its oracle. Recapture = fee / LVR with the same reference on both
sides is partly circular: the hook cannot miss a gap its own oracle cannot see, so
the ~99.9% recapture headline is recapture of the *oracle-visible* stale-loss, not
of the true LVR an arbitrageur extracts against the CEX price.

**The correction.** `research/lvr/studies/reference_lag_analysis.py` re-measures
every executed swap's stale-loss against a faster **Binance 1m** truth series while
keeping Chainlink as the hook oracle, using the study's own `correction_trade` LVR
function (fixed reserve scale, so the seen/true ratio is exact). Result across the
24 stratified 2026 windows (WETH/USDC, mainnet feeds):

| Metric | Value |
| --- | --- |
| Median Chainlink-vs-Binance lag at swap times | **21 bps** (p90 ~47–64 bps) |
| Oracle-visible fraction of true LVR | **78.3%** |
| Headline recapture (vs Chainlink) | 99.9% |
| **True recapture (vs Binance truth)** | **~78%** |

The visible fraction falls to ~40–45% in the high-volatility months (Mar–Jun) and
is ~85% in calm January: **the hook captures a smaller share of true LVR exactly
when LVR is largest**, because the CEX moves fastest then and Chainlink lags most.

**Two things that make this less bad than 78% suggests.** (a) These are *mainnet*
Chainlink feeds with a 0.5% deviation threshold; the Base ETH/USD feed is 0.15%
(~3× tighter), so a Base deployment's true recapture is materially higher than 78%
— the same conservative-for-Base direction as [`oracle_granularity.md`](oracle_granularity.md).
(b) The remaining gap is closable with a faster reference oracle (Chainlink Data
Streams / an aggregated oracle) behind the same `IReferenceOracle` interface; the
fee law and auction are unchanged.

**Honest headline going forward:** *"recovers roughly three-quarters of true
CEX-measured LVR on mainnet-granularity feeds, more on Base's tighter feeds, with
a faster oracle as the path to more"* — not "99.9%".

## 2. Zero-gas parameter tuning **[fixed]**

**The problem.** The recommended auction cell (10 bps start, 0.5 bps/s growth) was
selected in a zero-gas backtest that fills the instant the auction opens, so it
measured the *minimum* concession and gave LPs the maximum. That is the wrong
default for a chain with real gas.

**The correction.** `exports/study_recent/retune_grid.sh` re-ran the 6 cached 2026
months across a 14-cell grid with Base gas ($0.015/fill) and a realistic
`solver_cost` fill gate. Findings (medians across 6 months):

- **Concession *growth rate* is irrelevant** — s25_g0.5, s25_g2, s25_g5 are
  identical; fills clear before the growth term binds. The *start* concession is
  the only real lever.
- Start concession is a direct **LP↔filler dial**: 10 bps → $0.05/fill and LP keeps
  +$1338/window vs the hook-only counterfactual; 50 bps → $0.32/fill, +$1064; 200
  bps → $1.18/fill, +$537.
- **LP-vs-fixed-fee is flat (~$144k/window sample) across every cell** — the LP's
  dominant benefit is the 5-bps-vs-30-bps benign-flow advantage, not the auction
  split.

**Production guidance:** run your own keeper at launch with the low (current) cell
— LPs keep the most and fill rate stays ~99.7%. If/when decentralizing to external
searchers, raise the start concession to ~50 bps so a fill clears Base gas by ~20×
(making even routine fills profitable without cherry-picking), at a cost of only
~$275/window in LP recapture versus the ~$144k benign-flow benefit. See
[`concession_tuning_lvf.md`](concession_tuning_lvf.md) for the theory frame.

## 3. Flow invariance / induced benign volume **[caveat]**

The replay uses swaps that actually occurred on a 30-bps un-hooked pool. It
correctly *deters* toxic flow (measured `hook_volume_loss_rate` ~0.43 from the fee
cap), but it **cannot model benign flow that would route *to* a cheaper 5-bps
hooked pool** — that volume is exogenous. So the LP's benign-fee revenue (its
steady earner) is either understated (if routing brings more benign flow) or
absent (if the pool is never routed to). This is the Lucas-critique limit and ties
directly to the routing PMF gate in [`routing_integration.md`](routing_integration.md).
Mitigation: treat benign-flow-driven LP uplift as a range, not a point, and do not
claim benign revenue the pool cannot yet attract.

## 4. Single-solver, continuous-clock idealization **[caveat]**

The model grows the concession continuously and assumes one filler. On Base the
clock is block-sampled (2 s) and needs `pokeAuction`; multiple searchers would
compete the concession down; and the sequencer itself could self-fill at the
decaying price — the Loss-Versus-Fair leakage quantified in
[arXiv:2406.00113](https://arxiv.org/abs/2406.00113), amplified under Flashblocks.
The gas re-tune shows fill rate is robust (~99.7%) so this does not threaten
liveness, but the *split* of surplus in a competitive/sequencer-privileged setting
is not modeled and should not be over-claimed.

## 5. Toxic-label noise **[caveat / partially tested]**

Toxicity is labeled by swap direction versus the oracle gap; only ~22–38% of swaps
carry a confirmed outcome label (`confirmed_label_rate`). A noise trader who
happens to trade toward the oracle during a gap is labeled toxic and pays the
surcharge. `run_label_sensitivity.py` exists to test robustness of the LP result
to reasonable relabelings; run it before publishing the selectivity claim.

## 6. Fat-tailed solver payout **[caveat]**

Per-fill solver payout is heavily right-skewed (six-month mean ≈ 25× the median).
Report it as a distribution — median ~$0.003, mean ~$0.05–0.07, tail to ~$0.5 —
never as a single mean. Solver *self*-viability lives entirely in the tail
(dislocation days); 24 stratified windows under-sample that tail, so tail-driven
claims (external-solver profitability) carry wider error bars than the median.

## 7. Liquidity-depth mismatch **[caveat]**

Backtest pools are mainnet 0.3%/0.05% tiers (~$70M TVL); a Base launch pool is
$0.1–1M. Absolute per-fill payout scales ~linearly with depth (so it shrinks at
launch scale), but the recapture *ratio* and visible-fraction should be
depth-invariant — verify rather than assume when quoting ratios at launch TVL.

## Sample and reproduction

- Windows: 24 stratified daily windows (4 evenly-spaced days/month, Jan–Jun 2026),
  WETH/USDC 0.3%, recommended cell, mainnet Chainlink feeds.
- Reference truth: Binance 1m ETHUSDT/USDCUSDT from the public archive.
- Reproduce: `python3 -m research.lvr.studies.reference_lag_analysis`;
  gas re-tune via `exports/study_recent/retune_grid.sh`.
- The 24-window stratified sample was chosen for RPC-throughput reasons (free-tier
  10-block `eth_getLogs` cap); a paid archive tier would allow the full 181-window
  daily study and tighten the tail estimates.
