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

## 2b. Inverted price convention in the pool simulator **[fixed]**

**The problem.** `simulate_swap` builds virtual reserves as `reserve0 = L/sqrt(P)`,
`reserve1 = L*sqrt(P)`, so it requires `P` in the standard **amount1/amount0**
convention. The pipeline carries prices as token0-per-token1 and fed it the
reciprocal, which swaps the two reserve legs: swap amounts land on the wrong side
and simulated price impact is wrong by orders of magnitude. Measured against the
actual on-chain move for the same swap:

| Pool | as-fed | inverted | actual |
| --- | --- | --- | --- |
| PAXG/USDC | 2637.4 bps (**4,608x**) | 0.57234 bps | 0.57231 bps |
| WETH/USDC | 0.00000 bps | 0.00563 bps | 0.00562 bps |

The error is unbounded for pools whose price is far from 1, which is why it first
surfaced on tokenized gold (PAXG ~4,300 USDC) rather than on the majors.

**The correction.** `simulate_swap` now inverts on entry and converts back on
exit. After the fix, simulated impact matches on-chain reality on every studied
pool: WETH/USDC 0.99x, WBTC/USDC 0.96x, LINK/WETH 0.99x, UNI/WETH 0.97x.

**Impact on published results — smaller than feared.** The October 2025 grid was
re-run end to end (124/124 windows, 1,296 grid rows). The grid derives its gaps
from the observed pool series against the reference rather than from a simulated
trajectory, so it is largely insensitive to this bug:

- LINK/WETH, UNI/WETH, WBTC/USDC: **bit-identical** on recapture, clear rate,
  trigger events, and payout bps.
- WETH/USDC: unchanged except `lp_net_quote_token` (-3434.44 -> -0.869) and
  `fixed_fee_v3_recapture` (52.571% -> 52.101%).
- The recommended cell still wins, so `DeployPool.s.sol` defaults are unchanged.

The `lp_net` move is a **unit change, not an economic one**: quote units are
token1, so WETH/USDC is now denominated in WETH rather than USDC (-0.869 WETH at
the study's $4,215/ETH is -$3,663 vs. the old -$3,434). The USD conversion table
was corrected accordingly (WETH/USDC multiplier 1.0 -> the ETH price, matching
what LINK/WETH and UNI/WETH already used). Net effect on published USD figures:
solver payout `$1.74 -> $1.77` per fill and `$12.9k -> $13.2k` total; WETH/USDC
LP uplift `224 -> 241` bps of TVL/month. **Every headline range in the README is
unchanged** (`184-264`, `102-193`, LINK `2,069`/`539`).

### Reconciling the two trigger rates

The re-run surfaced an apparent contradiction: the October grid's trigger count
was unchanged by the fix (1,242 events), while the 2026 windows showed their
trigger rate collapse from ~2.7% to ~0.13%. Both are correct — they measure
different rules, and only one depends on the simulated trajectory.

| | rule | source of pool price | Oct 2025 WETH/USDC (25,726 swaps) |
| --- | --- | --- | --- |
| **Broad eligibility** | `gap >= trigger_gap_bps` and stale loss > 0 | observed on-chain series | 1,242 events = **4.83%** |
| **Selective** | `auction_beats_hook`: only when the hook's own fee still leaves LP net-negative | simulated trajectory | 46 events = **0.18%** |

The grid counts broad eligibility from the *observed* pool series, so it is
structurally immune to the `simulate_swap` bug — which is exactly why its numbers
came back bit-identical. The replay's default `trigger_mode="auction_beats_hook"`
gates on `hook_lp_net < 0`, which is computed from the *simulated* pool, so it
absorbed the full error: pre-fix the simulated pool diverged wildly, inflating
gross LVR until the hook looked net-negative and the auction opened constantly.

**The decisive evidence that the fix is right:** post-fix, the selective rate on
October (0.136%) and on the independent 2026 sample (0.133%) agree to three
decimal places. Pre-fix they disagreed by 20x. Two unrelated datasets converging
only after the correction is strong confirmation.

**What this means for the mechanism.** The auction is needed far less often than
the pre-fix numbers implied: the exact toxic-flow fee alone handles the large
majority of stale-price events, and the Dutch auction is the tail escape valve
for the ~0.2% of swaps where the fee alone would leave LPs net-negative. That
*strengthens* the selectivity claim (fewer interventions, none harmful) while
lowering the auction's share of LP economics. Recapture and clear-rate figures in
the grid are computed on the broad rule and are unaffected.

**Resolved 2026-07-30.** The 54-window observed-flow study was re-run under the
corrected convention. The selective rate rose to `2.28%` (broad `3.33%`), and LP
net improved in `14` windows, was unchanged in `40`, and **worsened in none**;
against the static-fee baseline LP net was higher in all `54`. The pre-fix
`0.98%`/`5.82%` pair overstated the selectivity gap. Re-running it required two
further fixes: the cached fixtures store `deep_pool` (pool-derived, always
token0-per-token1) and feed series in different orientations, so they need
separate inversion rules, and `INVERTED_EXTERNAL_REFERENCE_FAMILIES` is now the
complement of its pre-fix contents.

## 2c. Trigger-quality metrics: two defects, and what they revealed **[fixed]**

An independent research pass over the export corpus (656 windows, 402,846 swaps)
surfaced two defects in how trigger quality is measured. Both are fixed; together
they change the reading of the trigger from "barely works" to "precise but
oracle-limited".

**Precision was computed with the wrong denominator.**
`toxic_candidate_precision` divided true positives by *every* candidate,
including the ~92% whose ex-post outcome never resolves — silently scoring each
unresolved candidate as a failure. Reported precision was `0.017`, i.e. worse
than random against a 0.46 toxic base rate. Corrected to `TP / (TP + FP)`, with a
new `toxic_candidate_decided_count` column publishing the denominator so the
coverage behind the ratio stays visible. Recall and the false-positive rate were
already correct, so **the low recall is real, not a reporting artifact.**

**An off-chain reference could not be classified at all.** The row selector
(`oracle_precedes_swap`) claimed a same-second row precedes the swap, while the
classifier (`_is_ambiguous_ordering`) rejected that same row as unorderable.
For an on-chain feed this never bites, because block/log index breaks the tie.
For a 1-second CEX series it ties **100%** of the time and carries no block
number, so every swap resolved to `uncertain`: binance was 6,975/6,975 unusable.
The selector now declines same-second rows it cannot prove precede the swap and
falls back to the latest strictly-earlier row, whose ordering is provable — at
most one second of extra staleness on a 1s feed.

**What the corrected metrics show.** Same trigger rule, same swaps, same windows;
only the reference oracle changes:

| reference | recall | precision | uncertain |
| --- | ---: | ---: | ---: |
| binance (CEX, 1s) | **70.0%** | 95.0% | 443 |
| pyth | 68.3% | 94.5% | 455 |
| deep_pool | 51.8% | 88.5% | 348 |
| chainlink | **28.0%** | **100.0%** | 888 |

Chainlink is the most precise reference and the least complete: it fires only
when certain and misses 72% of confirmed-toxic flow. Moving to a
continuously-updating reference buys **+42pp of recall for 5pp of precision**.

**The design conclusion:** recall is oracle-limited, not rule-limited. The
classification rule is sound — the misses are swaps whose signed gap is negative
because the CEX moved before Chainlink updated, so the information is simply
absent from the signal at decision time. This is the same limit as the ~78%
oracle-visible LVR fraction in §1, measured from the classifier side, and it
prices the upgrade to a low-latency reference in concrete terms. Caveat: each
oracle labels its own ground truth, so `toxic_confirmed` differs slightly across
rows (1,014 for binance vs 1,022); treat the comparison as directional.

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

## 3b. Regime was declared, not measured **[fixed]**

**The problem.** `regime` was a manifest label, not an observation.
`build_month_backtest_manifest` defaults `--regime stress`, so every month-scale
window (October 2025, the 2026 months, PAXG, EURC) carried "stress" regardless of
what the market did — while `run_backtest_validation_report` requires a
normal/stress breakdown it could therefore never receive. The corpus contained
**no calm-market evidence at all**, so any claim that the hook generalises across
regimes was unsupported by data.

**The correction.** `research/lvr/core/regime.py` derives the label from the
primary reference series: realized volatility of the same feed the hook reads,
annualised, with windows at or above 100% annualised marked "stress". Calm crypto
majors sit near 40–70% and the hook's own bootstrap prior (5% daily) is already
~95% annualised, so the cut marks "unambiguously not calm" rather than splitting
the typical range. Unmeasurable windows return `None` rather than defaulting, so
they stay out of the breakdown instead of padding one side of it. Every window
summary now carries `realized_vol_annualised_pct` and `measured_regime` alongside
the declared `regime`, which is retained for provenance.

**What it shows on October 2025** (declared: 124/124 stress):

| pool | measured stress | measured normal | realized vol range |
| --- | ---: | ---: | --- |
| LINK/WETH | 14 | 17 | 47–684% |
| UNI/WETH | 11 | 20 | 40–776% |
| WETH/USDC | 3 | 28 | 24–246% |
| WBTC/USDC | 1 | 30 | 12–168% |
| **total** | **29** | **95** | |

Two independent sanity checks: the single most volatile window in **all four
pools is `w10` — October 10**, the known dislocation, recovered without being
told about it; and the ordering across pools (WBTC calmest, LINK/UNI most
volatile) matches their liquidity and market cap. The corpus now has **95
calm-market windows** where it previously had zero.

**Still open:** the regime split exists in the window summaries but the
validation report and the published tables have not yet been re-cut along it, so
"how the hook performs in calm markets specifically" is now answerable from the
data but is not yet a published result.

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
