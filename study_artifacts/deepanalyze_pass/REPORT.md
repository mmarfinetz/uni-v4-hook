# Open-Ended Data Research Pass — OracleAnchoredLVRHook Export Corpus

> **Status as of 2026-07-31 — read alongside [`docs/methodology_limitations.md`](../../docs/methodology_limitations.md).**
> Findings below are preserved as written; this header records what has since
> been acted on, so the "not applied" notes in the body are not read as current.
>
> - **§1 precision denominator — FIXED** (`dbf6186`). Now `TP/(TP+FP)`, with a
>   `toxic_candidate_decided_count` column publishing the denominator.
> - **§2 recall — answered, and a blocking defect found.** The `binance`
>   reference was unusable (6,975/6,975 `uncertain`): a 1-second CEX series ties
>   the swap's second 100% of the time and carries no block number, so the row
>   selector claimed precedence while the classifier rejected the same row as
>   unorderable. Fixed in `dbf6186`. Re-measured on identical windows:
>   binance 70.0% recall / 95.0% precision, pyth 68.3% / 94.5%,
>   deep_pool 51.8% / 88.5%, chainlink 28.0% / 100.0%. Recall is
>   **oracle-limited, not rule-limited**.
> - **Priority 0 (regenerate post-`b6d31ac` exports) — DONE.** October re-run
>   (124/124) and the 54-window observed-flow study both re-ran; the latter gives
>   14/40/0 vs the published 28/26/0, with zero windows worsened either way.
> - **Still open:** §3 markout framing, §4 regime is hardcoded `stress` with no
>   calm-regime evidence, and §5–§7/§9 which remain withdrawn pending re-derivation.

Method: DeepAnalyze-style agentic loop (plan → inspect data → write code → execute →
report), run open-ended over `exports/` with no predefined reporting spec. Every number
below was recomputed from raw per-swap rows, not read from `reports/`.

**Corpus:** 656 windows · 402,846 swaps · 26,958 oracle updates · 7 pool/study groups
(`study_recent/weth_usdc` 387w, `study_rwa/paxg_usdc` 73w, `study_eurc/eurc_usdc` 72w,
`oct2025/{weth_usdc,wbtc_usdc,link_weth,uni_weth}` 31w each). Single oracle source
throughout: `chainlink`.

**Scripts:** `harvest.py` → `analyze.py` → `confusion.py` → `verify.py` → `vintage.py` →
`fresh_grid.py` (the post-fix pass, §10).

---

## 0. READ FIRST — data vintage invalidates the replay-path findings

The first pass pooled five incompatible data vintages into one frame. Against the two
price-convention fixes — `d131079` (2026-07-27 14:45, data-layer) and `b6d31ac`
(2026-07-28 14:25, `simulate_swap` reserve-leg inversion, 127x on WETH/USDC, 4,608x on
PAXG/USDC):

| tree | windows | written | status |
|---|---|---|---|
| `study_recent/retune` | 336 | 07-24 | pre-**both** fixes |
| `oct2025/windows` | 124 | 07-27 14:56+ | post-data-layer, pre-simulator |
| `*/fixed` | 72 | 07-27 14:39–14:42 | the `d131079` re-validation output |
| `*/age25h` | 48 | 07-27 10:03 | pre-data-layer |
| month dirs `2026_01..06` | 76 | 07-22 → 07-26 | pre-both |

**No per-window replay summary on disk is post-simulator-fix.** `b6d31ac` re-ran October
end to end but its outputs went to `reports/` and `exports/oct2025/grid/` (8 files);
`exports/` is gitignored (`3dab2f8`) and `exports/oct2025/windows/` has **zero** files
newer than 07-28. `eaf1078` independently records the 2026 windows' trigger rate
collapsing **2.7% → 0.13%** post-fix, and flags the 54-window observed-flow study as
pending a re-run.

Per `vintage.py`, metrics split cleanly by which path feeds them:

| metric | pre_both | post_data | fixed_tree | verdict |
|---|---|---|---|---|
| `hook_volume_loss_rate` | 0.457 | 0.000 | 0.127 | **contaminated** |
| `hook_toxic_clip_rate` | 0.967 | 0.000 | 0.000 | **contaminated** |
| `dutch_auction_trigger_rate` | 0.047 | 0.000 | 0.077 | **contaminated** |
| solver surplus | 0.322 | 0.003 | 0.072 | **contaminated** |
| corrected precision | 0.932 | 1.000 | 1.000 | stable |
| recall | 0.116 | 0.174 | 0.241 | stable |
| missed toxicity | 88.4% | 82.6% | 75.9% | stable |

**Surviving (observed-pool path + source-code facts): §1, §2, §4, §8, and §3 in weakened
form. Withdrawn pending re-run: §5, §6, §7, §9.** Sections below are annotated
accordingly; the withdrawn numbers are kept only so the re-run has something to diff.

---

## 1. The reported trigger precision is wrong by ~57x — the trigger is not broken

> **SURVIVES.** Denominator bug in source code; corrected precision is 0.932–1.000 across
> all three vintages (`vintage.py`). Vintage-independent.

`reports/` and the per-window summaries carry `toxic_candidate_precision` with a median of
**0.017**. Read literally, the trigger fires on toxic flow less often than random guessing
(confirmed-toxic base rate is 0.46), which is what makes it look like a dead signal.

The metric is computed with the wrong denominator, at
[`oracle_gap_predictiveness.py`](../../research/lvr/core/oracle_gap_predictiveness.py):

```python
"toxic_candidate_precision": _ratio(toxic_true_positive_count, toxic_candidate_count),
```

`toxic_candidate_count` is *every* candidate, including those whose ex-post outcome never
resolved (`outcome_label == "uncertain"`). Across the corpus **92.5% of candidates are
unresolved**, and each one is silently counted as a failure. Precision in the standard
sense needs TP + FP.

Recomputed over all 656 windows, on the 92,735 rows (23.0%) where both the decision and
the outcome resolve:

| | TP | FP | FN | TN |
|---|---|---|---|---|
| all groups | 6,508 | 184 | 36,661 | 49,382 |

| metric | reported | corrected |
|---|---|---|
| precision | 0.017 | **0.9725** |
| false-positive rate | — | **0.0037** |
| recall | 0.071 | 0.1508 |

Per group, precision is **exactly 1.000** in five of seven groups (zero false positives);
only `study_recent/weth_usdc` has any at all (180 FP, precision 0.888).

**Fix:** divide by `toxic_true_positive_count + toxic_false_positive_count`, and publish
the decided-subset coverage (23.0%) next to it so the denominator is visible. Not applied
here — this changes published numbers and is your call.

## 2. The real weakness is recall, and it is severe

> **SURVIVES, as a range.** Recall 0.116–0.241 and missed toxicity 75.9–88.4% across
> vintages. Direction is stable; treat the corpus-wide point estimates below as the
> pre_both-weighted end of that range, not settled values.

The trigger is extremely conservative: when it fires it is almost always right, but it
**misses 84.9% of confirmed-toxic flow — 36,661 swaps flagged `benign_candidate` that
were confirmed toxic ex post.**

Recall by group: `paxg_usdc` 0.51, `link_weth` 0.34, `uni_weth` 0.25, `wbtc_usdc` 0.16,
`eurc_usdc` 0.09, `oct2025/weth_usdc` 0.087, `study_recent/weth_usdc` **0.067**.

Those 36,661 misses are swaps whose signed gap was *negative* — moving the pool away from
the oracle — that still paid the taker. An oracle-gap trigger structurally cannot see
CEX-lead flow that arrives before Chainlink updates. This is the same lag exposure your
`reference_lag_recapture.md` work touches, and it is where the headroom is, not precision.

## 3. Short-horizon markout partly restates the trigger's own input

> **SURVIVES WEAKENED.** The direction holds in every vintage (12 s coupling exceeds
> 3600 s: 0.315>0.247, 0.108>0.094, 0.193>0.150), but *mean per-window* coupling is only
> 0.11–0.32, far below the pooled +0.849 quoted below. The "~72% of variance" claim is
> an artifact of pooling and is withdrawn — use the per-window figures.

Markout is the validation label; the signed gap is the trigger input. Both are built from
`pool_price_before` against a reference price, so they are mechanically coupled.

Correlation of actual markout against a pure gap-restatement
(`±log(oracle_price / pool_price_before)`), 340,952 rows:

| horizon | corr | median abs. deviation | within 1 bps |
|---|---|---|---|
| 12 s | **+0.849** | 43.5 bps | 6.0% |
| 3600 s | **+0.422** | 41.4 bps | 5.1% |

Not a tautology — a 43.5 bps median deviation is large against a ~15–30 bps mean gap, so
the reference does move independently inside 12 s. But ~72% of 12 s markout variance
(0.849²) is attributable to information already visible at trade time, versus ~18% at
3600 s. **Lead validation claims with the 3600 s horizon**; it is the horizon that is
actually independent of the trigger.

Related: per-window signed-gap↔markout correlation has a median of only **+0.13**, while
the pooled figure is +0.76. The pooled number is inflated by cross-window variation in gap
range. Per-window is the honest measure of in-window predictive power.

## 4. Every window is `regime: "stress"`, and nothing classifies regime

> **SURVIVES.** Pure source-code fact, independent of data vintage.

All 656 windows carry `regime == "stress"`. It is not measured — it is hardcoded at
[`run_dutch_auction_ablation_study.py:122`](../../research/lvr/studies/run_dutch_auction_ablation_study.py)
and defaults to `"stress"` at
[`build_month_backtest_manifest.py:168`](../../research/lvr/studies/build_month_backtest_manifest.py).

Meanwhile [`run_backtest_validation_report.py:852`](../../research/lvr/reporting/run_backtest_validation_report.py)
*requires* both `normal` and `stress` in the regime breakdown, and lines 739/827 iterate
`("normal", "stress")`. The reporting layer expects a regime comparison the export layer
never produces. **There is no calm-regime evidence in this corpus at all**, so any claim
that the hook generalizes across regimes is currently unsupported by data.

## 5. The aggregate uplift is one pool wearing a trenchcoat

> **WITHDRAWN pending re-run.** `study_recent` is 363/387 pre-both-fixes, and `b6d31ac`
> changed WETH/USDC `lp_net_quote_token` to token1 units — so the quote-unit summation
> below also mixes WETH- and USDC-denominated legs. The *structural* point (one pool
> dominates any quote-weighted aggregate) is still worth checking post-re-run; the
> percentages are not usable.
>
> On the negative-LP-net windows: all 62 are `post_data`, the only vintage written after
> the data-layer fix, and `b6d31ac` reports three of four October pools bit-identical
> across the simulator fix (WETH/USDC alone moved). So this is probably *not* a stale-bug
> artifact — but it cannot be certified until per-window October output is regenerated.

`study_recent/weth_usdc` contributes **99.979%** of total LP net across the corpus
(783.75M of 783.92M quote units). Every quote-weighted aggregate is that pool.

| group | share of total LP net |
|---|---|
| study_recent/weth_usdc | 99.9791% |
| study_eurc/eurc_usdc | 0.0098% |
| oct2025/wbtc_usdc | 0.0070% |
| study_rwa/paxg_usdc | 0.0041% |
| oct2025/weth_usdc | ~0% |
| oct2025/uni_weth | **negative** |
| oct2025/link_weth | **negative** |

The headline "43.28 bps uplift vs fixed-fee v3" is a single-pool result. Report it
per-pool, equal-weighted, or explicitly as WETH/USDC — not as a corpus aggregate.

Also: **62 windows (9.5%) have negative LP net** — 31/31 `link_weth` and 25/31
`uni_weth`. In those pools the strategy loses money in essentially every window, and
because the base is negative the bps normalization produces nonsense
(`link_weth` reports −69,281 bps). Those cells should be suppressed, not printed.

## 6. The Dutch auction adds little over the hook it replaces

> **WITHDRAWN pending re-run.** Every figure here is replay-path
> (`lp_net_vs_hook`, `lp_net_vs_fixed_fee`), which absorbed the full simulator error per
> `eaf1078`. Re-derive before citing.

| comparison | total (quote) | bps of LP net |
|---|---|---|
| auction vs fixed-fee v3 | 3,393,039 | 43.28 |
| auction vs the hook itself | 319,479 | **4.08** |

The auction's marginal contribution is **9.42%** of its total advantage over fixed fee —
the other ~91% is the hook, which does not need an auction. Worse, in
`study_recent/weth_usdc` (the pool that is 99.98% of the corpus) the auction beats the
hook by **0.19 bps**.

- **29.9%** of windows: auction beats hook by < 1 quote unit
- **15.7%** of windows: auction is *worse* than the hook

Auction complexity needs to be justified on something other than these numbers.

## 7. The hook rejects a median 41% of volume

> **WITHDRAWN — this one was wrong.** The 41% median comes almost entirely from the 07-24
> `retune` tree (pre_both median 0.457). Post-fix vintages show 0.000 (`post_data`) and
> 0.127 (`fixed_tree`). Same for `hook_toxic_clip_rate`: 0.967 pre_both vs 0.000 in both
> newer vintages.

`hook_volume_loss_rate`: median **0.412**, mean 0.351. **66.2%** of windows reject >25% of
volume; **55.2%** reject >40%.

| group | median volume loss |
|---|---|
| study_rwa/paxg_usdc | 0.552 |
| study_recent/weth_usdc | 0.435 |
| study_eurc/eurc_usdc | 0.186 |
| oct2025/wbtc_usdc | 0.054 |
| oct2025/weth_usdc | 0.032 |
| oct2025/link_weth, uni_weth | 0.000 |

Rejection is concentrated in exactly the pools driving the reported gains. Given §2
(85% of toxic flow is missed anyway), a large share of that rejected volume is benign
flow turned away. `hook_benign_mean_overcharge_bps` is 0.000 in all 656 windows, which
measures overcharging but not *rejection* — the rejection cost to benign flow is
currently unmeasured.

## 8. `eurc_usdc` is not evidential

> **SURVIVES.** Reference-feed staleness is measured on the observed path, independent of
> the replay simulator.

Oracle stale rate median **0.839** — the reference is stale ~84% of the time — and 97.3%
of rows are `uncertain`. Median window yields 16 confirmed-toxic and 6 confirmed-benign
labels from 201 swaps. Whatever this pool shows, it is not measuring the trigger.
`paxg_usdc` is milder but same category (stale rate 0.581).

## 9. Solver surplus is thin enough to question fill viability

> **WITHDRAWN pending re-run.** Replay-path; ranges 0.003–0.322 across vintages.

Mean solver surplus per triggered auction: median across windows **0.225 quote units**,
p90 1.303, max 8.43. **61.4%** of windows sit below 1.0 quote unit. For USDC-quoted pairs
that is well under a dollar per fill. Cross-reference
`reports/solver_economics_gas_aware.md` — at mainnet gas most of this distribution does
not clear break-even, which bears on the `dutch_auction_fill_rate` of 0.96 being an
assumption rather than an observation.

---

## What this corpus cannot answer

- **Calm-regime behaviour** — no non-stress windows exist (§4).
- **Multi-oracle comparison** — `oracle_sources` is `chainlink` alone in all 656 windows;
  `oracle_ranking` has one entry, so the "ranking" is vacuous. Pyth/Binance exporters
  exist in `research/lvr/export/` but produced nothing here.
- **Anything about 77% of flow** — `outcome_label` is `uncertain` for 78.5% of swaps
  (`horizon_disagreement` 12,315, `stale_oracle` 10,720 in the 41-window sample).
- **Exact-replay fidelity** — `exact_replay_reliable`, `fee_identity_holds`, and
  `replay_error_p50/p99` are null in all 656 windows.

## Priority order

0. **Regenerate the per-window replay exports post-`b6d31ac`.** Nothing in §5–§9 can be
   assessed until `exports/*/windows/` and the `retune` tree are rebuilt on the fixed
   simulator. This also unblocks the 54-window observed-flow re-run `eaf1078` flagged.
1. Fix the precision denominator (§1) — currently understates the trigger ~57x, in every
   vintage. Independent of item 0.
2. Chase recall against reference lag (§2) — 76–88% of confirmed toxicity is missed in
   every vintage. This is the largest real gap and it is vintage-robust.
3. Either classify regime or drop the regime framing from the reporting layer (§4).
4. Re-frame validation on 3600 s markout and *per-window* correlation (§3).
5. After item 0: re-derive §5–§7 and §9, then decide whether the auction earns its
   complexity.

## Method note

Pooling vintages was the pass's own error, not the pipeline's: `harvest.py` globbed
`exports/*/**/window_summary.json` and silently mixed `retune`, `fixed`, `age25h`, and
month-dir trees whose mtimes straddle both convention fixes. Any future pass over
`exports/` should filter by vintage first — `vintage.py` has the classifier.


---

# 10. Second pass: the POST-FIX grid data

§0–§9 all ran on stale per-window trees. The grid path *was* regenerated after `b6d31ac`
(`reports/sensitivity_grid_windows.csv`, 40,176 rows, 07-28 14:18), so this section re-runs
what that data can support: October 2025, 4 pools x 31 windows x 324 parameter sets.

Caveat from `eaf1078`: the grid counts **broad eligibility** (gap >= trigger, off the
observed on-chain series). It is not the replay's selective `auction_beats_hook` rule, and
the two trigger rates are not comparable.

## 10.1 Three things I flagged that do NOT survive contact with fresh data

**Negative `lp_net_quote_token` is the designed sign, not a defect.** It is
`lp_fee - gross_lvr` ([`run_oracle_gap_sensitivity_grid.py:291`](../../research/lvr/backtest/run_oracle_gap_sensitivity_grid.py)),
i.e. residual LVR after fees, and every consumer negates it
(`build_lp_apr_uplift.py`, `build_oracle_gap_charts.py`, `build_gas_aware_solver_economics.py`).
It is negative in 100% of all 40,176 fresh rows across all four pools, exactly as intended.
§5's negative-LP-net observation concerned a *different* field —
`dutch_auction_lp_net_quote`, which aggregates `lp_net_all_flow_quote` — so this does not
settle §5 either way.

**`recapture_pct` is not clamped at 99.9%.** 9 rows exceed it; the clustering at
99.9/99.7/99.0 is the discrete fee grid, not a cap.

**The `-lp_net/(1-recapture)` reconstruction is not an amplification artifact.** Since
`recapture = lp_fee/gross_lvr` and `lp_net = lp_fee - gross_lvr`, the expression is
algebraically identical to `gross_lvr`. Verified empirically: max identity residual
1.455e-11 over 1,296 rows. The apparent "1000x" is exact reconstruction.

## 10.2 Trigger gap barely matters between 5 and 25 bps

Mean gain over each pool's own fixed-fee v3 baseline, post-fix:

| trigger_gap_bps | mean gain vs v3 (pp) | mean recapture | trigger events | mean clear rate |
|---|---|---|---|---|
| 5 | 65.921 | 98.709 | 662,967 | 0.715 |
| 10 | **65.952** | 98.740 | 619,659 | 0.757 |
| 25 | 65.435 | 98.223 | 530,100 | 0.776 |
| 50 | 62.935 | 95.724 | 421,560 | 0.828 |

**10 bps beats 5 bps by 0.03 pp** and 25 bps by 0.52 pp. The recommended trigger is
statistically indistinguishable from its neighbours on recapture, while trigger *volume*
swings 20% (662,967 → 530,100 events) across the same range. This is the fresh-data read
on the 15-vs-10 bps trigger concern: the choice is close to free on recapture and
expensive on event count, so it should be argued on solver load and gas, not recapture.

Holds per-pool — no pool prefers a different trigger:

| trigger_gap_bps | link_weth | uni_weth | wbtc_usdc | weth_usdc |
|---|---|---|---|---|
| 5 | 74.581 | 61.324 | 81.332 | 46.447 |
| 10 | 74.594 | 61.349 | 81.377 | 46.486 |
| 25 | 74.426 | 60.928 | 80.261 | 46.125 |
| 50 | 73.620 | 58.410 | 75.050 | 44.661 |

## 10.3 The acceptance criteria barely discriminate

**243 of 324 parameter sets pass** (216 with all four pools passing, 27 with 2/4); only 81
are rejected, all for low clear rate. Every one of the 1,296 pool-cells beats its fixed-fee
v3 baseline — `share beating baseline = 1.000` for all four pools.

A recommendation satisfied by 75% of the searched space is not being selected *by* the
grid. The recommended cell should be justified on the margin that actually separates it
(clear rate, solver payout, event count), because recapture does not.

## 10.4 The 124/124 clear rate is cell-specific

At the recommended cell, clear rate is 1.000 (0.9995 for UNI/WETH) and every window has
non-zero trigger events — the README's claim holds exactly where it is made. Across the
full grid it does not generalise: mean 0.769, and **378 of 1,296 cells clear below 50%**
(min 0.09). Worth stating as "at the recommended cell" wherever it appears.

## 10.5 Published figure worth a second look

`reports/lp_apr_uplift.md` prints LINK/WETH at **2,069 bps of TVL per month / 246%
annualized**. The document does flag it as dislocation-dominated and directs readers to the
ex-dislocation column (539 bps), and it is explicit throughout that these are a *modeled
ceiling* (single rational solver, zero gas, captive flow) rather than realized yield — the
caveating is genuinely careful. But a 246% annualized figure in a headline table will be
quoted without its caveat. Consider leading LINK/WETH with the ex-dislocation number.

## 10.6 What the fresh data still cannot settle

The grid has no `oracle_gap_analysis/` signal dataset, so **§1 and §2 cannot be re-verified
post-fix** — though §1 is a source-code defect and §2 was stable across all three stale
vintages, so neither depends on this. §5–§7 and §9 remain withdrawn: they need per-window
replay output that does not yet exist post-`b6d31ac`.
