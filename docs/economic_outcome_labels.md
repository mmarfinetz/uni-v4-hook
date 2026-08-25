# Economic outcome labels (version 3)

This is an offline research label. It does not change the hook or any Solidity.

Version 3 fixes two distinct failure modes in the previous composite outcome:

1. Missing future prices are now **unobservable**, not economically ambiguous.
2. Toxicity no longer requires the signs at 12, 60, 300, and 3,600 seconds to agree.

The old `outcome_label` remains in `oracle_signal_dataset.csv` for reproducibility.
New classifiers prefer `economic_outcome_label` whenever that column exists.

## Per-swap observation vector

For every configured horizon `h`, the dataset emits:

- `observed_{h}s` and `censoring_reason_{h}s`
- `markout_{h}s`, with `markout_lower_{h}s` and `markout_upper_{h}s`
- signed, baseline-fee-adjusted `lp_loss_quote_{h}s` and `lp_loss_usd_{h}s`
- lower and upper loss bounds in both native quote units and USD when conversion is available

Positive signed LP loss means adverse selection exceeds the baseline fee. Negative loss means
the baseline fee covers the observed markout. `censoring_reason` summarizes missing horizons;
`outcome_observability` reports the primary horizon separately from economic ambiguity.

## Primary target

The Dutch-auction replay runs before outcome labeling. The canonical release uses the nearest-rank
p90 of filled-auction `time_to_fill_seconds` from each pool's earliest training month, rounds it up
to a whole second, and freezes that pool-specific horizon across all later evaluation months.
Evaluation windows never choose their own target. Auction fill mechanics use only pre-outcome
information; future prices and toxicity labels are not inputs to horizon selection. A pool with no
training fills uses the explicitly recorded global training-panel fallback.

The 12/60/300/3,600-second horizons remain in the dataset as robustness checks.

At the primary horizon, the last reference update no later than the target and the first update no
earlier than it define a reference-sampling loss interval. This is an observation interval, not a
Gaussian confidence interval:

- `benign` only when `primary_lp_loss_upper_quote <= 0`
- `toxic` only when `primary_lp_loss_lower_quote > 0`
- `abstain` when the interval crosses zero, accounting/bounds are missing, or the horizon is censored

This makes false-benign labels deliberately difficult: a trade is never declared benign merely
because a sparse reference feed happened to print one favorable point.

## Reference-tail guarantee

New manifests declare `markout_extension_seconds: 3600`. Live exports resolve the end block from
timestamps and query one oracle-freshness interval beyond the required markout deadline so the
first reference update after the final target is normally present. Export summaries publish:

- `market_reference_required_through_timestamp`
- `market_reference_observed_through_timestamp`
- `market_reference_tail_complete`

Cached inputs are extended through the first available row after the deadline. If the cache itself
has no such row, the per-horizon observation bit remains false rather than inventing an outcome.
The first post-target reference must also arrive within the configured sampling-delay tolerance;
otherwise market closure or a sparse feed is reported as censored even when the file extends much
further in wall-clock time.

## Canonical release and audit

Run the complete frozen release with:

```bash
python3 -m research.lvr.studies.run_economic_label_release \
  --output-dir reports/economic_label_release_2026
```

It regenerates the 196 canonical October 2025 and January-June 2026 window datasets under each
window's `oracle_gap_analysis_v3/` directory and leaves the legacy dataset intact. The report emits
the frozen horizon table, per-window tail audit, and benign/toxic/abstain plus legacy/v3 disagreement
by pool, month, direction, measured regime, and their joint cells.

The current corpus has 135,752 swaps. Primary-horizon observability is 81.48%, economic quote-unit
accounting is complete, and direct raw USD conversion is missing on 34.45% of rows. The downstream
evaluator causally converts eligible WETH-denominated rows and reports its resulting USD coverage.
The v3 target is 19.90% benign, 28.49% toxic, and 51.60% abstain; legacy and v3 labels disagree on
49.23% of swaps. The 99% observability gate therefore fails, and this release authorizes no Solidity
change.
