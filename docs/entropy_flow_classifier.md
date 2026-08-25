# Offline Entropy/Confidence Flow Classifier

The entropy classifier is a research-only selective classifier. It does not
modify `OracleAnchoredLVRHook.sol`, the Dutch auction, or live fees.

Run the frozen local panel with:

```bash
python3 -m script.run_entropy_flow_classifier
```

Outputs are written to `reports/entropy_flow_classifier_2026/`:

- `predictions.csv.gz` contains every forward and pool-held-out prediction;
- `summary.csv` contains scalar metrics by fold;
- `evaluation.json` and `summary.md` contain the aggregate result; and
- `manifest.json` records the versioned config and SHA-256 hashes of all inputs.

## Model

The model uses only two fields available before execution:

- the signed Chainlink/pool gap in bps, positive when the swap closes the gap;
- the age of the causally preceding oracle observation.

Confirmed future markout labels are training targets only. They are never
features. Consequently, `toxicity_probability` estimates
`P(toxic_confirmed | outcome resolved, pre-swap cell)`, not unconditional
toxicity across unresolved flow. The estimator counts toxic and benign-confirmed outcomes in
`(gap sign, gap magnitude, oracle age)` cells. Sparse cells back off through
signed-gap, gap-sign, and global cells. The reported `toxicity_probability` is
the Jeffreys-smoothed Beta-Bernoulli posterior mean.

`confidence_lower` and `confidence_upper` conservatively combine:

1. a row-level Wilson interval; and
2. a replay-window-clustered sandwich interval.

Using the union of those intervals prevents a high-volume window from being
mistaken for thousands of independent market regimes. `predictive_entropy` is
normalized binary Shannon entropy, so zero means a one-sided probability and
one means maximum uncertainty at probability 0.5.

## Selective states

The output is deliberately three-way:

- `toxic` requires a fresh oracle, gap above the noise floor, low entropy, and
  the complete confidence interval above the toxic threshold;
- `benign` requires a gap-widening swap, low entropy, and the complete interval
  below the benign threshold;
- `abstain` covers missing/stale signals, the noise band, sparse cells, high
  entropy, and intervals crossing a decision boundary.

The version-1 thresholds live in
`research/lvr/config/entropy_classifier_config.json`. In the offline fee
counterfactual, only `toxic` receives the existing exact gap surcharge. Both
`benign` and `abstain` receive zero incremental surcharge. That convention is a
measurement device, not a deployment recommendation.

## Evaluation

The canonical panel contains 199 non-overlapping windows and 138,009 unique
swaps across six pool families. Experimental `fixed`, `retune`, and `age25h`
copies are excluded to prevent duplicate-event leakage.

Two independent evaluations are emitted:

- the chronological evaluation trains on complete earlier replay windows and
  tests on complete later windows; and
- leave-one-pool-out evaluation trains on every other pool family before
  scoring the held-out pool.

Metrics include precision, recall, false-positive rate, Brier score, log loss,
calibration error, abstention coverage, and incremental surcharge dollars.
Benign surcharge is the exact gap-fee counterfactual on confirmed-benign swaps
classified as toxic. Quote accounting is converted to dollars using USDC
directly and the causally preceding WETH/USDC reference for WETH-quoted pools.

Unresolved outcome rows are never counted as benign or toxic. Their potential
surcharge is reported separately as unresolved exposure; this prevents a small
confirmed-benign dollar value from being presented as proof that total user harm
is small.

The current result remains an offline diagnostic. Forward probability
calibration is materially weaker than selective toxic precision, the model
abstains on most confirmed outcomes, and the strict version-1 threshold emits no
confident-benign forward decisions. Those are explicit blockers for using this
model as a Solidity fee gate.

The follow-up purged rolling-origin calibration ablation is documented in
`docs/entropy_forward_calibration.md`. It materially improves forward
probability scores but does not clear the pool-slice, coverage, precision-bound,
or resolved-dollar deployment gates.
