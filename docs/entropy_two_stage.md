# Two-Stage Resolution/Toxicity Research

This experiment asks whether the offline entropy classifier has enough evidence
to tax flow once unresolved ex-post labels are treated as unknown rather than
implicitly ignored. It does not change Solidity, live fees, or the Dutch
auction.

Run it with:

```bash
python3 -m script.run_entropy_two_stage
```

Outputs are written to `reports/entropy_two_stage_2026/` and include 74,236
predictions, per-fold and per-slice metrics, fitted calibrators, acceptance
checks, and a checksum manifest.

## Model

The first stage estimates resolution on every row:

\[
q(x)=P(\text{outcome resolves}\mid x).
\]

The second stage estimates toxicity only among resolved rows:

\[
p(x)=P(\text{toxic}\mid\text{resolved},x).
\]

Both stages use the same pre-swap signed-gap/oracle-age hierarchy and
sign-specific Platt calibration. They are fit on older data, calibrated on one
complete month, purged by the full 3,600-second label horizon, and tested on the
following month. Cold-start folds remove each test pool from both stages.

No assumption is made about unresolved labels. Therefore unconditional toxicity
is only partially identified:

\[
P(\text{toxic}\mid x)\in[q(x)p(x),\ q(x)p(x)+1-q(x)].
\]

The interval width is exactly `1-q`. Confidence endpoints additionally union
the base-cell and replay-window-clustered calibrator uncertainty.

## Result

| evaluation | resolution ECE | conditional toxicity ECE | mean bound width | dollar-weighted bound width |
| --- | ---: | ---: | ---: | ---: |
| rolling chronological | 2.72% | 3.03% | 74.07% | **78.35%** |
| rolling pool-held-out | 3.92% | 8.84% | 71.32% | **78.05%** |

Aggregate calibration is respectable, but the uncertainty set is much too wide
to support fees. On chronological folds:

- observed resolution was 25.52%, versus 25.93% predicted;
- the mean unconditional toxicity set was `[11.67%, 85.74%]`;
- surcharge-weighted identified-toxic value ranged from $14,805 to $132,643;
- eligible resolution slices reached 38.47% ECE; and
- eligible conditional-toxicity slices reached 28.62% ECE.

The assumption-free gate consequently classified zero rows, charged $0 to
confirmed benign flow, left $0 of taxed unresolved exposure, and captured 0% of
confirmed-toxic surcharge dollars. That is safe abstention, not a useful fee
classifier.

For comparison, ignoring resolution and using conditional toxicity alone would
classify 0.39% of confirmed chronological rows, leave $64.11 of unresolved
taxed exposure, and capture only 0.036% of confirmed-toxic surcharge dollars.
The cold-start equivalent leaves $143.12 unresolved while capturing essentially
no toxic dollars.

Both chronological and pool-held-out evaluations fail the full acceptance
checklist. The two-stage model improves the honesty of the evidence, but does
not improve the deployable product.

## Interpretation

The binding constraint is label observability, not another probability
transform. With only about one quarter of forward rows resolving, an
assumption-free classifier cannot prove that either side of the remaining flow
is benign or toxic. Relaxing the bounds would require a substantive assumption
about unresolved swaps; doing so merely to increase coverage would recreate the
original benign-tax risk.

The next research work should target the outcome definition and data collection:

1. collect the first complete canonical month after June 2026 with all future
   markout horizons available;
2. keep `entropy_two_stage_config.json` unchanged for that confirmation run;
3. evaluate separately prespecified 12s, 60s, 300s, and 3,600s economic-loss
   targets instead of requiring sign unanimity across all horizons; and
4. compare those targets by confirmed-benign surcharge and captured-toxic
   dollars before deciding whether a less selective label is economically valid.

The local workspace currently has no canonical post-June month. The config is
frozen and the manifest records that the new-month confirmation requirement is
still unmet. Solidity should remain untouched.
