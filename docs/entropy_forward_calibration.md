# Forward Calibration Research

This study improves and stress-tests the probability layer of the offline
entropy flow classifier. It does not change Solidity, live fees, or the Dutch
auction.

Run the frozen local panel with:

```bash
python3 -m script.run_entropy_forward_calibration
```

Outputs are written to `reports/entropy_forward_calibration_2026/`. The report
contains 371,180 method-level predictions, fold and slice metrics, fitted
calibrator parameters, a checksum manifest, and explicit acceptance results.

## Why the original probability drifted

The original chronological split moved from a training resolved-toxic rate of
70.7% to 40.6% in the forward holdout while retaining a 69.7% mean predicted
probability. The failure is concentrated in gap-widening flow:

| pre-swap branch | old training toxic rate | old forward toxic rate | old forward mean probability |
| --- | ---: | ---: | ---: |
| closes gap | 99.60% | 98.98% | 99.22% |
| widens gap | 66.56% | 34.00% | 66.33% |

The close-gap arbitrage tail was already calibrated. The widening-gap branch
carried temporal and pool-composition shift. October 2025 supplied 82% of the
old training rows and included three pools absent from the forward test.

## Leakage-safe design

The new study uses five rolling origins from February through June 2026. In
each origin it:

1. fits the base cell model on older complete data;
2. purges the full 3,600-second ex-post label horizon at both boundaries;
3. fits calibration on one complete month; and
4. scores only the following untouched month.

A separate cold-start evaluation removes each test pool from both base training
and calibration. This is stricter than ordinary leave-one-pool-out evaluation,
which can accidentally train on future dates from other pools.

The ablation compares identity probabilities, global and sign-specific log-odds
offsets, and global and sign-specific Platt scaling. Every transformed
confidence interval is the union of:

- the transformed base-model confidence interval;
- model-based calibrator parameter uncertainty; and
- replay-window-clustered calibrator parameter uncertainty.

That union prevents recalibration from manufacturing a confident fee decision
from an uncertain base cell.

## Result

The strongest probability result was sign-specific Platt scaling:

| evaluation | method | Brier | log loss | ECE | folds improved on Brier / log loss |
| --- | --- | ---: | ---: | ---: | ---: |
| rolling chronological | identity | 0.3058 | 0.8184 | 0.2890 | baseline |
| rolling chronological | sign Platt | **0.2094** | **0.5975** | **0.0303** | 5/5 / 5/5 |
| rolling pool-held-out | identity | 0.3324 | 0.8885 | 0.3303 | baseline |
| rolling pool-held-out | sign Platt | **0.2293** | **0.6486** | **0.0884** | 11/15 / 11/15 |

This is a real calibration improvement, but not a deployment result:

- eligible pool/sign/month slices still reached 28.6% ECE;
- the sign-Platt selective policy covered only 0.39% of confirmed forward rows;
- its 100% observed toxic precision had only an 88.97% Wilson lower bound;
- it captured 0.036% of confirmed-toxic surcharge dollars;
- $64.11 of taxed dollars remained unresolved, and only 5.61% of its taxed
  dollars had resolved labels; and
- it still demonstrated no confirmed-benign decisions.

Zero measured benign surcharge therefore comes from conservative abstention,
not from having solved benign identification. Under the required worst case,
every unresolved taxed dollar is treated as potentially benign.

None of the five methods passed the complete acceptance checklist. In
particular, the 80% taxed-dollar-resolution requirement remains the dominant
economic blocker.

## How the supplied data-science paper was used

The supplied PDF, *DeepAnalyze: Agentic Large Language Models for Autonomous
Data Science* (arXiv:2510.16872), provides an experimental-process template, not
a probability-calibration algorithm. Applicable pieces were:

- iterative environment feedback (Sections 1 and 3.1, pp. 1 and 4–5);
- multi-objective evaluation of intermediate and final outputs (Section 3.2,
  p. 6);
- questioner/solver/inspector roles and checklist validation (Section 3.3,
  pp. 7–8, and Appendix A, pp. 16–17); and
- ablation across task families and explicit boundary re-examination (Sections
  4–5, pp. 8–11, and Appendix B, pp. 17–18).

Those ideas motivated the failure audit, purged splits, multiple economic
objectives, artifact checks, and calibrator ablation. The paper contains no
method for calibration under temporal drift, pool transfer, abstention, or
dollar-weighted classification harm, so it is not evidence for the statistical
model itself.

## Next research gate

The 2026 panel informed the candidate design, so even the rolling result remains
exploratory. Freeze sign-specific Platt scaling before collecting a genuinely
new forward month. In parallel, model outcome resolution separately:

\[
q(x)=P(\text{outcome resolves}\mid x),\qquad
p(x)=P(\text{toxic}\mid\text{resolved},x).
\]

Without assuming unresolved labels are benign or toxic, report the
partial-identification interval

\[
P(\text{toxic}\mid x)\in[q(x)p(x),\ q(x)p(x)+1-q(x)].
\]

That two-stage model, plus window-balanced recency and pool-level shrinkage, is
the next appropriate offline experiment. Solidity should remain untouched
until a frozen future holdout clears every calibration and economic gate.

The two-stage experiment has now been run and is documented in
`docs/entropy_two_stage.md`. Its assumption-free bounds correctly eliminate
unresolved tax exposure, but are too wide to classify any flow or capture toxic
dollars.

## Economic-label v3 rerun

The training-only horizon release was subsequently evaluated with the same five purged rolling
origins and 15 pool-held-out folds. This rerun consumes only `oracle_gap_analysis_v3` datasets and
uses `economic_outcome_label` as the target. Its frozen artifacts are in
`reports/economic_label_forward_calibration_2026/`.

No method passes the deployment checklist. The best forward probability fit is sign-specific Platt
scaling (Brier 0.1449, ECE 0.0508), but it abstains on every row and leaves all $422,103 of observed
positive toxic LP loss untaxed. Sign-specific offset classifies 1.48% of confirmed rows, charges
$20.25 to confirmed benign flow, and still leaves $417,065 (98.81%) of toxic LP loss untaxed.

Pool-held-out transfer is worse. Brier scores range from 0.1813 to 0.2378, ECE from 0.1581 to
0.2543, every method leaves at least 95.98% of toxic LP loss untaxed, and every method violates the
zero-dollar benign-surcharge budget in at least one held-out fold. Separate branch reporting shows
that widening-gap flow remains the calibration weakness; close-gap Brier is roughly 0.12, while
held-out widening-gap Brier ranges from 0.24 to 0.35.

This is a product-level rejection, not merely a weak accuracy result: confidence-based abstention
protects benign flow only by declining to tax almost all flow, while the methods that recover any
coverage do not preserve the benign-dollar budget across future months and unseen pools.
