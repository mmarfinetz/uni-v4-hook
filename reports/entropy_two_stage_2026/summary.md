# Two-Stage Resolution/Toxicity Experiment

Offline research only. No Solidity or live fee path changed.

The model estimates `q=P(outcome resolves|x)` on every row and `p=P(toxic|resolved,x)` on confirmed rows. Without assumptions about unresolved flow, unconditional toxicity lies in `[q*p, q*p + 1-q]`.

| evaluation | resolution ECE | conditional toxicity ECE | dollar-weighted bound width | partial coverage | partial toxic precision | partial benign surcharge | partial unresolved surcharge | toxic-dollar capture |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| rolling_chronological | 2.72% | 3.03% | 78.35% | 0.00% | n/a | $0.00 | $0.00 | 0.00% |
| rolling_pool_held_out | 3.92% | 8.84% | 78.05% | 0.00% | n/a | $0.00 | $0.00 | 0.00% |

## Conditional-only comparison

- `rolling_chronological`: ignoring the resolution stage would classify 0.39% of confirmed rows and leave $64.11 of taxed unresolved exposure.
- `rolling_pool_held_out`: ignoring the resolution stage would classify 0.04% of confirmed rows and leave $143.12 of taxed unresolved exposure.

## Acceptance

- `rolling_chronological`: FAIL; failed gates: resolution_slice_ece, conditional_toxicity_slice_ece, partial_interval_width, toxic_precision_lower_bound, taxed_dollar_resolution, toxic_surcharge_capture.
- `rolling_pool_held_out`: FAIL; failed gates: conditional_toxicity_ece, resolution_slice_ece, conditional_toxicity_slice_ece, partial_interval_width, toxic_precision_lower_bound, taxed_dollar_resolution, toxic_surcharge_capture.

The local canonical corpus ends in June 2026. The configuration is frozen for the first newly collected complete post-June month, but that genuinely new confirmation set is not yet available.
