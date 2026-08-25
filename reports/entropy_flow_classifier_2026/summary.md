# Offline Entropy/Confidence Flow Classifier

This is an offline counterfactual only. It does not change Solidity or live fees.

The classifier estimates a Jeffreys-smoothed toxicity probability from signed-gap/oracle-age cells, publishes Wilson confidence bounds and normalized binary predictive entropy, and abstains unless the complete confidence interval clears the configured decision boundary.

Corpus: 138,009 unique swaps from 199 non-overlapping windows across 6 pools.

| evaluation | confirmed test rows | classified coverage | toxic precision | toxic recall | false-positive rate | confirmed-benign surcharge | unresolved surcharge exposure |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| chronological | 6,716 | 4.94% | 99.70% | 12.14% | 0.03% | $0.01 | $6,736.63 |
| pool-held-out aggregate | 34,375 | 8.23% | 99.61% | 12.65% | 0.09% | $0.01 | $68,028,494.91 |

## Evaluation reading

- Forward holdout toxic precision is 99.70% (95% interval 98.31% to 99.95%) at 4.94% confirmed-label coverage.
- The strict benign upper bound produced 0 forward benign decisions. The safety mechanism is therefore abstention, not a claim that benign flow has been identified reliably.
- Forward expected calibration error is 0.291. The probabilities drift materially out of sample even though the selective toxic tail remains precise.
- Confirmed-benign surcharge fell from $0.27 under the current direction rule to $0.01 under the selective counterfactual.
- Forward toxic decisions also carry $6,736.63 of unresolved-label surcharge exposure. That amount is unknown harm, not validated toxic revenue.

These results support continued offline use, but not a Solidity fee gate: the classifier is precise only by abstaining heavily, emits no confident-benign forward decisions, and its raw probabilities are not yet time-calibrated.

## Guardrails

- Training uses only confirmed ex-post labels; predictions use only pre-swap signed gap and oracle age.
- Missing, stale, noisy, high-entropy, sparse, or confidence-overlapping rows abstain.
- Benign surcharge is incremental gap-fee dollars on confirmed-benign swaps incorrectly assigned `toxic`.
- Unresolved surcharge exposure is reported separately; it is not assumed benign or toxic and can dominate the identified-dollar total.
- `toxicity_probability` is conditional on the strict outcome label resolving; it is not an unconditional probability over unresolved flow.
- `abstain` receives zero incremental surcharge in this offline accounting. This is not an on-chain policy recommendation.
- Dollar conversion uses token1 USDC directly and a causally preceding WETH/USDC reference for WETH-quoted pools.
- Unresolved ex-post outcomes are not silently treated as benign or toxic, so dollar harm is measurable only on confirmed-benign rows.
