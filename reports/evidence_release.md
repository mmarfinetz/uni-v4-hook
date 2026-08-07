# Evidence Release 2026-08-03

This is the canonical, frozen result bundle for public claims. Quote the measured labels
and exclusions below; declared manifest labels are provenance only.

## Observed-flow policy ablation

Across 54 windows, the selective rule improves LP uplift versus the broad rule in
**19**, leaves **35** unchanged, and worsens **0**. Mean window trigger rate falls from
**7.17%** to **3.07%**. Against the fixed-fee policy, the selective rule is higher in
**54 of 54** windows, unchanged in 0, and lower in 0.

Mean LP uplift versus the base hook is 0.3609 for the broad policy and 1.3501 for the
selective policy, a mean delta of 0.9892 with a family-bootstrap 95% interval [0.0065,
2.2198]. Native quote units are directional within-window evidence, not a cross-pool
dollar total.

Measured volatility is available for 38 windows: all are normal at the 100% threshold
(14 improved, 24 unchanged, 0 worsened). The remaining 16 windows have too few
primary-feed observations and are excluded from regime claims (5 improved, 11 unchanged,
0 worsened). This ablation therefore does **not** establish stress-regime
generalisation.

Event-weighted execution counts:

| policy | swaps | triggered | filled | fallback | stale | trigger rate | fill rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| broad | 7106 | 298 | 285 | 13 | 13 | 4.19% | 95.64% |
| selective | 7106 | 130 | 119 | 11 | 11 | 1.83% | 91.54% |

## October 2025 measured regimes

All 124 windows are measurable: **95 normal / 29 stress** at the 100%
annualised-volatility threshold. No group has a materially negative window versus either
the base hook or fixed-fee control under the threshold sensitivity below.

| threshold | regime | windows | vs hook +/0/- | vs fixed +/0/- | mean trigger rate | mean fill rate |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 80% | normal | 68 | 30/38/0 | 68/0/0 | 6.47% | 95.84% |
| 80% | stress | 56 | 5/51/0 | 56/0/0 | 0.48% | 99.82% |
| 100% | normal | 95 | 31/64/0 | 95/0/0 | 4.64% | 95.97% |
| 100% | stress | 29 | 4/25/0 | 29/0/0 | 0.92% | 99.78% |
| 120% | normal | 107 | 31/76/0 | 107/0/0 | 4.12% | 95.97% |
| 120% | stress | 17 | 4/13/0 | 17/0/0 | 1.57% | 99.78% |

## Interpretation boundary

The observed-flow ablation supports selectivity and non-negative within-window
accounting under the replay. The October recut supplies calm/stress evidence for the
month-scale correction-trade corpus. Neither result models competitive routing, multiple
solvers, or guaranteed live inclusion.

## Frozen inputs

The release directory includes the policy summaries, a consolidated primary-reference
observation file for all October windows, per-window regime metrics, source hashes, and
hashes for every frozen artifact. The public claim check recomputes the volatility
labels from those committed observations.
