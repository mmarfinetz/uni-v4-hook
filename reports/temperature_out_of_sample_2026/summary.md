# Effective-temperature out-of-sample sweep

This paired sensitivity panel covers 72 windows and 360 simulations from 2026_01, 2026_02, 2026_03, 2026_04, 2026_05, 2026_06. The zero-multiplier arm is the control. This is a sensitivity test, not post-hoc hyperparameter selection.

Hysteresis is not implemented or evaluated. The absolute 10 bps trigger, linear 0.5 bps/second concession schedule, gas model, and solver edge are fixed in every arm.

| multiplier | clears / triggers | clear rate | weighted stale-time share | LP windows improved / worsened / unchanged |
|---:|---:|---:|---:|---:|
| 0 | 0 / 1416 | 0.00% | 54.40% | 0 / 0 / 72 |
| 0.25 | 0 / 1416 | 0.00% | 54.40% | 0 / 0 / 72 |
| 0.5 | 0 / 1416 | 0.00% | 54.40% | 0 / 0 / 72 |
| 1 | 0 / 1416 | 0.00% | 54.40% | 0 / 0 / 72 |
| 2 | 0 / 1416 | 0.00% | 54.40% | 0 / 0 / 72 |

**Product decision: do not promote temperature into the auction policy.** No nonzero multiplier changed a clear, stale-time exposure, or LP-net outcome. The median window-mean minimum solver compensation was 5,005.6 bps, so execution economics, not response-horizon volatility, was binding.

LP-net quote totals are intentionally reported only per pool in `summary.csv`; WETH, PAXG, and EURC quote units are not summed across pools.

Inputs and SHA-256 hashes are recorded in `manifest.json`; paired window-level outcomes are in `per_window.csv`.
