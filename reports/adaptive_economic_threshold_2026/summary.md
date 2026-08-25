# Bounded adaptive economic-threshold backtest

This paired panel covers 72 windows and 288 simulations from 2026_01, 2026_02, 2026_03, 2026_04, 2026_05, 2026_06. Every adaptive arm is compared with the fixed 10 bps control on the same window.

The model computes solver break-even from decision-time pool liquidity, reference price, gas, base fee, and the concession available at the arm's target horizon, then clamps the result to that arm's floor and ceiling.

| arm | clears / triggers | trades / fallback attempts | clear rate | mean trigger | weighted stale-time | executed-flow LP windows improved / worsened / unchanged |
|---|---:|---:|---:|---:|---:|---:|
| adaptive_10_1000_h600 | 14 / 23 | 23 / 9 | 60.87% | 320.02 bps | 83.78% | 57 / 1 / 14 |
| adaptive_5_1000_h600 | 14 / 23 | 23 / 9 | 60.87% | 320.02 bps | 83.78% | 57 / 1 / 14 |
| adaptive_5_100_h60 | 5 / 175 | 171 / 170 | 2.86% | 100.00 bps | 80.43% | 35 / 23 / 14 |
| fixed_10bps_control | 5 / 1417 | 771 / 1398 | 0.35% | 10.00 bps | 54.40% | 0 / 0 / 72 |

**Result: keep the fixed trigger as the product default.** Every tested adaptive candidate either worsened at least one LP window or increased aggregate stale time. Treat the adaptive model as a diagnostic until a follow-up design fixes that trade-off.

Even the least-stale adaptive arm, `adaptive_5_100_h60`, raised weighted stale time from 54.40% to 80.43% (+1,555,872 paired stale seconds).

The higher adaptive clear rates are conditional on opening far fewer auctions: they select only the largest gaps, so they are not evidence of better overall liveness. The 600-second arms' roughly 320 bps break-even is driven mainly by the undiscounted 5 bps base fee versus a 3.1% concession of stale loss at that horizon. The 60-second arm cannot reach break-even below its 100 bps ceiling.

The LP-window comparison includes only realized executed-flow accounting. It does not charge the policy for inventory risk while the pool remains stale, so fewer trades can look artificially favorable. Stale-time and gap-time exposure are the primary product-decision metrics here.

Executed-flow LP-net quote totals are reported only per pool in `summary.csv`; mixed WETH, PAXG, and EURC quote units are never summed.

This first test intentionally omits hysteresis. Inputs and hashes are in `manifest.json`; paired window outcomes are in `per_window.csv`.
