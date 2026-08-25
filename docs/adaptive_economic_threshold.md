# Bounded economic trigger policy

Status: research-only backtest. The Solidity hook still uses its configured
fixed `triggerGapBps`; this experiment does not change production behavior.

## Policy

For a candidate absolute pool/reference gap `g`, the model uses the exact
constant-product correction trade to calculate:

- `G(g)`: gross stale-price value;
- `N(g)`: toxic input notional;
- `LP(g, h) = baseFee * N(g) + alpha * G(g) * (1 - c(h))`;
- `solverProfit(g, h) = G(g) - LP(g, h)`; and
- `solverRequired(g) = gasCost + solverEdge * N(g)`.

`c(h)` is the Dutch-auction concession available at a pre-registered target
clearing horizon. The raw threshold is the smallest gap where solver profit is
strictly greater than solver required return. A bisection search finds that gap
using only decision-time state.

The policy then applies explicit safety bounds:

- Below the floor, it does not trigger even if the modeled trade is economic.
- Above the ceiling, it triggers even if modeled break-even has not been
  reached. This `maximum_escape` condition prevents unbounded deferral.
- If the retained surcharge at the target horizon is below the configured LP
  recovery reserve, the solver model is treated as infeasible and the ceiling
  applies.

The first version has no hysteresis and supports the replay's single-solver
economics only.

## Paired backtest

Run:

```bash
python3 -m script.run_adaptive_economic_threshold_sweep
```

The study replays 72 matched January-June 2026 windows across WETH/USDC 0.30%,
PAXG/USDC 0.05%, and EURC/USDC 0.05%. It holds the fee law, auction schedule,
fallback policy, gas assumption, and historical inputs constant while changing
only the trigger policy.

| Arm | Mean effective threshold | Clears / triggers | Repricing trades | Weighted stale time |
| --- | ---: | ---: | ---: | ---: |
| Fixed 10 bps control | 10.00 bps | 5 / 1,417 | 771 | 54.40% |
| Adaptive 5-100 bps, 60s target | 100.00 bps | 5 / 175 | 171 | 80.43% |
| Adaptive 5-1,000 bps, 600s target | 320.02 bps | 14 / 23 | 23 | 83.78% |
| Adaptive 10-1,000 bps, 600s target | 320.02 bps | 14 / 23 | 23 | 83.78% |

The 60-second model is infeasible within its bound and therefore stays at the
100 bps escape ceiling. At 600 seconds the linear schedule has conceded 310 bps,
or 3.1% of stale loss. That small concession must first overcome the
undiscounted 5 bps base fee, which drives the modeled break-even gap to roughly
320 bps. The identical 5 bps and 10 bps floor results confirm that neither floor
binds in that arm.

The higher conditional auction-clear rate is a selection effect: the adaptive
arms wait for much larger gaps and open far fewer auctions. Even the least-stale
adaptive arm increases weighted stale time from 54.40% to 80.43%, adding
1,555,872 paired stale seconds. The 600-second arms add 1,755,684 paired stale
seconds and about 266.1 million bps-seconds of gap exposure.

## Decision

Keep the fixed 10 bps trigger as the product default. The bounded economic
threshold is useful as a diagnostic, but this formulation optimizes solver
break-even at the cost of pool liveness.

The reported LP-net comparison covers executed flow only. It does not price the
inventory risk of leaving the pool stale, so policies that simply avoid trades
can appear artificially favorable. Stale-time and gap-time exposure are the
primary decision metrics for this experiment.

Reproducibility artifacts are under
`reports/adaptive_economic_threshold_2026/`: `manifest.json` contains input
hashes and fixed assumptions, `per_window.csv` contains paired outcomes, and
`summary.csv`, `summary.json`, and `summary.md` contain aggregates.
