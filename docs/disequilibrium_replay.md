# Stale-Price Disequilibrium Replay

The stateful agent replay has an opt-in nonequilibrium policy experiment. It does
not change Solidity or the published baseline. The experiment treats the
pool/reference log gap as a disequilibrium signal and adds four mechanisms around
the existing absolute stale-gap trigger. Hysteresis is deliberately not part of
this implementation.

## Policy

Enable the experiment with `--disequilibrium-policy`. Unless explicitly
overridden, that switch applies these replay defaults:

- estimate variance causally from the trailing 24 hours of reference updates;
- use `T = sigma2_per_second * 60 seconds` and add `sqrt(T)` in bps to the
  starting solver concession;
- use an exponential concession with a 60-second pool clearing constant; and
- refuse to open an auction if the maximum concession cannot cover the cheapest
  solver requirement while preserving the configured LP reserve.

The absolute opening threshold remains mandatory. Standardized disequilibrium
does not replace it.

```bash
python3 -m script.run_agent_simulation \
  --oracle-updates exports/example/oracle_updates.csv \
  --pool-snapshot exports/example/pool_snapshot.json \
  --output reports/disequilibrium_rows.csv \
  --summary-output reports/disequilibrium_summary.json \
  --disequilibrium-policy \
  --trigger-condition stale_gap_bps_before \
  --trigger-gap-bps 10 \
  --temperature-latency-seconds 60 \
  --temperature-concession-multiplier 1 \
  --concession-schedule exponential \
  --relaxation-tau-seconds 90
```

`relaxation_tau_seconds` is supplied per replay, so each pool can use a clearing
constant estimated from its own historical gaps. Set
`--temperature-concession-multiplier 0` to retain temperature as a diagnostic
without changing the concession.

## Row diagnostics

The replay CSV records:

- `log_price_gap` and `free_energy_gap_potential`, where the latter is
  `(exp(abs(z) / 2) - 1)^2`;
- `sigma2_per_second`, `market_temperature`, and
  `standardized_disequilibrium`;
- `effective_start_concession_bps` and `scheduled_concession_bps`;
- `minimum_solver_concession_bps`, equal to the cheapest solver's gas-plus-margin
  requirement divided by exact gross LVR; and
- `economically_correctable`, which additionally checks maximum concession,
  auction accounting, and the LP reserve.

An economically uncorrectable opportunity does not open an auction, which is the
replay equivalent of waiting for a batch or another correction path.

## Causality and scope

Variance uses only reference observations whose block is available at the replay
decision point. If fewer than two timed observations are available, the replay
uses `bootstrap_sigma2_per_second` rather than reading future prices. This avoids
the whole-window volatility leakage that would invalidate an online policy.

The probabilistic toxicity model and entropy-production statistic are
intentionally not on-chain policy inputs here. The first offline
entropy/confidence implementation and its evaluation boundary are documented in
[`entropy_flow_classifier.md`](entropy_flow_classifier.md); multi-oracle
dispersion and entropy production still require additional data and validation.

## 2026 out-of-sample result

The reproducible sweep in
[`script/run_temperature_out_of_sample_sweep.py`](../script/run_temperature_out_of_sample_sweep.py)
holds the 10 bps absolute trigger, linear 0.5 bps/second schedule, gas model,
solver edge, and disabled free-energy gate fixed while varying only the
temperature multiplier over `0, 0.25, 0.5, 1, 2`. It covers 72 January-June 2026
windows across WETH/USDC, PAXG/USDC, and EURC/USDC (66 measured normal, 4 stress,
2 unmeasurable).

The temperature increment moved mean starting concession from 10.0 to 23.16 bps
but changed no execution outcome: every arm had 0 auction clears out of 1,416
triggers, identical 54.40% weighted stale-time share, and 72/72 unchanged LP-net
window outcomes. The median window-mean minimum solver compensation was about
5,006 bps, so execution economics was binding. Temperature should remain a
diagnostic rather than a product policy under the tested design. See
[`reports/temperature_out_of_sample_2026/summary.md`](../reports/temperature_out_of_sample_2026/summary.md)
for the paired result and hashed manifest.
