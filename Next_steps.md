## Oracle-Anchored LVR Hook Research

## Focus Area

I am focusing the project on **historical replay validation of oracle-anchored toxic-flow pricing on liquid mainnet pools**.

This is the strongest near-term research angle because it directly tests whether the proposed hook can:

- recapture more toxic-flow LVR than a fixed fee,
- avoid overtaxing benign flow,
- fail safely when the oracle is stale or dislocated, and
- give a realistic basis for deciding whether a more complex execution path, such as an internal repricer, is worth building.

## Why This Matters

The core LP-protection question is whether an oracle-anchored dynamic fee can improve outcomes when the pool is stale relative to the external market.

If the fee is too low, LPs donate value to arbitrageurs during toxic repricing trades. If the fee is too high, benign order flow is overcharged or pushed away. The design also depends heavily on oracle quality and staleness handling, so replaying real swaps against real oracle updates is the right next validation step.

## Logic

- The exact toxic-flow fee formula `f*(z) = e^{|z|/2} - 1` is consistent with the production hook implementation and is now validated in-repo by deterministic exact-fee-identity tests.
- The width guard formula `Wmin = -4 log(1 - sigma^2 Delta / (8 epsilon))` is consistent with the hook's minimum-width logic.
- The main caveat also checks out: **exact fee neutralization with only public arbitrage leaves zero external arbitrage profit**, so production needs either:
  - a fee haircut `alpha < 1`, or
  - an internal repricer / custom-accounting branch.

One analytical claim is now automated in-repo:

- **Validated fee law / exact fee identity:** 10,000 deterministic random constant-product repricings now run in CI-style Python tests, and exact fee revenue matches one-step stale-price loss to machine precision.
- The GBM-style Monte Carlo path logic is for the separate `sigma^2/8` daily sanity check and the concentrated-liquidity width-amplification simulations.
- Current deterministic harness result in this repo: `max_absolute_error = 7.48e-81` for `sample_count=10_000`, `seed=7`, `gap_std=0.02`.
- Current frozen real-data proof in this repo: `44` replay-clean windows, `7,019` swaps, and `max_residual_error_exact = 1.0e-64` under `study_artifacts/one_page_proof_2026_03_31/`.


## What Is Already Implemented

- OCR-aware exporter for real Chainlink feeds, including modern `NewTransmission` logs.
- Storage-backed Uniswap v3 initialized tick extraction at exact `from_block`.
- Exact Uniswap v3 replay backend with replay-error gating, toxic/benign labeling, and markout generation.
- Manifest-driven multi-window, multi-oracle batch validation.
- Deterministic exact-fee-identity tests for the derived toxic-flow fee law.
- Dutch-auction backtest with a fixed-comparator baseline and policy ablation support.
- Expanded Dutch-auction ablation study with bootstrap confidence intervals and replay-clean non-WETH/USDC windows.
- Frozen reproducible study inputs and headline outputs for the current `44`-window Dutch-auction result under `study_artifacts/dutch_auction_ablation_2026_03_28/`.
- Frozen one-page proof artifacts for the fee-identity claim and stale-loss split under `study_artifacts/one_page_proof_2026_03_31/`.
- Solidity unit, fuzz, property, and fork coverage for:
  - toxic-direction fee logic,
  - oracle staleness handling,
  - width / centering admission logic,
  - live mainnet oracle wiring.

## Data Sources Identified

Initial focus:

- **Chain:** Ethereum mainnet
- **Primary pool:** WETH/USDC 0.3%
- **Primary oracle inputs:** Chainlink ETH/USD and USDC/USD

Data needed for the replay pipeline:

- pool swap logs,
- oracle update logs,
- block timestamps,
- pool snapshot state at `from_block`,
- initialized ticks,
- liquidity events,
- optional large/liquid external reference pool for faster markout comparison.

## Metrics We Will Use

- toxic recapture ratio,
- false-positive benign tax,
- false-negative missed toxic,
- stale-oracle rejection rate,
- fee-cap rejection rate,
- uncertain-label rate,
- post-trade markouts across multiple horizons,
- exact-replay error and reliability-gated analysis basis.

## Current Findings / Working Hypotheses

- Key fee result: for toxic repricing flow, `f*(z) = e^{|z|/2} - 1`, where `z = log(Pref / Ppool)` is the oracle-pool log gap. For small gaps, this is approximately the half-gap rule `f*(z) ~= |z| / 2`. That derivation is anchored in the equations and geometric LVR argument from Jason Milionis, Ciamac C. Moallemi, Tim Roughgarden, and Anthony Lee Zhang, *Automated Market Making and Loss-Versus-Rebalancing*, `arXiv:2208.06046`, version dated May 27, 2024, and is now supported by in-repo deterministic identity tests.
- Fee intuition: the Milionis et al. framework says LVR is driven by volatility and marginal liquidity. Specializing that logic to a finite-gap constant-product repricing event gives a toxic-flow premium of `e^{|z|/2} - 1`, which makes fee revenue line up with the one-step stale-price loss that would otherwise be donated to the arbitrageur.
- The fee law itself is no longer a live open question in this repo. The biggest remaining issue is **execution**, not accounting: the fee identity and replay machinery now work; the remaining execution question is how broadly the current Dutch-auction trigger and reserve policy generalizes across pools, regimes, and reference sources.
- The current production-oriented Dutch-auction policy is:
  - `trigger_mode=auction_beats_hook`
  - `reserve_mode=hook_counterfactual`
  - `start_concession_bps=25`
- The fixed-comparator Dutch-auction study now covers `44` replay-clean windows across `5` pools and `2` regimes, including three non-WETH/USDC pools and one replay-clean non-WETH/USDC stress family: `WBTC/USDC` 0.05%, 2h.
- On that expanded study, the current Dutch-auction policy delivers mean LP uplift vs hook of `+3.40`, with 95% bootstrap CI `[+0.93, +5.99]`.
- The same study shows mean trigger rate `1.21%` and mean fail-closed rate `0%`.
- Relative to the older `all_toxic` / `solver_cost` / `start_concession_bps=5` policy, the current policy improves mean LP uplift vs hook by `+6.92`, with 95% bootstrap CI `[+2.42, +11.72]`.
- The non-WETH/USDC evidence is mixed but useful:
  - DAI/USDC is neutral versus hook under the current setting, but materially better than the older policy baseline.
  - WBTC/USDC and WBTC/WETH are replay-clean but inert in the tested windows, including the added `WBTC/USDC` 2h stress family: the current policy stayed at zero triggers and zero uplift.
- Across the full `44`-window study, the Dutch auction is positive versus hook in `21` windows, zero in `23`, and negative in `0`.
- Relative to the older policy baseline, the current policy is positive in `28` windows, zero in `16`, and negative in `0`.
- The Dutch-auction branch is no longer just a design proposal. It is implemented, backtested, and now supported by a replay-clean ablation table. The remaining question is how broadly the current positive result generalizes.
- The repo now also carries a frozen one-page proof showing both sides of the argument:
  - the fee identity holds to machine precision on exact replay, and
  - the stale-loss split chart shows how much value a flat fee leaves with arbitrageurs across toxic swap sizes relative to the exact hook benchmark.
- The reason for the auction is that **exact fee neutralization by itself does not guarantee fast repricing**. If LPs charge the full stale-loss-recovery fee through the public path, outside arbitrage profit goes to roughly zero, so there may be no one left with a strong incentive to spend gas and inventory to move the pool back to the reference price.
- The Dutch auction is therefore **not** trying to discover the market price. The oracle snapshot already sets the target repricing state. The auction is discovering the minimum solver compensation needed to get the repricing trade executed quickly.
- The winning solver would settle the repricing trade against the snapped reference-aligned state, receive a concession `q`, and leave LPs with the remaining `(1 - q)` share of the stale-loss recovery.
- Concretely, the auction variable is the concession `q in [0, 1]`: the fraction of the exact stale-loss benchmark paid to the solver. It starts low and increases until a solver accepts, which means LPs only give up as much of the recovery as the market actually requires.
- The intended operational flow is: detect a toxic large-gap event, snapshot a fresh oracle reference, open a short solver window, grant the winner exclusive repricing rights for that window, then settle through an internal / custom-accounting path. If no solver clears the auction in time, the system falls back to the simpler public-searcher path with `alpha < 1`.
- A comparison against a deeper external reference pool is likely worth doing before considering anything more complex such as ML.


## Current Blockers / Open Questions

- The current empirical coverage is better, but still narrow:
  - active stress uplift remains concentrated in WETH/USDC,
  - the only replay-clean non-WETH/USDC stress family so far is `WBTC/USDC` 2h and it is economically inert,
  - and the replay-clean WBTC families remain zero-trigger / zero-uplift in the tested windows.
- Current local workbench issues, not frozen headline artifacts:
  - some otherwise interesting alt-token pools have failed exact replay on the first prefix windows, notably LINK/WETH and UNI/WETH; that is either a replay edge case or a pool-selection problem
  - deep-reference quality is pool-dependent, and some alt-pool comparators produced too few in-window reference updates to make multi-oracle ranking very informative
  - Binance coverage is not universal; for example, `DAIUSDT` 1-second archive data was unavailable for the tested DAI/USDC window, so that family currently enters the study as replay-clean 3-oracle coverage rather than full 4-oracle coverage
- We still need automated regression tests for the remaining analytical validation claims beyond the now-validated fee law / exact-fee identity and Dutch-auction accounting fixes.

## Next Steps

1. Add more replay-clean non-WETH/USDC **stress** families beyond the current inert `WBTC/USDC` 2h family, so the stress evidence does not rest entirely on WETH/USDC.
2. Investigate the exact-replay failures on LINK/WETH and UNI/WETH and either:
   - fix the replay edge case, or
   - replace those pools with replay-clean alternatives.
3. Improve external-reference selection for alt pools so the multi-oracle comparisons use genuinely active deep-pool comparators.
4. Promote the remaining analytical validation claims into automated tests while keeping the validated fee law explicit in repo summaries:
   - `sigma^2/8` Monte Carlo sanity check,
   - centered-range width amplification check,
   - optional hook-level regression around the already-validated fee surface if we want tighter implementation coverage than the current identity tests.
5. Decide how strongly to emphasize the current Dutch-auction result in repo summaries and future writeups:
   - supporting evidence if coverage stays narrow,
   - or a headline result if broader out-of-sample coverage lands cleanly.
