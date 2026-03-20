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

- The exact toxic-flow fee formula `f*(z) = e^{|z|/2} - 1` is consistent with the production hook implementation.
- The width guard formula `Wmin = -4 log(1 - sigma^2 Delta / (8 epsilon))` is consistent with the hook's minimum-width logic.
- The main caveat also checks out: **exact fee neutralization with only public arbitrage leaves zero external arbitrage profit**, so production needs either:
  - a fee haircut `alpha < 1`, or
  - an internal repricer / custom-accounting branch.

One claim is now automated in-repo:

- **Exact fee identity:** 10,000 deterministic random constant-product repricings now run in CI-style Python tests, and exact fee revenue matches one-step stale-price loss to machine precision.
- The GBM-style Monte Carlo path logic is for the separate `sigma^2/8` daily sanity check and the concentrated-liquidity width-amplification simulations.
- Current measured result in this repo: `max_absolute_error = 6.16911036144252e-17`.


## What Is Already Implemented

- OCR-aware exporter for real Chainlink feeds, including modern `NewTransmission` logs.
- Storage-backed Uniswap v3 initialized tick extraction at exact `from_block`.
- Historical replay harness with toxic/benign labeling and markout generation.
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
- replay error once exact concentrated-liquidity replay is wired in.

## Initial Findings / Working Hypotheses

- Key fee result: for toxic repricing flow, `f*(z) = e^{|z|/2} - 1`, where `z = log(Pref / Ppool)` is the oracle-pool log gap. For small gaps, this is approximately the half-gap rule `f*(z) ~= |z| / 2`. That derivation is anchored in the equations and geometric LVR argument from Jason Milionis, Ciamac C. Moallemi, Tim Roughgarden, and Anthony Lee Zhang, *Automated Market Making and Loss-Versus-Rebalancing*, `arXiv:2208.06046`, version dated May 27, 2024.
- Fee intuition: the Milionis et al. framework says LVR is driven by volatility and marginal liquidity. Specializing that logic to a finite-gap constant-product repricing event gives a toxic-flow premium of `e^{|z|/2} - 1`, which makes fee revenue line up with the one-step stale-price loss that would otherwise be donated to the arbitrageur.
- The biggest remaining issue is **execution**, not accounting.
- I expect the historical replay to show that oracle-anchored hook fees outperform fixed fees on clearly toxic flow, but that large-gap events will still expose the execution caveat that motivates `alpha < 1` or an internal repricer.
- The Dutch-auction branch in the design is meant to solve that execution problem: when the pool is materially stale or in stress mode, the hook would auction off exclusive repricing rights to a solver for a short window.
- The reason for the auction is that **exact fee neutralization by itself does not guarantee fast repricing**. If LPs charge the full stale-loss-recovery fee through the public path, outside arbitrage profit goes to roughly zero, so there may be no one left with a strong incentive to spend gas and inventory to move the pool back to the reference price.
- The Dutch auction is therefore **not** trying to discover the market price. The oracle snapshot already sets the target repricing state. The auction is discovering the minimum solver compensation needed to get the repricing trade executed quickly.
- The winning solver would settle the repricing trade against the snapped reference-aligned state, receive a concession `q`, and leave LPs with the remaining `(1 - q)` share of the stale-loss recovery.
- Concretely, the auction variable is the concession `q in [0, 1]`: the fraction of the exact stale-loss benchmark paid to the solver. It starts low and increases until a solver accepts, which means LPs only give up as much of the recovery as the market actually requires.
- The intended operational flow is: detect a toxic large-gap event, snapshot a fresh oracle reference, open a short solver window, grant the winner exclusive repricing rights for that window, then settle through an internal / custom-accounting path. If no solver clears the auction in time, the system falls back to the simpler public-searcher path with `alpha < 1`.
- A comparison against a deeper external reference pool is likely worth doing before considering anything more complex such as ML.


## Current Blockers / Open Questions

- The exact Uniswap v3 replay backend is not implemented yet, so current replay still uses the approximate path.
- The Dutch-auction repricer is still a design proposal, not an implemented or empirically validated mechanism.
- We still need automated regression tests for the remaining paper validation claims beyond the exact-fee identity.
- We need to decide whether the reference market for markouts should stay Chainlink-only or also include a highly liquid external pool.

## Next Steps

1. Finish the exact Uniswap v3 replay backend and validate replayed end-state accuracy against observed swaps.
2. Promote the remaining paper validation claims into automated tests:
   - half-gap recovery,
   - `sigma^2/8` Monte Carlo sanity check,
   - centered-range width amplification check.
3. Run the historical replay on at least one liquid mainnet pool over a larger real window and collect:
   - toxic vs benign flow labels,
   - recapture ratios,
   - false-positive / false-negative behavior,
   - stale-oracle rejection behavior.
4. Compare the target pool against a more liquid external reference pool to test whether price gaps can improve fee setting or rebalance logic.
5. Decide whether the Dutch-auction / internal repricer branch is worth carrying forward as future work.

