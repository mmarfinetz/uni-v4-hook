# uni-v4-hook

`uni-v4-hook` is a Foundry research repo for an oracle-anchored Uniswap v4 hook that targets loss-versus-rebalancing (LVR) on stale pools with dynamic toxic-flow fees, oracle freshness checks, LP width/centering guards, and an optional Dutch-auction repricing path.

## 30-Second Proof

**Fee identity holds to machine precision on real data.**

Across `44` replay-clean frozen windows (`7,019` swaps), every exact-replay fee-identity check passed. Maximum residual error on the exact series was `1.0e-64`.

![Fee identity vs oracle gap](study_artifacts/one_page_proof_2026_03_31/fee_identity_vs_oracle_gap.svg)

The identity chart proves the accounting claim. The next chart answers the economic question: for real toxic swaps, how much stale-loss goes back to LPs versus remaining with arbitrageurs across swap sizes and fee schedules?

![LVR split by swap size and fee rate](study_artifacts/one_page_proof_2026_03_31/lvr_split_by_size_and_fee_rate.svg)

| Policy | Toxic-flow handling | 44-window result |
| --- | --- | --- |
| Baseline (`5 bps` fixed fee) | Flat fee, independent of oracle gap. | Underperforms the hook in `44 / 44` windows; median LP delta vs hook = `-2,652.39` quote. |
| Oracle-anchored hook | Charges the exact stale-loss recovery premium on toxic flow. | Reference policy for the study; fee identity passed in all `44 / 44` replay-clean windows. |
| Dutch auction | Only opens when solver execution beats the hook counterfactual. | Mean LP uplift vs hook = `+3.40` quote, 95% CI `[+0.93, +5.99]`; positive / zero / negative = `21 / 23 / 0`; mean trigger rate = `1.21%`. |

## Key Results

- Exact toxic-flow surcharge law: `f*(z) = e^{|z|/2} - 1`, with `z = log(P_ref / P_pool)`.
- The fee-identity claim is no longer hypothetical in this repo: the replay pipeline and exact-fee pass agree to machine precision on frozen real data.
- The current Dutch-auction policy is positive versus hook in `21` windows, zero in `23`, and negative in `0`.
- The main remaining question is execution generalization, not fee accounting: the strongest stress evidence is still concentrated in WETH/USDC.

## What the Hook Does

`src/OracleAnchoredLVRHook.sol` implements two controls:

- `beforeSwap`: reads a fresh oracle price, classifies toxic direction, and overrides the LP fee.
- `beforeAddLiquidity`: rejects LP ranges that are too narrow or too far off-center relative to the oracle.

Core mechanics:

- benign flow pays the base fee; toxic flow pays a gap-scaled surcharge, capped by `maxFee`
- swaps and fee previews fail closed when the oracle is stale
- the hook tracks oracle volatility through an EWMA-style `sigma^2` update
- LP admission uses width and centering guards derived from oracle risk

`src/oracles/ChainlinkReferenceOracle.sol` supplies the reference price, either from one Chainlink feed or a base/quote ratio assembled from two feeds.

## Quick Start

Requires Foundry and Python 3.

Build and run the core test suites:

```bash
forge build
forge test
python3 -m unittest discover -s script -p 'test_*.py'
```

Regenerate the proof artifacts shown above:

```bash
python3 -m script.generate_one_page_proof
```

Fork tests and live historical export need `MAINNET_RPC_URL`.

## Repository Layout

- `src/`: hook, oracle, and interfaces
- `test/`: Foundry unit, fuzz, invariant, property, and fork tests
- `script/`: Python export, replay, backtest, reporting, and artifact-generation code
- `study_artifacts/`: frozen proof artifacts and replay-clean study outputs
- `Next_steps.md`: current research direction and backlog

## Open Questions

- Add more replay-clean non-WETH/USDC stress families so the auction evidence is less concentrated.
- Resolve replay-clean coverage for additional pools such as LINK/WETH and UNI/WETH, or replace them with better study families.
- Test how robust the current auction trigger/reserve policy remains out of sample once coverage broadens.

## Further Reading

- [Next_steps.md](Next_steps.md)
- [study_artifacts/one_page_proof_2026_03_31/README.md](study_artifacts/one_page_proof_2026_03_31/README.md)
- [study_artifacts/dutch_auction_ablation_2026_03_28/README.md](study_artifacts/dutch_auction_ablation_2026_03_28/README.md)
