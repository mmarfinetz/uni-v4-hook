# Dutch Auction Operational Spec

This document makes the backtested Dutch-auction path operationally explicit for the current research hook. It describes a proposed production architecture; it is not an audited deployment plan.

Observed ranges in the frozen `new_policy` batch at `.tmp/dutch_auction_ablation_artifact_build/new_policy/aggregate_manifest_summary.json`:

- `fill_rate`: `1.00` to `1.00`
- `fallback_rate`: `0.00` to `0.00`
- `oracle_failclosed_rate`: `0.00` to `0.00`

## 1. Lifecycle of an Auction-Eligible Swap

The current deployed research hook is synchronous and hook-only: [`beforeSwap`](/Users/mitch/uni-v4-hook/uni-v4-hook/src/OracleAnchoredLVRHook.sol#L289) reads the oracle, calls `_quoteFee`, and returns a fee override to `PoolManager`.

The proposed auction model is asynchronous across blocks, matching the backtest's `time_to_fill_seconds > 0` assumption in [`simulate_auction_swap`](/Users/mitch/uni-v4-hook/uni-v4-hook/script/run_dutch_auction_backtest.py).

Proposed lifecycle:

1. `beforeSwap` observes a toxic swap and computes the hook counterfactual after `_quoteFee` but before the fee override would be returned to `PoolManager`.
2. If the trigger condition is met, the hook or relay emits `AuctionOpened(poolId, swapHash, exactStaleLoss, startConcessionBps, deadline)`.
3. The swap enters a pending auction path controlled by an off-chain relay or dedicated auction module.
4. Solvers submit fills through `fillAuction(swapHash, paymentQuote)`.
5. If a solver clears before deadline and the oracle remains fresh, the swap settles on the auction path.
6. If no solver clears before deadline, execution falls back to the hook fee path.
7. If the oracle becomes stale before settlement, the auction cancels and the swap is denied.

The trigger decision should intercept exactly where [`beforeSwap`](/Users/mitch/uni-v4-hook/uni-v4-hook/src/OracleAnchoredLVRHook.sol#L289) currently converts `_quoteFee` output into `feeUnits | OVERRIDE_FEE_FLAG`.

## 2. Solver Interface

Minimal interface:

```solidity
function fillAuction(bytes32 swapHash, uint256 paymentQuote) external;
```

Solver-visible state should include:

- `exactStaleLossQuote`
- `currentConcessionBps`
- `hookFeeRevenueQuote`
- `deadline`
- `reserveMode`

Economic interpretation:

- Solver pays `paymentQuote`, which must cover `solverGasCostQuote + solverEdge`.
- LP receives the stale-loss recovery above the hook counterfactual: `exactStaleLossQuote - paymentQuote`, subject to reserve and uplift checks.
- Solver fill acceptance follows the backtest reserve gates in [`_time_to_fill`](/Users/mitch/uni-v4-hook/uni-v4-hook/script/run_dutch_auction_backtest.py).

## 3. Governance Parameters

All configurable fields come from [`DutchAuctionConfig`](/Users/mitch/uni-v4-hook/uni-v4-hook/script/run_dutch_auction_backtest.py#L31).

| Parameter | Type | Unit | Backtest default | Recommended updater | Recommended range / rationale |
| --- | --- | --- | --- | --- | --- |
| `startConcessionBps` | `float` | stale-loss bps | `25.0` | timelock / DAO | `5` to `100`; lower values reserve more recovery for LPs. |
| `concessionGrowthBpsPerSecond` | `float` | bps/sec | `10.0` | timelock / DAO | `1` to `100`; controls how fast the solver payment rises. |
| `maxConcessionBps` | `float` | stale-loss bps | `10000.0` | timelock / DAO | `500` to `10000`; hard ceiling on solver payment fraction. |
| `maxAuctionDurationSeconds` | `int` | seconds | `600` | timelock / DAO | `30` to `600`; longer windows improve fill odds but raise stale risk. |
| `solverGasCostQuote` | `float` | quote units | `0.25` | ops / timelock | update per chain and gas regime. |
| `solverEdgeBps` | `float` | toxic-notional bps | `0.0` | timelock / DAO | `0` to low tens of bps; captures solver margin. |
| `minAuctionStaleLossQuote` | `float` | quote units | `1.0` in batch CLI | timelock / DAO | use to skip dust auctions. |
| `triggerMode` | `string` | enum | `auction_beats_hook` | timelock / DAO | keep `auction_beats_hook` as default because it preserves the hook counterfactual. |
| `reserveMode` | `string` | enum | `hook_counterfactual` | timelock / DAO | `hook_counterfactual` is the recommended production reserve. |
| `reserveHookMarginBps` | `float` | stale-loss bps | `0.0` | timelock / DAO | positive values require extra LP improvement over hook. |
| `minLpUpliftQuote` | `float` | quote units | `0.0` | timelock / DAO | guards against operationally trivial fills. |
| `minLpUpliftStaleLossBps` | `float` | stale-loss bps | `100.0` in the study runner | timelock / DAO | adds a scaled LP-uplift threshold. |
| `solverPaymentHookCapMultiple` | `float` | multiple | `1.0` in the study runner, `999.0` CLI backward compat | timelock / DAO | near `1.0` keeps solver payment bounded by the hook counterfactual. |

Recommended governance model:

- Phase 1: owner-controlled `setConfig()` is acceptable because no auction module is live.
- Phase 2+: move auction parameters behind a `48h` timelock before external solver participation.

## 4. Fail-Closed Semantics

Backtest code paths:

- Oracle goes stale before fill: [`simulate_auction_swap`](/Users/mitch/uni-v4-hook/uni-v4-hook/script/run_dutch_auction_backtest.py) returns `oracle_stale_at_fill=True`, `fallback_triggered=True`, and zero LP fee revenue. Production interpretation: cancel the auction and deny settlement on stale data.
- No solver fill before deadline: `_time_to_fill(...)` returns `None`, and the path falls through to [`_no_auction_result_using_hook`](/Users/mitch/uni-v4-hook/uni-v4-hook/script/run_dutch_auction_backtest.py), meaning the swap executes at the hook counterfactual.
- Solver payment exceeds the configured hook-cap multiple: `_time_to_fill(...)` rejects the fill candidate.
- Hook fee above `maxFee`: `_hook_fallback_outcome(...)` uses the same fail-closed semantics as the Solidity hook, so the counterfactual fee path is denied rather than clipped.

## 5. Deployment Phases

### Phase 1: Hook Only

- Current implementation in [`src/OracleAnchoredLVRHook.sol`](/Users/mitch/uni-v4-hook/uni-v4-hook/src/OracleAnchoredLVRHook.sol)
- No auction path
- Governance: owner-managed `setConfig()` and `setRiskState()`

### Phase 2: Hook + Off-Chain Auction Relay

- Off-chain relay opens auctions, gathers solver bids, and submits the winning fill
- Best match for the current backtest because the model is asynchronous across blocks
- Governance: recommended `48h` timelock over all auction parameters plus emergency pause authority

### Phase 3: Hook + On-Chain Auction Module

- Dedicated auction contract or internal accounting module handles fills on-chain
- Only justified if gas economics and observed solver participation support it
- Governance: DAO / timelock ownership with explicit parameter update delays and audit requirements
