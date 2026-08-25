# FairFlow

> An ordering-independent MEV auction for Uniswap v4 that keeps benign swaps cheap, charges oracle-visible toxic flow, and returns the captured value to LPs through standard v4 fees.

## The Pitch

A stale AMM quote gives an informed repricer value at LPs' expense. FairFlow turns that race into a transparent price-discovery process inside a Uniswap v4 hook:

1. A fresh external oracle exposes the gap between the pool and the reference market.
2. Swaps that do not exploit that gap keep the low base fee.
3. Swaps in the toxic repricing direction pay a gap-scaled surcharge.
4. When the gap is large enough, a permissionless Dutch auction grows a solver concession over time until repricing is economical.
5. The surcharge retained by the pool is paid to LPs as the standard v4 LP fee. There is no sidecar vault, protocol skim, token, or custom claims process.

The result is a pool that can charge for oracle-visible adverse selection without making every user subsidize the protection.

## Why "Ordering-Independent"

FairFlow does not require a private transaction-order auction, trusted sequencer, solver allowlist, or an identity system that decides who is informed. The rule is computed from public state: pool price, fresh oracle price, swap direction, configured policy, and elapsed auction time. Any solver can inspect the same state and clear the auction with an ordinary v4 swap.

Transactions are still ordered by the chain, and earlier swaps can change pool state. "Ordering-independent" means that FairFlow's fee and concession are not allocated by a hidden queue position or a private bundle contest: the same public state produces the same terms for any caller.

## Mechanism At A Glance

| Flow | FairFlow response | LP outcome |
| --- | --- | --- |
| Benign swap | Charge the configured base fee. | Normal low-cost v4 execution. |
| Oracle-visible toxic swap | Add the exact gap-scaled toxic-flow surcharge. | Stale-price value is retained as LP fees. |
| Large stale gap | Open a permissionless, time-growing concession on the surcharge. | Repricing remains economically clearable while LPs keep the un-conceded value. |
| Stale oracle | Fail closed for swaps and quotes. | The hook does not price protection from a dead reference. |
| LP adds liquidity | Enforce oracle-relative width and centering guards. | Discourage fragile or deliberately off-market positions. |

## What Exists Today

- A live Base Sepolia USDC/WETH deployment using Chainlink USDC/USD and ETH/USD feeds.
- A standard Uniswap v4 integration: ordinary swaps clear auctions, dynamic fees accrue through PoolManager accounting, and `V4Quoter` can quote the pool without a custom router.
- A permissionless solver loop with measured fill gas and explicit break-even analysis.
- Solidity unit, fuzz, invariant, property, quoter, permissionless-fill, and fork coverage, plus Python-to-Solidity parity checks.
- A reproducible evidence release covering the exact fee law, trigger selectivity, measured market regimes, solver economics, and oracle-lag limits.
- A [live instrument](https://fairflow-v4.vercel.app) that exposes the pool/oracle gap, fee previews, auction state, wallet operations, and raw EVM calls.

## Evidence Without The Marketing Blur

- Exact replay validates the toxic-flow fee identity across `7,019` swaps with a maximum residual of `1.0e-64`.
- In the frozen observed-flow comparison, the selective policy improves LP net versus the broad all-stale policy in `19 / 54` windows, ties in `35 / 54`, and worsens none.
- The October 2025 grid has `95` measured normal windows and `29` measured stress windows at the documented 100% annualized-volatility threshold.
- The headline `~78%` is estimated recapture of true CEX-measured LVR with mainnet-granularity Chainlink data. It is not a guaranteed return, and the larger modeled October uplift figures are recovery ceilings from a volatile study month.

The frozen source for public claims is [reports/evidence_release.md](../reports/evidence_release.md); limitations are catalogued in [methodology_limitations.md](methodology_limitations.md).

## Ninety-Second Demo

1. Show the live pool/oracle gap and the two directional fee previews.
2. Point out that the benign direction stays at the base fee while the stale-price-taking direction is charged.
3. Open or inspect the permissionless auction and its time-growing concession.
4. Clear it with a normal v4 swap capped at the oracle price.
5. Show the resulting LP-fee split and raw PoolManager/hook events in the EVM bus.
6. Close on the integration property: no new settlement system, solver registry, or LP claim flow was introduced.

## Naming And Compatibility

FairFlow is the product and grant-facing name. `OracleAnchoredLVRHook` remains the Solidity contract name and ABI, and the current repository URL and Base Sepolia addresses remain canonical. This avoids a cosmetic redeployment and keeps every existing test, artifact, explorer link, integration, and research citation verifiable.

The deployment is a research-grade testnet system, not yet approved for real capital. [security_readiness.md](security_readiness.md) is the production gate.
