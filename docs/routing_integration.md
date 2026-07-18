# Routing Integration: Getting Benign Flow to the Pool

The LP-uplift results assume the hooked pool receives benign flow paying the base
fee — that is what out-earns the static-fee baseline between stale-price events.
Benign flow arrives through routers: the Uniswap interface (whose routing API
allowlists hook addresses per chain), third-party aggregators (which route through
hooked pools their solvers can simulate safely), and UniswapX fillers. A pool no
router quotes receives only toxic flow and solver fills, and the yield story
collapses. Routing integration is therefore the product-market-fit gate
identified in [`market_validation_2026_07.md`](market_validation_2026_07.md).

This page records what the contract already provides (with tests), the quote
semantics integrators need, and the distribution playbook.

## What the contract provides

**Standard revert-based quoting works, unmodified.**
[`test/OracleAnchoredLVRHookQuoter.t.sol`](../test/OracleAnchoredLVRHookQuoter.t.sol)
deploys Uniswap's own `V4Quoter` (the periphery lens contract routers and
aggregator simulators use) against the hooked pool and proves:

- a benign-direction quote equals the executed swap output exactly;
- a **toxic-direction quote with an open Dutch auction and an aged concession**
  equals the executed output exactly — the dynamic fee and the whole auction
  schedule are captured by plain simulation, because the fee is a deterministic
  function of chain state (oracle round, pool price, auction clock);
- a stale oracle makes the quote revert cleanly rather than misquote, so a
  router's simulator simply excludes the route.

No off-chain quoting service, signed quotes, custom SDK, or hook-specific
calldata are involved (`hookData` is empty). This is the structural difference
from RFQ-style protection designs: anyone who can simulate a v4 swap can price
this pool.

**Indexer-facing health check.** `quotable(key)` returns `true` when the pool is
configured and its oracle is fresh and in-range, and `false` — never a revert —
otherwise (including sequencer-down on L2s, which surfaces as an oracle revert
the view catches). Integrators can filter pools before spending a failed
simulation. Per-direction fee levels, including the toxic-direction fail-closed
above `maxFee`, come from `previewSwapFee(key, zeroForOne)`; solvers watch
`auctionStatus(key)`.

**Failure semantics are aligned with quoting.** The swap path and the quote path
run the same `beforeSwap` code, so there is no state in which a quote succeeds
but the swap fails closed (or vice versa) at the same block. Fail-closed is a
router feature, not a hazard: broken-oracle conditions price as "no route".

**Cost.** A hooked swap is ~231k gas including two Chainlink reads (measured;
see [`solver_economics_gas_aware.md`](../reports/solver_economics_gas_aware.md)).

## Quote drift between quote and execution

The fee can change between quote and execution only when a Chainlink round lands
in between (or the auction clock ticks the concession). Properties integrators
can rely on:

- **Benign-direction fee is pinned at `baseFee`** regardless of gap size; it
  changes only if the pool crosses the oracle price and the trade's direction
  becomes toxic. Retail-flow quotes are therefore as stable as any static pool's.
- **Toxic-direction fees move with the oracle**, which is the mechanism working
  as designed; arbitrage flow re-simulates at execution anyway.
- Feed updates are bounded in frequency by the deviation threshold and heartbeat
  ([`oracle_granularity.md`](oracle_granularity.md)), so drift events are rare at
  quote-to-execution timescales; normal slippage tolerance absorbs them.

## Distribution playbook

In order of leverage:

1. **Uniswap routing API allowlist (per chain).** The gate is literal and
   inspectable: the routing API only quotes hooked pools whose (lowercase) hook
   address appears in `HOOKS_ADDRESSES_ALLOWLIST[chainId]` in
   [`lib/util/hooksAddressesAllowlist.ts`](https://github.com/Uniswap/routing-api/blob/main/lib/util/hooksAddressesAllowlist.ts)
   of `Uniswap/routing-api` (mirrored in `smart-order-router`). Every entry is a
   named constant with an example-pool link — Flaunch, Clanker, Aegis, Renzo,
   Zora and others are on it, and **Aegis (an oracle-informed dynamic-fee hook)
   is precedent that this hook category passes review**. Two ways in, per
   [Uniswap Labs support](https://support.uniswap.org/hc/en-us/articles/33829289869965-How-do-custom-hooks-work-in-the-Labs-interface):
   the intake form, or a self-support PR against that file following the
   "Self-Support Allowlisting and TVL-Setting for Hooks" directions — either way
   subject to Labs' formal review. Practical prerequisites, judged from the
   incumbents: verified source on the target chain, a live example pool with
   real liquidity, an audit, and router-grade simulatability (which the quoter
   test above proves with Labs' own lens contract).
2. **Registry now, allowlist later.** [`Uniswap/hooklist`](https://github.com/Uniswap/hooklist)
   is the public hook registry (issue-template submission: chain + address;
   automated source analysis; maintainer-merged). Inclusion does not grant
   routing but creates the public record reviewers and aggregators look up.
   Zero cost — do this at mainnet deployment.
3. **Aggregators (1inch, CoW solvers, Paraswap, Matcha, OKX).** Their v4
   integrations route through hooked pools their simulators handle. The ask is
   inclusion in the simulation set; the evidence is that the official V4Quoter
   already prices the pool exactly and `quotable()` provides cheap filtering.
   The Foundation has also run **router gas rebates** (up to 80% of swap gas)
   for flow routed through hooked pools during incentive campaigns — a reason
   for aggregators to say yes.
4. **UniswapX fillers.** Profit-motivated and permissionless; `auctionStatus`
   plus the public fee preview makes hooked-pool routes discoverable without a
   business-development conversation.

Note the LP-side loophole that needs no allowlisting at all: anyone can LP a
custom-hook pool from the Labs interface by entering the hook address, and the
liquidity flow can be deep-linked with the hook pre-filled:
`https://app.uniswap.org/positions/create/v4?hook={hookAddress}`.

## Bootstrapping liquidity

The routing allowlist wants a live pool with liquidity; liquidity wants routed
flow. The realistic break-in sequence, cheapest first:

1. **Seed pool + live evidence loop (self-funded, small).** A modest ETH/USDC
   pool on Base (`DeployPool` with the recommended cell), the solver bot, and a
   side-by-side hookless baseline pool reproduce the testnet demo with real
   flow — the LP-vs-baseline comparison is the marketing artifact, and the pool
   doubles as the allowlist application's example pool.
2. **Merkl micro-campaign.** [Merkl supports permissionless incentive campaigns
   on any v4 pool](https://blog.merkl.xyz/merkl-brings-incentives-to-uniswap-v4-pools)
   (minutes to launch, position-size × duration rewards). A few thousand
   dollars of rewards is enough to pull demo-scale TVL while the organic story
   builds.
3. **Foundation rails.** The DAO/Gauntlet incentive programs explicitly
   featured hooked pools, and hooked variants captured
   [40–96% market share of their incentivized pairs](https://www.gauntlet.xyz/resources/gauntlet-generated-33b-in-volume-for-unichain)
   (Bunni's incentivized USDC/USDT earned 4.14% vs 1.66% on the vanilla pool) —
   the incentive machinery demonstrably moves LPs into hooks. The Hook Design
   Lab (milestone funding + go-to-market) and the Security Fund (audit) are the
   application channels; the 2026 hooks-marketplace incentive wave is the
   larger successor program.
4. **Anchor partner protocol-owned liquidity.** One token issuer or DAO
   treasury that chooses the hook at pool creation brings sticky TVL and a
   public case study; the pitch artifacts are
   [`lp_apr_uplift.md`](../reports/lp_apr_uplift.md) and the
   [traceability map](design_traceability.md). The screening criteria that
   matter: Chainlink feed coverage for both assets on the target chain, real
   relative-price volatility (same-peg pairs have nothing to recapture), and
   an active liquidity operation to pitch. (The ranked candidate list is
   maintained internally.)

Calibration: Angstrom — audited, Paradigm-funded, OKX-routed — holds ~$3.2M
TVL after a year on mainnet. Low-hundreds-of-thousands to low-millions of TVL
is the honest first-year target, and the width/centering guard means passive
LPs must post wide, centered ranges — a future vault wrapper is the UX answer
if organic LP onboarding matters.
