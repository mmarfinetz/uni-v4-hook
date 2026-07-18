# Testnet Deployment

This is the deployment path for the research hook on Uniswap v4 testnets. It covers
two forge scripts:

1. [`script/DeployHook.s.sol`](../script/DeployHook.s.sol): mines a CREATE2 salt so the
   hook address encodes the `BEFORE_ADD_LIQUIDITY | BEFORE_SWAP` permission bits
   (required by `Hooks.isValidHookAddress`), then deploys `OracleAnchoredLVRHook`
   through the deterministic CREATE2 deployer proxy.
2. [`script/DeployPool.s.sol`](../script/DeployPool.s.sol): deploys a
   `ChainlinkReferenceOracle` for a token pair, initializes a dynamic-fee pool on the
   hook exactly at the oracle reference price (zero stale gap), and writes the hook
   config. Defaults are the recommended cell from the Dutch-auction study: 5 bps base
   fee, 2500 bps max fee, 10 bps trigger gap, 10 bps starting concession, 0.5 bps/sec
   concession growth.

Base Sepolia is the primary target because both Uniswap v4 and Chainlink data feeds
are live there. Unichain Sepolia has a default PoolManager wired in, but Chainlink
feed coverage there has not been verified; pass feed addresses explicitly if you use it.

## Current deployment (Base Sepolia)

Deployed 2026-07-10 with the default config (recommended Dutch-auction cell,
`maxConcessionWad` ceiling at WAD):

| Contract | Address |
| --- | --- |
| OracleAnchoredLVRHook | `0x22081E668dC0f43B6166561Ac4A6Df359AA88880` |
| ChainlinkReferenceOracle (USDC/USD base, ETH/USD quote) | `0xA68812AA66A2417BDFAFF9a45BD9A7578C5A3202` |
| Hook owner (testnet deployer) | `0x0D433E34d812F443398Aa6e9C5B723779E4D66C1` |

USDC/WETH pool (tick spacing 60, dynamic fee):
`poolId 0x6a269352e17a2c717d4fbc96b74f5c19a26b28688c90ee545fc97ad7fd287ff7`,
initialized at tick 201414 from the live oracle price. Verified post-deploy: the
on-chain config carries the recommended auction cell including the new
`maxConcessionWad` ceiling, and both swap directions preview the 5 bps base fee
at zero gap. An earlier instance without `maxConcessionWad`
(`0x6Ac5834889Ee82A7f127271E52c41d84345f4880`) is deprecated. The deployer key
lives in the local gitignored `.env`; the hook has no ownership transfer, so
redeploy rather than reuse if that key is lost.

## Verified addresses

All addresses below were verified on-chain (bytecode / `description()` /
`decimals()` / `symbol()`) on 2026-07-10.

### Base Sepolia (chain id 84532)

| Contract | Address |
| --- | --- |
| Uniswap v4 PoolManager | `0x05E73354cFDd6745C338b50BcFDfA3Aa6fA03408` |
| WETH | `0x4200000000000000000000000000000000000006` |
| USDC (Circle testnet, 6 decimals) | `0x036CbD53842c5426634e7929541eC2318f3dCF7e` |
| Chainlink ETH/USD (8 decimals) | `0x4aDC67696bA383F43DD60A9e78F2C97Fbbfc7cb1` |
| Chainlink USDC/USD (8 decimals) | `0xd30e2101a97dcbAeBCBC04F14C3f624E67A35165` |

### Unichain Sepolia (chain id 1301)

| Contract | Address |
| --- | --- |
| Uniswap v4 PoolManager | `0x00B036B58a818B1BC34d502D3fE730Db729e62AC` |

The CREATE2 deployer proxy `0x4e59b44847b379578588920cA78FbF26c0B4956C` (which forge
uses for salted deployments) is present on both chains.

## Oracle wiring convention

`ChainlinkReferenceOracle.latestPriceWad()` returns the pool's raw-unit
`amount1/amount0` price in WAD, which is what the hook converts to `sqrtPriceX96`.

- `baseFeed` is **token0**'s asset/USD feed.
- `quoteFeed` is **token1**'s asset/USD feed (zero address if the base feed alone
  quotes the pair).
- The oracle rescales the whole-token ratio by `10^(token1Decimals - token0Decimals)`.

Example: for the Base Sepolia USDC/WETH pool (USDC is token0 because its address
sorts lower), `BASE_FEED` is USDC/USD and `QUOTE_FEED` is ETH/USD. At ETH = $1790 this
yields `priceWad ≈ 5.58e26` (raw price `5.58e8`, tick ≈ 201400), matching real
USDC/WETH pool levels.

## Deploying

Prerequisites: a funded deployer key on the target chain
([Base Sepolia faucet](https://faucets.chain.link/base-sepolia)) and the RPC env vars
referenced by [`foundry.toml`](../foundry.toml):

```bash
export BASE_SEPOLIA_RPC_URL=https://sepolia.base.org
export DEPLOYER_KEY=0x...   # never commit this
```

### 1. Deploy the hook

```bash
forge script script/DeployHook.s.sol:DeployHook \
  --rpc-url base_sepolia --private-key "$DEPLOYER_KEY" --broadcast
```

Environment (all optional):

| Variable | Default | Meaning |
| --- | --- | --- |
| `POOL_MANAGER` | per-chain default above | v4 PoolManager address |
| `HOOK_OWNER` | broadcast sender | Owner of `setConfig`/`setRiskState`. **The hook has no ownership transfer**, so this address is permanent. |

The script logs the mined hook address; export it for step 2.

### 2. Deploy the oracle, pool, and config

The broadcast sender must be the hook owner. `TOKEN0 < TOKEN1` must already be sorted.

```bash
HOOK=0x... \
TOKEN0=0x036CbD53842c5426634e7929541eC2318f3dCF7e \
TOKEN1=0x4200000000000000000000000000000000000006 \
BASE_FEED=0xd30e2101a97dcbAeBCBC04F14C3f624E67A35165 \
QUOTE_FEED=0x4aDC67696bA383F43DD60A9e78F2C97Fbbfc7cb1 \
forge script script/DeployPool.s.sol:DeployPool \
  --rpc-url base_sepolia --private-key "$DEPLOYER_KEY" --broadcast
```

Environment overrides (defaults in parentheses): `SEQUENCER_UPTIME_FEED` (unset =
disabled; on L2s set it to the chain's [Chainlink sequencer uptime feed](https://docs.chain.link/data-feeds/l2-sequencer-feeds)
so oracle reads fail closed during outages), `SEQUENCER_GRACE_PERIOD` (3600 s after
sequencer recovery before reads resume), `INVERT_BASE` / `INVERT_QUOTE`
(false), `TICK_SPACING` (60), `BASE_FEE` (500 ppm = 5 bps), `MAX_FEE` (250000 ppm =
2500 bps), `ALPHA_BPS` (10000), `MAX_ORACLE_AGE` (86400 s), `LATENCY_SECS` (60),
`CENTER_TOL_TICKS` (60), `LVR_BUDGET_WAD` (1e16), `BOOTSTRAP_SIGMA2_PER_SECOND_WAD`
(3e10, about 5% daily volatility), `TRIGGER_GAP_BPS` (10), `START_CONCESSION_WAD`
(1e15), `CONCESSION_GROWTH_WAD_PER_SEC` (5e13), `MAX_CONCESSION_WAD` (1e18; the
ceiling on the auction discount — at 1e18 fees can decay to the base fee, which
guarantees capped-out gaps become repriceable, while a lower ceiling guarantees LPs
a floor share of the surcharge but can leave extreme gaps deterred above `MAX_FEE`).

The 24-hour `MAX_ORACLE_AGE` default accommodates slow testnet feed heartbeats
(the Base Sepolia USDC/USD feed can go hours between updates). Tighten it for any
production-like configuration, and see the L2 note below.

### 3. Verify

```bash
KEY="($TOKEN0,$TOKEN1,8388608,60,$HOOK)"   # fee 8388608 = DYNAMIC_FEE_FLAG
cast call $HOOK "previewSwapFee((address,address,uint24,int24,address),bool)(bool,uint24,uint160,uint160)" "$KEY" true  --rpc-url base_sepolia
cast call $HOOK "auctionStatus((address,address,uint24,int24,address))(bool,uint64,uint256,uint256)" "$KEY" --rpc-url base_sepolia
cast call $HOOK "minWidthTicks((address,address,uint24,int24,address))(uint256)" "$KEY" --rpc-url base_sepolia
```

Immediately after `DeployPool`, both swap directions should preview the base fee
(pool sits exactly at the reference price) and the auction should be closed.

## Local rehearsal (no testnet ETH needed)

The full path can be exercised against an Anvil fork of Base Sepolia with Anvil's
prefunded key:

```bash
anvil --fork-url https://sepolia.base.org --port 8545
forge script script/DeployHook.s.sol:DeployHook \
  --rpc-url http://127.0.0.1:8545 \
  --private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 \
  --broadcast
# then DeployPool as in step 2 against the same RPC
```

This uses the real PoolManager bytecode, the real CREATE2 proxy, and live Chainlink
answers at the forked block.

## Known deployment caveats

- **Owner permanence**: `OracleAnchoredLVRHook` has no ownership transfer. Choose
  `HOOK_OWNER` deliberately; a lost owner key permanently freezes config updates.
- **CREATE2 determinism**: the mined salt depends on the constructor args (pool
  manager, owner) and the compiled bytecode. Re-running with identical args after a
  successful deployment finds the code at the mined address and moves to the next
  salt, producing a second hook instance.
- **L2 sequencer uptime**: `ChainlinkReferenceOracle` accepts an optional sequencer
  uptime feed (`SEQUENCER_UPTIME_FEED` / `SEQUENCER_GRACE_PERIOD` in `DeployPool`).
  When wired, `latestPriceWad` reverts while the sequencer is down and for the grace
  period after recovery, so swaps and previews fail closed. Set it on every
  production L2 deployment; feed addresses are listed in the
  [Chainlink L2 sequencer feeds docs](https://docs.chain.link/data-feeds/l2-sequencer-feeds).
  Note the testnet caveat: Base Sepolia has no official sequencer uptime feed, so
  testnet deployments run with the check disabled.
- **Reference-feed granularity**: the trigger measures the gap to the last posted
  Chainlink round, and dislocations below the feed's deviation threshold are
  invisible by construction; record each feed's deviation threshold and heartbeat
  alongside the addresses above. See
  [`oracle_granularity.md`](oracle_granularity.md) for why the backtest already
  embeds this granularity and when a re-run is required.
- **Solver economics**: per [`reports/solver_economics_table.md`](../reports/solver_economics_table.md),
  auction fills average $1.74 before gas, so low-cost L2s are the only viable venue;
  this is why the scripts target Base/Unichain rather than Ethereum Sepolia.
