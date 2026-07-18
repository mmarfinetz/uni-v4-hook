// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import { Script } from "forge-std/Script.sol";
import { console2 } from "forge-std/console2.sol";

import { Currency } from "v4-core/types/Currency.sol";
import { FullMath } from "v4-core/libraries/FullMath.sol";
import { IHooks } from "v4-core/interfaces/IHooks.sol";
import { IPoolManager } from "v4-core/interfaces/IPoolManager.sol";
import { LPFeeLibrary } from "v4-core/libraries/LPFeeLibrary.sol";
import { PoolId, PoolIdLibrary } from "v4-core/types/PoolId.sol";
import { PoolKey } from "v4-core/types/PoolKey.sol";
import { ModifyLiquidityParams } from "v4-core/types/PoolOperation.sol";
import { PoolModifyLiquidityTest } from "v4-core/test/PoolModifyLiquidityTest.sol";
import { PoolSwapTest } from "v4-core/test/PoolSwapTest.sol";
import { MockERC20 } from "solmate/src/test/utils/mocks/MockERC20.sol";

import { OracleAnchoredLVRHook } from "src/OracleAnchoredLVRHook.sol";
import { ChainlinkReferenceOracle } from "src/oracles/ChainlinkReferenceOracle.sol";
import { IChainlinkAggregatorV3 } from "src/interfaces/IChainlinkAggregatorV3.sol";
import { ManualAggregatorV3 } from "test/helpers/ManualAggregatorV3.sol";

/// @notice Deploys a fully controllable demo pool for the solver-bot loop: two
/// mintable mock tokens, two manually settable price feeds, swap/liquidity test
/// routers, and a seeded 1:1 pool on the hook. Because the feeds are manual, a
/// stale gap of any size can be created on demand (script/solver_bot.py make-gap),
/// which is impossible against slow-moving live testnet feeds.
///
/// The broadcast sender must be the hook owner. ManualAggregatorV3 setters are
/// permissionless, so this setup is strictly demo-grade.
///
/// Usage (see docs/deployment.md):
///   HOOK=0x... forge script script/DeployDemoPool.s.sol:DeployDemoPool \
///     --rpc-url base_sepolia --private-key $DEPLOYER_KEY --broadcast
contract DeployDemoPool is Script {
    using PoolIdLibrary for PoolKey;

    uint256 internal constant WAD = 1e18;
    uint160 internal constant SQRT_PRICE_1_1 = 79_228_162_514_264_337_593_543_950_336; // 2^96

    int24 internal constant TICK_SPACING = 60;
    int24 internal constant LIQUIDITY_HALF_WIDTH_TICKS = 12_000;
    int256 internal constant SEED_LIQUIDITY = 1000e18;
    uint256 internal constant MINT_AMOUNT = 1e24;

    function run() external {
        OracleAnchoredLVRHook hook = OracleAnchoredLVRHook(vm.envAddress("HOOK"));
        IPoolManager poolManager = hook.poolManager();

        vm.startBroadcast();

        MockERC20 tokenA = new MockERC20("LVR Demo Token A", "LVRA", 18);
        MockERC20 tokenB = new MockERC20("LVR Demo Token B", "LVRB", 18);
        (MockERC20 token0, MockERC20 token1) =
            address(tokenA) < address(tokenB) ? (tokenA, tokenB) : (tokenB, tokenA);

        ManualAggregatorV3 baseFeed = new ManualAggregatorV3(18, int256(WAD), block.timestamp);
        ManualAggregatorV3 quoteFeed = new ManualAggregatorV3(18, int256(WAD), block.timestamp);
        // Demo pool uses manual feeds, so the sequencer uptime check stays disabled.
        ChainlinkReferenceOracle oracle = new ChainlinkReferenceOracle(
            baseFeed, false, quoteFeed, false, 18, 18, IChainlinkAggregatorV3(address(0)), 0
        );

        PoolSwapTest swapRouter = new PoolSwapTest(poolManager);
        PoolModifyLiquidityTest liquidityRouter = new PoolModifyLiquidityTest(poolManager);

        PoolKey memory key = PoolKey({
            currency0: Currency.wrap(address(token0)),
            currency1: Currency.wrap(address(token1)),
            fee: LPFeeLibrary.DYNAMIC_FEE_FLAG,
            tickSpacing: TICK_SPACING,
            hooks: IHooks(address(hook))
        });
        poolManager.initialize(key, SQRT_PRICE_1_1);
        hook.setConfig(key, _demoConfig(oracle));

        token0.mint(msg.sender, MINT_AMOUNT);
        token1.mint(msg.sender, MINT_AMOUNT);
        token0.approve(address(swapRouter), type(uint256).max);
        token1.approve(address(swapRouter), type(uint256).max);
        token0.approve(address(liquidityRouter), type(uint256).max);
        token1.approve(address(liquidityRouter), type(uint256).max);

        liquidityRouter.modifyLiquidity(
            key,
            ModifyLiquidityParams({
                tickLower: -LIQUIDITY_HALF_WIDTH_TICKS,
                tickUpper: LIQUIDITY_HALF_WIDTH_TICKS,
                liquidityDelta: SEED_LIQUIDITY,
                salt: bytes32(0)
            }),
            ""
        );

        vm.stopBroadcast();

        console2.log("demo token0:      ", address(token0));
        console2.log("demo token1:      ", address(token1));
        console2.log("base feed:        ", address(baseFeed));
        console2.log("quote feed:       ", address(quoteFeed));
        console2.log("oracle:           ", address(oracle));
        console2.log("swap router:      ", address(swapRouter));
        console2.log("liquidity router: ", address(liquidityRouter));
        console2.log("pool id:");
        console2.logBytes32(PoolId.unwrap(key.toId()));
    }

    /// @dev Recommended auction cell over a demo-friendly base config. The manual
    /// feeds only advance when poked, so the max oracle age is generous; the demo
    /// keeper refreshes the feeds (solver_bot.py refresh-oracle / --keep-fresh).
    function _demoConfig(ChainlinkReferenceOracle oracle)
        internal
        view
        returns (OracleAnchoredLVRHook.Config memory cfg)
    {
        cfg = OracleAnchoredLVRHook.Config({
            oracle: oracle,
            baseFee: uint24(vm.envOr("BASE_FEE", uint256(500))),
            maxFee: uint24(vm.envOr("MAX_FEE", uint256(250_000))),
            alphaBps: uint24(vm.envOr("ALPHA_BPS", uint256(10_000))),
            maxOracleAge: uint32(vm.envOr("MAX_ORACLE_AGE", uint256(24 hours))),
            latencySecs: uint32(vm.envOr("LATENCY_SECS", uint256(60))),
            centerTolTicks: uint32(vm.envOr("CENTER_TOL_TICKS", uint256(60))),
            lvrBudgetWad: vm.envOr("LVR_BUDGET_WAD", uint256(1e16)),
            bootstrapSigma2PerSecondWad: vm.envOr("BOOTSTRAP_SIGMA2_PER_SECOND_WAD", uint256(3e10)),
            triggerGapBps: uint24(vm.envOr("TRIGGER_GAP_BPS", uint256(10))),
            startConcessionWad: vm.envOr("START_CONCESSION_WAD", uint256(1e15)),
            concessionGrowthWadPerSec: vm.envOr("CONCESSION_GROWTH_WAD_PER_SEC", uint256(5e13)),
            maxConcessionWad: vm.envOr("MAX_CONCESSION_WAD", uint256(1e18))
        });
    }
}
