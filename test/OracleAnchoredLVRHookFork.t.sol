// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { IERC20Metadata } from "@openzeppelin/contracts/token/ERC20/extensions/IERC20Metadata.sol";
import { Test } from "forge-std/Test.sol";
import { OracleAnchoredLVRHook } from "src/OracleAnchoredLVRHook.sol";
import { IChainlinkAggregatorV3 } from "src/interfaces/IChainlinkAggregatorV3.sol";
import { ChainlinkReferenceOracle } from "src/oracles/ChainlinkReferenceOracle.sol";
import { Deployers } from "../lib/v4-core/test/utils/Deployers.sol";
import { IHooks } from "v4-core/interfaces/IHooks.sol";
import { IPoolManager } from "v4-core/interfaces/IPoolManager.sol";
import { ModifyLiquidityParams } from "v4-core/types/PoolOperation.sol";
import { Hooks } from "v4-core/libraries/Hooks.sol";
import { LPFeeLibrary } from "v4-core/libraries/LPFeeLibrary.sol";
import { CustomRevert } from "v4-core/libraries/CustomRevert.sol";
import { TickMath } from "v4-core/libraries/TickMath.sol";
import { FullMath } from "v4-core/libraries/FullMath.sol";
import { PoolKey } from "v4-core/types/PoolKey.sol";
import { PoolIdLibrary } from "v4-core/types/PoolId.sol";
import { Currency } from "v4-core/types/Currency.sol";
import { PoolModifyLiquidityTest } from "v4-core/test/PoolModifyLiquidityTest.sol";
import { PoolSwapTest } from "v4-core/test/PoolSwapTest.sol";
import { FixedPointMathLib } from "solmate/src/utils/FixedPointMathLib.sol";

abstract contract MainnetForkBase is Test {
    uint256 internal constant MAINNET_FORK_BLOCK = 23_450_000;
    uint256 internal constant MAINNET_FORK_SAME_ORACLE_BLOCK = 23_450_100;
    uint256 internal constant MAINNET_FORK_FRESH_ORACLE_BLOCK = 23_450_500;

    address internal constant MAINNET_V4_POOL_MANAGER = 0x000000000004444c5dc75cB358380D2e3dE08A90;
    address internal constant WETH = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2;
    address internal constant USDC = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48;
    address internal constant LINK = 0x514910771AF9Ca656af840dff83E8264EcF986CA;

    address internal constant ETH_USD_FEED = 0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419;
    address internal constant USDC_USD_FEED = 0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6;
    address internal constant LINK_USD_FEED = 0x2c1d072e956AFFC0D435Cb7AC38EF18d24d9127c;

    uint256 internal constant ETH_USD_HEARTBEAT_WINDOW = 65 minutes;
    uint256 internal constant USDC_USD_HEARTBEAT_WINDOW = 25 hours;
    uint256 internal constant LINK_USD_MAX_AGE = 25 hours;
    uint256 internal constant WAD = 1e18;

    bool internal forkReady;

    function _createMainnetForkIfConfigured() internal returns (bool) {
        if (bytes(vm.envOr("MAINNET_RPC_URL", string(""))).length == 0) {
            return false;
        }

        vm.createSelectFork("mainnet", MAINNET_FORK_BLOCK);
        forkReady = true;
        return true;
    }
}

contract ChainlinkReferenceOracleForkTest is MainnetForkBase {
    function setUp() public {
        _createMainnetForkIfConfigured();
        vm.skip(!forkReady, "MAINNET_RPC_URL not set; skipping mainnet fork tests");
    }

    function testFork_latestPriceWad_usesLiveFeedsAndRealHeartbeats() public {
        assertEq(block.number, MAINNET_FORK_BLOCK);
        assertEq(IERC20Metadata(WETH).decimals(), 18);
        assertEq(IERC20Metadata(USDC).decimals(), 6);

        IChainlinkAggregatorV3 baseFeed = IChainlinkAggregatorV3(ETH_USD_FEED);
        IChainlinkAggregatorV3 quoteFeed = IChainlinkAggregatorV3(USDC_USD_FEED);
        ChainlinkReferenceOracle oracle =
            new ChainlinkReferenceOracle(baseFeed, false, quoteFeed, false, 6, 18);

        assertEq(baseFeed.decimals(), 8);
        assertEq(quoteFeed.decimals(), 8);

        (
            uint80 baseRoundId,
            int256 baseAnswer,,
            uint256 baseUpdatedAt,
            uint80 baseAnsweredInRound
        ) = baseFeed.latestRoundData();
        (
            uint80 quoteRoundId,
            int256 quoteAnswer,,
            uint256 quoteUpdatedAt,
            uint80 quoteAnsweredInRound
        ) = quoteFeed.latestRoundData();

        assertGt(baseAnswer, 0);
        assertGt(quoteAnswer, 0);
        assertGe(baseAnsweredInRound, baseRoundId);
        assertGe(quoteAnsweredInRound, quoteRoundId);
        assertLe(block.timestamp - baseUpdatedAt, ETH_USD_HEARTBEAT_WINDOW);
        assertLe(block.timestamp - quoteUpdatedAt, USDC_USD_HEARTBEAT_WINDOW);

        (uint256 priceWad, uint256 updatedAt, uint256 latestFeedTs) = oracle.latestPriceWad();

        uint256 expectedUpdatedAt = baseUpdatedAt < quoteUpdatedAt ? baseUpdatedAt : quoteUpdatedAt;
        uint256 expectedLatestFeedTs = baseUpdatedAt > quoteUpdatedAt ? baseUpdatedAt : quoteUpdatedAt;
        uint256 expectedPriceWad =
            _expectedPriceWad(baseFeed, quoteFeed, baseAnswer, quoteAnswer, 6, 18);

        assertEq(updatedAt, expectedUpdatedAt);
        assertEq(latestFeedTs, expectedLatestFeedTs);
        assertEq(priceWad, expectedPriceWad);
        assertGt(priceWad, 500e6);
    }

    function testFork_rollForwardAcrossBlocks_onlyAdvancesOracleWhenFeedsRefresh() public {
        ChainlinkReferenceOracle oracle = new ChainlinkReferenceOracle(
            IChainlinkAggregatorV3(ETH_USD_FEED),
            false,
            IChainlinkAggregatorV3(LINK_USD_FEED),
            false,
            18,
            18
        );

        (uint256 initialPriceWad, uint256 initialUpdatedAt, uint256 initialLatestFeedTs) =
            oracle.latestPriceWad();

        vm.rollFork(MAINNET_FORK_SAME_ORACLE_BLOCK);
        (uint256 sameBlockPriceWad, uint256 sameBlockUpdatedAt, uint256 sameBlockLatestFeedTs) =
            oracle.latestPriceWad();

        assertEq(block.number, MAINNET_FORK_SAME_ORACLE_BLOCK);
        assertEq(sameBlockPriceWad, initialPriceWad);
        assertEq(sameBlockUpdatedAt, initialUpdatedAt);
        assertEq(sameBlockLatestFeedTs, initialLatestFeedTs);

        vm.rollFork(MAINNET_FORK_FRESH_ORACLE_BLOCK);
        (uint256 freshBlockPriceWad, uint256 freshBlockUpdatedAt, uint256 freshBlockLatestFeedTs) =
            oracle.latestPriceWad();

        assertEq(block.number, MAINNET_FORK_FRESH_ORACLE_BLOCK);
        assertGe(freshBlockUpdatedAt, sameBlockUpdatedAt);
        assertGt(freshBlockLatestFeedTs, sameBlockLatestFeedTs);
        assertNotEq(freshBlockPriceWad, sameBlockPriceWad);
    }

    function _expectedPriceWad(
        IChainlinkAggregatorV3 baseFeed,
        IChainlinkAggregatorV3 quoteFeed,
        int256 baseAnswer,
        int256 quoteAnswer,
        uint8 token0Decimals,
        uint8 token1Decimals
    ) internal view returns (uint256) {
        uint256 basePriceWad = FullMath.mulDiv(uint256(baseAnswer), WAD, 10 ** baseFeed.decimals());
        uint256 quotePriceWad =
            FullMath.mulDiv(uint256(quoteAnswer), WAD, 10 ** quoteFeed.decimals());
        uint256 priceWad = FullMath.mulDiv(basePriceWad, WAD, quotePriceWad);
        if (token0Decimals >= token1Decimals) {
            return FullMath.mulDiv(priceWad, 10 ** (token0Decimals - token1Decimals), 1);
        }
        return FullMath.mulDiv(priceWad, 1, 10 ** (token1Decimals - token0Decimals));
    }
}

contract OracleAnchoredLVRHookForkTest is MainnetForkBase, Deployers {
    using PoolIdLibrary for PoolKey;

    uint24 internal constant BASE_FEE = 500;
    uint24 internal constant MAX_FEE = 50_000;
    uint24 internal constant ALPHA_BPS = 10_000;
    uint32 internal constant MAX_ORACLE_AGE = uint32(LINK_USD_MAX_AGE);
    uint32 internal constant CENTER_TOLERANCE_TICKS = 60;
    uint32 internal constant LATENCY_SECS = 60;
    uint256 internal constant LVR_BUDGET_WAD = 1e16;
    uint256 internal constant SIGMA2_PER_SECOND_WAD = 4e14;
    uint256 internal constant BOOTSTRAP_SIGMA2_PER_SECOND_WAD = 8e14;
    int24 internal constant TICK_SPACING = 60;
    int24 internal constant HALF_WIDTH_TICKS = 18_000;

    OracleAnchoredLVRHook internal hook;
    ChainlinkReferenceOracle internal oracle;

    function setUp() public {
        _createMainnetForkIfConfigured();
        vm.skip(!forkReady, "MAINNET_RPC_URL not set; skipping mainnet fork tests");

        _configureForkRouters();
        _configureLiveCurrencies();
        _fundAndApproveLiveTokens();
        _deployLiveOracleAndHook();
        _initializeLivePool();
    }

    function testFork_addLiquidity_acceptsCenteredWidePositionAgainstLiveOracle() public {
        assertEq(block.number, MAINNET_FORK_BLOCK);
        assertEq(IERC20Metadata(LINK).decimals(), 18);
        assertEq(IERC20Metadata(WETH).decimals(), 18);
        assertLt(hook.minWidthTicks(key), uint256(uint24(HALF_WIDTH_TICKS * 2)));

        _addCenteredLiquidity(HALF_WIDTH_TICKS + 6000, bytes32("fork-extra"));
    }

    function testFork_previewSwapFee_usesBaseFeeWhenMainnetPoolStartsOracleAligned() public view {
        (bool toxicZeroForOne, uint24 zeroForOneFee,,) = hook.previewSwapFee(key, true);
        (bool toxicOneForZero, uint24 oneForZeroFee,,) = hook.previewSwapFee(key, false);

        assertFalse(toxicZeroForOne);
        assertFalse(toxicOneForZero);
        assertEq(zeroForOneFee, BASE_FEE);
        assertEq(oneForZeroFee, BASE_FEE);
    }

    function testFork_swapThroughLivePoolManagerUpdatesRiskState() public {
        (uint256 liveOraclePriceWad,, uint256 liveOracleLatestFeedTs) = oracle.latestPriceWad();

        swap(key, false, -1e15, ZERO_BYTES);

        (uint256 sigma2PerSecondWad, uint256 lastOraclePriceWad, uint256 lastOracleTs) =
            hook.risk(key.toId());

        assertEq(lastOraclePriceWad, liveOraclePriceWad);
        assertEq(lastOracleTs, liveOracleLatestFeedTs);
        assertGt(sigma2PerSecondWad, 0);
    }

    function testFork_swapRevertsWhenLiveOracleIsWarpedPastConfiguredMaxAge() public {
        vm.warp(block.timestamp + MAX_ORACLE_AGE + 1);

        vm.expectRevert(
            abi.encodeWithSelector(
                CustomRevert.WrappedError.selector,
                address(hook),
                IHooks.beforeSwap.selector,
                abi.encodeWithSelector(OracleAnchoredLVRHook.OracleStale.selector),
                abi.encodeWithSelector(Hooks.HookCallFailed.selector)
            )
        );
        swap(key, false, -1e15, ZERO_BYTES);
    }

    function testFork_rollForwardWithoutFreshOracleRound_keepsRiskObservationStable() public {
        swap(key, false, -1e15, ZERO_BYTES);

        (uint256 sigmaBefore, uint256 priceBefore, uint256 tsBefore) = hook.risk(key.toId());
        (uint256 oraclePriceBefore,, uint256 oracleTsBefore) = oracle.latestPriceWad();

        vm.rollFork(MAINNET_FORK_SAME_ORACLE_BLOCK);
        _fundAndApproveLiveTokens();

        (uint256 oraclePriceAfterRoll,, uint256 oracleTsAfterRoll) = oracle.latestPriceWad();
        assertEq(oraclePriceAfterRoll, oraclePriceBefore);
        assertEq(oracleTsAfterRoll, oracleTsBefore);

        swap(key, false, -1e15, ZERO_BYTES);

        (uint256 sigmaAfter, uint256 priceAfter, uint256 tsAfter) = hook.risk(key.toId());

        assertEq(block.number, MAINNET_FORK_SAME_ORACLE_BLOCK);
        assertEq(sigmaAfter, sigmaBefore);
        assertEq(priceAfter, priceBefore);
        assertEq(tsAfter, tsBefore);
    }

    function testFork_rollForwardToFreshOracleRound_updatesFeeViewAndRisk() public {
        swap(key, false, -1e15, ZERO_BYTES);

        (uint256 sigmaBefore,, uint256 tsBefore) = hook.risk(key.toId());

        vm.rollFork(MAINNET_FORK_FRESH_ORACLE_BLOCK);
        _fundAndApproveLiveTokens();

        (uint256 freshOraclePriceWad,, uint256 freshOracleTs) = oracle.latestPriceWad();
        (bool toxicZeroForOne, uint24 zeroForOneFee,,) = hook.previewSwapFee(key, true);
        (bool toxicOneForZero, uint24 oneForZeroFee,,) = hook.previewSwapFee(key, false);

        assertEq(block.number, MAINNET_FORK_FRESH_ORACLE_BLOCK);
        assertGt(freshOracleTs, tsBefore);
        assertTrue(toxicZeroForOne != toxicOneForZero);

        bool toxicDirection = toxicZeroForOne;
        uint24 toxicFee = toxicZeroForOne ? zeroForOneFee : oneForZeroFee;
        uint24 benignFee = toxicZeroForOne ? oneForZeroFee : zeroForOneFee;

        assertGt(toxicFee, BASE_FEE);
        assertEq(benignFee, BASE_FEE);

        swap(key, toxicDirection, -1e15, ZERO_BYTES);

        (uint256 sigmaAfter, uint256 priceAfter, uint256 tsAfter) = hook.risk(key.toId());

        assertEq(priceAfter, freshOraclePriceWad);
        assertEq(tsAfter, freshOracleTs);
        assertNotEq(sigmaAfter, sigmaBefore);
    }

    function _configureForkRouters() internal {
        manager = IPoolManager(MAINNET_V4_POOL_MANAGER);
        swapRouter = new PoolSwapTest(manager);
        modifyLiquidityRouter = new PoolModifyLiquidityTest(manager);
    }

    function _configureLiveCurrencies() internal {
        if (LINK < WETH) {
            currency0 = Currency.wrap(LINK);
            currency1 = Currency.wrap(WETH);
        } else {
            currency0 = Currency.wrap(WETH);
            currency1 = Currency.wrap(LINK);
        }
    }

    function _fundAndApproveLiveTokens() internal {
        deal(LINK, address(this), 1_000_000e18);
        deal(WETH, address(this), 5000e18);

        IERC20(LINK).approve(address(modifyLiquidityRouter), type(uint256).max);
        IERC20(LINK).approve(address(swapRouter), type(uint256).max);
        IERC20(WETH).approve(address(modifyLiquidityRouter), type(uint256).max);
        IERC20(WETH).approve(address(swapRouter), type(uint256).max);
    }

    function _deployLiveOracleAndHook() internal {
        oracle = new ChainlinkReferenceOracle(
            IChainlinkAggregatorV3(ETH_USD_FEED),
            false,
            IChainlinkAggregatorV3(LINK_USD_FEED),
            false,
            18,
            18
        );

        address hookAddress = _permissionedHookAddress();
        deployCodeTo(
            "src/OracleAnchoredLVRHook.sol:OracleAnchoredLVRHook",
            abi.encode(IPoolManager(manager), address(this)),
            hookAddress
        );

        hook = OracleAnchoredLVRHook(hookAddress);

        address[] memory persistentAccounts = new address[](5);
        persistentAccounts[0] = address(oracle);
        persistentAccounts[1] = address(hook);
        persistentAccounts[2] = address(swapRouter);
        persistentAccounts[3] = address(modifyLiquidityRouter);
        persistentAccounts[4] = address(manager);
        vm.makePersistent(persistentAccounts);
    }

    function _initializeLivePool() internal {
        (uint256 referencePriceWad,, uint256 latestFeedTs) = oracle.latestPriceWad();

        (key,) = initPool(
            currency0,
            currency1,
            IHooks(address(hook)),
            LPFeeLibrary.DYNAMIC_FEE_FLAG,
            TICK_SPACING,
            _priceWadToSqrtPriceX96(referencePriceWad)
        );

        hook.setConfig(key, _defaultConfig());
        hook.setRiskState(
            key,
            SIGMA2_PER_SECOND_WAD,
            FullMath.mulDiv(referencePriceWad, 995, 1000),
            latestFeedTs > 300 ? latestFeedTs - 300 : latestFeedTs - 1
        );

        _addCenteredLiquidity(HALF_WIDTH_TICKS, bytes32("fork-seed"));
    }

    function _addCenteredLiquidity(int24 halfWidthTicks, bytes32 salt) internal {
        (uint256 livePriceWad,,) = oracle.latestPriceWad();
        int24 referenceTick = TickMath.getTickAtSqrtPrice(_priceWadToSqrtPriceX96(livePriceWad));
        int24 midpoint = _floorToSpacing(referenceTick, TICK_SPACING);

        modifyLiquidityRouter.modifyLiquidity(
            key,
            ModifyLiquidityParams({
                tickLower: midpoint - halfWidthTicks,
                tickUpper: midpoint + halfWidthTicks,
                liquidityDelta: 1e18,
                salt: salt
            }),
            ZERO_BYTES
        );
    }

    function _defaultConfig() internal view returns (OracleAnchoredLVRHook.Config memory cfg) {
        cfg = OracleAnchoredLVRHook.Config({
            oracle: oracle,
            baseFee: BASE_FEE,
            maxFee: MAX_FEE,
            alphaBps: ALPHA_BPS,
            maxOracleAge: MAX_ORACLE_AGE,
            latencySecs: LATENCY_SECS,
            centerTolTicks: CENTER_TOLERANCE_TICKS,
            lvrBudgetWad: LVR_BUDGET_WAD,
            bootstrapSigma2PerSecondWad: BOOTSTRAP_SIGMA2_PER_SECOND_WAD
        });
    }

    function _permissionedHookAddress() internal view returns (address) {
        uint160 permissions = Hooks.BEFORE_ADD_LIQUIDITY_FLAG | Hooks.BEFORE_SWAP_FLAG;
        uint160 mask = uint160(type(uint160).max) & clearAllHookPermissionsMask;
        return address(uint160(mask | permissions));
    }

    function _priceWadToSqrtPriceX96(uint256 priceWad)
        internal
        pure
        returns (uint160 sqrtPriceX96)
    {
        uint256 sqrtPriceWad = FixedPointMathLib.sqrt(priceWad);
        uint256 scaled = FullMath.mulDiv(sqrtPriceWad, 2 ** 96, 1e9);
        sqrtPriceX96 = uint160(scaled);
    }

    function _floorToSpacing(int24 tick, int24 spacing) internal pure returns (int24) {
        int24 compressed = tick / spacing;
        if (tick < 0 && tick % spacing != 0) compressed--;
        return compressed * spacing;
    }
}
