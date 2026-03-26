// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import { Test } from "forge-std/Test.sol";
import { OracleAnchoredLVRHook } from "src/OracleAnchoredLVRHook.sol";
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
import { ManualAggregatorV3 } from "./helpers/ManualAggregatorV3.sol";

contract OracleAnchoredLVRHookTest is Test, Deployers {
    using PoolIdLibrary for PoolKey;

    uint256 internal constant WAD = 1e18;
    uint256 internal constant SQRT_WAD = 1e9;
    uint24 internal constant BASE_FEE = 500;
    uint24 internal constant MAX_FEE = 50_000;
    uint24 internal constant ALPHA_BPS = 10_000;
    uint32 internal constant MAX_ORACLE_AGE = 1 hours;
    uint32 internal constant LATENCY_SECS = 60;
    uint32 internal constant CENTER_TOLERANCE_TICKS = 30;
    uint256 internal constant LVR_BUDGET_WAD = 1e16;
    uint256 internal constant SIGMA2_PER_SECOND_WAD = 4e14;
    uint256 internal constant BOOTSTRAP_SIGMA2_PER_SECOND_WAD = 8e14;

    OracleAnchoredLVRHook internal hook;
    ChainlinkReferenceOracle internal oracle;
    ManualAggregatorV3 internal baseFeed;
    ManualAggregatorV3 internal quoteFeed;

    function setUp() public {
        deployFreshManagerAndRouters();
        deployMintAndApprove2Currencies();

        address hookAddress = _permissionedHookAddress();
        deployCodeTo(
            "src/OracleAnchoredLVRHook.sol:OracleAnchoredLVRHook",
            abi.encode(IPoolManager(manager), address(this)),
            hookAddress
        );

        hook = OracleAnchoredLVRHook(hookAddress);

        baseFeed = new ManualAggregatorV3(18, int256(WAD), block.timestamp);
        quoteFeed = new ManualAggregatorV3(18, int256(WAD), block.timestamp);
        oracle = new ChainlinkReferenceOracle(baseFeed, false, quoteFeed, false, 18, 18);

        (key,) = initPool(
            currency0,
            currency1,
            IHooks(address(hook)),
            LPFeeLibrary.DYNAMIC_FEE_FLAG,
            SQRT_PRICE_1_1
        );

        hook.setConfig(key, _defaultConfig());
        hook.setRiskState(key, SIGMA2_PER_SECOND_WAD, WAD, block.timestamp);

        _addLiquidity(-12_000, 12_000);
    }

    function test_previewSwapFee_marksToxicOneForZeroAndAddsSurcharge() public {
        _setOraclePrice(_priceWadAtTick(20), block.timestamp);

        uint160 referenceSqrtPriceX96 = TickMath.getSqrtPriceAtTick(20);
        uint160 poolSqrtPriceX96 = SQRT_PRICE_1_1;
        uint24 expectedFee =
            _expectedFeeUnits(referenceSqrtPriceX96, poolSqrtPriceX96, false, ALPHA_BPS);

        (
            bool toxic,
            uint24 feeUnits,
            uint160 previewReferenceSqrtPriceX96,
            uint160 previewPoolSqrtPriceX96
        ) = hook.previewSwapFee(key, false);

        assertTrue(toxic);
        assertEq(feeUnits, expectedFee);
        assertGt(previewReferenceSqrtPriceX96, previewPoolSqrtPriceX96);
        assertEq(previewPoolSqrtPriceX96, poolSqrtPriceX96);
    }

    function test_previewSwapFee_keepsBaseFeeForBenignFlow() public {
        _setOraclePrice(_priceWadAtTick(20), block.timestamp);

        (bool toxic, uint24 feeUnits,,) = hook.previewSwapFee(key, true);

        assertFalse(toxic);
        assertEq(feeUnits, BASE_FEE);
    }

    function test_previewSwapFee_revertsWhenCapIsExceeded() public {
        _setOraclePrice(_priceWadAtTick(20), block.timestamp);

        OracleAnchoredLVRHook.Config memory cfg = _defaultConfig();
        cfg.maxFee = 900;
        hook.setConfig(key, cfg);

        vm.expectRevert(
            abi.encodeWithSelector(
                OracleAnchoredLVRHook.DeviationTooLarge.selector, _expectedFeeUnits(20, false), 900
            )
        );
        hook.previewSwapFee(key, false);
    }

    function test_previewSwapFee_appliesAlphaHaircutToToxicPremium() public {
        OracleAnchoredLVRHook.Config memory cfg = _defaultConfig();
        cfg.alphaBps = 5000;
        hook.setConfig(key, cfg);

        _setOraclePrice(_priceWadAtTick(20), block.timestamp);

        (bool toxic, uint24 feeUnits,,) = hook.previewSwapFee(key, false);

        assertTrue(toxic);
        assertEq(feeUnits, _expectedFeeUnits(20, false, cfg.alphaBps));
    }

    function test_swapRevertsWhenOracleIsStale() public {
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

    function test_swapUpdatesRiskStateOnSuccessfulOracleAnchoredSwap() public {
        uint256 referencePriceWad = _priceWadAtTick(20);

        vm.warp(block.timestamp + LATENCY_SECS);
        _setOraclePrice(referencePriceWad, block.timestamp);

        swap(key, false, -1e15, ZERO_BYTES);

        (uint256 sigma2PerSecondWad, uint256 lastOraclePriceWad, uint256 lastOracleTs) =
            hook.risk(key.toId());

        assertEq(lastOraclePriceWad, referencePriceWad);
        assertEq(lastOracleTs, block.timestamp);
        assertEq(sigma2PerSecondWad, _expectedUpdatedSigma(referencePriceWad, LATENCY_SECS));
    }

    function test_addLiquidityRevertsWhenPositionIsOffCenter() public {
        ModifyLiquidityParams memory params = ModifyLiquidityParams({
            tickLower: 600, tickUpper: 720, liquidityDelta: 1e18, salt: bytes32(0)
        });

        vm.expectRevert(
            abi.encodeWithSelector(
                CustomRevert.WrappedError.selector,
                address(hook),
                IHooks.beforeAddLiquidity.selector,
                abi.encodeWithSelector(
                    OracleAnchoredLVRHook.OffCenter.selector, int24(660), int24(0)
                ),
                abi.encodeWithSelector(Hooks.HookCallFailed.selector)
            )
        );
        modifyLiquidityRouter.modifyLiquidity(key, params, ZERO_BYTES);
    }

    function test_addLiquidityRevertsWhenWidePositionIsOffCenterAtNegativeOracleTick() public {
        _setOraclePrice(_priceWadAtTick(-200), block.timestamp);

        ModifyLiquidityParams memory params = ModifyLiquidityParams({
            tickLower: -11_940,
            tickUpper: 12_060,
            liquidityDelta: 1e18,
            salt: bytes32("neg-wide-offcenter")
        });

        vm.expectRevert();
        modifyLiquidityRouter.modifyLiquidity(key, params, ZERO_BYTES);
    }

    function test_addLiquidityRevertsWhenPositionIsTooNarrow() public {
        uint256 minWidthTicks = hook.minWidthTicks(key);
        assertGt(minWidthTicks, 12_000);

        ModifyLiquidityParams memory params = ModifyLiquidityParams({
            tickLower: -6000, tickUpper: 6000, liquidityDelta: 1e18, salt: bytes32(0)
        });

        vm.expectRevert(
            abi.encodeWithSelector(
                CustomRevert.WrappedError.selector,
                address(hook),
                IHooks.beforeAddLiquidity.selector,
                abi.encodeWithSelector(
                    OracleAnchoredLVRHook.WidthTooNarrow.selector, uint256(12_000), minWidthTicks
                ),
                abi.encodeWithSelector(Hooks.HookCallFailed.selector)
            )
        );
        modifyLiquidityRouter.modifyLiquidity(key, params, ZERO_BYTES);
    }

    function test_addLiquidityUsesConservativeBootstrapWhenRiskIsUnset() public {
        hook.setRiskState(key, 0, 0, 0);

        uint256 minWidthTicks = hook.minWidthTicks(key);
        assertGt(minWidthTicks, 12_000);

        ModifyLiquidityParams memory params = ModifyLiquidityParams({
            tickLower: -6000, tickUpper: 6000, liquidityDelta: 1e18, salt: bytes32("bootstrap")
        });

        vm.expectRevert(
            abi.encodeWithSelector(
                CustomRevert.WrappedError.selector,
                address(hook),
                IHooks.beforeAddLiquidity.selector,
                abi.encodeWithSelector(
                    OracleAnchoredLVRHook.WidthTooNarrow.selector, uint256(12_000), minWidthTicks
                ),
                abi.encodeWithSelector(Hooks.HookCallFailed.selector)
            )
        );
        modifyLiquidityRouter.modifyLiquidity(key, params, ZERO_BYTES);
    }

    function test_addLiquiditySucceedsWhenCenteredAndWideEnough() public {
        ModifyLiquidityParams memory params = ModifyLiquidityParams({
            tickLower: -18_000, tickUpper: 18_000, liquidityDelta: 1e18, salt: bytes32("wide")
        });

        modifyLiquidityRouter.modifyLiquidity(key, params, ZERO_BYTES);
    }

    function test_constructorRevertsWhenOwnerIsZeroAddress() public {
        vm.expectRevert(OracleAnchoredLVRHook.InvalidOwner.selector);
        new OracleAnchoredLVRHook(IPoolManager(manager), address(0));
    }

    function test_beforeSwap_revertsOnZeroOraclePrice() public {
        _setOraclePrice(0, block.timestamp);

        vm.expectRevert(
            abi.encodeWithSelector(
                CustomRevert.WrappedError.selector,
                address(hook),
                IHooks.beforeSwap.selector,
                abi.encodeWithSelector(OracleAnchoredLVRHook.InvalidOraclePrice.selector),
                abi.encodeWithSelector(Hooks.HookCallFailed.selector)
            )
        );
        swap(key, false, -1e15, ZERO_BYTES);
    }

    function test_beforeSwap_revertsOnOracleStalenessAtExactBoundary() public {
        vm.warp(block.timestamp + MAX_ORACLE_AGE);
        swap(key, false, -1e15, ZERO_BYTES);

        vm.warp(block.timestamp + 1);
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

    function test_beforeSwap_maxFeeClipRevertPropagatesCorrectValues() public {
        OracleAnchoredLVRHook.Config memory cfg = _defaultConfig();
        cfg.maxFee = BASE_FEE + 1;
        hook.setConfig(key, cfg);
        _setOraclePrice(_priceWadAtTick(100), block.timestamp);

        vm.expectRevert(
            abi.encodeWithSelector(
                CustomRevert.WrappedError.selector,
                address(hook),
                IHooks.beforeSwap.selector,
                abi.encodeWithSelector(
                    OracleAnchoredLVRHook.DeviationTooLarge.selector,
                    _expectedFeeUnits(100, false),
                    BASE_FEE + 1
                ),
                abi.encodeWithSelector(Hooks.HookCallFailed.selector)
            )
        );
        swap(key, false, -1e15, ZERO_BYTES);
    }

    function test_riskState_doesNotAdvanceOnRepeatedSameOracleTimestamp() public {
        uint256 firstUpdatedAt = block.timestamp + LATENCY_SECS;

        vm.warp(firstUpdatedAt);
        _setOraclePrice(_priceWadAtTick(20), firstUpdatedAt);
        swap(key, false, -1e15, ZERO_BYTES);

        (uint256 sigmaBefore,, uint256 lastOracleTsBefore) = hook.risk(key.toId());
        assertEq(lastOracleTsBefore, firstUpdatedAt);

        _setOraclePrice(_priceWadAtTick(40), firstUpdatedAt);
        swap(key, false, -1e15, ZERO_BYTES);

        (uint256 sigmaAfter, uint256 lastOraclePriceWadAfter, uint256 lastOracleTsAfter) =
            hook.risk(key.toId());

        assertEq(sigmaAfter, sigmaBefore);
        assertEq(lastOraclePriceWadAfter, _priceWadAtTick(40));
        assertEq(lastOracleTsAfter, firstUpdatedAt);
    }

    function test_swapUpdatesRiskStateWhenOnlyBaseFeedTimestampAdvances() public {
        uint256 priorLatestFeedTs = 100;
        uint256 quoteUpdatedAt = 90;
        uint256 nextLatestFeedTs = 110;
        uint256 updatedReferencePriceWad = _priceWadAtTick(20);

        baseFeed.setRoundData(int256(WAD), priorLatestFeedTs);
        quoteFeed.setRoundData(int256(WAD), quoteUpdatedAt);
        hook.setRiskState(key, SIGMA2_PER_SECOND_WAD, WAD, priorLatestFeedTs);

        vm.warp(nextLatestFeedTs);
        baseFeed.setRoundData(int256(updatedReferencePriceWad), nextLatestFeedTs);

        swap(key, false, -1e15, ZERO_BYTES);

        (uint256 sigma2PerSecondWad, uint256 lastOraclePriceWad, uint256 lastOracleTs) =
            hook.risk(key.toId());

        assertNotEq(sigma2PerSecondWad, SIGMA2_PER_SECOND_WAD);
        assertEq(lastOraclePriceWad, updatedReferencePriceWad);
        assertEq(lastOracleTs, nextLatestFeedTs);
    }

    function test_riskState_bootstrapSigmaUsedWhenStateIsUninitialized() public {
        hook.setRiskState(key, 0, 0, 0);
        uint256 bootstrapWidth = hook.minWidthTicks(key);

        hook.setRiskState(key, BOOTSTRAP_SIGMA2_PER_SECOND_WAD, WAD, block.timestamp);
        uint256 explicitBootstrapWidth = hook.minWidthTicks(key);

        assertEq(bootstrapWidth, explicitBootstrapWidth);
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

    function _addLiquidity(int24 tickLower, int24 tickUpper) internal {
        modifyLiquidityRouter.modifyLiquidity(
            key,
            ModifyLiquidityParams({
                tickLower: tickLower, tickUpper: tickUpper, liquidityDelta: 1e18, salt: bytes32(0)
            }),
            ZERO_BYTES
        );
    }

    function _setOraclePrice(uint256 priceWad, uint256 updatedAt) internal {
        baseFeed.setRoundData(int256(priceWad), updatedAt);
        quoteFeed.setRoundData(int256(WAD), updatedAt);
    }

    function _expectedUpdatedSigma(uint256 nextOraclePriceWad, uint256 dt)
        internal
        pure
        returns (uint256)
    {
        uint256 absoluteChange =
            nextOraclePriceWad > WAD ? nextOraclePriceWad - WAD : WAD - nextOraclePriceWad;
        uint256 returnWad = FullMath.mulDiv(absoluteChange, WAD, WAD);
        uint256 sampleSigma2PerSecondWad = FullMath.mulDiv(returnWad, returnWad, WAD * dt);
        uint256 retained = FullMath.mulDiv(SIGMA2_PER_SECOND_WAD, 8000, 10_000);
        uint256 updated = FullMath.mulDiv(sampleSigma2PerSecondWad, 2000, 10_000);
        return retained + updated;
    }

    function _expectedFeeUnits(int24 oracleTick, bool zeroForOne) internal pure returns (uint24) {
        return _expectedFeeUnits(oracleTick, zeroForOne, ALPHA_BPS);
    }

    function _expectedFeeUnits(int24 oracleTick, bool zeroForOne, uint24 alphaBps)
        internal
        pure
        returns (uint24)
    {
        return _expectedFeeUnits(
            TickMath.getSqrtPriceAtTick(oracleTick), SQRT_PRICE_1_1, zeroForOne, alphaBps
        );
    }

    function _expectedFeeUnits(
        uint160 referenceSqrtPriceX96,
        uint160 poolSqrtPriceX96,
        bool zeroForOne,
        uint24 alphaBps
    ) internal pure returns (uint24) {
        uint256 feeWad = uint256(BASE_FEE) * 1e12;

        if (referenceSqrtPriceX96 > poolSqrtPriceX96 && !zeroForOne) {
            uint256 exactPremiumWad =
                FullMath.mulDiv(referenceSqrtPriceX96, WAD, poolSqrtPriceX96) - WAD;
            feeWad += FullMath.mulDiv(exactPremiumWad, alphaBps, 10_000);
        } else if (referenceSqrtPriceX96 < poolSqrtPriceX96 && zeroForOne) {
            uint256 exactPremiumWad =
                FullMath.mulDiv(poolSqrtPriceX96, WAD, referenceSqrtPriceX96) - WAD;
            feeWad += FullMath.mulDiv(exactPremiumWad, alphaBps, 10_000);
        }

        return uint24(feeWad / 1e12);
    }

    function _priceWadAtTick(int24 tick) internal pure returns (uint256) {
        uint160 sqrtPriceX96 = TickMath.getSqrtPriceAtTick(tick);
        uint256 sqrtPriceWad = FullMath.mulDiv(sqrtPriceX96, SQRT_WAD, 2 ** 96);
        return FullMath.mulDiv(sqrtPriceWad, sqrtPriceWad, 1);
    }
}
