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
import { ManualAggregatorV3 } from "./helpers/ManualAggregatorV3.sol";

contract OracleAnchoredLVRHookPropertiesTest is Test, Deployers {
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
    int24 internal constant TICK_SPACING = 60;

    OracleAnchoredLVRHook internal hook;
    ChainlinkReferenceOracle internal oracle;
    ManualAggregatorV3 internal baseFeed;
    ManualAggregatorV3 internal quoteFeed;

    function setUp() public {
        deployFreshManagerAndRouters();
        deployMintAndApprove2Currencies();

        OracleAnchoredLVRHook implementation = new OracleAnchoredLVRHook(IPoolManager(manager));
        address hookAddress = _permissionedHookAddress();
        vm.etch(hookAddress, address(implementation).code);

        hook = OracleAnchoredLVRHook(hookAddress);
        hook.initializeOwner(address(this));

        baseFeed = new ManualAggregatorV3(18, int256(WAD), block.timestamp);
        quoteFeed = new ManualAggregatorV3(18, int256(WAD), block.timestamp);
        oracle = new ChainlinkReferenceOracle(baseFeed, false, quoteFeed, false);

        (key,) = initPool(
            currency0,
            currency1,
            IHooks(address(hook)),
            LPFeeLibrary.DYNAMIC_FEE_FLAG,
            TICK_SPACING,
            SQRT_PRICE_1_1
        );

        hook.setConfig(key, _defaultConfig());
        hook.setRiskState(key, SIGMA2_PER_SECOND_WAD, WAD, block.timestamp);

        _addLiquidity(-18_000, 18_000, bytes32("seed"));
    }

    function testFuzz_previewSwapFee_positiveGapClassifiesOnlyOneToxic(uint16 rawGapTicks) public {
        int24 gapTicks = int24(int256(bound(uint256(rawGapTicks), 1, 900)));
        _setOracleAtTick(gapTicks);

        (bool toxicOneForZero, uint24 oneForZeroFee,,) = hook.previewSwapFee(key, false);
        (bool toxicZeroForOne, uint24 zeroForOneFee,,) = hook.previewSwapFee(key, true);

        assertTrue(toxicOneForZero);
        assertFalse(toxicZeroForOne);
        assertGt(oneForZeroFee, BASE_FEE);
        assertEq(zeroForOneFee, BASE_FEE);
    }

    function testFuzz_previewSwapFee_negativeGapClassifiesOnlyZeroForOneToxic(uint16 rawGapTicks)
        public
    {
        int24 gapTicks = int24(int256(bound(uint256(rawGapTicks), 1, 900)));
        _setOracleAtTick(-gapTicks);

        (bool toxicZeroForOne, uint24 zeroForOneFee,,) = hook.previewSwapFee(key, true);
        (bool toxicOneForZero, uint24 oneForZeroFee,,) = hook.previewSwapFee(key, false);

        assertTrue(toxicZeroForOne);
        assertFalse(toxicOneForZero);
        assertGt(zeroForOneFee, BASE_FEE);
        assertEq(oneForZeroFee, BASE_FEE);
    }

    function testFuzz_previewSwapFee_toxicSurchargeMonotonicInGap(uint16 rawGapA, uint16 rawGapB)
        public
    {
        int24 gapA = int24(int256(bound(uint256(rawGapA), 1, 900)));
        int24 gapB = int24(int256(bound(uint256(rawGapB), 1, 900)));

        if (gapA > gapB) (gapA, gapB) = (gapB, gapA);

        _setOracleAtTick(gapA);
        (, uint24 smallFee,,) = hook.previewSwapFee(key, false);

        _setOracleAtTick(gapB);
        (, uint24 largeFee,,) = hook.previewSwapFee(key, false);

        assertGe(largeFee, smallFee);
    }

    function testFuzz_previewSwapFee_revertsWhenComputedFeeExceedsConfiguredCap(
        uint16 rawGapTicks,
        uint24 rawMaxFee
    ) public {
        int24 gapTicks = int24(int256(bound(uint256(rawGapTicks), 50, 900)));
        _setOracleAtTick(gapTicks);

        uint24 computedFee = _expectedFeeUnits(gapTicks, false);
        uint24 maxFee = uint24(bound(rawMaxFee, BASE_FEE, computedFee - 1));

        OracleAnchoredLVRHook.Config memory cfg = _defaultConfig();
        cfg.maxFee = maxFee;
        hook.setConfig(key, cfg);

        vm.expectRevert(
            abi.encodeWithSelector(
                OracleAnchoredLVRHook.DeviationTooLarge.selector, computedFee, maxFee
            )
        );
        hook.previewSwapFee(key, false);
    }

    function testFuzz_minWidthTicks_increasesWithSigma(uint256 rawSigmaA, uint256 rawSigmaB)
        public
    {
        uint256 sigmaA = bound(rawSigmaA, 1, 6e14);
        uint256 sigmaB = bound(rawSigmaB, 1, 6e14);

        if (sigmaA > sigmaB) (sigmaA, sigmaB) = (sigmaB, sigmaA);

        hook.setRiskState(key, sigmaA, WAD, block.timestamp);
        uint256 narrowRequirement = hook.minWidthTicks(key);

        hook.setRiskState(key, sigmaB, WAD, block.timestamp);
        uint256 wideRequirement = hook.minWidthTicks(key);

        assertGe(wideRequirement, narrowRequirement);
    }

    function testFuzz_minWidthTicks_increasesWithLatency(uint32 rawLatencyA, uint32 rawLatencyB)
        public
    {
        uint32 latencyA = uint32(bound(rawLatencyA, 1, 180));
        uint32 latencyB = uint32(bound(rawLatencyB, 1, 180));

        if (latencyA > latencyB) (latencyA, latencyB) = (latencyB, latencyA);

        OracleAnchoredLVRHook.Config memory cfg = _defaultConfig();

        cfg.latencySecs = latencyA;
        hook.setConfig(key, cfg);
        uint256 lowLatencyRequirement = hook.minWidthTicks(key);

        cfg.latencySecs = latencyB;
        hook.setConfig(key, cfg);
        uint256 highLatencyRequirement = hook.minWidthTicks(key);

        assertGe(highLatencyRequirement, lowLatencyRequirement);
    }

    function test_centerToleranceBoundary_acceptsAtToleranceAndRejectsPastTolerance() public {
        OracleAnchoredLVRHook.Config memory cfg = _defaultConfig();
        cfg.centerTolTicks = 120;
        hook.setConfig(key, cfg);

        _setOracleAtTick(0);

        _addLiquidity(-11_880, 12_120, bytes32("boundary-ok"));

        ModifyLiquidityParams memory rejectedParams = ModifyLiquidityParams({
            tickLower: -11_820,
            tickUpper: 12_180,
            liquidityDelta: 1e18,
            salt: bytes32("boundary-revert")
        });

        vm.expectRevert(
            abi.encodeWithSelector(
                CustomRevert.WrappedError.selector,
                address(hook),
                IHooks.beforeAddLiquidity.selector,
                abi.encodeWithSelector(
                    OracleAnchoredLVRHook.OffCenter.selector, int24(180), int24(0)
                ),
                abi.encodeWithSelector(Hooks.HookCallFailed.selector)
            )
        );
        modifyLiquidityRouter.modifyLiquidity(key, rejectedParams, ZERO_BYTES);
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

    function _addLiquidity(int24 tickLower, int24 tickUpper, bytes32 salt) internal {
        modifyLiquidityRouter.modifyLiquidity(
            key,
            ModifyLiquidityParams({
                tickLower: tickLower, tickUpper: tickUpper, liquidityDelta: 1e18, salt: salt
            }),
            ZERO_BYTES
        );
    }

    function _setOracleAtTick(int24 tick) internal {
        baseFeed.setRoundData(int256(_priceWadAtTick(tick)), block.timestamp);
        quoteFeed.setRoundData(int256(WAD), block.timestamp);
    }

    function _expectedFeeUnits(int24 oracleTick, bool zeroForOne) internal pure returns (uint24) {
        return
            _expectedFeeUnits(TickMath.getSqrtPriceAtTick(oracleTick), SQRT_PRICE_1_1, zeroForOne);
    }

    function _expectedFeeUnits(
        uint160 referenceSqrtPriceX96,
        uint160 poolSqrtPriceX96,
        bool zeroForOne
    ) internal pure returns (uint24) {
        uint256 feeWad = uint256(BASE_FEE) * 1e12;

        if (referenceSqrtPriceX96 > poolSqrtPriceX96 && !zeroForOne) {
            feeWad += FullMath.mulDiv(referenceSqrtPriceX96, WAD, poolSqrtPriceX96) - WAD;
        } else if (referenceSqrtPriceX96 < poolSqrtPriceX96 && zeroForOne) {
            feeWad += FullMath.mulDiv(poolSqrtPriceX96, WAD, referenceSqrtPriceX96) - WAD;
        }

        return uint24(feeWad / 1e12);
    }

    function _priceWadAtTick(int24 tick) internal pure returns (uint256) {
        uint160 sqrtPriceX96 = TickMath.getSqrtPriceAtTick(tick);
        uint256 sqrtPriceWad = FullMath.mulDiv(sqrtPriceX96, SQRT_WAD, 2 ** 96);
        return FullMath.mulDiv(sqrtPriceWad, sqrtPriceWad, 1);
    }
}
