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

contract OracleAnchoredLVRHookFuzzTest is Test, Deployers {
    uint256 internal constant WAD = 1e18;
    uint256 internal constant SQRT_WAD = 1e9;
    uint24 internal constant BASE_FEE = 500;
    uint24 internal constant MAX_FEE = 50_000;
    uint24 internal constant ALPHA_BPS = 10_000;
    uint32 internal constant MAX_ORACLE_AGE = 1 hours;
    uint32 internal constant LATENCY_SECS = 60;
    uint32 internal constant CENTER_TOLERANCE_TICKS = 30;
    uint256 internal constant LVR_BUDGET_WAD = 1e16;
    uint256 internal constant DEFAULT_SIGMA2_WAD = 4e14;
    uint256 internal constant BOOTSTRAP_SIGMA2_PER_SECOND_WAD = 8e14;

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
            SQRT_PRICE_1_1
        );

        hook.setConfig(key, _defaultConfig(BASE_FEE, MAX_FEE, LATENCY_SECS, CENTER_TOLERANCE_TICKS));
        hook.setRiskState(key, DEFAULT_SIGMA2_WAD, WAD, block.timestamp);

        _addLiquidity(-12_000, 12_000);
    }

    function testFuzz_previewSwapFee_positiveGapIsDirectionallySymmetric(uint16 rawGapTicks)
        public
    {
        uint24 gapTicks = uint24(bound(rawGapTicks, 1, 800));

        _setOraclePrice(_priceWadAtTick(int24(uint24(gapTicks))), block.timestamp);
        (, uint24 feeWhenBuyingToken0,,) = hook.previewSwapFee(key, false);

        _setOraclePrice(_priceWadAtTick(-int24(uint24(gapTicks))), block.timestamp);
        (, uint24 feeWhenSellingToken0,,) = hook.previewSwapFee(key, true);

        uint24 diff = feeWhenBuyingToken0 > feeWhenSellingToken0
            ? feeWhenBuyingToken0 - feeWhenSellingToken0
            : feeWhenSellingToken0 - feeWhenBuyingToken0;
        assertLe(diff, 1);
    }

    function testFuzz_previewSwapFee_benignFlowStaysAtBaseFee(uint16 rawGapTicks) public {
        uint24 gapTicks = uint24(bound(rawGapTicks, 1, 800));

        _setOraclePrice(_priceWadAtTick(int24(uint24(gapTicks))), block.timestamp);
        (bool toxicPositive, uint24 benignPositive,,) = hook.previewSwapFee(key, true);
        assertFalse(toxicPositive);
        assertEq(benignPositive, BASE_FEE);

        _setOraclePrice(_priceWadAtTick(-int24(uint24(gapTicks))), block.timestamp);
        (bool toxicNegative, uint24 benignNegative,,) = hook.previewSwapFee(key, false);
        assertFalse(toxicNegative);
        assertEq(benignNegative, BASE_FEE);
    }

    function testFuzz_previewSwapFee_toxicSurchargeIsMonotoneInPositiveGap(
        uint16 rawGapA,
        uint16 rawGapB
    ) public {
        uint24 gapA = uint24(bound(rawGapA, 1, 800));
        uint24 gapB = uint24(bound(rawGapB, 1, 800));

        if (gapA > gapB) (gapA, gapB) = (gapB, gapA);

        hook.setConfig(
            key,
            _defaultConfig(BASE_FEE, LPFeeLibrary.MAX_LP_FEE, LATENCY_SECS, CENTER_TOLERANCE_TICKS)
        );

        _setOraclePrice(_priceWadAtTick(int24(uint24(gapA))), block.timestamp);
        (, uint24 feeA,,) = hook.previewSwapFee(key, false);

        _setOraclePrice(_priceWadAtTick(int24(uint24(gapB))), block.timestamp);
        (, uint24 feeB,,) = hook.previewSwapFee(key, false);

        assertGe(feeB, feeA);
        assertGe(feeA, BASE_FEE);
    }

    function testFuzz_previewSwapFee_revertsWhenCapFallsBelowComputedFee(uint16 rawGapTicks)
        public
    {
        uint24 gapTicks = uint24(bound(rawGapTicks, 1, 800));
        hook.setConfig(
            key,
            _defaultConfig(BASE_FEE, LPFeeLibrary.MAX_LP_FEE, LATENCY_SECS, CENTER_TOLERANCE_TICKS)
        );
        _setOraclePrice(_priceWadAtTick(int24(uint24(gapTicks))), block.timestamp);

        (, uint24 requiredFee,,) = hook.previewSwapFee(key, false);
        vm.assume(requiredFee > BASE_FEE);

        hook.setConfig(
            key, _defaultConfig(BASE_FEE, requiredFee - 1, LATENCY_SECS, CENTER_TOLERANCE_TICKS)
        );

        vm.expectRevert(
            abi.encodeWithSelector(
                OracleAnchoredLVRHook.DeviationTooLarge.selector, requiredFee, requiredFee - 1
            )
        );
        hook.previewSwapFee(key, false);
    }

    function testFuzz_minWidthTicks_isMonotoneInSigma(uint64 rawSigmaLow, uint64 rawSigmaHigh)
        public
    {
        uint256 sigmaLow = bound(uint256(rawSigmaLow), 1, 1e14);
        uint256 sigmaHigh = bound(uint256(rawSigmaHigh), sigmaLow, 1e14);

        hook.setRiskState(key, sigmaLow, WAD, block.timestamp);
        uint256 minLow = hook.minWidthTicks(key);

        hook.setRiskState(key, sigmaHigh, WAD, block.timestamp);
        uint256 minHigh = hook.minWidthTicks(key);

        assertGe(minHigh, minLow);
    }

    function testFuzz_minWidthTicks_isMonotoneInLatency(uint16 rawLatencyLow, uint16 rawLatencyHigh)
        public
    {
        uint32 latencyLow = uint32(bound(uint256(rawLatencyLow), 1, 600));
        uint32 latencyHigh = uint32(bound(uint256(rawLatencyHigh), latencyLow, 600));

        hook.setRiskState(key, 1e14, WAD, block.timestamp);

        hook.setConfig(key, _defaultConfig(BASE_FEE, MAX_FEE, latencyLow, CENTER_TOLERANCE_TICKS));
        uint256 minLow = hook.minWidthTicks(key);

        hook.setConfig(key, _defaultConfig(BASE_FEE, MAX_FEE, latencyHigh, CENTER_TOLERANCE_TICKS));
        uint256 minHigh = hook.minWidthTicks(key);

        assertGe(minHigh, minLow);
    }

    function testFuzz_beforeAddLiquidity_acceptsAtCenterToleranceBoundary(uint8 rawOffsetUnits)
        public
    {
        uint32 offsetUnits = uint32(bound(uint256(rawOffsetUnits), 0, 10));
        uint32 offsetTicks = offsetUnits * uint32(uint24(key.tickSpacing));

        hook.setConfig(key, _defaultConfig(BASE_FEE, MAX_FEE, LATENCY_SECS, offsetTicks));

        int24 midpoint = int24(uint24(offsetTicks));
        ModifyLiquidityParams memory params = ModifyLiquidityParams({
            tickLower: midpoint - 18_000,
            tickUpper: midpoint + 18_000,
            liquidityDelta: 1e18,
            salt: bytes32("boundary")
        });

        modifyLiquidityRouter.modifyLiquidity(key, params, ZERO_BYTES);
    }

    function testFuzz_beforeAddLiquidity_rejectsJustOutsideCenterTolerance(uint8 rawOffsetUnits)
        public
    {
        uint32 offsetUnits = uint32(bound(uint256(rawOffsetUnits), 0, 10));
        uint32 spacing = uint32(uint24(key.tickSpacing));
        uint32 toleranceTicks = offsetUnits * spacing;
        int24 midpoint = int24(uint24(toleranceTicks + spacing));

        hook.setConfig(key, _defaultConfig(BASE_FEE, MAX_FEE, LATENCY_SECS, toleranceTicks));

        ModifyLiquidityParams memory params = ModifyLiquidityParams({
            tickLower: midpoint - 18_000,
            tickUpper: midpoint + 18_000,
            liquidityDelta: 1e18,
            salt: bytes32("outside")
        });

        vm.expectRevert(
            abi.encodeWithSelector(
                CustomRevert.WrappedError.selector,
                address(hook),
                IHooks.beforeAddLiquidity.selector,
                abi.encodeWithSelector(
                    OracleAnchoredLVRHook.OffCenter.selector, midpoint, int24(0)
                ),
                abi.encodeWithSelector(Hooks.HookCallFailed.selector)
            )
        );
        modifyLiquidityRouter.modifyLiquidity(key, params, ZERO_BYTES);
    }

    function _defaultConfig(
        uint24 baseFee,
        uint24 maxFee,
        uint32 latencySecs,
        uint32 centerTolTicks
    ) internal view returns (OracleAnchoredLVRHook.Config memory cfg) {
        cfg = OracleAnchoredLVRHook.Config({
                oracle: oracle,
                baseFee: baseFee,
                maxFee: maxFee,
                alphaBps: ALPHA_BPS,
                maxOracleAge: MAX_ORACLE_AGE,
                latencySecs: latencySecs,
                centerTolTicks: centerTolTicks,
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
