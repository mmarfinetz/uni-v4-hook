// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import { StdInvariant } from "forge-std/StdInvariant.sol";
import { OracleAnchoredLVRHook } from "src/OracleAnchoredLVRHook.sol";
import { ChainlinkReferenceOracle } from "src/oracles/ChainlinkReferenceOracle.sol";
import { Deployers } from "../lib/v4-core/test/utils/Deployers.sol";
import { IHooks } from "v4-core/interfaces/IHooks.sol";
import { IPoolManager } from "v4-core/interfaces/IPoolManager.sol";
import { Hooks } from "v4-core/libraries/Hooks.sol";
import { FullMath } from "v4-core/libraries/FullMath.sol";
import { LPFeeLibrary } from "v4-core/libraries/LPFeeLibrary.sol";
import { TickMath } from "v4-core/libraries/TickMath.sol";
import { PoolKey } from "v4-core/types/PoolKey.sol";
import { PoolIdLibrary } from "v4-core/types/PoolId.sol";
import { ModifyLiquidityParams } from "v4-core/types/PoolOperation.sol";
import { FixedPointMathLib } from "solmate/src/utils/FixedPointMathLib.sol";
import { ManualAggregatorV3 } from "./helpers/ManualAggregatorV3.sol";
import { OracleAnchoredLVRHookHandler } from "./helpers/OracleAnchoredLVRHookHandler.sol";

contract OracleAnchoredLVRHookInvariantTest is StdInvariant, Deployers {
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
    uint256 internal constant MAX_WIDTH_TICKS = 1_774_544;
    int24 internal constant TICK_SPACING = 60;

    OracleAnchoredLVRHook internal hook;
    ChainlinkReferenceOracle internal oracle;
    ManualAggregatorV3 internal baseFeed;
    ManualAggregatorV3 internal quoteFeed;
    OracleAnchoredLVRHookHandler internal handler;

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

        handler = new OracleAnchoredLVRHookHandler(
            hook, oracle, baseFeed, quoteFeed, modifyLiquidityRouter, swapRouter, key
        );

        bytes4[] memory selectors = new bytes4[](5);
        selectors[0] = handler.advanceOracle.selector;
        selectors[1] = handler.warpTime.selector;
        selectors[2] = handler.swapZeroForOne.selector;
        selectors[3] = handler.swapOneForZero.selector;
        selectors[4] = handler.addCenteredLiquidity.selector;

        targetContract(address(handler));
        targetSelector(FuzzSelector({ addr: address(handler), selectors: selectors }));
    }

    function invariant_InvalidLiquidityGuardsRejectBadShapes() public {
        uint256 minWidth = hook.minWidthTicks(key);
        int24 spacing = key.tickSpacing;
        int24 centeredMidpoint = _floorToSpacing(_currentReferenceTick(), spacing);

        uint256 offCenterHalfWidthSteps = _minHalfWidthSteps(minWidth, spacing) + 4;
        int24 offCenterHalfWidth = int24(int256(offCenterHalfWidthSteps * uint256(uint24(spacing))));
        int24 offCenterShift = spacing * 2;
        ModifyLiquidityParams memory offCenterParams = ModifyLiquidityParams({
            tickLower: centeredMidpoint - offCenterHalfWidth + offCenterShift,
            tickUpper: centeredMidpoint + offCenterHalfWidth + offCenterShift,
            liquidityDelta: 1e18,
            salt: bytes32("invariant-offcenter")
        });

        vm.prank(address(manager));
        (bool offCenterOk,) = address(hook)
            .call(
                abi.encodeCall(
                    OracleAnchoredLVRHook.beforeAddLiquidity,
                    (address(this), key, offCenterParams, hex"")
                )
            );
        assertFalse(offCenterOk);

        uint256 minHalfWidthSteps = _minHalfWidthSteps(minWidth, spacing);
        if (minHalfWidthSteps <= 1) return;

        int24 narrowHalfWidth = int24(int256((minHalfWidthSteps - 1) * uint256(uint24(spacing))));
        ModifyLiquidityParams memory narrowParams = ModifyLiquidityParams({
            tickLower: centeredMidpoint - narrowHalfWidth,
            tickUpper: centeredMidpoint + narrowHalfWidth,
            liquidityDelta: 1e18,
            salt: bytes32("invariant-narrow")
        });

        vm.prank(address(manager));
        (bool narrowOk,) = address(hook)
            .call(
                abi.encodeCall(
                    OracleAnchoredLVRHook.beforeAddLiquidity,
                    (address(this), key, narrowParams, hex"")
                )
            );
        assertFalse(narrowOk);
    }

    function invariant_StaleOracleNeverAllowsSwapThroughHandler() public view {
        assertEq(handler.ghostUnexpectedStaleSwapSuccesses(), 0);
    }

    function invariant_RiskStateMatchesLastSuccessfulSwapObservation() public view {
        uint256 observedTs = handler.ghostLastSuccessfulSwapOracleTs();
        if (observedTs == 0) return;

        (uint256 sigma2PerSecondWad, uint256 lastOraclePriceWad, uint256 lastOracleTs) =
            hook.risk(key.toId());

        assertGt(sigma2PerSecondWad, 0);
        assertEq(lastOraclePriceWad, handler.ghostLastSuccessfulSwapOraclePriceWad());
        assertEq(lastOracleTs, observedTs);
    }

    function invariant_QuotesStayWithinCapAndClassifyDirectionsConsistently() public view {
        (bool okOneForZero, bool toxicOneForZero, uint24 feeOneForZero) = _previewSwapFee(false);
        (bool okZeroForOne, bool toxicZeroForOne, uint24 feeZeroForOne) = _previewSwapFee(true);

        if (okOneForZero) {
            assertLe(feeOneForZero, MAX_FEE);
            if (!toxicOneForZero) assertEq(feeOneForZero, BASE_FEE);
        }

        if (okZeroForOne) {
            assertLe(feeZeroForOne, MAX_FEE);
            if (!toxicZeroForOne) assertEq(feeZeroForOne, BASE_FEE);
        }

        if (okOneForZero && okZeroForOne) {
            assertFalse(toxicOneForZero && toxicZeroForOne);
        } else if (okOneForZero) {
            assertFalse(toxicOneForZero);
            assertEq(feeOneForZero, BASE_FEE);
        } else if (okZeroForOne) {
            assertFalse(toxicZeroForOne);
            assertEq(feeZeroForOne, BASE_FEE);
        }
    }

    function invariant_MinWidthRemainsSpacingAlignedAndBounded() public view {
        uint256 minWidthTicks = hook.minWidthTicks(key);

        assertEq(minWidthTicks % uint256(uint24(key.tickSpacing)), 0);
        assertLe(minWidthTicks, MAX_WIDTH_TICKS);
    }

    function invariant_StaleOracleBlocksPreviewFeeReads() public view {
        if (!handler.oracleIsStale()) return;

        (bool okOneForZero,,) = _previewSwapFee(false);
        (bool okZeroForOne,,) = _previewSwapFee(true);

        assertFalse(okOneForZero);
        assertFalse(okZeroForOne);
    }

    function _previewSwapFee(bool zeroForOne)
        internal
        view
        returns (bool ok, bool toxic, uint24 feeUnits)
    {
        bytes memory data;
        (ok, data) = address(hook)
            .staticcall(abi.encodeCall(OracleAnchoredLVRHook.previewSwapFee, (key, zeroForOne)));

        if (!ok) return (false, false, 0);

        (toxic, feeUnits,,) = abi.decode(data, (bool, uint24, uint160, uint160));
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

    function _currentReferenceTick() internal view returns (int24) {
        (uint256 priceWad,) = oracle.latestPriceWad();
        return TickMath.getTickAtSqrtPrice(_priceWadToSqrtPriceX96(priceWad));
    }

    function _priceWadToSqrtPriceX96(uint256 priceWad)
        internal
        pure
        returns (uint160 sqrtPriceX96)
    {
        uint256 sqrtPriceWad = FixedPointMathLib.sqrt(priceWad);
        uint256 scaled = FullMath.mulDiv(sqrtPriceWad, 2 ** 96, SQRT_WAD);
        sqrtPriceX96 = uint160(scaled);
    }

    function _minHalfWidthSteps(uint256 minWidth, int24 spacing) internal pure returns (uint256) {
        uint256 spacingUint = uint256(uint24(spacing));
        uint256 widthPerHalfStep = spacingUint * 2;
        return (minWidth + widthPerHalfStep - 1) / widthPerHalfStep;
    }

    function _floorToSpacing(int24 tick, int24 spacing) internal pure returns (int24) {
        int24 compressed = tick / spacing;
        if (tick < 0 && tick % spacing != 0) compressed--;
        return compressed * spacing;
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
}
