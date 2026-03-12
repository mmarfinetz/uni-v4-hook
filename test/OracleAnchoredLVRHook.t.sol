// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import {Test} from "forge-std/Test.sol";
import {OracleAnchoredLVRHook} from "src/OracleAnchoredLVRHook.sol";
import {MockReferenceOracle} from "src/mocks/MockReferenceOracle.sol";
import {Deployers} from "../lib/v4-core/test/utils/Deployers.sol";
import {IHooks} from "v4-core/interfaces/IHooks.sol";
import {IPoolManager} from "v4-core/interfaces/IPoolManager.sol";
import {ModifyLiquidityParams} from "v4-core/types/PoolOperation.sol";
import {Hooks} from "v4-core/libraries/Hooks.sol";
import {LPFeeLibrary} from "v4-core/libraries/LPFeeLibrary.sol";
import {CustomRevert} from "v4-core/libraries/CustomRevert.sol";
import {TickMath} from "v4-core/libraries/TickMath.sol";
import {FullMath} from "v4-core/libraries/FullMath.sol";

contract OracleAnchoredLVRHookTest is Test, Deployers {
    uint256 internal constant WAD = 1e18;
    uint256 internal constant SQRT_WAD = 1e9;
    uint24 internal constant BASE_FEE = 500;
    uint24 internal constant MAX_FEE = 50_000;
    uint32 internal constant MAX_ORACLE_AGE = 1 hours;
    uint32 internal constant LATENCY_SECS = 60;
    uint32 internal constant CENTER_TOLERANCE_TICKS = 30;
    uint256 internal constant LVR_BUDGET_WAD = 1e16;
    uint256 internal constant SIGMA2_PER_SECOND_WAD = 4e14;

    OracleAnchoredLVRHook internal hook;
    MockReferenceOracle internal oracle;

    function setUp() public {
        deployFreshManagerAndRouters();
        deployMintAndApprove2Currencies();

        OracleAnchoredLVRHook implementation = new OracleAnchoredLVRHook(IPoolManager(manager));
        address hookAddress = _permissionedHookAddress();
        vm.etch(hookAddress, address(implementation).code);

        hook = OracleAnchoredLVRHook(hookAddress);
        hook.initializeOwner(address(this));

        oracle = new MockReferenceOracle();
        oracle.setLatestPrice(WAD, block.timestamp);

        (key,) = initPool(currency0, currency1, IHooks(address(hook)), LPFeeLibrary.DYNAMIC_FEE_FLAG, SQRT_PRICE_1_1);

        hook.setConfig(key, _defaultConfig());
        hook.setRiskState(key, SIGMA2_PER_SECOND_WAD, WAD, block.timestamp);

        _addLiquidity(-12_000, 12_000);
    }

    function test_previewSwapFee_marksToxicOneForZeroAndAddsSurcharge() public {
        oracle.setLatestPrice(_priceWadAtTick(20), block.timestamp);

        uint160 referenceSqrtPriceX96 = TickMath.getSqrtPriceAtTick(20);
        uint160 poolSqrtPriceX96 = SQRT_PRICE_1_1;
        uint24 expectedFee = _expectedFeeUnits(referenceSqrtPriceX96, poolSqrtPriceX96, false);

        (bool toxic, uint24 feeUnits, uint160 previewReferenceSqrtPriceX96, uint160 previewPoolSqrtPriceX96) =
            hook.previewSwapFee(key, false);

        assertTrue(toxic);
        assertEq(feeUnits, expectedFee);
        assertGt(previewReferenceSqrtPriceX96, previewPoolSqrtPriceX96);
        assertEq(previewPoolSqrtPriceX96, poolSqrtPriceX96);
    }

    function test_previewSwapFee_keepsBaseFeeForBenignFlow() public {
        oracle.setLatestPrice(_priceWadAtTick(20), block.timestamp);

        (bool toxic, uint24 feeUnits,,) = hook.previewSwapFee(key, true);

        assertFalse(toxic);
        assertEq(feeUnits, BASE_FEE);
    }

    function test_previewSwapFee_revertsWhenCapIsExceeded() public {
        oracle.setLatestPrice(_priceWadAtTick(20), block.timestamp);

        OracleAnchoredLVRHook.Config memory cfg = _defaultConfig();
        cfg.maxFee = 900;
        hook.setConfig(key, cfg);

        vm.expectRevert(
            abi.encodeWithSelector(OracleAnchoredLVRHook.DeviationTooLarge.selector, _expectedFeeUnits(20, false), 900)
        );
        hook.previewSwapFee(key, false);
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

    function test_addLiquidityRevertsWhenPositionIsOffCenter() public {
        ModifyLiquidityParams memory params =
            ModifyLiquidityParams({tickLower: 600, tickUpper: 720, liquidityDelta: 1e18, salt: bytes32(0)});

        vm.expectRevert(
            abi.encodeWithSelector(
                CustomRevert.WrappedError.selector,
                address(hook),
                IHooks.beforeAddLiquidity.selector,
                abi.encodeWithSelector(OracleAnchoredLVRHook.OffCenter.selector, int24(660), int24(0)),
                abi.encodeWithSelector(Hooks.HookCallFailed.selector)
            )
        );
        modifyLiquidityRouter.modifyLiquidity(key, params, ZERO_BYTES);
    }

    function test_addLiquidityRevertsWhenPositionIsTooNarrow() public {
        uint256 minWidthTicks = hook.minWidthTicks(key);
        assertGt(minWidthTicks, 12_000);

        ModifyLiquidityParams memory params =
            ModifyLiquidityParams({tickLower: -6_000, tickUpper: 6_000, liquidityDelta: 1e18, salt: bytes32(0)});

        vm.expectRevert(
            abi.encodeWithSelector(
                CustomRevert.WrappedError.selector,
                address(hook),
                IHooks.beforeAddLiquidity.selector,
                abi.encodeWithSelector(OracleAnchoredLVRHook.WidthTooNarrow.selector, uint256(12_000), minWidthTicks),
                abi.encodeWithSelector(Hooks.HookCallFailed.selector)
            )
        );
        modifyLiquidityRouter.modifyLiquidity(key, params, ZERO_BYTES);
    }

    function test_addLiquiditySucceedsWhenCenteredAndWideEnough() public {
        ModifyLiquidityParams memory params =
            ModifyLiquidityParams({tickLower: -18_000, tickUpper: 18_000, liquidityDelta: 1e18, salt: bytes32("wide")});

        modifyLiquidityRouter.modifyLiquidity(key, params, ZERO_BYTES);
    }

    function _defaultConfig() internal view returns (OracleAnchoredLVRHook.Config memory cfg) {
        cfg = OracleAnchoredLVRHook.Config({
            oracle: oracle,
            baseFee: BASE_FEE,
            maxFee: MAX_FEE,
            maxOracleAge: MAX_ORACLE_AGE,
            latencySecs: LATENCY_SECS,
            centerTolTicks: CENTER_TOLERANCE_TICKS,
            lvrBudgetWad: LVR_BUDGET_WAD
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
            ModifyLiquidityParams({tickLower: tickLower, tickUpper: tickUpper, liquidityDelta: 1e18, salt: bytes32(0)}),
            ZERO_BYTES
        );
    }

    function _expectedFeeUnits(int24 oracleTick, bool zeroForOne) internal pure returns (uint24) {
        return _expectedFeeUnits(TickMath.getSqrtPriceAtTick(oracleTick), SQRT_PRICE_1_1, zeroForOne);
    }

    function _expectedFeeUnits(uint160 referenceSqrtPriceX96, uint160 poolSqrtPriceX96, bool zeroForOne)
        internal
        pure
        returns (uint24)
    {
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
