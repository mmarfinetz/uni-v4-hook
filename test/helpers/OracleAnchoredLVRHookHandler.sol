// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { Test } from "forge-std/Test.sol";
import { OracleAnchoredLVRHook } from "src/OracleAnchoredLVRHook.sol";
import { ChainlinkReferenceOracle } from "src/oracles/ChainlinkReferenceOracle.sol";
import { ManualAggregatorV3 } from "./ManualAggregatorV3.sol";
import { BalanceDelta } from "v4-core/types/BalanceDelta.sol";
import { Currency } from "v4-core/types/Currency.sol";
import { FullMath } from "v4-core/libraries/FullMath.sol";
import { ModifyLiquidityParams, SwapParams } from "v4-core/types/PoolOperation.sol";
import { PoolKey } from "v4-core/types/PoolKey.sol";
import { PoolModifyLiquidityTest } from "v4-core/test/PoolModifyLiquidityTest.sol";
import { PoolSwapTest } from "v4-core/test/PoolSwapTest.sol";
import { TickMath } from "v4-core/libraries/TickMath.sol";
import { FixedPointMathLib } from "solmate/src/utils/FixedPointMathLib.sol";

contract OracleAnchoredLVRHookHandler is Test {
    uint256 internal constant WAD = 1e18;
    uint256 internal constant SQRT_WAD = 1e9;
    uint256 internal constant TOKEN_BALANCE = 1e30;
    uint160 internal constant MIN_PRICE_LIMIT = TickMath.MIN_SQRT_PRICE + 1;
    uint160 internal constant MAX_PRICE_LIMIT = TickMath.MAX_SQRT_PRICE - 1;
    uint32 internal constant MAX_ORACLE_AGE = 1 hours;
    uint32 internal constant CENTER_TOLERANCE_TICKS = 30;
    int24 internal constant MAX_ORACLE_TICK = 200;

    OracleAnchoredLVRHook internal immutable hook;
    ChainlinkReferenceOracle internal immutable oracle;
    ManualAggregatorV3 internal immutable baseFeed;
    ManualAggregatorV3 internal immutable quoteFeed;
    PoolModifyLiquidityTest internal immutable modifyLiquidityRouter;
    PoolSwapTest internal immutable swapRouter;
    PoolKey internal key;

    int24 public ghostCurrentOracleTick;
    uint256 public ghostCenteredAddSuccesses;
    uint256 public ghostUnexpectedOffCenterSuccesses;
    uint256 public ghostUnexpectedNarrowSuccesses;
    uint256 public ghostUnexpectedStaleSwapSuccesses;
    uint256 public ghostLastSuccessfulSwapOraclePriceWad;
    uint256 public ghostLastSuccessfulSwapOracleTs;

    uint256 internal saltNonce;

    constructor(
        OracleAnchoredLVRHook hook_,
        ChainlinkReferenceOracle oracle_,
        ManualAggregatorV3 baseFeed_,
        ManualAggregatorV3 quoteFeed_,
        PoolModifyLiquidityTest modifyLiquidityRouter_,
        PoolSwapTest swapRouter_,
        PoolKey memory key_
    ) {
        hook = hook_;
        oracle = oracle_;
        baseFeed = baseFeed_;
        quoteFeed = quoteFeed_;
        modifyLiquidityRouter = modifyLiquidityRouter_;
        swapRouter = swapRouter_;
        key = key_;

        IERC20(Currency.unwrap(key.currency0))
            .approve(address(modifyLiquidityRouter), type(uint256).max);
        IERC20(Currency.unwrap(key.currency0)).approve(address(swapRouter), type(uint256).max);
        IERC20(Currency.unwrap(key.currency1))
            .approve(address(modifyLiquidityRouter), type(uint256).max);
        IERC20(Currency.unwrap(key.currency1)).approve(address(swapRouter), type(uint256).max);

        _setOracleTick(0);
        _topUpBalances();
    }

    function advanceOracle(uint32 rawSecondsForward, int24 rawTick) external {
        uint256 secondsForward = bound(uint256(rawSecondsForward), 1, 15 minutes);
        vm.warp(block.timestamp + secondsForward);
        _setOracleTick(_clampTick(rawTick));
    }

    function warpTime(uint32 rawSecondsForward) external {
        uint256 secondsForward = bound(uint256(rawSecondsForward), 1, MAX_ORACLE_AGE * 2);
        vm.warp(block.timestamp + secondsForward);
    }

    function swapZeroForOne(uint256 rawAmount) external {
        _attemptSwap(true, rawAmount);
    }

    function swapOneForZero(uint256 rawAmount) external {
        _attemptSwap(false, rawAmount);
    }

    function addCenteredLiquidity(uint16 rawExtraHalfWidthSteps) external {
        _topUpBalances();

        uint256 minWidth;
        try hook.minWidthTicks(key) returns (uint256 width) {
            minWidth = width;
        } catch {
            return;
        }

        int24 spacing = key.tickSpacing;
        uint256 halfWidthSteps = _minHalfWidthSteps(minWidth, spacing);
        halfWidthSteps += bound(uint256(rawExtraHalfWidthSteps), 0, 4);

        int24 midpoint = _floorToSpacing(_currentReferenceTick(), spacing);
        int24 halfWidth = int24(int256(halfWidthSteps * uint256(uint24(spacing))));

        try modifyLiquidityRouter.modifyLiquidity(
            key,
            ModifyLiquidityParams({
                tickLower: midpoint - halfWidth,
                tickUpper: midpoint + halfWidth,
                liquidityDelta: 1e18,
                salt: bytes32(++saltNonce)
            }),
            hex""
        ) returns (
            BalanceDelta
        ) {
            ghostCenteredAddSuccesses++;
        } catch { }
    }

    function attemptOffCenterLiquidity(uint16 rawOffsetSteps, uint16 rawExtraHalfWidthSteps)
        external
    {
        _topUpBalances();

        uint256 minWidth;
        try hook.minWidthTicks(key) returns (uint256 width) {
            minWidth = width;
        } catch {
            return;
        }

        int24 spacing = key.tickSpacing;
        uint256 halfWidthSteps = _minHalfWidthSteps(minWidth, spacing);
        halfWidthSteps += bound(uint256(rawExtraHalfWidthSteps), 0, 4);

        uint256 minOffsetSteps = uint256(CENTER_TOLERANCE_TICKS) / uint256(uint24(spacing)) + 1;
        uint256 offsetSteps = minOffsetSteps + bound(uint256(rawOffsetSteps), 0, 4);

        int24 midpoint = _floorToSpacing(_currentReferenceTick(), spacing)
            + int24(int256(offsetSteps * uint256(uint24(spacing))));
        int24 halfWidth = int24(int256(halfWidthSteps * uint256(uint24(spacing))));

        try modifyLiquidityRouter.modifyLiquidity(
            key,
            ModifyLiquidityParams({
                tickLower: midpoint - halfWidth,
                tickUpper: midpoint + halfWidth,
                liquidityDelta: 1e18,
                salt: bytes32(++saltNonce)
            }),
            hex""
        ) returns (
            BalanceDelta
        ) {
            ghostUnexpectedOffCenterSuccesses++;
        } catch { }
    }

    function attemptTooNarrowLiquidity(uint16 rawShortfallSteps) external {
        _topUpBalances();

        uint256 minWidth;
        try hook.minWidthTicks(key) returns (uint256 width) {
            minWidth = width;
        } catch {
            return;
        }

        int24 spacing = key.tickSpacing;
        uint256 minHalfWidthSteps = _minHalfWidthSteps(minWidth, spacing);
        if (minHalfWidthSteps <= 1) return;

        uint256 shortfallSteps = bound(uint256(rawShortfallSteps), 1, minHalfWidthSteps - 1);
        uint256 halfWidthSteps = minHalfWidthSteps - shortfallSteps;

        int24 midpoint = _floorToSpacing(_currentReferenceTick(), spacing);
        int24 halfWidth = int24(int256(halfWidthSteps * uint256(uint24(spacing))));

        try modifyLiquidityRouter.modifyLiquidity(
            key,
            ModifyLiquidityParams({
                tickLower: midpoint - halfWidth,
                tickUpper: midpoint + halfWidth,
                liquidityDelta: 1e18,
                salt: bytes32(++saltNonce)
            }),
            hex""
        ) returns (
            BalanceDelta
        ) {
            ghostUnexpectedNarrowSuccesses++;
        } catch { }
    }

    function _attemptSwap(bool zeroForOne, uint256 rawAmount) internal {
        _topUpBalances();

        uint256 amount = bound(rawAmount, 1e12, 5e15);
        bool stale = oracleIsStale();

        uint256 oraclePriceWad;
        uint256 oracleLatestFeedTs;
        if (!stale) {
            (oraclePriceWad,, oracleLatestFeedTs) = oracle.latestPriceWad();
        }

        try swapRouter.swap(
            key,
            SwapParams({
                zeroForOne: zeroForOne,
                amountSpecified: -int256(amount),
                sqrtPriceLimitX96: zeroForOne ? MIN_PRICE_LIMIT : MAX_PRICE_LIMIT
            }),
            PoolSwapTest.TestSettings({ takeClaims: false, settleUsingBurn: false }),
            hex""
        ) returns (
            BalanceDelta
        ) {
            if (stale) {
                ghostUnexpectedStaleSwapSuccesses++;
            } else {
                ghostLastSuccessfulSwapOraclePriceWad = oraclePriceWad;
                ghostLastSuccessfulSwapOracleTs = oracleLatestFeedTs;
            }
        } catch { }
    }

    function oracleIsStale() public view returns (bool) {
        (, uint256 updatedAt,) = oracle.latestPriceWad();
        return block.timestamp > updatedAt + MAX_ORACLE_AGE;
    }

    function _setOracleTick(int24 tick) internal {
        ghostCurrentOracleTick = tick;

        uint256 priceWad = _priceWadAtTick(tick);
        baseFeed.setRoundData(int256(priceWad), block.timestamp);
        quoteFeed.setRoundData(int256(WAD), block.timestamp);
    }

    function _topUpBalances() internal {
        deal(Currency.unwrap(key.currency0), address(this), TOKEN_BALANCE);
        deal(Currency.unwrap(key.currency1), address(this), TOKEN_BALANCE);
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

    function _clampTick(int24 tick) internal pure returns (int24) {
        if (tick > MAX_ORACLE_TICK) return MAX_ORACLE_TICK;
        if (tick < -MAX_ORACLE_TICK) return -MAX_ORACLE_TICK;
        return tick;
    }

    function _currentReferenceTick() internal view returns (int24) {
        (uint256 priceWad,,) = oracle.latestPriceWad();
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

    function _priceWadAtTick(int24 tick) internal pure returns (uint256) {
        uint160 sqrtPriceX96 = TickMath.getSqrtPriceAtTick(tick);
        uint256 sqrtPriceWad = FullMath.mulDiv(sqrtPriceX96, SQRT_WAD, 2 ** 96);
        return FullMath.mulDiv(sqrtPriceWad, sqrtPriceWad, 1);
    }
}
