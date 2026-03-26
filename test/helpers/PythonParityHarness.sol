// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import { OracleAnchoredLVRHook } from "../../src/OracleAnchoredLVRHook.sol";
import { ChainlinkReferenceOracle } from "../../src/oracles/ChainlinkReferenceOracle.sol";
import { ManualAggregatorV3 } from "./ManualAggregatorV3.sol";
import { IHooks } from "v4-core/interfaces/IHooks.sol";
import { IPoolManager } from "v4-core/interfaces/IPoolManager.sol";
import { TickMath } from "v4-core/libraries/TickMath.sol";
import { LPFeeLibrary } from "v4-core/libraries/LPFeeLibrary.sol";
import { Currency } from "v4-core/types/Currency.sol";
import { PoolKey } from "v4-core/types/PoolKey.sol";

contract PythonParityHarness {
    uint256 internal constant WAD = 1e18;

    MockExtsloadPoolManager public immutable manager;
    OracleAnchoredLVRHook public immutable hook;
    ChainlinkReferenceOracle public immutable oracle;
    ManualAggregatorV3 public immutable baseFeed;
    ManualAggregatorV3 public immutable quoteFeed;
    PoolKey public key;

    constructor() {
        manager = new MockExtsloadPoolManager();
        hook = new OracleAnchoredLVRHook(IPoolManager(address(manager)), address(this));

        baseFeed = new ManualAggregatorV3(18, int256(WAD), block.timestamp);
        quoteFeed = new ManualAggregatorV3(18, int256(WAD), block.timestamp);
        oracle = new ChainlinkReferenceOracle(baseFeed, false, quoteFeed, false, 18, 18);

        key = PoolKey({
            currency0: Currency.wrap(address(0x1000)),
            currency1: Currency.wrap(address(0x2000)),
            fee: LPFeeLibrary.DYNAMIC_FEE_FLAG,
            tickSpacing: 60,
            hooks: IHooks(address(hook))
        });
        _setConfig(500, 50_000, 10_000, 60, 1e16, 8e14);
        hook.setRiskState(key, 8e14, WAD, block.timestamp);
        manager.setSlot0(TickMath.getSqrtPriceAtTick(0), 0, 3000);
    }

    function previewSwapFeeForPrices(
        uint256 referencePriceWad,
        uint160 poolSqrtPriceX96,
        bool zeroForOne,
        uint24 baseFee,
        uint24 maxFee,
        uint24 alphaBps
    ) external returns (bool toxic, uint24 feeUnits) {
        _setConfig(baseFee, maxFee, alphaBps, 60, 1e16, 8e14);
        baseFeed.setRoundData(int256(referencePriceWad), block.timestamp);
        quoteFeed.setRoundData(int256(WAD), block.timestamp);
        manager.setSlot0(poolSqrtPriceX96, TickMath.getTickAtSqrtPrice(poolSqrtPriceX96), 3000);
        (toxic, feeUnits,,) = hook.previewSwapFee(key, zeroForOne);
    }

    function minWidthForRisk(
        uint256 sigma2PerSecondWad,
        uint32 latencySecs,
        uint256 lvrBudgetWad,
        uint256 bootstrapSigma2PerSecondWad
    ) external returns (uint256) {
        _setConfig(500, 50_000, 10_000, latencySecs, lvrBudgetWad, bootstrapSigma2PerSecondWad);
        hook.setRiskState(key, sigma2PerSecondWad, WAD, block.timestamp);
        return hook.minWidthTicks(key);
    }

    function _setConfig(
        uint24 baseFee,
        uint24 maxFee,
        uint24 alphaBps,
        uint32 latencySecs,
        uint256 lvrBudgetWad,
        uint256 bootstrapSigma2PerSecondWad
    ) internal {
        hook.setConfig(
            key,
            OracleAnchoredLVRHook.Config({
                oracle: oracle,
                baseFee: baseFee,
                maxFee: maxFee,
                alphaBps: alphaBps,
                maxOracleAge: 3600,
                latencySecs: latencySecs,
                centerTolTicks: 30,
                lvrBudgetWad: lvrBudgetWad,
                bootstrapSigma2PerSecondWad: bootstrapSigma2PerSecondWad
            })
        );
    }
}

contract MockExtsloadPoolManager {
    bytes32 internal slot0Word;

    function setSlot0(uint160 sqrtPriceX96, int24 tick, uint24 lpFee) external {
        uint24 tickBits = uint24(uint32(int32(tick)));
        slot0Word = bytes32(
            uint256(sqrtPriceX96) | (uint256(tickBits) << 160) | (uint256(lpFee) << 208)
        );
    }

    function extsload(bytes32) external view returns (bytes32) {
        return slot0Word;
    }
}
