// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import { Script } from "forge-std/Script.sol";
import { console2 } from "forge-std/console2.sol";

import { Currency } from "v4-core/types/Currency.sol";
import { FullMath } from "v4-core/libraries/FullMath.sol";
import { IHooks } from "v4-core/interfaces/IHooks.sol";
import { IPoolManager } from "v4-core/interfaces/IPoolManager.sol";
import { PoolId, PoolIdLibrary } from "v4-core/types/PoolId.sol";
import { PoolKey } from "v4-core/types/PoolKey.sol";
import { ModifyLiquidityParams } from "v4-core/types/PoolOperation.sol";
import { PoolModifyLiquidityTest } from "v4-core/test/PoolModifyLiquidityTest.sol";
import { TickMath } from "v4-core/libraries/TickMath.sol";
import { FixedPointMathLib } from "solmate/src/utils/FixedPointMathLib.sol";

import { OracleAnchoredLVRHook } from "src/OracleAnchoredLVRHook.sol";
import { ChainlinkReferenceOracle } from "src/oracles/ChainlinkReferenceOracle.sol";

/// @notice Deploys the hookless control pool for the side-by-side demo: same
/// demo tokens, static fee tier (default 30 bps, the classic v3 tier), no hook,
/// initialized at the demo oracle's current reference price and seeded with the
/// same liquidity shape as the hooked demo pool. From that point both pools see
/// identical reference moves, so LP outcomes are directly comparable
/// (solver_bot.py compare).
///
/// Usage:
///   HOOK=0x... ORACLE=0x... TOKEN0=0x... TOKEN1=0x... LIQUIDITY_ROUTER=0x... \
///   forge script script/DeployBaselinePool.s.sol:DeployBaselinePool \
///     --rpc-url base_sepolia --private-key $DEPLOYER_KEY --broadcast
contract DeployBaselinePool is Script {
    using PoolIdLibrary for PoolKey;

    uint256 internal constant WAD = 1e18;
    uint256 internal constant Q96 = 2 ** 96;
    uint256 internal constant SQRT_WAD = 1e9;

    int24 internal constant TICK_SPACING = 60;
    int24 internal constant LIQUIDITY_HALF_WIDTH_TICKS = 12_000;
    int256 internal constant SEED_LIQUIDITY = 1000e18;

    function run() external {
        OracleAnchoredLVRHook hook = OracleAnchoredLVRHook(vm.envAddress("HOOK"));
        IPoolManager poolManager = hook.poolManager();
        ChainlinkReferenceOracle oracle = ChainlinkReferenceOracle(vm.envAddress("ORACLE"));
        PoolModifyLiquidityTest liquidityRouter =
            PoolModifyLiquidityTest(vm.envAddress("LIQUIDITY_ROUTER"));

        PoolKey memory key = PoolKey({
            currency0: Currency.wrap(vm.envAddress("TOKEN0")),
            currency1: Currency.wrap(vm.envAddress("TOKEN1")),
            fee: uint24(vm.envOr("STATIC_FEE", uint256(3000))),
            tickSpacing: TICK_SPACING,
            hooks: IHooks(address(0))
        });

        (uint256 referencePriceWad,,) = oracle.latestPriceWad();
        uint160 sqrtPriceX96 = _priceWadToSqrtPriceX96(referencePriceWad);

        vm.startBroadcast();
        int24 tick = poolManager.initialize(key, sqrtPriceX96);
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

        console2.log("baseline static fee:", key.fee);
        console2.log("initialized at tick:", tick);
        console2.log("pool id:");
        console2.logBytes32(PoolId.unwrap(key.toId()));
    }

    function _priceWadToSqrtPriceX96(uint256 priceWad) internal pure returns (uint160) {
        require(priceWad != 0, "DeployBaselinePool: zero oracle price");
        uint256 scaled = FullMath.mulDiv(FixedPointMathLib.sqrt(priceWad), Q96, SQRT_WAD);
        require(
            scaled >= TickMath.MIN_SQRT_PRICE && scaled < TickMath.MAX_SQRT_PRICE,
            "DeployBaselinePool: price outside range"
        );
        return uint160(scaled);
    }
}
