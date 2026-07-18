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
import { TickMath } from "v4-core/libraries/TickMath.sol";
import { FixedPointMathLib } from "solmate/src/utils/FixedPointMathLib.sol";

import { IChainlinkAggregatorV3 } from "src/interfaces/IChainlinkAggregatorV3.sol";
import { OracleAnchoredLVRHook } from "src/OracleAnchoredLVRHook.sol";
import { ChainlinkReferenceOracle } from "src/oracles/ChainlinkReferenceOracle.sol";

interface IERC20Decimals {
    function decimals() external view returns (uint8);
}

/// @notice Deploys a ChainlinkReferenceOracle for a token pair, initializes a
/// dynamic-fee pool on the hook at the oracle price, and writes the hook config
/// (defaults are the recommended cell from the Dutch-auction study).
///
/// The broadcast sender must be the hook owner, and TOKEN0 < TOKEN1 must already
/// be sorted. BASE_FEED is token0's asset/USD feed and QUOTE_FEED is token1's
/// asset/USD feed (leave QUOTE_FEED unset if the base feed alone quotes the pair).
///
/// Usage (see docs/deployment.md):
///   HOOK=0x... TOKEN0=0x... TOKEN1=0x... BASE_FEED=0x... QUOTE_FEED=0x... \
///   forge script script/DeployPool.s.sol:DeployPool \
///     --rpc-url base_sepolia --private-key $DEPLOYER_KEY --broadcast
contract DeployPool is Script {
    using PoolIdLibrary for PoolKey;

    uint256 internal constant WAD = 1e18;
    uint256 internal constant Q96 = 2 ** 96;
    uint256 internal constant SQRT_WAD = 1e9;

    function run() external returns (ChainlinkReferenceOracle oracle, PoolKey memory key) {
        OracleAnchoredLVRHook hook = OracleAnchoredLVRHook(vm.envAddress("HOOK"));
        IPoolManager poolManager = hook.poolManager();

        address token0 = vm.envAddress("TOKEN0");
        address token1 = vm.envAddress("TOKEN1");
        require(token0 < token1, "DeployPool: TOKEN0/TOKEN1 must be sorted ascending");

        IChainlinkAggregatorV3 baseFeed = IChainlinkAggregatorV3(vm.envAddress("BASE_FEED"));
        IChainlinkAggregatorV3 quoteFeed =
            IChainlinkAggregatorV3(vm.envOr("QUOTE_FEED", address(0)));
        bool invertBase = vm.envOr("INVERT_BASE", false);
        bool invertQuote = vm.envOr("INVERT_QUOTE", false);

        // On L2 targets, set SEQUENCER_UPTIME_FEED to the chain's Chainlink
        // sequencer uptime feed (see https://docs.chain.link/data-feeds/l2-sequencer-feeds)
        // so oracle reads fail closed during outages and for the grace period after
        // recovery. Leave unset to disable (L1 or chains without a feed).
        IChainlinkAggregatorV3 sequencerUptimeFeed =
            IChainlinkAggregatorV3(vm.envOr("SEQUENCER_UPTIME_FEED", address(0)));
        uint256 sequencerGracePeriod = vm.envOr("SEQUENCER_GRACE_PERIOD", uint256(3600));

        int24 tickSpacing = int24(uint24(vm.envOr("TICK_SPACING", uint256(60))));
        key = PoolKey({
            currency0: Currency.wrap(token0),
            currency1: Currency.wrap(token1),
            fee: LPFeeLibrary.DYNAMIC_FEE_FLAG,
            tickSpacing: tickSpacing,
            hooks: IHooks(address(hook))
        });

        OracleAnchoredLVRHook.Config memory cfg = _configFromEnv();

        vm.startBroadcast();
        oracle = new ChainlinkReferenceOracle(
            baseFeed,
            invertBase,
            quoteFeed,
            invertQuote,
            IERC20Decimals(token0).decimals(),
            IERC20Decimals(token1).decimals(),
            sequencerUptimeFeed,
            sequencerGracePeriod
        );
        cfg.oracle = oracle;

        (uint256 referencePriceWad,,) = oracle.latestPriceWad();
        uint160 sqrtPriceX96 = _priceWadToSqrtPriceX96(referencePriceWad);
        int24 tick = poolManager.initialize(key, sqrtPriceX96);

        hook.setConfig(key, cfg);
        vm.stopBroadcast();

        console2.log("ChainlinkReferenceOracle:", address(oracle));
        console2.log("  reference price (WAD):  ", referencePriceWad);
        console2.log("  initialized at tick:    ", tick);
        console2.log("  pool id:");
        console2.logBytes32(PoolId.unwrap(key.toId()));
    }

    /// @dev Defaults are the recommended Dutch-auction cell from reports/: 5 bps
    /// base fee, 2500 bps max fee, 10 bps trigger gap, 10 bps starting concession,
    /// 0.5 bps/sec concession growth. The bootstrap sigma^2 default corresponds to
    /// roughly 5% daily volatility and is replaced by the EWMA after first swaps.
    function _configFromEnv() internal view returns (OracleAnchoredLVRHook.Config memory cfg) {
        cfg = OracleAnchoredLVRHook.Config({
            oracle: ChainlinkReferenceOracle(address(0)),
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

    /// @dev Mirrors OracleAnchoredLVRHook._priceWadToSqrtPriceX96 so the pool
    /// starts exactly at the oracle reference price (zero stale gap).
    function _priceWadToSqrtPriceX96(uint256 priceWad) internal pure returns (uint160) {
        require(priceWad != 0, "DeployPool: zero oracle price");
        uint256 sqrtPriceWad = FixedPointMathLib.sqrt(priceWad);
        uint256 scaled = FullMath.mulDiv(sqrtPriceWad, Q96, SQRT_WAD);
        require(
            scaled >= TickMath.MIN_SQRT_PRICE && scaled < TickMath.MAX_SQRT_PRICE,
            "DeployPool: oracle price outside pool range"
        );
        return uint160(scaled);
    }
}
