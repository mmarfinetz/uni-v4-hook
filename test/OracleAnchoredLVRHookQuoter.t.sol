// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import { Test } from "forge-std/Test.sol";
import { OracleAnchoredLVRHook } from "src/OracleAnchoredLVRHook.sol";
import { ChainlinkReferenceOracle } from "src/oracles/ChainlinkReferenceOracle.sol";
import { IChainlinkAggregatorV3 } from "src/interfaces/IChainlinkAggregatorV3.sol";
import { Deployers } from "../lib/v4-core/test/utils/Deployers.sol";
import { IHooks } from "v4-core/interfaces/IHooks.sol";
import { IPoolManager } from "v4-core/interfaces/IPoolManager.sol";
import { Hooks } from "v4-core/libraries/Hooks.sol";
import { LPFeeLibrary } from "v4-core/libraries/LPFeeLibrary.sol";
import { FullMath } from "v4-core/libraries/FullMath.sol";
import { TickMath } from "v4-core/libraries/TickMath.sol";
import { ModifyLiquidityParams } from "v4-core/types/PoolOperation.sol";
import { BalanceDelta } from "v4-core/types/BalanceDelta.sol";
import { PoolKey } from "v4-core/types/PoolKey.sol";
import { PoolIdLibrary } from "v4-core/types/PoolId.sol";
import { V4Quoter } from "v4-periphery/src/lens/V4Quoter.sol";
import { IV4Quoter } from "v4-periphery/src/interfaces/IV4Quoter.sol";
import { ManualAggregatorV3 } from "./helpers/ManualAggregatorV3.sol";

/// @notice Proves the hooked pool is quotable by Uniswap's own revert-based
/// V4Quoter — the same simulation pattern routers and aggregators use. The
/// dynamic fee, the open Dutch auction, and the fail-closed oracle path are all
/// captured by a standard quote with no hook-specific integration.
contract OracleAnchoredLVRHookQuoterTest is Test, Deployers {
    using PoolIdLibrary for PoolKey;

    uint256 internal constant WAD = 1e18;
    uint256 internal constant SQRT_WAD = 1e9;
    uint24 internal constant BASE_FEE = 500;
    uint24 internal constant MAX_FEE = 250_000;
    uint24 internal constant ALPHA_BPS = 10_000;
    uint32 internal constant MAX_ORACLE_AGE = 1 hours;
    uint32 internal constant LATENCY_SECS = 60;
    uint32 internal constant CENTER_TOLERANCE_TICKS = 30;
    uint256 internal constant LVR_BUDGET_WAD = 1e16;
    uint256 internal constant SIGMA2_PER_SECOND_WAD = 4e14;
    uint256 internal constant BOOTSTRAP_SIGMA2_PER_SECOND_WAD = 8e14;
    uint24 internal constant TRIGGER_GAP_BPS = 10;
    uint256 internal constant START_CONCESSION_WAD = 1e15;
    uint256 internal constant CONCESSION_GROWTH_WAD_PER_SEC = 5e13;
    uint256 internal constant MAX_CONCESSION_WAD = WAD;

    OracleAnchoredLVRHook internal hook;
    ChainlinkReferenceOracle internal oracle;
    ManualAggregatorV3 internal baseFeed;
    ManualAggregatorV3 internal quoteFeed;
    V4Quoter internal quoter;

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
        oracle = new ChainlinkReferenceOracle(
            baseFeed, false, quoteFeed, false, 18, 18, IChainlinkAggregatorV3(address(0)), 0
        );

        (key,) = initPool(
            currency0,
            currency1,
            IHooks(address(hook)),
            LPFeeLibrary.DYNAMIC_FEE_FLAG,
            SQRT_PRICE_1_1
        );

        hook.setConfig(key, _config());
        hook.setRiskState(key, SIGMA2_PER_SECOND_WAD, WAD, block.timestamp);
        _addLiquidity(-12_000, 12_000);

        quoter = new V4Quoter(IPoolManager(manager));
    }

    function test_quoter_benignSwapQuoteMatchesExecution() public {
        // Pool at the oracle price: both directions benign, base fee applies.
        (uint256 quoted,) = quoter.quoteExactInputSingle(
            IV4Quoter.QuoteExactSingleParams({
                poolKey: key,
                zeroForOne: true,
                exactAmount: 1e15,
                hookData: ZERO_BYTES
            })
        );

        BalanceDelta delta = swap(key, true, -1e15, ZERO_BYTES);

        assertGt(quoted, 0);
        assertEq(int256(quoted), int256(delta.amount1()));
    }

    function test_quoter_toxicSwapWithOpenAuctionQuoteMatchesExecution() public {
        // Gap above trigger opens the auction; the aged concession discounts the
        // toxic surcharge. The quote must capture the whole schedule.
        _setOraclePrice(_priceWadAtTick(20), block.timestamp);
        hook.pokeAuction(key);
        vm.warp(block.timestamp + 100);
        _setOraclePrice(_priceWadAtTick(20), block.timestamp);

        (uint256 quoted,) = quoter.quoteExactInputSingle(
            IV4Quoter.QuoteExactSingleParams({
                poolKey: key,
                zeroForOne: false,
                exactAmount: 1e15,
                hookData: ZERO_BYTES
            })
        );

        BalanceDelta delta = swap(key, false, -1e15, ZERO_BYTES);

        assertGt(quoted, 0);
        assertEq(int256(quoted), int256(delta.amount0()));
    }

    function test_quoter_staleOracleRevertsInsteadOfMisquoting() public {
        vm.warp(block.timestamp + MAX_ORACLE_AGE + 1);

        vm.expectRevert();
        quoter.quoteExactInputSingle(
            IV4Quoter.QuoteExactSingleParams({
                poolKey: key,
                zeroForOne: true,
                exactAmount: 1e15,
                hookData: ZERO_BYTES
            })
        );
    }

    function test_quotable_tracksOracleHealthWithoutReverting() public {
        assertTrue(hook.quotable(key));

        vm.warp(block.timestamp + MAX_ORACLE_AGE + 1);
        assertFalse(hook.quotable(key));

        _setOraclePrice(WAD, block.timestamp);
        assertTrue(hook.quotable(key));
    }

    function test_quotable_falseForUnconfiguredPool() public view {
        PoolKey memory unconfigured = PoolKey({
            currency0: key.currency0,
            currency1: key.currency1,
            fee: LPFeeLibrary.DYNAMIC_FEE_FLAG,
            tickSpacing: key.tickSpacing + 1,
            hooks: key.hooks
        });
        assertFalse(hook.quotable(unconfigured));
    }

    function _config() internal view returns (OracleAnchoredLVRHook.Config memory cfg) {
        cfg = OracleAnchoredLVRHook.Config({
            oracle: oracle,
            baseFee: BASE_FEE,
            maxFee: MAX_FEE,
            alphaBps: ALPHA_BPS,
            maxOracleAge: MAX_ORACLE_AGE,
            latencySecs: LATENCY_SECS,
            centerTolTicks: CENTER_TOLERANCE_TICKS,
            lvrBudgetWad: LVR_BUDGET_WAD,
            bootstrapSigma2PerSecondWad: BOOTSTRAP_SIGMA2_PER_SECOND_WAD,
            triggerGapBps: TRIGGER_GAP_BPS,
            startConcessionWad: START_CONCESSION_WAD,
            concessionGrowthWadPerSec: CONCESSION_GROWTH_WAD_PER_SEC,
            maxConcessionWad: MAX_CONCESSION_WAD
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

    function _priceWadAtTick(int24 tick) internal pure returns (uint256) {
        uint160 sqrtPriceX96 = TickMath.getSqrtPriceAtTick(tick);
        uint256 sqrtPriceWad = FullMath.mulDiv(sqrtPriceX96, SQRT_WAD, 2 ** 96);
        return FullMath.mulDiv(sqrtPriceWad, sqrtPriceWad, 1);
    }
}
