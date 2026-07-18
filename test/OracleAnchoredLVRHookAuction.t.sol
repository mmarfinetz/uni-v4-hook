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
import { ModifyLiquidityParams, SwapParams } from "v4-core/types/PoolOperation.sol";
import { PoolSwapTest } from "v4-core/test/PoolSwapTest.sol";
import { PoolKey } from "v4-core/types/PoolKey.sol";
import { PoolId, PoolIdLibrary } from "v4-core/types/PoolId.sol";
import { Currency } from "v4-core/types/Currency.sol";
import { StateLibrary } from "v4-core/libraries/StateLibrary.sol";
import { MockERC20 } from "solmate/src/test/utils/mocks/MockERC20.sol";
import { ManualAggregatorV3 } from "./helpers/ManualAggregatorV3.sol";

contract OracleAnchoredLVRHookAuctionTest is Test, Deployers {
    using PoolIdLibrary for PoolKey;
    using StateLibrary for IPoolManager;

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

    // Recommended grid cell: 10 bps trigger, 10 bps start concession, 0.5 bps/sec growth.
    uint24 internal constant TRIGGER_GAP_BPS = 10;
    uint256 internal constant START_CONCESSION_WAD = 1e15;
    uint256 internal constant CONCESSION_GROWTH_WAD_PER_SEC = 5e13;
    uint256 internal constant MAX_CONCESSION_WAD = WAD;

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

        hook.setConfig(key, _auctionConfig());
        hook.setRiskState(key, SIGMA2_PER_SECOND_WAD, WAD, block.timestamp);

        _addLiquidity(-12_000, 12_000);
    }

    function test_auction_belowTriggerKeepsExactFee() public {
        // Tick 5 is roughly a 5 bps gap, below the 10 bps trigger.
        _setOraclePrice(_priceWadAtTick(5), block.timestamp);

        (bool toxic, uint24 feeUnits,,) = hook.previewSwapFee(key, false);

        assertTrue(toxic);
        assertEq(feeUnits, _expectedFeeUnits(5, 0));

        (bool eligible, uint64 startTs,,) = hook.auctionStatus(key);
        assertFalse(eligible);
        assertEq(startTs, 0);
    }

    function test_auction_eligibleGapAppliesStartConcession() public {
        // Tick 20 is roughly a 20 bps gap, above the 10 bps trigger.
        _setOraclePrice(_priceWadAtTick(20), block.timestamp);

        (bool eligible,, uint256 concessionWad,) = hook.auctionStatus(key);
        assertTrue(eligible);
        assertEq(concessionWad, START_CONCESSION_WAD);

        (bool toxic, uint24 feeUnits,,) = hook.previewSwapFee(key, false);
        assertTrue(toxic);
        assertEq(feeUnits, _expectedFeeUnits(20, START_CONCESSION_WAD));
        assertLt(feeUnits, _expectedFeeUnits(20, 0));
    }

    function test_auction_benignDirectionStillPaysBaseFeeDuringAuction() public {
        _setOraclePrice(_priceWadAtTick(20), block.timestamp);

        (bool toxic, uint24 feeUnits,,) = hook.previewSwapFee(key, true);

        assertFalse(toxic);
        assertEq(feeUnits, BASE_FEE);
    }

    function test_pokeAuction_opensClockWithoutSwap() public {
        _setOraclePrice(_priceWadAtTick(20), block.timestamp);

        (bool open, uint256 concessionWad) = hook.pokeAuction(key);

        assertTrue(open);
        assertEq(concessionWad, START_CONCESSION_WAD);
        assertEq(hook.auctionStartTs(key.toId()), uint64(block.timestamp));
    }

    function test_auction_permissionless_strangerPokesAndStrangerFills() public {
        _setOraclePrice(_priceWadAtTick(20), block.timestamp);

        // The poker is neither the owner nor an LP and holds no tokens; the
        // clock still opens from the visible gap.
        vm.prank(makeAddr("poker"));
        (bool open, uint256 concessionWad) = hook.pokeAuction(key);
        assertTrue(open);
        assertEq(concessionWad, START_CONCESSION_WAD);

        uint256 elapsed = 100;
        vm.warp(block.timestamp + elapsed);
        _setOraclePrice(_priceWadAtTick(20), block.timestamp);
        uint256 agedConcession = START_CONCESSION_WAD + elapsed * CONCESSION_GROWTH_WAD_PER_SEC;
        (, uint24 quotedFee,,) = hook.previewSwapFee(key, false);
        assertEq(quotedFee, _expectedFeeUnits(20, agedConcession));

        // A different stranger fills through the standard swap router with plain
        // hook data — no allowlist, no solver registration, no special calldata.
        address solver = makeAddr("solver");
        MockERC20 token1 = MockERC20(Currency.unwrap(currency1));
        token1.transfer(solver, 1e18);
        (uint160 poolPriceBefore,,,) = manager.getSlot0(key.toId());

        vm.startPrank(solver);
        token1.approve(address(swapRouter), type(uint256).max);
        swapRouter.swap(
            key,
            SwapParams({
                zeroForOne: false,
                amountSpecified: -1e15,
                sqrtPriceLimitX96: TickMath.MAX_SQRT_PRICE - 1
            }),
            PoolSwapTest.TestSettings({ takeClaims: false, settleUsingBurn: false }),
            ZERO_BYTES
        );
        vm.stopPrank();

        // The fill moved the pool toward the oracle price.
        (uint160 poolPriceAfter,,,) = manager.getSlot0(key.toId());
        assertGt(poolPriceAfter, poolPriceBefore);
    }

    function test_auction_concessionGrowsWithElapsedTime() public {
        _setOraclePrice(_priceWadAtTick(20), block.timestamp);
        hook.pokeAuction(key);

        uint256 elapsed = 100;
        vm.warp(block.timestamp + elapsed);
        _setOraclePrice(_priceWadAtTick(20), block.timestamp);

        uint256 expectedConcession = START_CONCESSION_WAD + elapsed * CONCESSION_GROWTH_WAD_PER_SEC;

        (,, uint256 concessionWad,) = hook.auctionStatus(key);
        assertEq(concessionWad, expectedConcession);

        (, uint24 feeUnits,,) = hook.previewSwapFee(key, false);
        assertEq(feeUnits, _expectedFeeUnits(20, expectedConcession));
    }

    function test_auction_concessionCapsAtWadAndFeeFloorsAtBase() public {
        _setOraclePrice(_priceWadAtTick(20), block.timestamp);
        hook.pokeAuction(key);

        // Long enough for the linear schedule to exceed 100% of the surcharge.
        vm.warp(block.timestamp + 30_000);
        _setOraclePrice(_priceWadAtTick(20), block.timestamp);

        (,, uint256 concessionWad,) = hook.auctionStatus(key);
        assertEq(concessionWad, WAD);

        (, uint24 feeUnits,,) = hook.previewSwapFee(key, false);
        assertEq(feeUnits, BASE_FEE);
    }

    function test_auction_swapOpensClockLazily() public {
        _setOraclePrice(_priceWadAtTick(100), block.timestamp);

        assertEq(hook.auctionStartTs(key.toId()), 0);
        swap(key, false, -1e15, ZERO_BYTES);
        assertEq(hook.auctionStartTs(key.toId()), uint64(block.timestamp));
    }

    function test_auction_closesAfterRepricing() public {
        _setOraclePrice(_priceWadAtTick(20), block.timestamp);
        hook.pokeAuction(key);
        assertGt(hook.auctionStartTs(key.toId()), 0);

        // Gap closes (oracle back at the pool price); the next poke clears the clock.
        _setOraclePrice(_priceWadAtTick(0), block.timestamp);
        (bool open,) = hook.pokeAuction(key);

        assertFalse(open);
        assertEq(hook.auctionStartTs(key.toId()), 0);
    }

    function test_auction_escapesMaxFeeDeadlockAsConcessionGrows() public {
        OracleAnchoredLVRHook.Config memory cfg = _auctionConfig();
        cfg.maxFee = 10_000;
        hook.setConfig(key, cfg);

        // Roughly a 2% gap: the undiscounted exact fee (~2%) exceeds maxFee (1%),
        // so repricing is initially deterred (fail closed).
        _setOraclePrice(_priceWadAtTick(400), block.timestamp);
        hook.pokeAuction(key);

        vm.expectRevert();
        swap(key, false, -1e15, ZERO_BYTES);

        // The concession keeps accruing; once the discounted fee drops under maxFee
        // the repricing swap clears. 11,000s => concession ~0.551.
        vm.warp(block.timestamp + 11_000);
        _setOraclePrice(_priceWadAtTick(400), block.timestamp);

        (, uint24 feeUnits,,) = hook.previewSwapFee(key, false);
        assertLe(feeUnits, cfg.maxFee);
        assertGe(feeUnits, BASE_FEE);

        swap(key, false, -1e15, ZERO_BYTES);
    }

    function test_auction_disabledWhenTriggerIsZero() public {
        OracleAnchoredLVRHook.Config memory cfg = _auctionConfig();
        cfg.triggerGapBps = 0;
        hook.setConfig(key, cfg);

        _setOraclePrice(_priceWadAtTick(100), block.timestamp);

        (, uint24 feeUnits,,) = hook.previewSwapFee(key, false);
        assertEq(feeUnits, _expectedFeeUnits(100, 0));

        swap(key, false, -1e15, ZERO_BYTES);
        assertEq(hook.auctionStartTs(key.toId()), 0);
    }

    function test_setConfig_rejectsConcessionAboveWad() public {
        OracleAnchoredLVRHook.Config memory cfg = _auctionConfig();
        cfg.startConcessionWad = WAD + 1;

        vm.expectRevert(OracleAnchoredLVRHook.InvalidConfig.selector);
        hook.setConfig(key, cfg);

        cfg = _auctionConfig();
        cfg.concessionGrowthWadPerSec = WAD + 1;

        vm.expectRevert(OracleAnchoredLVRHook.InvalidConfig.selector);
        hook.setConfig(key, cfg);
    }

    function test_setConfig_rejectsInvalidMaxConcession() public {
        // Zero ceiling with the auction enabled would silently disable all concessions.
        OracleAnchoredLVRHook.Config memory cfg = _auctionConfig();
        cfg.maxConcessionWad = 0;
        vm.expectRevert(OracleAnchoredLVRHook.InvalidConfig.selector);
        hook.setConfig(key, cfg);

        cfg = _auctionConfig();
        cfg.maxConcessionWad = WAD + 1;
        vm.expectRevert(OracleAnchoredLVRHook.InvalidConfig.selector);
        hook.setConfig(key, cfg);

        cfg = _auctionConfig();
        cfg.startConcessionWad = 2e17;
        cfg.maxConcessionWad = 1e17;
        vm.expectRevert(OracleAnchoredLVRHook.InvalidConfig.selector);
        hook.setConfig(key, cfg);

        // A disabled auction does not require the ceiling.
        cfg = _auctionConfig();
        cfg.triggerGapBps = 0;
        cfg.startConcessionWad = 0;
        cfg.concessionGrowthWadPerSec = 0;
        cfg.maxConcessionWad = 0;
        hook.setConfig(key, cfg);
    }

    function test_auction_maxConcessionCapsScheduledConcessionAndFeeFloor() public {
        OracleAnchoredLVRHook.Config memory cfg = _auctionConfig();
        cfg.maxConcessionWad = 5e17;
        hook.setConfig(key, cfg);

        _setOraclePrice(_priceWadAtTick(20), block.timestamp);
        hook.pokeAuction(key);

        // Long enough for the linear schedule to exceed 100% of the surcharge; the
        // ceiling holds the concession at 50%, so the LP always keeps half.
        vm.warp(block.timestamp + 30_000);
        _setOraclePrice(_priceWadAtTick(20), block.timestamp);

        (,, uint256 concessionWad,) = hook.auctionStatus(key);
        assertEq(concessionWad, 5e17);

        (, uint24 feeUnits,,) = hook.previewSwapFee(key, false);
        assertEq(feeUnits, _expectedFeeUnits(20, 5e17));
        assertGt(feeUnits, BASE_FEE);
    }

    function test_auction_maxConcessionBelowWadKeepsExtremeGapDeterred() public {
        // Counterpart of test_auction_escapesMaxFeeDeadlockAsConcessionGrows: a
        // sub-WAD ceiling caps how far the discounted fee can fall, so a gap whose
        // floored fee still exceeds maxFee stays fail-closed no matter how long the
        // auction runs. Governance trades patient-solver protection against
        // repriceability of extreme gaps.
        OracleAnchoredLVRHook.Config memory cfg = _auctionConfig();
        cfg.maxFee = 10_000;
        cfg.maxConcessionWad = 2e17;
        hook.setConfig(key, cfg);

        // Roughly a 2% gap: floored fee = base + 0.8 * surcharge (~1.66%) > maxFee (1%).
        _setOraclePrice(_priceWadAtTick(400), block.timestamp);
        hook.pokeAuction(key);

        vm.expectRevert();
        swap(key, false, -1e15, ZERO_BYTES);

        vm.warp(block.timestamp + 100_000);
        _setOraclePrice(_priceWadAtTick(400), block.timestamp);

        vm.expectRevert(
            abi.encodeWithSelector(
                OracleAnchoredLVRHook.DeviationTooLarge.selector,
                _expectedFeeUnits(400, 2e17),
                cfg.maxFee
            )
        );
        hook.previewSwapFee(key, false);

        vm.expectRevert();
        swap(key, false, -1e15, ZERO_BYTES);
    }

    function test_auction_clockResetGriefing_partialRepriceRestartsConcession() public {
        // Adversarial vector: a griefer who wants repricing to stay expensive nudges
        // the pool just inside the trigger. The auction closes, the clock is deleted,
        // and when the gap re-widens the concession restarts from the beginning, so
        // accrued discount is destroyed at the cost of one small toxic swap.
        _setOraclePrice(_priceWadAtTick(20), block.timestamp);
        hook.pokeAuction(key);

        uint256 elapsed = 4000;
        vm.warp(block.timestamp + elapsed);
        _setOraclePrice(_priceWadAtTick(20), block.timestamp);

        uint256 agedConcession = START_CONCESSION_WAD + elapsed * CONCESSION_GROWTH_WAD_PER_SEC;
        (,, uint256 concessionBefore,) = hook.auctionStatus(key);
        assertEq(concessionBefore, agedConcession);

        // Partial reprice to tick 12: the remaining 8-tick gap (~4 bps premium) sits
        // below the 10 bps trigger, so the next interaction closes the auction.
        swapRouter.swap(
            key,
            SwapParams({
                zeroForOne: false,
                amountSpecified: -1e18,
                sqrtPriceLimitX96: TickMath.getSqrtPriceAtTick(12)
            }),
            PoolSwapTest.TestSettings({ takeClaims: false, settleUsingBurn: false }),
            ZERO_BYTES
        );

        (bool open,) = hook.pokeAuction(key);
        assertFalse(open);
        assertEq(hook.auctionStartTs(key.toId()), 0);

        // The gap re-widens past the trigger; the clock restarts at the starting
        // concession instead of the accrued one.
        _setOraclePrice(_priceWadAtTick(32), block.timestamp);
        (bool reopened, uint256 concessionAfter) = hook.pokeAuction(key);

        assertTrue(reopened);
        assertEq(concessionAfter, START_CONCESSION_WAD);
        assertLt(concessionAfter, agedConcession);
    }

    function test_auction_gapDirectionFlipInheritsAgedConcession() public {
        // The gap premium is unsigned, so if the reference crosses the pool price
        // while an auction is open, the clock keeps running and the brand-new gap in
        // the opposite direction inherits the aged (larger) concession.
        _setOraclePrice(_priceWadAtTick(20), block.timestamp);
        hook.pokeAuction(key);
        uint64 startTs = hook.auctionStartTs(key.toId());

        uint256 elapsed = 4000;
        vm.warp(block.timestamp + elapsed);
        uint256 agedConcession = START_CONCESSION_WAD + elapsed * CONCESSION_GROWTH_WAD_PER_SEC;

        // Reference flips to the other side of the pool with the same 20 bps size.
        _setOraclePrice(_priceWadAtTick(-20), block.timestamp);

        (bool eligible,, uint256 concessionWad,) = hook.auctionStatus(key);
        assertTrue(eligible);
        assertEq(hook.auctionStartTs(key.toId()), startTs);
        assertEq(concessionWad, agedConcession);

        // Toxic direction flipped to zeroForOne and pays the aged discount from the
        // first swap in the new direction.
        (bool toxic, uint24 discountedFee,,) = hook.previewSwapFee(key, true);
        assertTrue(toxic);

        uint160 referenceSqrtPriceX96 = TickMath.getSqrtPriceAtTick(-20);
        uint256 premiumWad = FullMath.mulDiv(SQRT_PRICE_1_1, WAD, referenceSqrtPriceX96) - WAD;
        uint256 surchargeWad = FullMath.mulDiv(premiumWad, ALPHA_BPS, 10_000);
        surchargeWad -= FullMath.mulDiv(surchargeWad, agedConcession, WAD);
        assertEq(discountedFee, uint24((uint256(BASE_FEE) * 1e12 + surchargeWad) / 1e12));
    }

    function testFuzz_auction_concessionMonotoneWhileGapPersists(uint32 earlier, uint32 later)
        public
    {
        earlier = uint32(bound(earlier, 0, 50_000));
        later = uint32(bound(later, earlier, 60_000));

        _setOraclePrice(_priceWadAtTick(20), block.timestamp);
        hook.pokeAuction(key);
        uint256 openTs = block.timestamp;

        vm.warp(openTs + earlier);
        _setOraclePrice(_priceWadAtTick(20), block.timestamp);
        (,, uint256 concessionEarlier,) = hook.auctionStatus(key);

        vm.warp(openTs + later);
        _setOraclePrice(_priceWadAtTick(20), block.timestamp);
        (,, uint256 concessionLater,) = hook.auctionStatus(key);

        assertGe(concessionLater, concessionEarlier);
        assertLe(concessionLater, MAX_CONCESSION_WAD);
    }

    function _auctionConfig() internal view returns (OracleAnchoredLVRHook.Config memory cfg) {
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

    /// @dev Expected fee with the multiplicative Dutch-auction concession applied to
    /// the toxic surcharge, for an oracle at `oracleTick` against a 1:1 pool.
    function _expectedFeeUnits(int24 oracleTick, uint256 concessionWad)
        internal
        pure
        returns (uint24)
    {
        uint160 referenceSqrtPriceX96 = TickMath.getSqrtPriceAtTick(oracleTick);
        uint256 premiumWad = FullMath.mulDiv(referenceSqrtPriceX96, WAD, SQRT_PRICE_1_1) - WAD;
        uint256 surchargeWad = FullMath.mulDiv(premiumWad, ALPHA_BPS, 10_000);
        if (concessionWad != 0) {
            surchargeWad -= FullMath.mulDiv(surchargeWad, concessionWad, WAD);
        }
        return uint24((uint256(BASE_FEE) * 1e12 + surchargeWad) / 1e12);
    }

    function _priceWadAtTick(int24 tick) internal pure returns (uint256) {
        uint160 sqrtPriceX96 = TickMath.getSqrtPriceAtTick(tick);
        uint256 sqrtPriceWad = FullMath.mulDiv(sqrtPriceX96, SQRT_WAD, 2 ** 96);
        return FullMath.mulDiv(sqrtPriceWad, sqrtPriceWad, 1);
    }
}
