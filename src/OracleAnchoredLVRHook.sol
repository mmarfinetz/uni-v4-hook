// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import { IReferenceOracle } from "./interfaces/IReferenceOracle.sol";
import { IHooks } from "v4-core/interfaces/IHooks.sol";
import { IPoolManager } from "v4-core/interfaces/IPoolManager.sol";
import { BalanceDelta, BalanceDeltaLibrary } from "v4-core/types/BalanceDelta.sol";
import { BeforeSwapDelta, BeforeSwapDeltaLibrary } from "v4-core/types/BeforeSwapDelta.sol";
import { Hooks } from "v4-core/libraries/Hooks.sol";
import { LPFeeLibrary } from "v4-core/libraries/LPFeeLibrary.sol";
import { FullMath } from "v4-core/libraries/FullMath.sol";
import { StateLibrary } from "v4-core/libraries/StateLibrary.sol";
import { TickMath } from "v4-core/libraries/TickMath.sol";
import { PoolKey } from "v4-core/types/PoolKey.sol";
import { PoolId, PoolIdLibrary } from "v4-core/types/PoolId.sol";
import { ModifyLiquidityParams, SwapParams } from "v4-core/types/PoolOperation.sol";
import { FixedPointMathLib } from "solmate/src/utils/FixedPointMathLib.sol";

contract OracleAnchoredLVRHook is IHooks {
    using LPFeeLibrary for uint24;
    using PoolIdLibrary for PoolKey;
    using StateLibrary for IPoolManager;

    error NotOwner();
    error NotPoolManager();
    error InvalidOwner();
    error InvalidConfig();
    error InvalidPool();
    error InvalidOraclePrice();
    error InvalidTickRange();
    error OracleStale();
    error WidthTooNarrow(uint256 widthTicks, uint256 minWidthTicks);
    error OffCenter(int24 midpointTick, int24 referenceTick);
    error DeviationTooLarge(uint24 computedFee, uint24 maxFee);
    error ImpossibleBudget();

    uint256 internal constant WAD = 1e18;
    uint256 internal constant SQRT_WAD = 1e9;
    uint256 internal constant Q96 = 2 ** 96;
    uint256 internal constant EWMA_ALPHA_BPS = 2000;
    /// @dev Scaling factor: WAD (1e18) / LP_FEE_DENOMINATOR (1e6) = 1e12.
    /// Fee units are in ppm (parts per million); WAD-scale fees must be divided
    /// by this factor to convert back to ppm for the PoolManager.
    uint256 internal constant FEE_SCALE = 1e12;
    /// @dev Basis-point denominator: 10_000 bps = 100%.
    uint256 internal constant BPS_DENOMINATOR = 10_000;
    uint256 internal constant MAX_ABS_TICK = 887_272;
    uint256 internal constant MAX_WIDTH_TICKS = 1_774_544;

    IPoolManager public immutable poolManager;
    address public owner;

    struct Config {
        IReferenceOracle oracle;
        uint24 baseFee;
        uint24 maxFee;
        uint24 alphaBps;
        uint32 maxOracleAge;
        uint32 latencySecs;
        uint32 centerTolTicks;
        uint256 lvrBudgetWad;
        uint256 bootstrapSigma2PerSecondWad;
    }

    struct RiskState {
        uint256 sigma2PerSecondWad;
        uint256 lastOraclePriceWad;
        uint256 lastOracleTs;
    }

    mapping(PoolId => Config) public config;
    mapping(PoolId => RiskState) public risk;

    event OwnerInitialized(address indexed owner);
    event ConfigSet(
        PoolId indexed poolId,
        address indexed oracle,
        uint24 baseFee,
        uint24 maxFee,
        uint24 alphaBps,
        uint32 maxOracleAge,
        uint32 latencySecs,
        uint32 centerTolTicks,
        uint256 lvrBudgetWad,
        uint256 bootstrapSigma2PerSecondWad
    );
    event RiskStateSet(
        PoolId indexed poolId,
        uint256 sigma2PerSecondWad,
        uint256 lastOraclePriceWad,
        uint256 lastOracleTs
    );
    event RiskUpdated(
        PoolId indexed poolId,
        uint256 sigma2PerSecondWad,
        uint256 lastOraclePriceWad,
        uint256 lastOracleTs
    );

    constructor(IPoolManager _poolManager, address initialOwner) {
        if (initialOwner == address(0)) revert InvalidOwner();
        poolManager = _poolManager;
        owner = initialOwner;
        emit OwnerInitialized(initialOwner);
    }

    modifier onlyOwner() {
        if (msg.sender != owner) revert NotOwner();
        _;
    }

    modifier onlyPoolManager() {
        if (msg.sender != address(poolManager)) revert NotPoolManager();
        _;
    }

    function getHookPermissions() external pure returns (Hooks.Permissions memory permissions) {
        permissions = Hooks.Permissions({
            beforeInitialize: false,
            afterInitialize: false,
            beforeAddLiquidity: true,
            afterAddLiquidity: false,
            beforeRemoveLiquidity: false,
            afterRemoveLiquidity: false,
            beforeSwap: true,
            afterSwap: false,
            beforeDonate: false,
            afterDonate: false,
            beforeSwapReturnDelta: false,
            afterSwapReturnDelta: false,
            afterAddLiquidityReturnDelta: false,
            afterRemoveLiquidityReturnDelta: false
        });
    }

    function setConfig(PoolKey calldata key, Config calldata cfg) external onlyOwner {
        if (address(key.hooks) != address(this)) revert InvalidPool();
        if (!key.fee.isDynamicFee()) revert InvalidPool();
        if (
            address(cfg.oracle) == address(0) || cfg.baseFee > cfg.maxFee
                || cfg.maxFee > LPFeeLibrary.MAX_LP_FEE || cfg.maxOracleAge == 0
                || cfg.lvrBudgetWad == 0 || cfg.alphaBps == 0 || cfg.alphaBps > BPS_DENOMINATOR
                || cfg.bootstrapSigma2PerSecondWad == 0
        ) revert InvalidConfig();

        PoolId id = key.toId();
        config[id] = cfg;

        emit ConfigSet(
            id,
            address(cfg.oracle),
            cfg.baseFee,
            cfg.maxFee,
            cfg.alphaBps,
            cfg.maxOracleAge,
            cfg.latencySecs,
            cfg.centerTolTicks,
            cfg.lvrBudgetWad,
            cfg.bootstrapSigma2PerSecondWad
        );
    }

    function setRiskState(
        PoolKey calldata key,
        uint256 sigma2PerSecondWad,
        uint256 lastOraclePriceWad,
        uint256 lastOracleTs
    ) external onlyOwner {
        PoolId id = key.toId();
        risk[id] = RiskState({
            sigma2PerSecondWad: sigma2PerSecondWad,
            lastOraclePriceWad: lastOraclePriceWad,
            lastOracleTs: lastOracleTs
        });
        emit RiskStateSet(id, sigma2PerSecondWad, lastOraclePriceWad, lastOracleTs);
    }

    function previewSwapFee(PoolKey calldata key, bool zeroForOne)
        external
        view
        returns (
            bool toxic,
            uint24 feeUnits,
            uint160 referenceSqrtPriceX96,
            uint160 poolSqrtPriceX96
        )
    {
        PoolId id = key.toId();
        Config memory cfg = _loadConfig(id);
        (uint256 referencePriceWad,,) = _loadFreshOracle(cfg);
        referenceSqrtPriceX96 = _priceWadToSqrtPriceX96(referencePriceWad);
        (poolSqrtPriceX96,,,) = poolManager.getSlot0(id);
        (toxic, feeUnits) = _quoteFee(cfg, zeroForOne, referenceSqrtPriceX96, poolSqrtPriceX96);
    }

    function minWidthTicks(PoolKey calldata key) external view returns (uint256) {
        PoolId id = key.toId();
        Config memory cfg = _loadConfig(id);
        return _minWidthTicks(
            _effectiveSigma2PerSecondWad(cfg, risk[id]),
            cfg.latencySecs,
            cfg.lvrBudgetWad,
            key.tickSpacing
        );
    }

    function beforeInitialize(address, PoolKey calldata, uint160)
        external
        view
        override
        onlyPoolManager
        returns (bytes4)
    {
        return IHooks.beforeInitialize.selector;
    }

    function afterInitialize(address, PoolKey calldata, uint160, int24)
        external
        view
        override
        onlyPoolManager
        returns (bytes4)
    {
        return IHooks.afterInitialize.selector;
    }

    function beforeAddLiquidity(
        address,
        PoolKey calldata key,
        ModifyLiquidityParams calldata params,
        bytes calldata
    ) external view override onlyPoolManager returns (bytes4) {
        if (params.tickLower >= params.tickUpper) revert InvalidTickRange();

        PoolId id = key.toId();
        Config memory cfg = _loadConfig(id);
        (uint256 referencePriceWad,,) = _loadFreshOracle(cfg);
        uint160 referenceSqrtPriceX96 = _priceWadToSqrtPriceX96(referencePriceWad);
        int24 referenceTick = TickMath.getTickAtSqrtPrice(referenceSqrtPriceX96);
        int24 midpointTick = int24((int256(params.tickLower) + int256(params.tickUpper)) / 2);

        if (_absDiff(midpointTick, referenceTick) > cfg.centerTolTicks) {
            revert OffCenter(midpointTick, referenceTick);
        }

        uint256 widthTicks = uint256(int256(params.tickUpper - params.tickLower));
        uint256 minTicks = _minWidthTicks(
            _effectiveSigma2PerSecondWad(cfg, risk[id]),
            cfg.latencySecs,
            cfg.lvrBudgetWad,
            key.tickSpacing
        );
        if (widthTicks < minTicks) revert WidthTooNarrow(widthTicks, minTicks);

        return IHooks.beforeAddLiquidity.selector;
    }

    function afterAddLiquidity(
        address,
        PoolKey calldata,
        ModifyLiquidityParams calldata,
        BalanceDelta,
        BalanceDelta,
        bytes calldata
    ) external view override onlyPoolManager returns (bytes4, BalanceDelta) {
        return (IHooks.afterAddLiquidity.selector, BalanceDeltaLibrary.ZERO_DELTA);
    }

    function beforeRemoveLiquidity(
        address,
        PoolKey calldata,
        ModifyLiquidityParams calldata,
        bytes calldata
    ) external view override onlyPoolManager returns (bytes4) {
        return IHooks.beforeRemoveLiquidity.selector;
    }

    function afterRemoveLiquidity(
        address,
        PoolKey calldata,
        ModifyLiquidityParams calldata,
        BalanceDelta,
        BalanceDelta,
        bytes calldata
    ) external view override onlyPoolManager returns (bytes4, BalanceDelta) {
        return (IHooks.afterRemoveLiquidity.selector, BalanceDeltaLibrary.ZERO_DELTA);
    }

    function beforeSwap(address, PoolKey calldata key, SwapParams calldata params, bytes calldata)
        external
        override
        onlyPoolManager
        returns (bytes4, BeforeSwapDelta, uint24)
    {
        PoolId id = key.toId();
        Config memory cfg = _loadConfig(id);
        (uint256 referencePriceWad,, uint256 latestFeedTs) = _loadFreshOracle(cfg);
        _refreshRisk(id, referencePriceWad, latestFeedTs);

        uint160 referenceSqrtPriceX96 = _priceWadToSqrtPriceX96(referencePriceWad);
        (uint160 poolSqrtPriceX96,,,) = poolManager.getSlot0(id);
        (, uint24 feeUnits) =
            _quoteFee(cfg, params.zeroForOne, referenceSqrtPriceX96, poolSqrtPriceX96);

        return (
            IHooks.beforeSwap.selector,
            BeforeSwapDeltaLibrary.ZERO_DELTA,
            feeUnits | LPFeeLibrary.OVERRIDE_FEE_FLAG
        );
    }

    function afterSwap(address, PoolKey calldata, SwapParams calldata, BalanceDelta, bytes calldata)
        external
        view
        override
        onlyPoolManager
        returns (bytes4, int128)
    {
        return (IHooks.afterSwap.selector, 0);
    }

    function beforeDonate(address, PoolKey calldata, uint256, uint256, bytes calldata)
        external
        view
        override
        onlyPoolManager
        returns (bytes4)
    {
        return IHooks.beforeDonate.selector;
    }

    function afterDonate(address, PoolKey calldata, uint256, uint256, bytes calldata)
        external
        view
        override
        onlyPoolManager
        returns (bytes4)
    {
        return IHooks.afterDonate.selector;
    }

    function _loadConfig(PoolId id) internal view returns (Config memory cfg) {
        cfg = config[id];
        if (address(cfg.oracle) == address(0)) revert InvalidConfig();
    }

    function _loadFreshOracle(Config memory cfg)
        internal
        view
        returns (uint256 priceWad, uint256 updatedAt, uint256 latestFeedTs)
    {
        try cfg.oracle.latestPriceWad() returns (
            uint256 fetchedPriceWad,
            uint256 fetchedUpdatedAt,
            uint256 fetchedLatestFeedTs
        ) {
            priceWad = fetchedPriceWad;
            updatedAt = fetchedUpdatedAt;
            latestFeedTs = fetchedLatestFeedTs;
        } catch {
            revert InvalidOraclePrice();
        }
        if (priceWad == 0) revert InvalidOraclePrice();
        if (block.timestamp > updatedAt + cfg.maxOracleAge) revert OracleStale();
    }

    function _quoteFee(
        Config memory cfg,
        bool zeroForOne,
        uint160 referenceSqrtPriceX96,
        uint160 poolSqrtPriceX96
    ) internal pure returns (bool toxic, uint24 feeUnits) {
        uint256 feeWad = uint256(cfg.baseFee) * FEE_SCALE;

        if (referenceSqrtPriceX96 > poolSqrtPriceX96) {
            toxic = !zeroForOne;
            if (toxic) {
                uint256 exactPremiumWad =
                    FullMath.mulDiv(referenceSqrtPriceX96, WAD, poolSqrtPriceX96) - WAD;
                feeWad += FullMath.mulDiv(exactPremiumWad, cfg.alphaBps, BPS_DENOMINATOR);
            }
        } else if (referenceSqrtPriceX96 < poolSqrtPriceX96) {
            toxic = zeroForOne;
            if (toxic) {
                uint256 exactPremiumWad =
                    FullMath.mulDiv(poolSqrtPriceX96, WAD, referenceSqrtPriceX96) - WAD;
                feeWad += FullMath.mulDiv(exactPremiumWad, cfg.alphaBps, BPS_DENOMINATOR);
            }
        }

        uint256 feeUnits256 = feeWad / FEE_SCALE;
        if (feeUnits256 > type(uint24).max) {
            revert DeviationTooLarge(type(uint24).max, cfg.maxFee);
        }
        feeUnits = uint24(feeUnits256);
        if (feeUnits > cfg.maxFee) revert DeviationTooLarge(feeUnits, cfg.maxFee);
    }

    function _refreshRisk(PoolId id, uint256 referencePriceWad, uint256 latestFeedTs) internal {
        RiskState memory current = risk[id];
        if (latestFeedTs <= current.lastOracleTs || current.lastOraclePriceWad == 0) {
            current.lastOraclePriceWad = referencePriceWad;
            current.lastOracleTs = latestFeedTs;
            risk[id] = current;
            emit RiskUpdated(
                id, current.sigma2PerSecondWad, current.lastOraclePriceWad, current.lastOracleTs
            );
            return;
        }

        uint256 dt = latestFeedTs - current.lastOracleTs;
        uint256 absoluteChange = referencePriceWad > current.lastOraclePriceWad
            ? referencePriceWad - current.lastOraclePriceWad
            : current.lastOraclePriceWad - referencePriceWad;

        uint256 returnWad = FullMath.mulDiv(absoluteChange, WAD, current.lastOraclePriceWad);
        uint256 sampleSigma2PerSecondWad = FullMath.mulDiv(returnWad, returnWad, WAD * dt);

        if (current.sigma2PerSecondWad == 0) {
            current.sigma2PerSecondWad = sampleSigma2PerSecondWad;
        } else {
            uint256 retained = FullMath.mulDiv(
                current.sigma2PerSecondWad, BPS_DENOMINATOR - EWMA_ALPHA_BPS, BPS_DENOMINATOR
            );
            uint256 updated =
                FullMath.mulDiv(sampleSigma2PerSecondWad, EWMA_ALPHA_BPS, BPS_DENOMINATOR);
            current.sigma2PerSecondWad = retained + updated;
        }

        current.lastOraclePriceWad = referencePriceWad;
        current.lastOracleTs = latestFeedTs;
        risk[id] = current;

        emit RiskUpdated(
            id, current.sigma2PerSecondWad, current.lastOraclePriceWad, current.lastOracleTs
        );
    }

    function _effectiveSigma2PerSecondWad(Config memory cfg, RiskState storage state)
        internal
        view
        returns (uint256)
    {
        if (state.sigma2PerSecondWad != 0) return state.sigma2PerSecondWad;
        return cfg.bootstrapSigma2PerSecondWad;
    }

    function _minWidthTicks(
        uint256 sigma2PerSecondWad,
        uint256 latencySecs,
        uint256 lvrBudgetWad,
        int24 tickSpacing
    ) internal pure returns (uint256) {
        if (lvrBudgetWad == 0) revert InvalidConfig();
        if (sigma2PerSecondWad == 0 || latencySecs == 0) return 0;

        uint256 uWad = FullMath.mulDiv(sigma2PerSecondWad, latencySecs, 8);
        if (uWad >= lvrBudgetWad) revert ImpossibleBudget();

        uint256 requiredWidthFactorWad = FullMath.mulDiv(uWad, WAD, lvrBudgetWad);
        uint256 low = 0;
        uint256 high = MAX_WIDTH_TICKS;

        while (low < high) {
            uint256 mid = (low + high) / 2;
            if (_widthFactorWad(mid) >= requiredWidthFactorWad) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }

        uint256 spacing = uint256(uint24(tickSpacing));
        if (spacing == 0) return low;

        uint256 rounded = _roundUp(low, spacing);
        if (rounded > MAX_WIDTH_TICKS) return MAX_WIDTH_TICKS;
        return rounded;
    }

    function _widthFactorWad(uint256 widthTicks) internal pure returns (uint256) {
        if (widthTicks == 0) return 0;

        uint256 halfWidthTicks = widthTicks / 2;
        if (halfWidthTicks > MAX_ABS_TICK) halfWidthTicks = MAX_ABS_TICK;

        uint160 expNegQuarterX96 = TickMath.getSqrtPriceAtTick(-int24(int256(halfWidthTicks)));
        uint256 expNegQuarterWad = FullMath.mulDiv(expNegQuarterX96, WAD, Q96);
        return WAD - expNegQuarterWad;
    }

    function _priceWadToSqrtPriceX96(uint256 priceWad)
        internal
        pure
        returns (uint160 sqrtPriceX96)
    {
        if (priceWad == 0) revert InvalidOraclePrice();

        uint256 sqrtPriceWad = FixedPointMathLib.sqrt(priceWad);
        uint256 scaled = FullMath.mulDiv(sqrtPriceWad, Q96, SQRT_WAD);

        if (scaled < TickMath.MIN_SQRT_PRICE || scaled >= TickMath.MAX_SQRT_PRICE) {
            revert InvalidOraclePrice();
        }
        sqrtPriceX96 = uint160(scaled);
    }

    function _roundUp(uint256 value, uint256 spacing) internal pure returns (uint256) {
        uint256 remainder = value % spacing;
        if (remainder == 0) return value;
        return value + spacing - remainder;
    }

    function _absDiff(int24 a, int24 b) internal pure returns (uint32) {
        return a >= b ? uint32(uint24(a - b)) : uint32(uint24(b - a));
    }
}
