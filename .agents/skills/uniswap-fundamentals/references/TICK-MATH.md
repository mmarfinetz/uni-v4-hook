# Tick Math

Mathematical foundations for price representation in Uniswap V3 and V4.

---

## Q Notation (Fixed-Point Arithmetic) **[V3+V4]**

Variables ending in `X96` or `X128` use Q notation for fixed-point math.

**Conversion**:
```
actualValue = qValue ÷ 2^k
```

Where `k` is the number after "X" (e.g., X96 means divide by 2^96).

**Example**:
```
sqrtPrice = sqrtPriceX96 ÷ 2^96
```

**Purpose**: Allows integers to function like floating-point numbers while preserving precision across 256-bit representations.

[Source: Uniswap V3 Math Primer](https://blog.uniswap.org/uniswap-v3-math-primer)

---

## sqrtPriceX96 **[V3+V4]**

The primary price representation in both V3 and V4.

**What it stores**: The square root of the price, scaled by 2^96.

**Why square root**: Gas efficiency - many AMM calculations involve square roots, so storing it directly avoids repeated computation.

**Conversion to price**:
```
sqrtPrice = sqrtPriceX96 ÷ 2^96
price = sqrtPrice²
```

**Combined formula**:
```
price = (sqrtPriceX96 ÷ 2^96)²
```

**Price meaning**: `price` represents how many `token1` you get for 1 `token0`.

[Source: Uniswap V3 Math Primer](https://blog.uniswap.org/uniswap-v3-math-primer)

---

## Tick System **[V3+V4]**

Ticks discretize the price space for gas-efficient storage.

### Tick-to-Price Formula

```
price = 1.0001^tick
```

Each tick represents a 0.01% (1 basis point) price change.

### Price-to-Tick Formula

```
tick = floor(log(price) / log(1.0001))
```

**Note**: This produces a fractional result; the protocol stores the floor.

### From sqrtPriceX96 to Tick

```
tick = floor(log((sqrtPriceX96 ÷ 2^96)²) / log(1.0001))
```

**Precision warning**: sqrtPriceX96 retains more precision than integer tick values. Converting tick→price→tick may not round-trip exactly.

[Source: Uniswap V3 Math Primer](https://blog.uniswap.org/uniswap-v3-math-primer)

---

## Tick Spacing **[V3]** vs **[V4]**

### V3: Fixed Tick Spacing by Fee Tier

In V3, tick spacing is determined by the pool's fee tier:

| Fee Tier | Tick Spacing | Price Step per Tick |
|----------|--------------|---------------------|
| 0.01% | 1 | ~0.01% |
| 0.05% | 10 | ~0.10% |
| 0.30% | 60 | ~0.60% |
| 1.00% | 200 | ~2.00% |

**V3 constraint**: Position boundaries must align to tick spacing multiples.

[Source: Uniswap V3 Math Primer](https://blog.uniswap.org/uniswap-v3-math-primer)

### V4: Dynamic Tick Spacing

In V4, tick spacing is:
- **Set per pool** during initialization
- **Independent of fee tier** (fees can be dynamic)
- **Maximum of 32767** (int16 max, though practical limits are lower)

**V4 allows**: Same tick spacing with different fee configurations.

[Source: Uniswap V4 Docs](https://docs.uniswap.org/contracts/v4/overview)

---

## Tick Alignment **[V3+V4]**

Positions must have tick boundaries aligned to tick spacing.

### Basic Alignment Formula

```
alignedTick = floor(tick / tickSpacing) * tickSpacing
```

### Direction-Aware Alignment (Doppler Pattern)

The rounding direction depends on which token is being sold:

**When selling token0** (`isToken0 = true`): Round DOWN
```solidity
if (tick < 0) {
    return (tick - tickSpacing + 1) / tickSpacing * tickSpacing;
} else {
    return tick / tickSpacing * tickSpacing;
}
```

**When selling token1** (`isToken0 = false`): Round UP
```solidity
if (tick < 0) {
    return tick / tickSpacing * tickSpacing;
} else {
    return (tick + tickSpacing - 1) / tickSpacing * tickSpacing;
}
```

**Why**: Ensures positions are aligned in the direction that favors the protocol (conservative rounding).

---

## Decimal Adjustment **[V3+V4]**

Raw prices must be adjusted for token decimals.

**Formula**:
```
adjustedPrice = rawPrice ÷ 10^(token1Decimals - token0Decimals)
```

**Example**: USDC (6 decimals) / WETH (18 decimals)
```
adjustedPrice = rawPrice ÷ 10^(18 - 6) = rawPrice ÷ 10^12
```

**Inverse price**:
```
inversePrice = 1 ÷ adjustedPrice
```

[Source: Uniswap V3 Math Primer](https://blog.uniswap.org/uniswap-v3-math-primer)

---

## Token Ordering **[V2+V3+V4]**

**Universal rule**: `token0 < token1` (compared as addresses).

### Impact on Calculations

| If your asset is... | isToken0 | Tick increases mean... |
|---------------------|----------|------------------------|
| token0 | true | Asset price DECREASES (in token1 terms) |
| token1 | false | Asset price INCREASES (in token0 terms) |

### Impact on Doppler

All Doppler auction types must track `isToken0` and flip logic accordingly:
- Tick direction for price movement
- Rounding direction for alignment
- Far tick comparison direction

---

## Common Library Functions **[V3+V4]**

### TickMath (from Uniswap)

```solidity
// Get sqrt price from tick
uint160 sqrtPriceX96 = TickMath.getSqrtPriceAtTick(tick);

// Get tick from sqrt price
int24 tick = TickMath.getTickAtSqrtPrice(sqrtPriceX96);
```

**Bounds**:
- `MIN_TICK = -887272`
- `MAX_TICK = 887272`
- `MIN_SQRT_PRICE = 4295128739`
- `MAX_SQRT_PRICE = 1461446703485210103287273052203988822378723970342`

### Tick Validity

```solidity
// Valid tick range
require(tick >= TickMath.MIN_TICK && tick <= TickMath.MAX_TICK);

// Aligned to spacing
require(tick % tickSpacing == 0);
```
