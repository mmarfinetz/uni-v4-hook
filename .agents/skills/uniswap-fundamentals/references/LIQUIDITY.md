# Liquidity

Liquidity mathematics for Uniswap V3 and V4 concentrated liquidity.

---

## V2 vs V3+V4 Liquidity **[Comparison]**

| Aspect | V2 | V3+V4 |
|--------|----|----|
| Range | Full price range (0 to ∞) | Concentrated within [tickLower, tickUpper] |
| Capital efficiency | Low (spread across all prices) | High (focused where trading occurs) |
| Representation | `L = √(x × y)` | Same formula, but within range |
| Position type | Fungible (ERC-20 LP tokens) | Non-fungible (V3: NFT, V4: PoolManager state) |

---

## Core Liquidity Formula **[V3+V4]**

```
L = √(x_virtual × y_virtual)
```

Where:
- `L` = liquidity
- `x_virtual` = virtual reserve of token0
- `y_virtual` = virtual reserve of token1

**Stored as square root** for gas efficiency.

[Source: Uniswap V3 Math Primer 2](https://blog.uniswap.org/uniswap-v3-math-primer-2)

---

## Liquidity from Token Amounts **[V3+V4]**

Given token amounts, calculate required liquidity:

### From token0 amount:
```
L = x × (√p_upper × √p_current) / (√p_upper - √p_current)
```

### From token1 amount:
```
L = y / (√p_current - √p_lower)
```

Where:
- `x` = token0 amount
- `y` = token1 amount
- `p_lower` = price at tickLower
- `p_upper` = price at tickUpper
- `p_current` = current price

[Source: Uniswap V3 Math Primer 2](https://blog.uniswap.org/uniswap-v3-math-primer-2)

---

## Token Amounts from Liquidity **[V3+V4]**

### In-Range Position (p_lower < p_current < p_upper)

Both tokens are held:

```
token0 = L × (√p_upper - √p_current) / (√p_current × √p_upper)
token1 = L × (√p_current - √p_lower)
```

### Out-of-Range: Price Below Position (p_current < p_lower)

Only token0 is held (waiting to be sold):

```
token0 = L × (√p_upper - √p_lower) / (√p_lower × √p_upper)
token1 = 0
```

### Out-of-Range: Price Above Position (p_current > p_upper)

Only token1 is held (token0 was sold):

```
token0 = 0
token1 = L × (√p_upper - √p_lower)
```

[Source: Uniswap V3 Math Primer 2](https://blog.uniswap.org/uniswap-v3-math-primer-2)

---

## Concentrated Liquidity Benefits **[V3+V4]**

**Capital efficiency**: By concentrating within a price range, LPs earn more fees on the same capital compared to V2.

**Example**: If you expect ETH/USDC to trade between $1800-$2200:
- V2: Liquidity spread from $0 to $∞
- V3/V4: Liquidity concentrated in $1800-$2200 range
- Result: Same capital provides ~10x deeper liquidity in that range

**Tradeoff**: If price moves outside your range, you stop earning fees and hold only one token.

---

## Common Library Functions **[V3+V4]**

### LiquidityAmounts (from Uniswap)

```solidity
// Get liquidity for a given amount of token0
uint128 liquidity = LiquidityAmounts.getLiquidityForAmount0(
    sqrtPriceLower,
    sqrtPriceUpper,
    amount0
);

// Get liquidity for a given amount of token1
uint128 liquidity = LiquidityAmounts.getLiquidityForAmount1(
    sqrtPriceLower,
    sqrtPriceUpper,
    amount1
);

// Get liquidity for both token amounts
uint128 liquidity = LiquidityAmounts.getLiquidityForAmounts(
    sqrtPriceCurrent,
    sqrtPriceLower,
    sqrtPriceUpper,
    amount0,
    amount1
);
```

### SqrtPriceMath (from Uniswap)

```solidity
// Get token0 amount for liquidity
uint256 amount0 = SqrtPriceMath.getAmount0Delta(
    sqrtPriceLower,
    sqrtPriceUpper,
    liquidity,
    roundUp
);

// Get token1 amount for liquidity
uint256 amount1 = SqrtPriceMath.getAmount1Delta(
    sqrtPriceLower,
    sqrtPriceUpper,
    liquidity,
    roundUp
);
```

---

## Position Storage **[V3]** vs **[V4]**

### V3: NFT-Based Positions

- Each position is an NFT (ERC-721)
- Managed by NonfungiblePositionManager
- Position data stored in NFT contract
- One position per NFT

### V4: PoolManager State

- Positions stored in PoolManager singleton
- Identified by `(poolId, owner, tickLower, tickUpper, salt)`
- Salt allows multiple positions at same ticks
- No NFT required (though wrappers can add this)

**V4 difference**: Salt parameter enables multiple positions with identical tick ranges, which Doppler uses for different slug types.

---

## Doppler Usage **[V3+V4]**

Doppler contracts use these functions for:

| Function | Doppler Purpose |
|----------|-----------------|
| `getLiquidityForAmount0` | Size token0 positions (asset is token0) |
| `getLiquidityForAmount1` | Size token1 positions (asset is token1) |
| `getAmount0Delta` | Calculate token0 needed for liquidity |
| `getAmount1Delta` | Calculate token1 needed for liquidity |

**Pattern in Doppler**:
```solidity
function _computeLiquidity(
    bool forToken0,
    uint160 sqrtPriceLower,
    uint160 sqrtPriceUpper,
    uint256 amount
) internal pure returns (uint128) {
    if (forToken0) {
        return LiquidityAmounts.getLiquidityForAmount0(sqrtPriceLower, sqrtPriceUpper, amount);
    } else {
        return LiquidityAmounts.getLiquidityForAmount1(sqrtPriceLower, sqrtPriceUpper, amount);
    }
}
```
