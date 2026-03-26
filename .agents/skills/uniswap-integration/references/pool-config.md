# Uniswap V3 Pool Configuration

## Pool Mechanics

Uniswap V3 uses concentrated liquidity, where LPs provide liquidity within specific price ranges (ticks).

## Fee Tiers and Tick Spacing

| Fee | Tick Spacing | Typical Pairs |
|-----|-------------|---------------|
| 100 (0.01%) | 1 | Stablecoin-stablecoin (USDC/USDT) |
| 500 (0.05%) | 10 | Correlated assets (ETH/stETH) |
| 3000 (0.3%) | 60 | Most pairs (WETH/USDC) |
| 10000 (1%) | 200 | Exotic/volatile pairs |

## Price Calculations

### sqrtPriceX96 to Price

```typescript
const price = (Number(sqrtPriceX96) / 2 ** 96) ** 2 * 10 ** (decimals0 - decimals1);
```

### Tick to Price

```typescript
const price = Math.pow(1.0001, tick);
```

### Price to Tick

```typescript
const tick = Math.floor(Math.log(price) / Math.log(1.0001));
```

## Common Pool Addresses (Ethereum)

| Pair | Fee | Address |
|------|-----|---------|
| USDC/WETH | 0.05% | `0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640` |
| USDC/WETH | 0.3% | `0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8` |
| WBTC/WETH | 0.3% | `0xCBCdF9626bC03E24f779434178A73a0B4bad62eD` |
| DAI/USDC | 0.05% | `0x5777d92f208679DB4b9778590Fa3CAB3aC9e2168` |

## Liquidity Range Selection

### Full Range Position

- tickLower: -887220 (for fee 3000)
- tickUpper: 887220 (for fee 3000)
- Similar to Uniswap V2 but less capital efficient

### Narrow Range Position

- More capital efficient (higher fee earnings per unit)
- Higher risk of going out of range (impermanent loss risk)
- Requires active management

### Price Range Formula

```
priceLower = 1.0001^tickLower
priceUpper = 1.0001^tickUpper
```
