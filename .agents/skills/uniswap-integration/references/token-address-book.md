# Uniswap V3 Token Address Book

## Ethereum Mainnet (chainId: 1)

| Symbol | Address | Decimals | Notes |
|--------|---------|----------|-------|
| WETH | `0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2` | 18 | Wrapped Ether |
| USDC | `0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48` | 6 | USD Coin (Circle) |
| USDT | `0xdAC17F958D2ee523a2206206994597C13D831ec7` | 6 | Tether USD |
| WBTC | `0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599` | 8 | Wrapped Bitcoin |
| DAI | `0x6B175474E89094C44Da98b954EedeAC495271d0F` | 18 | DAI Stablecoin |

## Arbitrum One (chainId: 42161)

| Symbol | Address | Decimals | Notes |
|--------|---------|----------|-------|
| WETH | `0x82aF49447D8a07e3bd95BD0d56f35241523fBab1` | 18 | Wrapped Ether |
| USDC | `0xaf88d065e77c8cC2239327C5EDb3A432268e5831` | 6 | USDC (native) |
| USDT | `0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9` | 6 | Tether USD |
| WBTC | `0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f` | 8 | Wrapped Bitcoin |
| DAI | `0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1` | 18 | DAI Stablecoin |

## Important Notes

- On Arbitrum, USDC has two variants: native USDC (`0xaf88...`) and USDC.e (`0xFF97...`)
- Always verify token addresses before use as they may change with protocol upgrades
- WBTC has 8 decimals (same as Bitcoin), not 18
- Stablecoins (USDC, USDT, DAI) have 6 or 18 decimals — be careful with unit conversions
