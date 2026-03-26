---
name: uniswap-integration
description: This skill should be used when the user needs to interact with Uniswap V3 protocol contracts directly, read on-chain pool data, get token prices, fetch swap quotes, simulate swaps, or query liquidity positions programmatically. Provides low-level access to Uniswap V3 Factory, SwapRouter02, QuoterV2, and NonfungiblePositionManager contracts for token swaps and liquidity operations on Ethereum and Arbitrum.
license: MIT
metadata:
  author: Uniswap AI Contributors
  version: 1.1.0
---

# Uniswap V3 Integration

Low-level integration with Uniswap V3 protocol contracts for reading on-chain data and generating swap/liquidity quotes.

## Overview

This skill provides:

1. **Contract Interface Definitions** - ABIs for Uniswap V3 contracts
2. **Swap Quotes** - Scripts to get exact-input and exact-output swap quotes via QuoterV2
3. **Pool Data** - Scripts to fetch pool state (price, tick, liquidity)
4. **Token Prices** - Scripts to get current token prices from Uniswap V3 pools
5. **Liquidity Quotes** - Calculate token amounts needed for LP positions
6. **Swap Simulation** - Preview price impact, slippage, and fees before executing

## Contract Addresses

### Ethereum Mainnet (chainId: 1)

| Contract | Address |
|----------|---------|
| UniswapV3Factory | `0x1F98431c8aD98523631AE4a59f267346ea31F984` |
| SwapRouter02 | `0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45` |
| QuoterV2 | `0x61fFE014bA17989E743c5F6cB21bF9697530B21e` |
| NonfungiblePositionManager | `0xC36442b4a4522E871399CD717aBDD847Ab11FE88` |

### Arbitrum One (chainId: 42161)

| Contract | Address |
|----------|---------|
| UniswapV3Factory | `0x1F98431c8aD98523631AE4a59f267346ea31F984` |
| SwapRouter02 | `0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45` |
| QuoterV2 | `0x61fFE014bA17989E743c5F6cB21bF9697530B21e` |
| NonfungiblePositionManager | `0xC36442b4a4522E871399CD717aBDD847Ab11FE88` |

## Fee Tiers

| Fee | Label | Typical Use |
|-----|-------|-------------|
| 100 | 0.01% | Stablecoin pairs |
| 500 | 0.05% | Correlated pairs (e.g., USDC/USDT) |
| 3000 | 0.3% | Most standard pairs |
| 10000 | 1% | Exotic/volatile pairs |

## Token Address Reference

### Ethereum (chainId: 1)

| Symbol | Address | Decimals |
|--------|---------|----------|
| WETH | `0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2` | 18 |
| USDC | `0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48` | 6 |
| USDT | `0xdAC17F958D2ee523a2206206994597C13D831ec7` | 6 |
| WBTC | `0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599` | 8 |
| DAI | `0x6B175474E89094C44Da98b954EedeAC495271d0F` | 18 |

### Arbitrum (chainId: 42161)

| Symbol | Address | Decimals |
|--------|---------|----------|
| WETH | `0x82aF49447D8a07e3bd95BD0d56f35241523fBab1` | 18 |
| USDC | `0xaf88d065e77c8cC2239327C5EDb3A432268e5831` | 6 |
| USDT | `0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9` | 6 |
| WBTC | `0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f` | 8 |
| DAI | `0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1` | 18 |

## Scripts

### quote-swap.ts

Gets a swap quote using QuoterV2.

```bash
npx tsx packages/plugins/uniswap-integration/scripts/quote-swap.ts <chainId> <tokenIn> <tokenOut> <amountIn> [fee] [slippage%]
```

Example:

```bash
npx tsx packages/plugins/uniswap-integration/scripts/quote-swap.ts 1 USDC WETH 1000 3000 0.5
```

### get-pool-data.ts

Fetches current pool state.

```bash
npx tsx packages/plugins/uniswap-integration/scripts/get-pool-data.ts <chainId> <tokenA> <tokenB> [fee]
```

### get-token-price.ts

Gets the current USD price of a token.

```bash
npx tsx packages/plugins/uniswap-integration/scripts/get-token-price.ts <chainId> <token>
```

### simulate-swap.ts

Simulates a swap showing price impact and slippage.

```bash
npx tsx packages/plugins/uniswap-integration/scripts/simulate-swap.ts <chainId> <tokenIn> <tokenOut> <amountIn> [slippage%] [fee]
```

### quote-liquidity.ts

Calculates token amounts needed for a liquidity position.

```bash
npx tsx packages/plugins/uniswap-integration/scripts/quote-liquidity.ts <chainId> <token0> <token1> <fee> <tickLower> <tickUpper> <amount0>
```

## Pre-flight Check

**ALWAYS run the pre-flight check before any execution script.** This validates signer configuration, required env vars, npm packages, and RPC endpoints — and gives the user clear fix instructions if anything is missing.

```bash
npx tsx scripts/check-signer.ts
# or with explicit signer type:
npx tsx scripts/check-signer.ts --signerType turnkey
npx tsx scripts/check-signer.ts --signerType kms --chainId 42161
```

**If the check fails:**
1. Show the full output to the user — it contains exact `export` commands and setup links
2. Do NOT proceed with execution scripts until the check passes
3. Explain the available signer options (privateKey / Turnkey / KMS) and recommend Turnkey for production

**If the check passes (exit code 0):**
- Proceed with the requested execution script
- Note the wallet address and RPC URLs from the output

Example workflow:
```bash
# Step 1: always check first
npx tsx scripts/check-signer.ts --chainId 1

# Step 2: only if check passes
npx tsx scripts/simulate-swap.ts 1 USDC WETH 1000 0.5 3000
```

## Signing

Execution scripts support three signer backends via `lib/signers.ts`. Set `UNISWAP_SIGNER_TYPE` to select:

| Signer | `UNISWAP_SIGNER_TYPE` | Use Case |
|--------|----------------------|----------|
| Private Key (default) | `privateKey` | Local development only |
| Turnkey | `turnkey` | Production (TEE-backed, non-custodial) |
| AWS KMS | `kms` | Enterprise (HSM-backed) |

**Private Key** — set `UNISWAP_EXEC_PRIVATE_KEY` or pass `--privateKey`. Dev only.

**Turnkey** — requires `TURNKEY_API_PUBLIC_KEY`, `TURNKEY_API_PRIVATE_KEY`, `TURNKEY_ORGANIZATION_ID`, `TURNKEY_WALLET_ADDRESS`. Install: `npm install @turnkey/sdk-server @turnkey/viem`. See https://app.turnkey.com

**AWS KMS** — requires `AWS_KMS_KEY_ID` (ECC_SECG_P256K1 key), `AWS_REGION`. Install: `npm install @aws-sdk/client-kms`. Instance profiles supported (no static credentials needed in AWS environments).

```typescript
import { getSignerWalletClient } from './lib/clients.js';

const { walletClient, address } = await getSignerWalletClient(chainId, {
  signerType: 'turnkey', // or 'kms' or 'privateKey'
});
```

## External Documentation

- Uniswap V3 Developer Docs: https://docs.uniswap.org/contracts/v3/overview
- Uniswap V3 Core Contracts: https://github.com/Uniswap/v3-core
- Uniswap V3 Periphery: https://github.com/Uniswap/v3-periphery
- Turnkey Docs: https://docs.turnkey.com
- AWS KMS Docs: https://docs.aws.amazon.com/kms/latest/developerguide/create-keys.html
