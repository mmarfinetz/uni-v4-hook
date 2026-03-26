#!/usr/bin/env ts-node
/**
 * Uniswap V3 Token Price Script
 *
 * Fetches the current price of a token in USD using Uniswap V3 pools.
 *
 * Usage:
 *   npx tsx scripts/get-token-price.ts <chainId> <token>
 *
 * Examples:
 *   npx tsx scripts/get-token-price.ts 1 WETH
 *   npx tsx scripts/get-token-price.ts 42161 WBTC
 */

import { createPublicClient, http } from 'viem';
import { mainnet, arbitrum } from 'viem/chains';

const FACTORY_ADDRESS: Record<number, `0x${string}`> = {
  1: '0x1F98431c8aD98523631AE4a59f267346ea31F984',
  42161: '0x1F98431c8aD98523631AE4a59f267346ea31F984',
};

const TOKENS: Record<number, Record<string, { address: `0x${string}`; decimals: number }>> = {
  1: {
    WETH: { address: '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', decimals: 18 },
    USDC: { address: '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', decimals: 6 },
    USDT: { address: '0xdAC17F958D2ee523a2206206994597C13D831ec7', decimals: 6 },
    WBTC: { address: '0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599', decimals: 8 },
    DAI: { address: '0x6B175474E89094C44Da98b954EedeAC495271d0F', decimals: 18 },
  },
  42161: {
    WETH: { address: '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1', decimals: 18 },
    USDC: { address: '0xaf88d065e77c8cC2239327C5EDb3A432268e5831', decimals: 6 },
    USDT: { address: '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9', decimals: 6 },
    WBTC: { address: '0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f', decimals: 8 },
    DAI: { address: '0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1', decimals: 18 },
  },
};

// Preferred fee tiers for price discovery
const PRICE_DISCOVERY_FEES = [500, 3000, 10000];
// Quote stablecoin
const QUOTE_TOKEN = 'USDC';

const FACTORY_ABI = [
  {
    name: 'getPool',
    type: 'function',
    stateMutability: 'view',
    inputs: [
      { name: 'tokenA', type: 'address' },
      { name: 'tokenB', type: 'address' },
      { name: 'fee', type: 'uint24' },
    ],
    outputs: [{ name: 'pool', type: 'address' }],
  },
] as const;

const POOL_ABI = [
  {
    name: 'slot0',
    type: 'function',
    stateMutability: 'view',
    inputs: [],
    outputs: [
      { name: 'sqrtPriceX96', type: 'uint160' },
      { name: 'tick', type: 'int24' },
      { name: 'observationIndex', type: 'uint16' },
      { name: 'observationCardinality', type: 'uint16' },
      { name: 'observationCardinalityNext', type: 'uint16' },
      { name: 'feeProtocol', type: 'uint8' },
      { name: 'unlocked', type: 'bool' },
    ],
  },
  {
    name: 'liquidity',
    type: 'function',
    stateMutability: 'view',
    inputs: [],
    outputs: [{ name: '', type: 'uint128' }],
  },
  {
    name: 'token0',
    type: 'function',
    stateMutability: 'view',
    inputs: [],
    outputs: [{ name: '', type: 'address' }],
  },
] as const;

function getRpcUrl(chainId: number): string {
  const envVar = chainId === 1 ? 'ETHEREUM_RPC_URL' : 'ARBITRUM_RPC_URL';
  const defaultRpc = chainId === 1 ? 'https://ethereum.publicnode.com' : 'https://arbitrum.publicnode.com';
  return process.env[envVar] || defaultRpc;
}

export async function getTokenPrice(chainId: number, tokenSymbol: string) {
  const tokens = TOKENS[chainId];
  if (!tokens) throw new Error(`Unsupported chainId: ${chainId}`);

  const normalized = tokenSymbol.toUpperCase();

  if (normalized === QUOTE_TOKEN || normalized === 'USDT' || normalized === 'DAI') {
    return { token: normalized, priceUSD: '1.00', source: 'stablecoin', chainId };
  }

  const tokenInfo = tokens[normalized];
  if (!tokenInfo) throw new Error(`Unknown token: ${tokenSymbol}. Supported: ${Object.keys(tokens).join(', ')}`);

  const quoteTokenInfo = tokens[QUOTE_TOKEN];
  if (!quoteTokenInfo) throw new Error(`Quote token ${QUOTE_TOKEN} not available on chainId ${chainId}`);

  const client = createPublicClient({
    chain: chainId === 1 ? mainnet : arbitrum,
    transport: http(getRpcUrl(chainId)),
  });

  const factoryAddress = FACTORY_ADDRESS[chainId];

  // Try each fee tier and pick the pool with highest liquidity
  let bestPool: { address: `0x${string}`; fee: number; liquidity: bigint } | null = null;

  for (const fee of PRICE_DISCOVERY_FEES) {
    const poolAddress = (await client.readContract({
      address: factoryAddress,
      abi: FACTORY_ABI,
      functionName: 'getPool',
      args: [tokenInfo.address, quoteTokenInfo.address, fee],
    })) as `0x${string}`;

    if (poolAddress === '0x0000000000000000000000000000000000000000') continue;

    const liquidity = (await client.readContract({
      address: poolAddress,
      abi: POOL_ABI,
      functionName: 'liquidity',
    })) as bigint;

    if (!bestPool || liquidity > bestPool.liquidity) {
      bestPool = { address: poolAddress, fee, liquidity };
    }
  }

  if (!bestPool) {
    throw new Error(`No ${normalized}/USDC pool found on chain ${chainId}`);
  }

  const [slot0, token0Addr] = await Promise.all([
    client.readContract({ address: bestPool.address, abi: POOL_ABI, functionName: 'slot0' }),
    client.readContract({ address: bestPool.address, abi: POOL_ABI, functionName: 'token0' }),
  ]);

  const slot0Result = slot0 as readonly [bigint, number, number, number, number, number, boolean];
  const sqrtPriceX96 = slot0Result[0];

  const isToken0 = (token0Addr as string).toLowerCase() === tokenInfo.address.toLowerCase();
  const dec0 = isToken0 ? tokenInfo.decimals : quoteTokenInfo.decimals;
  const dec1 = isToken0 ? quoteTokenInfo.decimals : tokenInfo.decimals;

  const rawPrice = (Number(sqrtPriceX96) / 2 ** 96) ** 2 * 10 ** (dec0 - dec1);
  const priceUSD = isToken0 ? rawPrice : 1 / rawPrice;

  return {
    token: normalized,
    priceUSD: priceUSD.toFixed(4),
    source: `${normalized}/USDC pool (fee: ${bestPool.fee / 10000}%)`,
    poolAddress: bestPool.address,
    feeTier: bestPool.fee,
    chainId,
  };
}

async function main() {
  const [chainIdStr, tokenSymbol] = process.argv.slice(2);

  if (!chainIdStr || !tokenSymbol) {
    console.error('Usage: npx tsx scripts/get-token-price.ts <chainId> <token>');
    console.error('Example: npx tsx scripts/get-token-price.ts 1 WETH');
    process.exit(1);
  }

  const chainId = parseInt(chainIdStr, 10);

  if (![1, 42161].includes(chainId)) {
    console.error('Unsupported chainId. Use 1 (Ethereum) or 42161 (Arbitrum)');
    process.exit(1);
  }

  const result = await getTokenPrice(chainId, tokenSymbol);
  console.log(JSON.stringify(result, null, 2));
}

if (require.main === module) {
  main().catch((error) => {
    console.error('Error:', error instanceof Error ? error.message : String(error));
    process.exit(1);
  });
}
