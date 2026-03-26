#!/usr/bin/env ts-node
/**
 * Uniswap V3 Pool Data Script
 *
 * Fetches current pool state including price, tick, and liquidity.
 *
 * Usage:
 *   npx tsx scripts/get-pool-data.ts <chainId> <tokenA> <tokenB> [fee]
 *
 * Examples:
 *   npx tsx scripts/get-pool-data.ts 1 USDC WETH 3000
 *   npx tsx scripts/get-pool-data.ts 42161 USDC WETH 500
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
  {
    name: 'token1',
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

export async function getPoolData(chainId: number, tokenAInput: string, tokenBInput: string, fee: number) {
  const tokens = TOKENS[chainId];
  if (!tokens) throw new Error(`Unsupported chainId: ${chainId}`);

  const tokenAInfo = tokens[tokenAInput.toUpperCase()];
  const tokenBInfo = tokens[tokenBInput.toUpperCase()];

  if (!tokenAInfo) throw new Error(`Unknown token: ${tokenAInput}`);
  if (!tokenBInfo) throw new Error(`Unknown token: ${tokenBInput}`);

  const client = createPublicClient({
    chain: chainId === 1 ? mainnet : arbitrum,
    transport: http(getRpcUrl(chainId)),
  });

  const factoryAddress = FACTORY_ADDRESS[chainId];

  const poolAddress = (await client.readContract({
    address: factoryAddress,
    abi: FACTORY_ABI,
    functionName: 'getPool',
    args: [tokenAInfo.address, tokenBInfo.address, fee],
  })) as `0x${string}`;

  if (poolAddress === '0x0000000000000000000000000000000000000000') {
    throw new Error(`No pool found for ${tokenAInput}/${tokenBInput} at fee ${fee}`);
  }

  const [slot0, liquidity, token0Addr, token1Addr] = await Promise.all([
    client.readContract({ address: poolAddress, abi: POOL_ABI, functionName: 'slot0' }),
    client.readContract({ address: poolAddress, abi: POOL_ABI, functionName: 'liquidity' }),
    client.readContract({ address: poolAddress, abi: POOL_ABI, functionName: 'token0' }),
    client.readContract({ address: poolAddress, abi: POOL_ABI, functionName: 'token1' }),
  ]);

  const slot0Result = slot0 as readonly [bigint, number, number, number, number, number, boolean];
  const sqrtPriceX96 = slot0Result[0];
  const tick = slot0Result[1];

  // Find token info by address
  const findToken = (addr: string) =>
    Object.entries(tokens).find(([, t]) => t.address.toLowerCase() === (addr as string).toLowerCase());

  const t0 = findToken(token0Addr as string);
  const t1 = findToken(token1Addr as string);

  const dec0 = t0?.[1].decimals ?? 18;
  const dec1 = t1?.[1].decimals ?? 18;

  // Price = (sqrtPriceX96 / 2^96)^2 * 10^(dec0 - dec1)
  const price = (Number(sqrtPriceX96) / 2 ** 96) ** 2 * 10 ** (dec0 - dec1);

  return {
    poolAddress,
    token0: { address: token0Addr, symbol: t0?.[0] ?? 'UNKNOWN', decimals: dec0 },
    token1: { address: token1Addr, symbol: t1?.[0] ?? 'UNKNOWN', decimals: dec1 },
    fee,
    feeTierLabel: `${fee / 10000}%`,
    sqrtPriceX96: sqrtPriceX96.toString(),
    tick,
    liquidity: (liquidity as bigint).toString(),
    token0Price: price.toFixed(8),
    token1Price: (1 / price).toFixed(8),
  };
}

async function main() {
  const [chainIdStr, tokenA, tokenB, feeStr] = process.argv.slice(2);

  if (!chainIdStr || !tokenA || !tokenB) {
    console.error('Usage: npx tsx scripts/get-pool-data.ts <chainId> <tokenA> <tokenB> [fee]');
    console.error('Example: npx tsx scripts/get-pool-data.ts 1 USDC WETH 3000');
    process.exit(1);
  }

  const chainId = parseInt(chainIdStr, 10);
  const fee = feeStr ? parseInt(feeStr, 10) : 3000;

  if (![1, 42161].includes(chainId)) {
    console.error('Unsupported chainId. Use 1 (Ethereum) or 42161 (Arbitrum)');
    process.exit(1);
  }

  const data = await getPoolData(chainId, tokenA, tokenB, fee);
  console.log(JSON.stringify(data, null, 2));
}

if (require.main === module) {
  main().catch((error) => {
    console.error('Error:', error instanceof Error ? error.message : String(error));
    process.exit(1);
  });
}
