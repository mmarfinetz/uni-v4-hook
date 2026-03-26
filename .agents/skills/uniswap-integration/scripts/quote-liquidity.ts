#!/usr/bin/env ts-node
/**
 * Uniswap V3 Liquidity Quote Script
 *
 * Calculates token amounts needed to add liquidity in a given tick range.
 *
 * Usage:
 *   npx tsx scripts/quote-liquidity.ts <chainId> <token0> <token1> <fee> <tickLower> <tickUpper> <amount0>
 *
 * Examples:
 *   npx tsx scripts/quote-liquidity.ts 1 USDC WETH 3000 -887220 887220 1000
 *   npx tsx scripts/quote-liquidity.ts 42161 USDC WETH 500 -887270 887270 5000
 */

import { createPublicClient, http, parseUnits, formatUnits } from 'viem';
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
] as const;

function getRpcUrl(chainId: number): string {
  const envVar = chainId === 1 ? 'ETHEREUM_RPC_URL' : 'ARBITRUM_RPC_URL';
  const defaultRpc = chainId === 1 ? 'https://ethereum.publicnode.com' : 'https://arbitrum.publicnode.com';
  return process.env[envVar] || defaultRpc;
}

function sqrtPriceFromTick(tick: number): bigint {
  const price = Math.pow(1.0001, tick);
  const sqrtPrice = Math.sqrt(price);
  return BigInt(Math.floor(sqrtPrice * 2 ** 96));
}

function computeAmount1(sqrtRatioA: bigint, sqrtRatioB: bigint, liquidity: bigint): bigint {
  const [low, high] = sqrtRatioA < sqrtRatioB ? [sqrtRatioA, sqrtRatioB] : [sqrtRatioB, sqrtRatioA];
  return (liquidity * (high - low)) / (2n ** 96n);
}

function computeAmount0(sqrtRatioA: bigint, sqrtRatioB: bigint, liquidity: bigint): bigint {
  const [low, high] = sqrtRatioA < sqrtRatioB ? [sqrtRatioA, sqrtRatioB] : [sqrtRatioB, sqrtRatioA];
  return (liquidity * 2n ** 96n * (high - low)) / low / high;
}

export async function quoteLiquidity(
  chainId: number,
  token0Symbol: string,
  token1Symbol: string,
  fee: number,
  tickLower: number,
  tickUpper: number,
  amount0: string
) {
  const tokens = TOKENS[chainId];
  if (!tokens) throw new Error(`Unsupported chainId: ${chainId}`);

  const token0Info = tokens[token0Symbol.toUpperCase()];
  const token1Info = tokens[token1Symbol.toUpperCase()];

  if (!token0Info) throw new Error(`Unknown token0: ${token0Symbol}`);
  if (!token1Info) throw new Error(`Unknown token1: ${token1Symbol}`);

  const client = createPublicClient({
    chain: chainId === 1 ? mainnet : arbitrum,
    transport: http(getRpcUrl(chainId)),
  });

  const factoryAddress = FACTORY_ADDRESS[chainId];
  const poolAddress = (await client.readContract({
    address: factoryAddress,
    abi: FACTORY_ABI,
    functionName: 'getPool',
    args: [token0Info.address, token1Info.address, fee],
  })) as `0x${string}`;

  if (poolAddress === '0x0000000000000000000000000000000000000000') {
    throw new Error(`No pool found for ${token0Symbol}/${token1Symbol} at fee ${fee}`);
  }

  const slot0 = (await client.readContract({
    address: poolAddress,
    abi: POOL_ABI,
    functionName: 'slot0',
  })) as readonly [bigint, number, number, number, number, number, boolean];

  const currentSqrtPriceX96 = slot0[0];
  const currentTick = slot0[1];

  const sqrtLower = sqrtPriceFromTick(tickLower);
  const sqrtUpper = sqrtPriceFromTick(tickUpper);
  const sqrtCurrent = currentSqrtPriceX96;

  const amount0Wei = parseUnits(amount0, token0Info.decimals);

  // Estimate liquidity from amount0
  const liquidity =
    sqrtCurrent < sqrtLower
      ? (amount0Wei * sqrtLower * sqrtUpper) / ((sqrtUpper - sqrtLower) * 2n ** 96n)
      : sqrtCurrent < sqrtUpper
        ? (amount0Wei * sqrtCurrent * sqrtUpper) / ((sqrtUpper - sqrtCurrent) * 2n ** 96n)
        : 0n;

  // Compute amount1 needed
  const effectiveSqrtLower = sqrtCurrent > sqrtLower ? sqrtCurrent : sqrtLower;
  const amount1Wei =
    liquidity > 0n ? computeAmount1(effectiveSqrtLower, sqrtUpper, liquidity) : 0n;

  const currentPrice = (Number(currentSqrtPriceX96) / 2 ** 96) ** 2 * 10 ** (token0Info.decimals - token1Info.decimals);

  const priceLower = (Number(sqrtLower) / 2 ** 96) ** 2 * 10 ** (token0Info.decimals - token1Info.decimals);
  const priceUpper = (Number(sqrtUpper) / 2 ** 96) ** 2 * 10 ** (token0Info.decimals - token1Info.decimals);

  return {
    token0: token0Symbol.toUpperCase(),
    token1: token1Symbol.toUpperCase(),
    fee,
    feeTierLabel: `${fee / 10000}%`,
    tickLower,
    tickUpper,
    currentTick,
    currentPrice: currentPrice.toFixed(8),
    priceLower: priceLower.toFixed(8),
    priceUpper: priceUpper.toFixed(8),
    amount0,
    amount0Wei: amount0Wei.toString(),
    amount1: formatUnits(amount1Wei, token1Info.decimals),
    amount1Wei: amount1Wei.toString(),
    estimatedLiquidity: liquidity.toString(),
    inRange: currentTick >= tickLower && currentTick < tickUpper,
  };
}

async function main() {
  const [chainIdStr, token0, token1, feeStr, tickLowerStr, tickUpperStr, amount0] = process.argv.slice(2);

  if (!chainIdStr || !token0 || !token1 || !feeStr || !tickLowerStr || !tickUpperStr || !amount0) {
    console.error(
      'Usage: npx tsx scripts/quote-liquidity.ts <chainId> <token0> <token1> <fee> <tickLower> <tickUpper> <amount0>'
    );
    console.error('Example: npx tsx scripts/quote-liquidity.ts 1 USDC WETH 3000 -887220 887220 1000');
    process.exit(1);
  }

  const chainId = parseInt(chainIdStr, 10);
  const fee = parseInt(feeStr, 10);
  const tickLower = parseInt(tickLowerStr, 10);
  const tickUpper = parseInt(tickUpperStr, 10);

  if (![1, 42161].includes(chainId)) {
    console.error('Unsupported chainId. Use 1 (Ethereum) or 42161 (Arbitrum)');
    process.exit(1);
  }

  const result = await quoteLiquidity(chainId, token0, token1, fee, tickLower, tickUpper, amount0);
  console.log(JSON.stringify(result, null, 2));
}

if (require.main === module) {
  main().catch((error) => {
    console.error('Error:', error instanceof Error ? error.message : String(error));
    process.exit(1);
  });
}
