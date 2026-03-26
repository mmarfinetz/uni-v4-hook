#!/usr/bin/env ts-node
/**
 * Uniswap V3 Swap Simulator
 *
 * Simulates a swap and shows price impact, slippage, and execution details.
 *
 * Usage:
 *   npx tsx scripts/simulate-swap.ts <chainId> <tokenIn> <tokenOut> <amountIn> [slippage%] [fee]
 *
 * Examples:
 *   npx tsx scripts/simulate-swap.ts 1 USDC WETH 1000 0.5 3000
 *   npx tsx scripts/simulate-swap.ts 42161 WETH USDC 1 1.0 500
 */

import { createPublicClient, http, parseUnits, formatUnits } from 'viem';
import { mainnet, arbitrum } from 'viem/chains';

const QUOTER_V2_ADDRESS: Record<number, `0x${string}`> = {
  1: '0x61fFE014bA17989E743c5F6cB21bF9697530B21e',
  42161: '0x61fFE014bA17989E743c5F6cB21bF9697530B21e',
};

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

const QUOTER_V2_ABI = [
  {
    name: 'quoteExactInputSingle',
    type: 'function',
    stateMutability: 'nonpayable',
    inputs: [
      {
        name: 'params',
        type: 'tuple',
        components: [
          { name: 'tokenIn', type: 'address' },
          { name: 'tokenOut', type: 'address' },
          { name: 'amountIn', type: 'uint256' },
          { name: 'fee', type: 'uint24' },
          { name: 'sqrtPriceLimitX96', type: 'uint160' },
        ],
      },
    ],
    outputs: [
      { name: 'amountOut', type: 'uint256' },
      { name: 'sqrtPriceX96After', type: 'uint160' },
      { name: 'initializedTicksCrossed', type: 'uint32' },
      { name: 'gasEstimate', type: 'uint256' },
    ],
  },
] as const;

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

function getPriceImpactLevel(priceImpact: number): string {
  if (priceImpact < 0.1) return 'minimal';
  if (priceImpact < 0.5) return 'low';
  if (priceImpact < 2.0) return 'moderate';
  if (priceImpact < 5.0) return 'high';
  return 'severe';
}

export async function simulateSwap(
  chainId: number,
  tokenInSymbol: string,
  tokenOutSymbol: string,
  amountIn: string,
  slippagePercent: number = 0.5,
  fee: number = 3000
) {
  const tokens = TOKENS[chainId];
  if (!tokens) throw new Error(`Unsupported chainId: ${chainId}`);

  const tokenIn = tokens[tokenInSymbol.toUpperCase()];
  const tokenOut = tokens[tokenOutSymbol.toUpperCase()];

  if (!tokenIn) throw new Error(`Unknown tokenIn: ${tokenInSymbol}`);
  if (!tokenOut) throw new Error(`Unknown tokenOut: ${tokenOutSymbol}`);

  const client = createPublicClient({
    chain: chainId === 1 ? mainnet : arbitrum,
    transport: http(getRpcUrl(chainId)),
  });

  const amountInWei = parseUnits(amountIn, tokenIn.decimals);

  // Get pool data for price before swap
  const factoryAddress = FACTORY_ADDRESS[chainId];
  const poolAddress = (await client.readContract({
    address: factoryAddress,
    abi: FACTORY_ABI,
    functionName: 'getPool',
    args: [tokenIn.address, tokenOut.address, fee],
  })) as `0x${string}`;

  if (poolAddress === '0x0000000000000000000000000000000000000000') {
    throw new Error(`No pool found for ${tokenInSymbol}/${tokenOutSymbol} at fee ${fee}`);
  }

  const slot0Before = (await client.readContract({
    address: poolAddress,
    abi: POOL_ABI,
    functionName: 'slot0',
  })) as readonly [bigint, number, number, number, number, number, boolean];

  const sqrtPriceBefore = slot0Before[0];
  const tickBefore = slot0Before[1];

  // Get quote
  const quoteResult = (await client.simulateContract({
    address: QUOTER_V2_ADDRESS[chainId],
    abi: QUOTER_V2_ABI,
    functionName: 'quoteExactInputSingle',
    args: [
      {
        tokenIn: tokenIn.address,
        tokenOut: tokenOut.address,
        amountIn: amountInWei,
        fee,
        sqrtPriceLimitX96: 0n,
      },
    ],
  })) as { result: readonly [bigint, bigint, number, bigint] };

  const [amountOutWei, sqrtPriceAfter, ticksCrossed, gasEstimate] = quoteResult.result;

  const slippageBps = Math.round(slippagePercent * 100);
  const amountOutMinWei = (amountOutWei * BigInt(10000 - slippageBps)) / 10000n;

  const amountOut = formatUnits(amountOutWei, tokenOut.decimals);
  const amountOutMin = formatUnits(amountOutMinWei, tokenOut.decimals);

  // Estimate price impact from sqrtPrice change
  const priceBefore = (Number(sqrtPriceBefore) / 2 ** 96) ** 2;
  const priceAfter = (Number(sqrtPriceAfter) / 2 ** 96) ** 2;
  const priceImpactRaw = Math.abs((priceAfter - priceBefore) / priceBefore) * 100;

  // Fee cost
  const feeCost = (Number(amountIn) * fee) / 1_000_000;

  return {
    simulation: {
      tokenIn: tokenInSymbol.toUpperCase(),
      tokenOut: tokenOutSymbol.toUpperCase(),
      amountIn,
      amountOut,
      amountOutMin,
      feeTier: fee,
      feeTierLabel: `${fee / 10000}%`,
    },
    priceImpact: {
      percentage: priceImpactRaw.toFixed(4),
      level: getPriceImpactLevel(priceImpactRaw),
      tickBefore,
      tickAfter: tickBefore + ticksCrossed,
      ticksCrossed,
    },
    slippage: {
      tolerancePercent: slippagePercent,
      toleranceBps: slippageBps,
      minimumReceived: amountOutMin,
    },
    fees: {
      protocolFeeTier: `${fee / 10000}%`,
      estimatedFeeAmount: feeCost.toFixed(6),
      feeTokenIn: tokenInSymbol.toUpperCase(),
    },
    gas: {
      estimate: gasEstimate.toString(),
    },
    warnings: priceImpactRaw > 5
      ? ['⚠️ High price impact detected. Consider splitting the swap or using a different route.']
      : [],
    chainId,
  };
}

async function main() {
  const [chainIdStr, tokenIn, tokenOut, amountIn, slippageStr, feeStr] = process.argv.slice(2);

  if (!chainIdStr || !tokenIn || !tokenOut || !amountIn) {
    console.error(
      'Usage: npx tsx scripts/simulate-swap.ts <chainId> <tokenIn> <tokenOut> <amountIn> [slippage%] [fee]'
    );
    console.error('Example: npx tsx scripts/simulate-swap.ts 1 USDC WETH 1000 0.5 3000');
    process.exit(1);
  }

  const chainId = parseInt(chainIdStr, 10);
  const slippage = slippageStr ? parseFloat(slippageStr) : 0.5;
  const fee = feeStr ? parseInt(feeStr, 10) : 3000;

  if (![1, 42161].includes(chainId)) {
    console.error('Unsupported chainId. Use 1 (Ethereum) or 42161 (Arbitrum)');
    process.exit(1);
  }

  const result = await simulateSwap(chainId, tokenIn, tokenOut, amountIn, slippage, fee);

  console.log('\n=== Uniswap V3 Swap Simulation ===\n');
  console.log(`Swap: ${result.simulation.amountIn} ${result.simulation.tokenIn} → ${result.simulation.amountOut} ${result.simulation.tokenOut}`);
  console.log(`Fee Tier: ${result.simulation.feeTierLabel}`);
  console.log(`Min Received: ${result.slippage.minimumReceived} ${result.simulation.tokenOut} (${slippage}% slippage)`);
  console.log(`Price Impact: ${result.priceImpact.percentage}% (${result.priceImpact.level})`);
  console.log(`Ticks Crossed: ${result.priceImpact.ticksCrossed}`);
  console.log(`Protocol Fee: ~${result.fees.estimatedFeeAmount} ${result.fees.feeTokenIn}`);
  console.log(`Gas Estimate: ${result.gas.estimate}`);

  if (result.warnings.length > 0) {
    console.log('\nWarnings:');
    result.warnings.forEach((w) => console.log(` ${w}`));
  }

  console.log('\n=== JSON Output ===');
  console.log(JSON.stringify(result, null, 2));
}

if (require.main === module) {
  main().catch((error) => {
    console.error('Error:', error instanceof Error ? error.message : String(error));
    process.exit(1);
  });
}
