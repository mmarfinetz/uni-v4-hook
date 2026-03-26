#!/usr/bin/env ts-node
/**
 * Uniswap V3 Swap Quote Script
 *
 * Gets a swap quote using QuoterV2 contract.
 *
 * Usage:
 *   npx tsx scripts/quote-swap.ts <chainId> <tokenIn> <tokenOut> <amountIn> [fee] [slippage]
 *
 * Examples:
 *   npx tsx scripts/quote-swap.ts 1 USDC WETH 1000 3000 0.5
 *   npx tsx scripts/quote-swap.ts 42161 WETH USDC 1 500 1.0
 */

import { createPublicClient, http, parseUnits, formatUnits } from 'viem';
import { mainnet, arbitrum } from 'viem/chains';

const QUOTER_V2_ADDRESS: Record<number, `0x${string}`> = {
  1: '0x61fFE014bA17989E743c5F6cB21bF9697530B21e',
  42161: '0x61fFE014bA17989E743c5F6cB21bF9697530B21e',
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

function getRpcUrl(chainId: number): string {
  const envVar = chainId === 1 ? 'ETHEREUM_RPC_URL' : 'ARBITRUM_RPC_URL';
  const defaultRpc = chainId === 1 ? 'https://ethereum.publicnode.com' : 'https://arbitrum.publicnode.com';
  return process.env[envVar] || defaultRpc;
}

export interface SwapQuoteResult {
  tokenIn: string;
  tokenOut: string;
  amountIn: string;
  amountInWei: string;
  amountOut: string;
  amountOutWei: string;
  amountOutMin: string;
  amountOutMinWei: string;
  feeTier: number;
  feeTierLabel: string;
  slippageBps: number;
  gasEstimate: string;
  priceRatio: string;
  chainId: number;
}

export async function quoteSwap(
  chainId: number,
  tokenInSymbol: string,
  tokenOutSymbol: string,
  amountIn: string,
  fee: number = 3000,
  slippagePercent: number = 0.5
): Promise<SwapQuoteResult> {
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
  const quoterAddress = QUOTER_V2_ADDRESS[chainId];

  const result = (await client.simulateContract({
    address: quoterAddress,
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

  const [amountOutWei, , , gasEstimate] = result.result;

  const slippageBps = Math.round(slippagePercent * 100);
  const amountOutMinWei = (amountOutWei * BigInt(10000 - slippageBps)) / 10000n;

  const amountOut = formatUnits(amountOutWei, tokenOut.decimals);
  const amountOutMin = formatUnits(amountOutMinWei, tokenOut.decimals);

  return {
    tokenIn: tokenInSymbol.toUpperCase(),
    tokenOut: tokenOutSymbol.toUpperCase(),
    amountIn,
    amountInWei: amountInWei.toString(),
    amountOut,
    amountOutWei: amountOutWei.toString(),
    amountOutMin,
    amountOutMinWei: amountOutMinWei.toString(),
    feeTier: fee,
    feeTierLabel: `${fee / 10000}%`,
    slippageBps,
    gasEstimate: gasEstimate.toString(),
    priceRatio: (Number(amountOut) / Number(amountIn)).toFixed(8),
    chainId,
  };
}

async function main() {
  const [chainIdStr, tokenIn, tokenOut, amountIn, feeStr, slippageStr] = process.argv.slice(2);

  if (!chainIdStr || !tokenIn || !tokenOut || !amountIn) {
    console.error('Usage: npx tsx scripts/quote-swap.ts <chainId> <tokenIn> <tokenOut> <amountIn> [fee] [slippage%]');
    console.error('Example: npx tsx scripts/quote-swap.ts 1 USDC WETH 1000 3000 0.5');
    process.exit(1);
  }

  const chainId = parseInt(chainIdStr, 10);
  const fee = feeStr ? parseInt(feeStr, 10) : 3000;
  const slippage = slippageStr ? parseFloat(slippageStr) : 0.5;

  if (![1, 42161].includes(chainId)) {
    console.error('Unsupported chainId. Use 1 (Ethereum) or 42161 (Arbitrum)');
    process.exit(1);
  }

  const quote = await quoteSwap(chainId, tokenIn, tokenOut, amountIn, fee, slippage);
  console.log(JSON.stringify(quote, null, 2));
}

if (require.main === module) {
  main().catch((error) => {
    console.error('Error:', error instanceof Error ? error.message : String(error));
    process.exit(1);
  });
}
