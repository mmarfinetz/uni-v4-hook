export type SupportedChainId = 1 | 42161;

export type FeeTier = 100 | 500 | 3000 | 10000;

export type SwapAction = 'exactInput' | 'exactOutput';

export interface ExecutionReceipt {
  status: 'success' | 'reverted';
  gasUsed: string;
  blockNumber: string;
  transactionHash: string;
}

export interface SwapResult {
  ok: boolean;
  action: SwapAction;
  chainId: SupportedChainId;
  tokenIn: string;
  tokenOut: string;
  amountIn: string;
  amountOut: string;
  txHash?: string;
  receipt?: ExecutionReceipt;
  warnings: string[];
  error?: string;
}

export interface PoolData {
  token0: string;
  token1: string;
  fee: number;
  sqrtPriceX96: string;
  tick: number;
  liquidity: string;
  token0Price: string;
  token1Price: string;
}

export interface SwapQuote {
  tokenIn: string;
  tokenOut: string;
  amountIn: string;
  amountOut: string;
  amountOutMin: string;
  priceImpact: string;
  feeTier: number;
  route: string[];
}

export interface LiquidityQuote {
  token0: string;
  token1: string;
  feeTier: number;
  tickLower: number;
  tickUpper: number;
  amount0: string;
  amount1: string;
  liquidity: string;
  currentTick: number;
  currentPrice: string;
}
