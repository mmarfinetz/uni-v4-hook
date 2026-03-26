import type { SwapAction, SwapResult, ExecutionReceipt, SupportedChainId } from './types.js';

export function successResult(
  action: SwapAction,
  chainId: SupportedChainId,
  tokenIn: string,
  tokenOut: string,
  amountIn: string,
  amountOut: string,
  txHash: string,
  receipt: ExecutionReceipt,
  warnings: string[] = []
): SwapResult {
  return {
    ok: true,
    action,
    chainId,
    tokenIn,
    tokenOut,
    amountIn,
    amountOut,
    txHash,
    receipt,
    warnings,
  };
}

export function dryRunResult(
  action: SwapAction,
  chainId: SupportedChainId,
  tokenIn: string,
  tokenOut: string,
  amountIn: string,
  amountOut: string,
  warnings: string[] = []
): SwapResult {
  return {
    ok: true,
    action,
    chainId,
    tokenIn,
    tokenOut,
    amountIn,
    amountOut,
    warnings,
  };
}

export function failureResult(
  action: SwapAction,
  chainId: SupportedChainId,
  tokenIn: string,
  tokenOut: string,
  amountIn: string,
  amountOut: string,
  error: string,
  warnings: string[] = []
): SwapResult {
  return {
    ok: false,
    action,
    chainId,
    tokenIn,
    tokenOut,
    amountIn,
    amountOut,
    warnings,
    error,
  };
}
