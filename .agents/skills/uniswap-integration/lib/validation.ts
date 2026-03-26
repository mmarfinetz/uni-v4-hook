import { parseUnits } from 'viem';
import { CHAIN_IDS, TOKENS, FEE_TIERS } from './addresses.js';
import type { SupportedChainId, FeeTier } from './types.js';

export function assertAddress(address: string, field: string): `0x${string}` {
  if (!/^0x[a-fA-F0-9]{40}$/.test(address)) {
    throw new Error(`Invalid ${field}: ${address}`);
  }
  return address as `0x${string}`;
}

export function assertChainId(chainId: number): SupportedChainId {
  if (!CHAIN_IDS.includes(chainId as SupportedChainId)) {
    throw new Error(`Unsupported chainId: ${chainId}. Supported: 1, 42161`);
  }
  return chainId as SupportedChainId;
}

export function assertPositiveAmount(amount: string): string {
  if (!/^[0-9]+(\.[0-9]+)?$/.test(amount)) {
    throw new Error(`Invalid amount format: ${amount}`);
  }
  if (Number(amount) <= 0) {
    throw new Error(`Amount must be positive: ${amount}`);
  }
  return amount;
}

export function assertFeeTier(fee: number): FeeTier {
  if (!FEE_TIERS.includes(fee as FeeTier)) {
    throw new Error(`Invalid fee tier: ${fee}. Supported: 100, 500, 3000, 10000`);
  }
  return fee as FeeTier;
}

export function resolveToken(
  chainId: SupportedChainId,
  tokenInput: string
): { address: `0x${string}`; symbol: string; decimals?: number } {
  const normalized = tokenInput.toUpperCase();
  const tokenMap = TOKENS[chainId];
  if (tokenMap[normalized]) {
    return {
      address: tokenMap[normalized].address,
      symbol: normalized,
      decimals: tokenMap[normalized].decimals,
    };
  }
  return {
    address: assertAddress(tokenInput, 'token'),
    symbol: tokenInput,
  };
}

export function toAmountWei(amount: string, decimals: number): bigint {
  return parseUnits(assertPositiveAmount(amount), decimals);
}

export function assertSlippage(slippage: number): number {
  if (slippage < 0 || slippage > 100) {
    throw new Error(`Slippage must be between 0 and 100, got: ${slippage}`);
  }
  return slippage;
}
