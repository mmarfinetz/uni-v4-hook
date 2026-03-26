import type { PublicClient } from 'viem';
import { uniswapV3FactoryAbi, uniswapV3PoolAbi } from './abis.js';
import { UNISWAP_V3_FACTORY } from './addresses.js';
import type { SupportedChainId, PoolData } from './types.js';

export async function getPoolAddress(
  client: PublicClient,
  chainId: SupportedChainId,
  token0: `0x${string}`,
  token1: `0x${string}`,
  fee: number
): Promise<`0x${string}`> {
  const poolAddress = (await client.readContract({
    address: UNISWAP_V3_FACTORY[chainId],
    abi: uniswapV3FactoryAbi,
    functionName: 'getPool',
    args: [token0, token1, fee],
  })) as `0x${string}`;

  if (poolAddress === '0x0000000000000000000000000000000000000000') {
    throw new Error(`No pool found for ${token0}/${token1} at fee tier ${fee}`);
  }

  return poolAddress;
}

export async function getPoolData(
  client: PublicClient,
  chainId: SupportedChainId,
  token0: `0x${string}`,
  token1: `0x${string}`,
  fee: number
): Promise<PoolData> {
  const poolAddress = await getPoolAddress(client, chainId, token0, token1, fee);

  const [slot0, liquidity, poolToken0, poolToken1, poolFee] = await Promise.all([
    client.readContract({
      address: poolAddress,
      abi: uniswapV3PoolAbi,
      functionName: 'slot0',
    }),
    client.readContract({
      address: poolAddress,
      abi: uniswapV3PoolAbi,
      functionName: 'liquidity',
    }),
    client.readContract({
      address: poolAddress,
      abi: uniswapV3PoolAbi,
      functionName: 'token0',
    }),
    client.readContract({
      address: poolAddress,
      abi: uniswapV3PoolAbi,
      functionName: 'token1',
    }),
    client.readContract({
      address: poolAddress,
      abi: uniswapV3PoolAbi,
      functionName: 'fee',
    }),
  ]);

  const slot0Result = slot0 as readonly [bigint, number, number, number, number, number, boolean];
  const sqrtPriceX96 = slot0Result[0];
  const tick = slot0Result[1];

  // Calculate price from sqrtPriceX96
  const price = Number(sqrtPriceX96) ** 2 / 2 ** 192;

  return {
    token0: poolToken0 as string,
    token1: poolToken1 as string,
    fee: Number(poolFee),
    sqrtPriceX96: sqrtPriceX96.toString(),
    tick,
    liquidity: (liquidity as bigint).toString(),
    token0Price: price.toFixed(10),
    token1Price: (1 / price).toFixed(10),
  };
}

export function tickToPrice(tick: number): number {
  return Math.pow(1.0001, tick);
}

export function priceToTick(price: number): number {
  return Math.floor(Math.log(price) / Math.log(1.0001));
}

export function nearestUsableTick(tick: number, tickSpacing: number): number {
  return Math.round(tick / tickSpacing) * tickSpacing;
}

export function getTickSpacing(fee: number): number {
  const spacings: Record<number, number> = {
    100: 1,
    500: 10,
    3000: 60,
    10000: 200,
  };
  return spacings[fee] ?? 60;
}
