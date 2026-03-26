# V4 Architecture

Uniswap V4-specific architectural features. **All content on this page is V4-only.**

---

## V3 → V4 Architecture Comparison

| Aspect | V3 | V4 |
|--------|----|----|
| Pool contracts | One contract per pool | Single PoolManager for all pools |
| Pool creation | Deploy new contract | Call `PoolManager.initialize()` |
| Pool identifier | Contract address | PoolKey hash (PoolId) |
| Token transfers | Per-operation | Batched via flash accounting |
| Fee configuration | Fixed at creation | Dynamic (can change per-swap) |
| Native ETH | No (WETH required) | Yes |
| Extensibility | Limited | Hooks system |

---

## Singleton Design **[V4 Only]**

All pools are managed by a single `PoolManager.sol` contract.

### Benefits
- **Gas efficiency**: No cross-contract calls between pools
- **Simplified routing**: Multi-hop swaps in single transaction
- **Flash accounting**: Net token transfers across operations

### Pool Identification

Pools are identified by `PoolKey`:

```solidity
struct PoolKey {
    Currency currency0;      // First token (lower address)
    Currency currency1;      // Second token (higher address)
    uint24 fee;             // Fee tier
    int24 tickSpacing;      // Tick spacing
    IHooks hooks;           // Hook contract address
}
```

`PoolId` is the keccak256 hash of the PoolKey.

**V3 difference**: In V3, each pool has its own contract address. In V4, pools don't have addresses - they're identified by their PoolKey.

[Source: Uniswap V4 Docs](https://docs.uniswap.org/contracts/v4/overview)

---

## Flash Accounting (EIP-1153) **[V4 Only]**

V4 uses transient storage (EIP-1153) for efficient balance tracking.

### How It Works

1. **Start**: Call `PoolManager.unlock()`
2. **Operations**: Perform swaps, mints, burns (no token transfers yet)
3. **Tracking**: PoolManager tracks net balance changes in transient storage
4. **Settlement**: At end, settle net amounts with actual transfers
5. **Clear**: Transient storage auto-clears after transaction

### Benefits

- **Gas savings**: Only net amounts transferred
- **Multi-operation efficiency**: Swap A→B→C with single A-in, C-out transfer
- **Atomicity**: All operations succeed or all fail

### Code Pattern

```solidity
// V4 pattern with flash accounting
poolManager.unlock(callbackData);

function unlockCallback(bytes calldata data) external {
    // Perform multiple operations
    poolManager.swap(...);
    poolManager.modifyLiquidity(...);

    // Settle net balances
    poolManager.settle(currency);  // Pay what you owe
    poolManager.take(currency);    // Receive what you're owed
}
```

**V3 difference**: V3 transfers tokens for each operation. V4 batches all transfers to the end.

[Source: Uniswap V4 Docs](https://docs.uniswap.org/contracts/v4/overview)

---

## Dynamic Fees **[V4 Only]**

V4 allows fees to change dynamically.

### Fee Types

| Type | Description |
|------|-------------|
| Static | Set at pool creation, never changes (like V3) |
| Dynamic | Can be updated by hooks |

### Dynamic Fee Implementation

Hooks can implement `getFee()` to return custom fees:

```solidity
function getFee(
    address sender,
    PoolKey calldata key,
    IPoolManager.SwapParams calldata params,
    bytes calldata hookData
) external view returns (uint24 fee);
```

### Fee Update Methods

- **Per-swap**: Hook returns fee in `beforeSwap`
- **Per-block**: Hook tracks block number, updates periodically
- **Custom schedule**: Any logic the hook implements

**V3 difference**: V3 fees are immutable after pool creation (0.01%, 0.05%, 0.30%, or 1.00%).

[Source: Uniswap V4 Docs](https://docs.uniswap.org/contracts/v4/overview)

---

## Native ETH Support **[V4 Only]**

V4 supports native Ether directly, without WETH wrapping.

### How It Works

- Pools can use `address(0)` or a special ETH sentinel as currency
- Swaps can send/receive native ETH
- No WETH9 wrapping/unwrapping required

### Benefits

- **Gas savings**: No wrap/unwrap transactions
- **UX improvement**: Users transact with native ETH
- **Fewer approvals**: No WETH approval needed

**V3 difference**: V3 requires WETH9 wrapping for all ETH operations.

[Source: Uniswap V4 Docs](https://docs.uniswap.org/contracts/v4/overview)

---

## Custom Accounting **[V4 Only]**

Hooks can modify token amounts in swaps and liquidity operations.

### Capabilities

- **Custom curves**: Implement non-x*y=k pricing
- **Hook fees**: Take additional fees beyond LP fees
- **Rebates**: Return tokens to users under certain conditions
- **Synthetic assets**: Mint/burn tokens based on swap activity

### Implementation

Hooks use return deltas to adjust amounts:

```solidity
function afterSwap(
    address sender,
    PoolKey calldata key,
    IPoolManager.SwapParams calldata params,
    BalanceDelta delta,
    bytes calldata hookData
) external returns (bytes4, int128 hookDelta);
```

**V3 difference**: V3 has no mechanism for custom accounting - amounts are determined solely by the AMM formula.

---

## Pool Initialization **[V4 Only]**

Creating a V4 pool:

```solidity
poolManager.initialize(
    PoolKey({
        currency0: Currency.wrap(token0),
        currency1: Currency.wrap(token1),
        fee: 3000,           // 0.30%
        tickSpacing: 60,
        hooks: IHooks(hookAddress)  // or address(0) for no hooks
    }),
    sqrtPriceX96           // Initial price
);
```

**V3 difference**: V3 uses a factory pattern (`UniswapV3Factory.createPool()`), which deploys a new contract.

---

## Doppler V4 Usage

Doppler's V4 contracts (Doppler hook, UniswapV4MulticurveInitializer) use:

| V4 Feature | Doppler Usage |
|------------|---------------|
| Singleton | All pools via single PoolManager |
| Flash accounting | Unlock pattern for position management |
| Hooks | Doppler.sol implements beforeSwap/afterSwap for rebalancing |
| Dynamic fees | initialLpFee set at init, can be overridden |
| PoolKey identification | Store state by PoolId hash |
