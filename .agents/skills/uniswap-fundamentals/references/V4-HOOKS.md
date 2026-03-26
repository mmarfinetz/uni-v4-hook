# V4 Hooks

Uniswap V4 hooks system for custom pool logic. **All content on this page is V4-only.**

---

## What Are Hooks? **[V4 Only]**

Hooks are external smart contracts that execute custom logic at specific points in a pool's lifecycle.

### Key Properties

- **One hook per pool**: Each pool can have at most one hook contract
- **One hook serves many pools**: A single hook contract can be used by unlimited pools
- **Optional**: Pools can have no hook (`hooks: address(0)`)
- **Immutable per pool**: Hook cannot be changed after pool creation

**V3 difference**: V3 has no hook system. Pool behavior is fixed by the core protocol.

[Source: Uniswap V4 Docs](https://docs.uniswap.org/contracts/v4/concepts/hooks)

---

## Available Hook Functions **[V4 Only]**

| Lifecycle Phase | Before Hook | After Hook |
|-----------------|-------------|------------|
| Pool creation | `beforeInitialize` | `afterInitialize` |
| Add liquidity | `beforeAddLiquidity` | `afterAddLiquidity` |
| Remove liquidity | `beforeRemoveLiquidity` | `afterRemoveLiquidity` |
| Swap | `beforeSwap` | `afterSwap` |
| Donate | `beforeDonate` | `afterDonate` |

### When Each Hook Fires

```
Pool Lifecycle:
  initialize() → beforeInitialize → [create pool] → afterInitialize

Swap:
  swap() → beforeSwap → [execute swap] → afterSwap

Liquidity:
  modifyLiquidity(+) → beforeAddLiquidity → [add] → afterAddLiquidity
  modifyLiquidity(-) → beforeRemoveLiquidity → [remove] → afterRemoveLiquidity

Donate:
  donate() → beforeDonate → [donate] → afterDonate
```

[Source: Uniswap V4 Docs](https://docs.uniswap.org/contracts/v4/concepts/hooks)

---

## Hook Permissions (Address Encoding) **[V4 Only]**

Hook permissions are encoded in the hook contract's address.

### How It Works

The PoolManager checks specific bits of the hook address to determine which functions the hook implements.

```
Hook Address: 0x...XXXX
                  ^^^^
                  Permission bits
```

### Permission Flags

| Bit Position | Permission |
|--------------|------------|
| 0 | beforeInitialize |
| 1 | afterInitialize |
| 2 | beforeAddLiquidity |
| 3 | afterAddLiquidity |
| 4 | beforeRemoveLiquidity |
| 5 | afterRemoveLiquidity |
| 6 | beforeSwap |
| 7 | afterSwap |
| 8 | beforeDonate |
| 9 | afterDonate |
| 10 | beforeSwapReturnDelta |
| 11 | afterSwapReturnDelta |
| 12 | afterAddLiquidityReturnDelta |
| 13 | afterRemoveLiquidityReturnDelta |

### Mining Hook Addresses

Hook addresses must be "mined" using CREATE2 to have the correct permission bits. This is done at deployment time.

**Example**: If your hook only needs `beforeSwap` and `afterSwap`, the address must have bits 6 and 7 set.

[Source: Uniswap V4 Docs](https://docs.uniswap.org/contracts/v4/concepts/hooks)

---

## Hook Function Signatures **[V4 Only]**

### beforeSwap

```solidity
function beforeSwap(
    address sender,
    PoolKey calldata key,
    IPoolManager.SwapParams calldata params,
    bytes calldata hookData
) external returns (bytes4, BeforeSwapDelta, uint24 lpFeeOverride);
```

**Returns**:
- `bytes4`: Function selector (for validation)
- `BeforeSwapDelta`: Token amount adjustments
- `uint24`: Fee override (or 0 to use pool default)

### afterSwap

```solidity
function afterSwap(
    address sender,
    PoolKey calldata key,
    IPoolManager.SwapParams calldata params,
    BalanceDelta delta,
    bytes calldata hookData
) external returns (bytes4, int128);
```

**Returns**:
- `bytes4`: Function selector
- `int128`: Hook delta (additional token adjustment)

### beforeInitialize / afterInitialize

```solidity
function beforeInitialize(
    address sender,
    PoolKey calldata key,
    uint160 sqrtPriceX96
) external returns (bytes4);

function afterInitialize(
    address sender,
    PoolKey calldata key,
    uint160 sqrtPriceX96,
    int24 tick
) external returns (bytes4);
```

[Source: Uniswap V4 Docs](https://docs.uniswap.org/contracts/v4/concepts/hooks)

---

## Hook Use Cases **[V4 Only]**

| Use Case | Hooks Used | Description |
|----------|------------|-------------|
| Limit orders | beforeSwap | Execute pending orders when price crosses |
| Dynamic fees | beforeSwap | Adjust fees based on volatility, time, etc. |
| TWAMM | beforeSwap | Time-weighted average market maker |
| Oracle | afterSwap | Record price observations |
| MEV protection | beforeSwap | Detect and mitigate sandwich attacks |
| Rebalancing | beforeSwap, afterSwap | Adjust positions based on conditions |
| Access control | beforeSwap | Whitelist/blacklist traders |
| Custom curves | beforeSwap | Implement non-standard pricing |

[Source: Uniswap V4 Docs](https://docs.uniswap.org/contracts/v4/concepts/hooks)

---

## Hook Implementation Pattern **[V4 Only]**

### Base Hook Contract

```solidity
import {BaseHook} from "@v4-periphery/BaseHook.sol";

contract MyHook is BaseHook {
    constructor(IPoolManager _poolManager) BaseHook(_poolManager) {}

    // Declare which hooks you implement
    function getHookPermissions() public pure override returns (Hooks.Permissions memory) {
        return Hooks.Permissions({
            beforeInitialize: false,
            afterInitialize: true,
            beforeAddLiquidity: false,
            afterAddLiquidity: false,
            beforeRemoveLiquidity: false,
            afterRemoveLiquidity: false,
            beforeSwap: true,
            afterSwap: true,
            beforeDonate: false,
            afterDonate: false,
            beforeSwapReturnDelta: false,
            afterSwapReturnDelta: false,
            afterAddLiquidityReturnDelta: false,
            afterRemoveLiquidityReturnDelta: false
        });
    }

    // Implement your hooks
    function _beforeSwap(...) internal override returns (...) {
        // Your logic
    }
}
```

---

## Doppler Hook Implementation **[V4 Only]**

Doppler's `Doppler.sol` is a V4 hook that implements:

| Hook | Doppler Usage |
|------|---------------|
| `beforeInitialize` | Validate pool parameters |
| `afterInitialize` | Place initial liquidity slugs |
| `beforeAddLiquidity` | Block external liquidity adds (only hook can add) |
| `beforeSwap` | Rebalance if new epoch; check exit conditions |
| `afterSwap` | Update totals; enforce price bounds; check early exit |
| `beforeDonate` | Always reverts (donations blocked) |

### Doppler Hook Permissions

```solidity
Hooks.Permissions({
    beforeInitialize: true,
    afterInitialize: true,
    beforeAddLiquidity: true,   // Blocks external adds
    beforeRemoveLiquidity: false,
    afterAddLiquidity: false,
    afterRemoveLiquidity: false,
    beforeSwap: true,           // Rebalancing
    afterSwap: true,            // Accounting
    beforeDonate: true,         // Always reverts
    afterDonate: false,
    beforeSwapReturnDelta: false,
    afterSwapReturnDelta: false,
    afterAddLiquidityReturnDelta: false,
    afterRemoveLiquidityReturnDelta: false
})
```

[Source: Doppler.sol](https://raw.githubusercontent.com/whetstoneresearch/doppler/46bad16d/src/initializers/Doppler.sol)

---

## Hook Gotchas **[V4 Only]**

1. **Address mining required**: Hook permissions are encoded in the address - must use CREATE2 with correct salt

2. **One hook per pool**: Cannot combine multiple hooks - must implement all needed logic in one contract

3. **Gas costs**: Hooks add gas to every operation - keep logic efficient

4. **Reentrancy**: Hooks can call back into PoolManager - be careful with state

5. **No hook changes**: Once a pool is created with a hook, it cannot be changed

6. **Return values matter**: Must return correct selector or call reverts
