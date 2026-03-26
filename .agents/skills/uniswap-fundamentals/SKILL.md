---
name: uniswap-fundamentals
description: Reference for Uniswap V3/V4 concepts used in Doppler development, including tick math, sqrtPriceX96, concentrated liquidity formulas, and V4 hooks/singleton architecture.
license: MIT
metadata:
  author: uniswap
  version: "2.0"
  source: docs.uniswap.org
---

# Uniswap Fundamentals (Doppler-focused)

## When to use
- You need math/context for Doppler pool parameters and pricing
- You are interpreting tick movement, liquidity ranges, or sqrtPriceX96 values
- You are debugging V4 hook lifecycle behavior

## Scope
This skill is intentionally V3/V4-centric for current Doppler development.

## Core concepts map
| Concept | Version | Reference |
|---|---|---|
| sqrtPriceX96 and tick math | V3+V4 | [TICK-MATH.md](references/TICK-MATH.md) |
| Liquidity formulas | V3+V4 | [LIQUIDITY.md](references/LIQUIDITY.md) |
| Singleton + flash accounting | V4 | [V4-ARCHITECTURE.md](references/V4-ARCHITECTURE.md) |
| Hook lifecycle and flags | V4 | [V4-HOOKS.md](references/V4-HOOKS.md) |

## Doppler usage map
| Doppler skill | Uniswap concepts used |
|---|---|
| `pda-static` | V3 tick spacing, range liquidity, far-tick exits |
| `pda-dynamic` | V4 hooks, epoch rebalancing, dynamic liquidity placement |
| `pda-multicurve` | V4 concentrated ranges, multicurve allocation |
| `doppler-hooks` | V4 hook permissions and callback integration |

## Critical invariant
Token ordering (`token0 < token1`) drives:
- Tick direction interpretation
- Price direction assumptions
- Asset/numeraire orientation in calculations and migration logic

## References
- [TICK-MATH.md](references/TICK-MATH.md)
- [LIQUIDITY.md](references/LIQUIDITY.md)
- [V4-ARCHITECTURE.md](references/V4-ARCHITECTURE.md)
- [V4-HOOKS.md](references/V4-HOOKS.md)
