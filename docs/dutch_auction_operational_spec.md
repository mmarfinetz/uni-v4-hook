# Dutch Auction Operational Specification

This document describes the auction that is implemented in
`OracleAnchoredLVRHook.sol` and the reference keeper in `script/solver_bot.py`.
It is an operating specification, not an audit report or a claim of production
readiness.

## Settlement model

The auction is an on-chain, permissionless fee schedule. It does not escrow a
user swap, collect sealed bids, depend on a relay, or expose a bespoke
`fillAuction` function.

1. A fresh reference price and the current pool price determine the unsigned
   stale-gap premium.
2. At or above `triggerGapBps`, `pokeAuction(key)` or the next swap records
   `auctionStartTs[poolId]` and emits `AuctionOpened`.
3. The concession starts at `startConcessionWad` and grows linearly by
   `concessionGrowthWadPerSec`, capped by `maxConcessionWad`.
4. A toxic repricing swap pays
   `baseFee + alpha * gapPremium * (1 - concession)`. Benign flow pays
   `baseFee`.
5. Any account can submit the ordinary v4 swap. The hook enforces the current
   fee inside `beforeSwap`; solver identity is irrelevant.
6. Once the observed gap falls below the trigger, a swap or permissionless poke
   deletes the clock and emits `AuctionClosed`.

If the oracle is invalid or stale, the state reads and swap fail closed. If the
computed fee exceeds `maxFee`, the toxic direction reverts until either the gap
shrinks or the scheduled concession brings the fee under the cap.

Auction transitions are lazy: the contract learns that a gap opened or closed
only on a swap or `pokeAuction`. Operators should therefore run at least two
independent pokers, and should alert when an eligible gap has no clock.

## Configured policy surface

All auction terms are part of the pool's owner-gated `Config`:

| Field | Unit | Operational effect |
| --- | --- | --- |
| `triggerGapBps` | price-gap bps | Zero disables the auction; otherwise opens at this gap. |
| `startConcessionWad` | fraction of toxic surcharge | Discount available at clock open. |
| `concessionGrowthWadPerSec` | fraction per second | Linear descent speed. |
| `maxConcessionWad` | fraction of toxic surcharge | Hard discount ceiling; cannot exceed `1e18`. |
| `baseFee` | ppm | Floor charged to every executable swap. |
| `maxFee` | ppm | Fail-closed ceiling for a toxic fee. |
| `alphaBps` | bps | Fraction of measured stale premium charged as surcharge. |
| `maxOracleAge` | seconds | Maximum accepted age of the older reference leg. |

The owner can also seed or repair `RiskState`; the auction itself does not need
owner intervention. Production ownership must be a Safe reached through the
two-step `transferOwnership` / `acceptOwnership` flow. Keeper keys must never be
owners.

## Reference solver policy

Before broadcasting a fill, `solver_bot.py`:

1. simulates the exact router calldata with `eth_call` from the solver address;
2. decodes the returned `BalanceDelta` and values it at the current reference;
3. estimates gas for the same calldata and applies a configurable gas buffer;
4. converts gas into token1 units using the explicitly supplied native/token1
   rate;
5. requires simulated gross surplus to cover gas, `solver-edge-bps`, and
   `min-profit-token1`;
6. reports `c_min = (gas + edge + minimum profit) / available stale-gap value`,
   together with the squared hook premium used as the free-energy proxy;
7. refuses gas above `max-gas-price-gwei` and concession above the first-party
   `max-concession-wad` reserve; and
8. persists nonce, transaction hashes, receipts, counters, health state, and the
   last fill-economics gauges.

Run mode rejects raw private keys by default. It supports encrypted Foundry
keystores, RPC failover, explicit nonces, same-nonce fee-bumped replacements,
confirmation tracking, single-instance locking, bounded exponential backoff,
JSONL event logs, a health snapshot, and Prometheus text metrics.

The native/token1 conversion rate is an operator input, not an oracle read. It
must be refreshed conservatively; a stale rate can make the bot overestimate
profit. Independent solvers are still expected—the bundled bot is a reference
implementation, not a protocol dependency.

## Relationship to the research simulator

`research/lvr/backtest/run_dutch_auction_backtest.py` is a counterfactual study.
Its `solverGasCostQuote`, `solverEdgeBps`, reserve modes, maximum duration, and
fallback outcomes decide whether a historical opportunity would plausibly have
cleared. Those fields are not additional on-chain state.

The comparable production controls are the solver's preflight profitability
gate, the hook's fee/concession schedule, and `maxFee` fail-closed behavior. A
paper result about modeled fill rate must not be presented as an on-chain liveness
guarantee.

## Required alerts

- oracle invalid, stale, or sequencer check failing;
- `quotable(key) == false`;
- eligible gap without an open clock;
- open clock above the expected maximum age;
- concession or gas price above the operator reserve;
- preflight reverts or persistent unprofitable fills;
- unresolved pending transaction or exhausted replacement budget;
- consecutive tick errors, stale health heartbeat, or no successful tick;
- `OwnerInitialized`, `OwnershipTransferStarted`, `OwnershipTransferCancelled`,
  `OwnershipTransferred`, `ConfigSet`, or `RiskStateSet` outside an approved
  governance change.

## Production boundary

No real-capital launch is approved by this document. The release gates in
`docs/security_readiness.md`—especially independent audit, verified source,
Safe ownership, fork rehearsal, monitoring, and incident-response sign-off—must
all be closed first.
