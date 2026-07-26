# Solver / Keeper Bot

[`script/solver_bot.py`](../script/solver_bot.py) closes the loop the Dutch-auction
backtests model, against a live chain: it watches a hooked pool's stale gap, opens
the auction clock the moment a trigger-eligible gap appears (`pokeAuction`), and
executes the repricing swap once the scheduled concession clears its threshold. The
fill swaps through a `PoolSwapTest` router with the reference sqrt price as the
price limit, so the pool lands exactly on the oracle price and the auction closes.

Stdlib-only Python (3.9+); all chain access goes through the Foundry `cast` binary.
Pure decision/math helpers are unit-tested in
[`script/test_solver_bot.py`](../script/test_solver_bot.py).

## An open agent market

This bot is not "the solver" — it is one reference agent in a permissionless market of
autonomous repricing agents. The auction turns stale-price correction into an open agent
market: any address can perceive the auction state, decide from a public policy, and act,
with no registration, allowlist, or hook-specific calldata. Permissionlessness is what
makes this a *market* of competing agents rather than a bot the protocol operates.

The agent loop is three public functions and an ordinary swap:

- **Perceive** — `auctionStatus(key)` and `previewSwapFee(key, dir)` are public views, so
  any keeper or arb bot can watch the gap, the auction clock, and the scheduled
  concession.
- **Decide** — the pure policy helpers (`should_poke`, `should_fill`,
  `toxic_zero_for_one` in [`script/solver_bot.py`](../script/solver_bot.py), unit-tested
  in [`script/test_solver_bot.py`](../script/test_solver_bot.py)) map that state to an
  action, with no operator in the loop.
- **Act** — `pokeAuction(key)` is callable by any address (it only reads the oracle and
  pool price and updates the clock), and the fill itself is a plain v4 swap through any
  router; the concession-discounted fee is applied by `beforeSwap` regardless of who the
  swap sender is. There is no bespoke fill call to integrate.

The only owner-gated functions are `setConfig` and `setRiskState` (pool parameters); the
owner has no role in auction operation.

**Coordination and policy compliance are on-chain, not off-chain.** Agent standards such
as ERC-8001 (multi-agent EIP-712 acceptance attestations) and ERC-8196
(agent-authenticated wallets that prove an action complies with the owner's policy) place
coordination and compliance in an off-chain identity layer. This mechanism achieves the
same guarantees at the settlement layer, which is precisely why it stays permissionless:

| Concern | Agent-identity ERCs (8001 / 8196) | This mechanism |
| --- | --- | --- |
| Coordinate competing agents | Off-chain EIP-712 acceptance attestations | Public descending-concession clock; agents race the price, no messaging |
| Enforce the owner's (LP) policy | Agent supplies a cryptographic proof of compliance | `beforeSwap` enforces the fee / oracle / concession policy at execution and fails closed on any non-compliant fill |
| Authorize the agent | Registry or agent-authenticated wallet | None — any address; policy binds the *action*, not the *actor* |

The inversion is the point: ERC-8196 wants an agent to *prove* its action complies with
the owner's policy; the hook makes the proof unnecessary because a non-compliant fill
cannot settle. LPs get the guarantee without an agent-identity layer.

`test_auction_permissionless_strangerPokesAndStrangerFills` in
[`test/OracleAnchoredLVRHookAuction.t.sol`](../test/OracleAnchoredLVRHookAuction.t.sol)
pins this end-to-end: a non-owner address opens the clock and a second non-owner fills
through the standard router at the aged concession. Consequence for solver economics:
existing arbitrage bots are already compatible agents — they can fill profitable auctions
without integrating anything beyond a fee preview, so the mechanism does not depend on a
bespoke solver network forming.

> These are *autonomous economic agents* — deterministic policy over public state, not
> learned models. The "agent market" claim is about open, unlicensed participation and
> on-chain coordination, not AI.

## Demo pool

Live testnet Chainlink feeds barely move, so auction triggering cannot be
demonstrated against them on demand.
[`script/DeployDemoPool.s.sol`](../script/DeployDemoPool.s.sol) deploys a fully
controllable environment on the real Base Sepolia PoolManager: two mintable mock
tokens, two manually settable `ManualAggregatorV3` feeds behind a real
`ChainlinkReferenceOracle`, swap/liquidity routers, and a seeded 1:1 pool on the
hook with the recommended auction cell. The feed setters are permissionless, so
this setup is strictly demo-grade.

Current demo deployment (Base Sepolia, 2026-07-10, on the hook in
[deployment.md](deployment.md)):

| Contract | Address |
| --- | --- |
| token0 (LVRA) | `0xd1B218DA32f265fF35475487C13655AC7babdA5c` |
| token1 (LVRB) | `0xF6D695a5bEa82046beFa23E7209bc70B7c7c08ce` |
| base feed | `0x9936f7b86eA965aC29e009e286f5c38f55870Ec0` |
| quote feed | `0xC71D016856e2E9C59c96D5553193C7D324690c2e` |
| oracle | `0x30028311F29C1843b20C723b8ac4f1b54BC416e2` |
| swap router | `0x8054C37cF5C23d0186EFc0F61D7F021b5DF854e4` |
| liquidity router | `0xB9729C4Ff9ffbe34F65a2DbBFDB412A344Cc5154` |
| pool id | `0x257440152bad8c4d934962a3a3fd8aaa0424ed2e9a346f7f08ce37f75f3b9559` |

## Running the loop

Configuration comes from flags or the environment (`HOOK`, `TOKEN0`, `TOKEN1`,
`SWAP_ROUTER`, `BASE_FEED`, `QUOTE_FEED`, `RPC_URL`/`BASE_SEPOLIA_RPC_URL`,
`PRIVATE_KEY`/`DEPLOYER_KEY`); the repo-local `.env` carries the demo values.

### Signing

`cast send --private-key <KEY>` would put the key in the process table, where any
local user can read it with `ps`. The bot therefore prefers an **encrypted
keystore account**, which keeps every secret off argv — cast decrypts the
keystore itself and takes the password from the file named by `ETH_PASSWORD`:

```bash
cast wallet import keeper --interactive          # one-time; prompts for the key
printf '<password>' > ~/.keeper.pass && chmod 600 ~/.keeper.pass
export KEYSTORE_ACCOUNT=keeper ETH_PASSWORD=~/.keeper.pass
```

`PRIVATE_KEY`/`DEPLOYER_KEY` still works and takes the argv path, but warns once
per run. Keep it for throwaway testnet keys only; use the keystore for any key
that controls real liquidity. (`cast --interactive` cannot be used
programmatically — it prompts on `/dev/tty`, not stdin.)

```bash
set -a; source .env; set +a
python3 script/solver_bot.py status
python3 script/solver_bot.py make-gap --bps 30      # move the reference 30 bps
python3 script/solver_bot.py run --interval 15 --min-concession-wad 3e15 --keep-fresh
```

`run` polls continuously, pokes when the current gap and stored auction clock are
out of sync, and fills once the concession reaches `--min-concession-wad`. That
means it starts a fresh clock when an above-trigger gap appears, and it also
closes any old stored clock while the gap is below trigger so the next auction
does not inherit stale concession. `--keep-fresh` re-stamps the demo feeds when
they approach staleness. `make-gap` accepts negative bps to move the reference
below the pool.

## Real USDC/WETH pool

The real Base Sepolia USDC/WETH deployment uses live Chainlink testnet feeds, so
do not use `make-gap`, `refresh-oracle`, or `--keep-fresh` against it. Those
commands are only for the permissionless manual-feed demo pool above.

For the real pool, run the keeper with explicit token decimals and conservative
directional fill caps:

```bash
python3 script/solver_bot.py \
  --rpc-url https://sepolia.base.org \
  --hook 0x22081E668dC0f43B6166561Ac4A6Df359AA88880 \
  --token0 0x036CbD53842c5426634e7929541eC2318f3dCF7e \
  --token1 0x4200000000000000000000000000000000000006 \
  --base-feed 0xd30e2101a97dcbAeBCBC04F14C3f624E67A35165 \
  --quote-feed 0x4aDC67696bA383F43DD60A9e78F2C97Fbbfc7cb1 \
  --swap-router 0x8054C37cF5C23d0186EFc0F61D7F021b5DF854e4 \
  --token0-decimals 6 \
  --token1-decimals 18 \
  --amount0-in 10 \
  --amount1-in 0.003 \
  status
```

Replace `status` with `run --interval 15 --min-concession-wad 3e15` to operate
the loop. `pokeAuction` is permissionless and cheap; fills require the keeper
wallet to hold and approve the input token for the swap router.

## What the live demo showed (Base Sepolia, 2026-07-10)

Cycle 1 — 30 bps gap above the pool:

```
gap 29.98 trigger-bps | eligible=True clock=- concession=0.1000% | toxic fee 1997 ppm
poked auction clock: 0xe50210...70c77
gap 29.98 trigger-bps | clock=1783722148 concession=0.1800% | toxic fee 1996 ppm
gap 29.98 trigger-bps | clock=1783722148 concession=0.3800% | toxic fee 1993 ppm
filled repricing swap: 0x4fabad...12e1a5
gap 0.00 trigger-bps | eligible=False
```

The toxic fee matched the fee law exactly (30 bps gap => 1500 ppm surcharge + 500
ppm base, minus the concession), the concession grew on the configured 0.5 bps/sec
schedule, and the fill repriced the pool to a zero gap.

Cycle 2 — reference moved below the pool — reproduced live the aged-clock edge the
adversarial tests document (`test_auction_gapDirectionFlipInheritsAgedConcession`):
the auction close is lazy, no interaction had observed the closed gap, so the new
opposite-direction gap inherited the old clock's accrued 2.23% concession and the
bot filled immediately.

## Side-by-side static-fee baseline

[`script/DeployBaselinePool.s.sol`](../script/DeployBaselinePool.s.sol) deploys a
hookless control pool on the same demo tokens — static 30 bps fee (the classic
v3 tier and what a hookless v4 pool would charge), same tick range and seeded
liquidity, initialized at the same reference price. `solver_bot.py compare`
then reads both LPs' exact withdrawable value (fees included) via simulated
full withdrawal, so the comparison is on-chain state, not a model.

Baseline pool on Base Sepolia (2026-07-10): static fee `3000`, pool id
`0x1d0f8d083126f3731abe0cc71566db605946d93079b1419f4bf95ef9af7b2456`.

Live run of one 100 bps reference move pushed through both pools:

```
SNAPSHOT 1: hooked LP - baseline LP = +0.010379 tokens   (fees from earlier cycles)
make-gap --bps 100
hooked fill:   toxic fee 5,474 ppm (gap-scaled, net of 0.25% concession)
baseline fill: static fee 3,000 ppm
SNAPSHOT 2: hooked LP - baseline LP = +0.022879 tokens
event-attributable difference: +0.012500 tokens on one repricing event
```

The +0.0125 matches the fee arithmetic exactly: (5,474 - 3,000) ppm on the
~5.05-token repricing notional, a 1.82x fee capture on this event. Honest
caveat for the pitch: the gap-scaled fee only exceeds a 30 bps static fee for
gaps above roughly 50 bps (`5 + gap/2` bps vs `30` bps), so on small gaps the
static pool captures more per event. The aggregate studies still favor the hook
because stale-loss value is dominated by large gaps, and benign flow pays 5 bps
instead of 30 — both halves of that argument are demonstrable on this pair of
pools.

Operational notes:

- The public `sepolia.base.org` RPC is load-balanced with inconsistent mempool
  views; back-to-back sends can transiently report `replacement transaction
  underpriced` (sometimes for a transaction that was actually accepted). The bot
  retries transient nonce errors; treat "failed" demo-feed moves as possibly
  applied.
- A production solver would replace the time-threshold fill policy with the
  profitability gates from the backtest (`solverGasCostQuote`, `solverEdgeBps`,
  reserve checks in `script/run_dutch_auction_backtest.py`).
