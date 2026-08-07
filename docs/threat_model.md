# Threat Model

## Scope and security goals

The scope is `OracleAnchoredLVRHook`, `ChainlinkReferenceOracle`, deployment and
configuration, and the reference solver. Uniswap v4 `PoolManager`, external
routers, Chainlink feeds, the L2 sequencer, RPC providers, and solver inventory
are trust boundaries rather than code owned by this repository.

The primary goals are:

- never quote or swap from invalid, incomplete, stale, or sequencer-unsafe data;
- never charge a toxic-direction fee above the configured cap or below the
  policy's computed floor;
- keep liquidity removal available even when swap quoting fails;
- prevent unauthorized configuration or risk-state changes;
- make keeper failure economically bounded and observable; and
- preserve reproducibility between measured evidence and public claims.

## Threats and controls

| Threat | Existing control | Residual risk / required action |
| --- | --- | --- |
| Stale, zero, incomplete, or negative feed | Oracle and hook fail closed; freshness uses the older feed leg. | A valid but economically stale Chainlink round remains possible within the configured heartbeat. Record feed heartbeat/deviation and set `maxOracleAge` accordingly. |
| L2 sequencer outage | Optional uptime feed and post-recovery grace period. | A production L2 deployment must wire the correct feed; deployment review must reject zero address. |
| Feed inversion, decimals, or token ordering error | Constructor immutables, tests, and post-deploy address checks. | Human misconfiguration can still produce a plausible wrong price. Rehearse from a pinned fork and independently verify the initial tick. |
| Pool/oracle price dislocation | Dynamic toxic surcharge and `maxFee` fail-closed cap. | A cap can halt toxic repricing until concession accrues. Alert on capped directions and prolonged gaps. |
| Auction clock manipulation | Permissionless poke only derives state from public oracle/pool prices; concession is capped. | Transitions are lazy and direction flips can inherit an aged clock while the gap never crossed below trigger on-chain. This is tested and must remain in monitoring/runbooks. |
| Owner compromise or erroneous transfer | Two-step transfer, zero-address rejection, cancellation, events. | Owner can replace config/oracle immediately. Use a Safe, separate signers, transaction simulation, and an external change-control delay/process. The contract itself has no timelock. |
| Solver broadcasts an uneconomic fill | Exact-call simulation, gas estimate/buffer, gas-price cap, edge/min-profit reserve, concession cap. | Native/token1 rate and RPC simulation can be stale or dishonest. Use independent RPCs, conservative rate updates, small allowances, and bounded wallet inventory. |
| Duplicate keeper or nonce collision | Single-instance lock, explicit pending nonce, durable pending hashes, same-nonce replacements, confirmation tracking. | Multiple hosts can still share a signer. Assign one signer per active instance or use an external nonce coordinator. |
| RPC outage or inconsistent mempool | Read failover, receipt search across endpoints, bounded retries/backoff. | Correlated provider failure remains. Use administratively independent endpoints and alert before error budget exhaustion. |
| Secret leakage | Production run rejects argv raw keys; keystore path; tracked-secret CI gate. | Keystore password and host remain sensitive. Use a dedicated low-balance solver key and host secret manager. |
| Malicious router/token | Hook assumes standard v4 settlement; preflight uses the exact configured router. | Only approve reviewed routers and known tokens; cap approvals and inventory. |
| Hook defect after launch | Non-upgradeable bytecode limits governance takeover; liquidity removal hooks are disabled. | There is no pause or upgrade. Router/front-end delisting and LP withdrawal are the emergency path. Independent audit is mandatory. |
| Research/publication drift | Measured regimes, threshold sensitivity, frozen release artifacts, claim checks in CI. | The evidence release must be regenerated whenever source data or policy code changes. |

## Privileged operations

Only `setConfig`, `setRiskState`, ownership nomination, and ownership-transfer
cancellation require the owner. Acceptance requires the nominated address.
`pokeAuction` and swaps are permissionless. The hook is non-upgradeable and has
no pause, token custody, or owner withdrawal function.

## Explicit non-goals

The system does not guarantee a solver will appear, a transaction will beat
competing MEV, Chainlink reflects every sub-heartbeat market move, LPs earn a
positive return, or an external router/PoolManager is bug-free.
