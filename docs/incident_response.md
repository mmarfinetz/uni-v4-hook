# Incident Response Runbook

This hook is non-upgradeable and has no pause. Incident response therefore
focuses on stopping new routing, preserving evidence, correcting configuration
only when safe, and helping LPs exit. Do not improvise a configuration change
from a compromised owner.

## Severity

- **SEV-0:** confirmed owner compromise, exploitable hook/oracle defect, wrong
  production bytecode, or active loss.
- **SEV-1:** invalid feed wiring, prolonged fail-closed swaps, sequencer control
  absent during an outage, or repeated harmful fills.
- **SEV-2:** solver/RPC outage with the hook otherwise safe, stale monitoring,
  or an unresolved transaction.

## First 15 minutes

1. Open an incident log with UTC time, chain, pool ID, hook/oracle addresses,
   block number, reporter, and observed transactions. Assign one incident lead.
2. Preserve solver state, JSONL metrics, health files, RPC responses, transaction
   traces, and Safe activity. Do not delete or rotate the affected files.
3. Stop the reference solver if its key, economics, nonce state, or router is in
   doubt. Permissionless third-party solvers may continue.
4. Ask routers, frontends, and indexers to mark the pool non-quotable and stop new
   routing. Publish a concise status message with the exact affected addresses.
5. For SEV-0, remove the solver key from service, revoke router token allowances,
   and rotate RPC/API credentials. Do not use the suspected owner signer.

## Diagnose before changing state

Run `script/check_deployment.sh`, `quotable`, both `previewSwapFee` directions,
oracle `latestPriceWad`, feed round data, sequencer status, current config, owner,
pending owner, and recent ownership/config/risk events against two independent
RPC providers. Trace representative failed and successful swaps at pinned blocks.

Classify the incident as oracle/feed, configuration/governance, hook logic,
PoolManager/router/token, solver economics, nonce/RPC, or monitoring-only.

## Containment paths

- **Oracle or sequencer unsafe:** keep swaps delisted. A Safe may install a
  previously reviewed replacement oracle/config only after independent
  simulation and signer review. Failing closed is preferable to guessing a
  price.
- **Owner compromise:** the current owner cannot be overridden. If a safe pending
  owner was already nominated and uncompromised, it may accept after review;
  otherwise treat the deployment as lost and migrate.
- **Hook defect:** delist and direct LPs to remove liquidity. Removal is not hooked,
  so oracle failure does not intentionally block exit. Deploy audited replacement
  bytecode; do not claim an in-place fix.
- **Solver/RPC failure:** leave the hook live if quotes are safe, stop the affected
  signer, reconcile all pending hashes/nonces, and fail over to an independent
  operator or endpoint.

Every Safe transaction needs an incident ID, decoded calldata, fork simulation,
two-person review, and recorded signer approvals.

## Recovery and closure

Recovery requires the root cause, affected blocks/value, fixed or replacement
artifact hash, clean deployment checks, source verification, healthy monitoring,
and an explicit go/no-go by governance plus security lead. Run a 24-hour canary
with bounded liquidity before widening exposure.

Within five business days, publish a postmortem covering timeline, impact, root
cause, contributing controls, corrective actions, and evidence links. Add a
regression test for every code-level failure and rehearse this runbook before any
real-capital launch.
