# Security Readiness and Release Gates

Status: **research/testnet only; not approved for real capital**.

## Automated gates

Every pull request must pass:

- Solidity unit, fuzz, and invariant tests;
- pinned-block fork tests when the protected RPC secret is available;
- Python research and operator tests;
- deployed bytecode size checks;
- tracked-secret detection; and
- Slither 0.11.5, with high-severity findings failing CI.

Run the local equivalent with:

```bash
forge test -vvv --no-match-path test/OracleAnchoredLVRHookFork.t.sol
python3 -m unittest discover -s script -p 'test_*.py'
forge build --sizes
./script/security_checks.sh --slither
```

Slither's currently accepted informational/medium signals are triaged in
`docs/slither_triage.md`; an accepted signal is not an audit waiver.

## Required real-capital launch gates

All boxes must be checked against one immutable commit and compiler profile:

- [ ] Independent human audit covers the hook, oracle, ownership flow, deployment
      scripts, and exact v4 dependency commits; no unresolved critical/high issue.
- [ ] Audit remediations have regression tests and auditor re-review.
- [ ] Hook and oracle source are verified on the target explorer using
      `script/verify_deployment.sh`.
- [ ] `script/check_deployment.sh` passes against two independent RPCs; initial
      tick, feeds, decimals, heartbeat/deviation, sequencer feed, and config are
      independently reviewed.
- [ ] Owner is the intended Safe, `pendingOwner` is zero, signer policy is
      recorded, and the deployer EOA no longer has authority.
- [ ] Keeper uses a dedicated low-balance keystore, bounded approvals/inventory,
      independent RPCs, profitability reserves, alerts, and a tested standby.
- [ ] Threat model and incident runbook have completed review and tabletop.
- [ ] Pinned fork rehearsal covers deployment, configuration, ownership handoff,
      quoting, swap, auction poke/fill, oracle failure, and LP removal.
- [ ] Frozen evidence release and public claim check pass; no modeled fill rate is
      described as guaranteed production liveness.
- [ ] 24-hour bounded-liquidity canary and explicit governance/security go/no-go.

The AuditAgent wrapper is a pre-audit aid only. Its output does not satisfy the
independent audit gate.

## Ownership handoff

The hook uses a two-step handoff:

1. current owner submits `transferOwnership(SAFE)`;
2. reviewers verify the pending address and decoded transaction;
3. the Safe submits `acceptOwnership()`;
4. deployment checks confirm `owner == SAFE` and `pendingOwner == 0`.

There is deliberately no renounce function. A keeper signer must never be the
owner or a Safe signer merely for operational convenience.

## Source-verification gate

Set the exact constructor inputs and run:

```bash
CHAIN_ID=... ETHERSCAN_API_KEY=... \
HOOK=... POOL_MANAGER=... INITIAL_HOOK_OWNER=... \
ORACLE=... BASE_FEED=... QUOTE_FEED=... \
TOKEN0_DECIMALS=... TOKEN1_DECIMALS=... \
./script/verify_deployment.sh
```

`INITIAL_HOOK_OWNER` is the constructor argument, not necessarily the current Safe
after handoff. Store explorer links, compiler version, optimizer/via-IR settings, constructor
arguments, runtime code hashes, dependency commits, deployment transaction hashes,
and the release commit in the deployment record. A bytecode/address check alone is
not source verification.
