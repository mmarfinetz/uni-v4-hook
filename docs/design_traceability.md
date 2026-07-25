# Design Traceability: Research Claim → Contract → Test

Every design decision in [`src/OracleAnchoredLVRHook.sol`](../src/OracleAnchoredLVRHook.sol)
traces to a research artifact in this repo, and every on-chain mechanism is pinned
by a test. This page is the audit trail: what the data says, where the contract
implements it, and what breaks if the implementation drifts.

The strongest link is live parity: `script/test_python_solidity_parity.py` deploys
[`test/helpers/PythonParityHarness.sol`](../test/helpers/PythonParityHarness.sol)
to an Anvil node and asserts that the **deployed Solidity** returns the same fee,
toxicity classification, and width-guard outputs as the **Python replay engine
that produced the backtest results** (`ANVIL_URL=... python3 -m unittest
script.test_python_solidity_parity`). The backtest math and the contract math are
the same math, checked mechanically, not by inspection.

## Claim-by-claim map

| Research claim (artifact) | Contract mechanism | Pinned by |
| --- | --- | --- |
| Exact toxic surcharge law `f*(z) = e^{\|z\|/2} − 1`, validated by exact replay: 44 frozen windows, 7,019 swaps, max residual 1e-64 (README fee-law proof, `study_artifacts/one_page_proof_2026_03_31`) | `_gapPremiumWad` computes `sqrt(P_hi/P_lo) − 1` from sqrt prices — identically `e^{\|z\|/2} − 1`; `_feeUnits` adds `alphaBps` × premium to the base fee | `test_fee_parity_matrix` (live Solidity vs replay engine), `reports/checks` fee-identity suite |
| Stale-price repricing is toxic in exactly one direction | `beforeSwap`: `toxic = premium != 0 && (oracleAbovePool ? !zeroForOne : zeroForOne)` | `test_classification_parity_matrix`, `testFuzz_previewSwapFee_positiveGapClassifiesOnlyOneToxic` (and the negative-gap twin) |
| Auction trigger `stale_gap_bps ≥ 10` holds a 1.0 clear rate on 124/124 pool-windows (October 2025 grid, `reports/sensitivity_grid_combined.csv`) | `_auctionEligible`: premium ≥ `triggerGapBps × HALF_BPS_WAD` (premium ≈ half the log gap; difference is second-order, documented at the constant) | `test_auction_belowTriggerKeepsExactFee`, `test_auction_eligibleGapAppliesStartConcession` |
| Linear concession schedule — 10 bps start, 0.5 bps/sec growth — selected by the 324-set grid and consistent with the LVF-optimal rate derived in [`concession_tuning_lvf.md`](concession_tuning_lvf.md) (g* = 0.43–1.9 bps/sec) | `_auctionConcessionWad`: `startConcessionWad + concessionGrowthWadPerSec × elapsed`, capped at `maxConcessionWad` | `test_auction_concessionGrowsWithElapsedTime`, `testFuzz_auction_concessionMonotoneWhileGapPersists` |
| LPs keep `(1 − c)` of the surcharge; fee never falls below base fee (the 99.9% recapture of *oracle-visible* stale-loss is `c ≈ 0.1%`; true recapture vs a faster CEX reference is ~78% on mainnet feeds — see [`methodology_limitations.md`](methodology_limitations.md)) at clear) | `_feeUnits`: concession discounts only the surcharge term, base fee untouched | `test_auction_concessionCapsAtWadAndFeeFloorsAtBase`, `test_auction_benignDirectionStillPaysBaseFeeDuringAuction` |
| Exact fees can deter repricing (negative control in the policy table); the auction must reopen capped-out gaps | `_feeUnits` applies the concession **before** the `maxFee` fail-closed check, so a growing concession brings capped fees back under `maxFee` | `test_auction_escapesMaxFeeDeadlockAsConcessionGrows` |
| Governance ceiling trade-off: sub-WAD `maxConcessionWad` guarantees LPs a surcharge floor at the cost of the escape property (adversarial-auction study, 2026-07-10) | `Config.maxConcessionWad` + `setConfig` validation (nonzero, ≥ start when auction enabled) | `test_auction_maxConcessionCapsScheduledConcessionAndFeeFloor`, `test_auction_maxConcessionBelowWadKeepsExtremeGapDeterred`, clock-reset griefing and direction-flip tests |
| Swaps and previews fail closed on stale oracles (replay treats stale-oracle windows as unusable) | `_loadFreshOracle`: reverts `OracleStale` past `maxOracleAge`, `InvalidOraclePrice` on bad reads | `invariant_StaleOracleNeverAllowsSwapThroughHandler` (128k calls), `test_beforeSwap_revertsOnOracleStalenessAtExactBoundary` |
| L2 sequencer outages must not serve fresh-looking prices ([`oracle_granularity.md`](oracle_granularity.md), [`deployment.md`](deployment.md)) | `ChainlinkReferenceOracle._checkSequencerUp`: reverts `SequencerDown` / `SequencerGracePeriodNotOver`; uninitialized rounds fail closed | Four sequencer tests in `test/ChainlinkReferenceOracle.t.sol` |
| LP admission bounded by the LVR rate: reject widths where `σ²·latency/8` exceeds the LP's loss budget (same `σ²/8` LVR rate as the LVF tuning note) | `_minWidthTicks` binary-search inversion of the width factor; `beforeAddLiquidity` enforces width + `centerTolTicks` centering | `test_width_parity_matrix` (live Solidity vs `script/lvr_validation.py`), width/centering unit + fuzz boundary tests |
| Volatility is tracked from oracle returns, seeded by a bootstrap prior (5% daily ≙ `3e10` WAD/sec, the same σ used in the LVF floor calculation) | `_refreshRisk` EWMA on squared per-second returns (`EWMA_ALPHA_BPS = 2000`); `_effectiveSigma2PerSecondWad` falls back to bootstrap | Risk-state update tests incl. same-timestamp and single-feed-advance edge cases |
| Solver economics assume open participation — $1.74/fill leaves no room for integration overhead ([`solver_economics_gas_aware.md`](../reports/solver_economics_gas_aware.md)) | `pokeAuction` has no access modifier; fills are plain swaps; only `setConfig`/`setRiskState` are owner-gated | `test_auction_permissionless_strangerPokesAndStrangerFills`; measured fill gas (231k) feeds the gas-aware table |
| Solvers need pre-trade observability to time fills (live demo loop, `docs/solver_bot.md`) | `auctionStatus` and `previewSwapFee` views | Auction status assertions throughout the auction suite; two live auction cycles on Base Sepolia |
| Benign-flow routing requires router-grade simulatability ([`routing_integration.md`](routing_integration.md)) | Fee is a deterministic function of chain state; `quotable(key)` non-reverting health check | `test/OracleAnchoredLVRHookQuoter.t.sol`: Uniswap's `V4Quoter` quotes benign and auction-open toxic swaps exactly, stale oracle reverts cleanly |
| The recommended grid cell is the deployment default | `script/DeployPool.s.sol::_configFromEnv` defaults = `trigger 10 bps / base 5 bps / start 10 bps / growth 0.5 bps/sec / max 2500 bps` | Deployment doc; defaults mirror `RECOMMENDED_CELL` in the report builders |

## What is deliberately *not* in the contract

Honesty about the boundary matters as much as the map:

- **Parameter values are config, not constants.** The contract implements the fee
  law and auction *shape*; the recommended cell lives in deploy defaults so other
  pools/chains can re-derive parameters (see the re-run rule in
  [`oracle_granularity.md`](oracle_granularity.md)).
- **Feed granularity is a documented scope limit, not an on-chain check** — a hook
  cannot read a Chainlink feed's deviation threshold. Sub-threshold LVR is
  unrecapturable by construction and stated as such.
- **Gas-aware solver economics** inform venue choice (Base-first); the contract is
  venue-agnostic.
- **No protocol fee exists in code.** LPs keep 100% of recaptured value; a fee
  switch is a documented governance possibility only.
- **Routing distribution is the open item** — the technical prerequisite is done
  (the pool is provably quotable by the standard `V4Quoter`, see
  [`routing_integration.md`](routing_integration.md)), but the allowlist and
  aggregator outreach that convert a solver venue into an LP yield product is
  business development, not code.
