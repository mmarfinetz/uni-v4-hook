# Market & Design Validation Research — July 2026

Research pass to validate the oracle-anchored LVR hook design and assess product-market
fit before committing further. Sources are linked inline; traction numbers are as
reported by the linked sources in June–July 2026 and should be re-checked before citing.

## TL;DR

- The **problem is real and academically validated**, but **LP adoption of protective
  hooks is behaviorally weak**: the best-funded direct competitor (Angstrom, $7.5M
  Paradigm seed, Uniswap Foundation support, OKX routing) has ~$3.2M TVL after ~a year
  on mainnet.
- The **mechanism appears novel** — no other oracle-gap-triggered Dutch-auction
  concession on a toxic-flow surcharge was found — and it has one genuine strategic
  differentiator: it does not depend on transaction priority ordering, which recent
  empirical work shows is degrading on fast-finality L2s exactly where competitors'
  MEV-tax designs need it.
- The **binding constraint is distribution, not mechanism quality**: only ~8% of v4
  swaps flow through hooked pools, and the Uniswap routing API allowlists hooks
  per chain. Without benign flow, the LP-yield pitch collapses.
- **Realistic PMF path is public-good / grant-funded infrastructure** (UHI 10 →
  Uniswap Foundation Hook Design Lab → Security Fund audits), not venture-style
  traction. The UHI 10 theme ("Fair Flow Frontier: MEV protection and sustainable
  low-fee liquidity") is a near-exact match for this design.
- **One design-level red flag to resolve**: Chainlink's ETH/USD feed on Base has a
  0.15% (15 bps) deviation threshold — larger than the recommended
  `trigger_gap_bps = 10`. The on-chain trigger measures gaps against a reference that
  itself lags by up to 15 bps.

## 1. Problem validation — strong

- The canonical empirical study ([Measuring Arbitrage Losses and Profitability of AMM
  Liquidity](https://arxiv.org/abs/2404.05803)) finds fees earned are **smaller than
  losses to arbitrageurs in the majority of the largest Uniswap pools**, and that v2
  pools beat v3 for passive LPs.
- The Roughgarden-team result frequently cited by competitors: LVR costs ETH-USDC LPs
  roughly **11% of principal per year** before fees ([Arrakis problem
  statement](https://docs.arrakis.finance/text/modules/hotAmm/problemStatement.html)).
- A 2025 paper ([Optimal Fees for Liquidity Provision in
  AMMs](https://arxiv.org/abs/2508.08152)) calibrated to Jan-2025 ETH/USDC data
  confirms static fees fail to offset adverse selection and dynamic fees materially
  improve LP PnL.
- Closest empirical prior art: [Ahlroos & Eloranta, "Redistributing LVR to AMM LPs
  with dynamic fees and first-access auctions"
  (Nov 2024)](https://hackmd.io/@anteroe/BkIbSfwmJx). Their findings frame this
  repo's contribution well: dynamic fees gave better LVR/fee trade-offs on liquid
  pools but "no universal optimal fee function exists," dynamic fees alone cannot
  eliminate LVR, and their first-access auctions only redistributed value when
  dislocations exceeded ~1%. This repo's combined exact-fee-law + Dutch-auction
  design, with a workable ~10 bps trigger and the 0-worsened-windows selectivity
  result, is a direct answer to the gaps they identified.
- Theory anchor worth citing in the paper: [Loss-Versus-Fair: Efficiency of Dutch
  Auctions on Blockchains](https://arxiv.org/abs/2406.00113) (Moallemi et al.) gives
  closed forms for how much a decaying-price mechanism leaks to arbitrageurs as a
  function of decay rate, volatility, and interblock time — that is exactly the
  `concessionGrowthWadPerSec` trade-off, and connecting to it would strengthen the
  research draft.

## 2. Competitive landscape

### Angstrom (Sorella Labs) — the direct competitor
- $7.5M seed led by Paradigm ([The Block](https://www.theblock.co/post/312222/paradigm-sorella-labs-ethereum-mev-problem));
  built with Uniswap Foundation Hook Design Lab support; live on Ethereum mainnet
  since July 2025.
- Mainnet mechanism: off-chain app-level sequencing + per-block arbitrage auction +
  batch clearing, secured by an EigenLayer-backed network.
- Traction ([DefiLlama](https://defillama.com/protocol/angstrom)): **~$3.2M TVL**,
  $622M+ cumulative volume, ~$649k/yr fee run-rate, OKX routes trades through it.
- **L2 version in active development** (`l2-angstrom` repo, updated April 2026):
  drops the validator network entirely and applies a ["Priority Is All You
  Need"](https://www.paradigm.xyz/2024/06/priority-is-all-you-need) MEV tax inside a
  v4 hook — `SWAP_MEV_TAX_FACTOR = 49` captures ~98% of marginal priority fee, no
  oracle, no solver bots, synchronous LP distribution
  ([Angstrom L2 docs](https://docs.angstrom.xyz/l2/arbitrage-auction)). This targets
  exactly the OP-Stack chains (Base) this repo deploys to.
- **Their weakness / our differentiator**: MEV taxes require the sequencer's
  competitive priority ordering to actually function. Empirical work on
  fast-finality rollups ([When Priority Fails](https://arxiv.org/abs/2506.01462))
  finds only the **first Flashblock is fee-ordered**, priority fees have negligible
  influence in subsequent slots, and MEV bots favor duplicate-spam over bidding. An
  oracle-anchored Dutch auction prices in gap-space and time, not tip-space, so it
  keeps working where priority-fee auctions degrade — including Flashblocks-era Base.

### Aegis DFM (Solo Labs)
- Uniswap Foundation Hook Design Lab grantee, live
  ([docs](https://docs.aegis.markets/)); dynamic base fee (volatility-indexed) +
  surge fee that decays after cap events. Fee *protection* only — no recapture
  auction, no repricing guarantee. Overlaps on the surcharge half of this design,
  not the auction half.

### Arrakis HOT / Arrakis Pro hook
- RFQ signed quotes + dynamic fees on Valantis; Pro hook targets **token issuers**
  specifically and was the first whitelisted dynamic-fee hook
  ([Arrakis](https://arrakis.finance/blog/the-arrakis-pro-hook-dynamic-fees-for-token-issuers-on-uniswap-v4)).
  Commercial, off-chain quoting service — different trust model (this repo's design
  is fully on-chain).

### Bunni — the cautionary tale and the vacated niche
- Was the leading LP-optimization hook (at peak >90% of v4 volume per
  [Auditless](https://research.auditless.com/p/bunni-how-to-build-a-leading-uniswap));
  combined volatility fees, surge fees, and am-AMM auctions.
- Exploited for ~$8.4M in Sept 2025 (rounding flaw in its own liquidity-distribution
  logic, despite Trail of Bits and Cyfrin audits); shut down October 2025, citing
  **6–7 figures in audit and monitoring costs** to relaunch securely
  ([CoinDesk](https://www.coindesk.com/business/2025/10/23/bunni-dex-shuts-down-cites-recovery-costs-after-usd8-4m-exploit),
  [Bunni announcement](https://x.com/bunni_xyz/status/1981160279871558114)).
- Lessons: (a) the am-AMM/LP-optimization niche lost its leader and is open;
  (b) production security cost is the real moat/barrier — the [Uniswap Foundation
  Security Fund](https://developers.uniswap.org/docs/ecosystem/builder-support/get-funded)
  (reduced/free audits) is the intended answer and matters more than any feature.

### Mechanism-space novelty check
No other oracle-gap-triggered, time-growing concession on a toxic-flow surcharge was
found. Adjacent designs, none identical:
- [am-AMM](https://arxiv.org/abs/2403.03367) (published FC 2025/Springer 2026):
  ex-ante Harberger-lease auction for pool-manager rights.
- Diamond-style per-block arbitrage profit sharing.
- Protocol Fee Discount Auctions (est. LP uplift $0.06–$0.26 per $10k traded).
- UniswapX Dutch orders (order-level, not pool-level).
The anti-deterrence "escape" property (concession eventually brings capped-out fees
back under `maxFee` so large gaps stay repriceable) does not appear in any of these.

### Chain-level competition
Unichain's [Rollup-Boost](https://writings.flashbots.net/introducing-rollup-boost)
explicitly returns MEV to LPs at the chain layer; if that generalizes, hook-level
capture gets squeezed on Unichain. Base's Flashblocks rollout is framed around
speed, not LP redistribution — leaving app-layer room on Base, which is where this
repo already deploys.

## 3. Distribution — the binding constraint

- Only ~**8.3% of v4 swaps** flow through hooked pools; ~32% of v4 pools use hooks
  ([Chaisomsri](https://medium.com/@chaisomsri96/uniswap-v4-part2-the-current-situation-and-outlook-of-uniswap-v4-e5c2e7042b1b),
  [Datawallet](https://www.datawallet.com/crypto/uniswap-v4-explained)). Revealed
  preference: most LPs stay in vanilla pools even though protective hooks exist.
- The Uniswap routing API maintains a **per-chain allowlist of hook addresses** it
  will route through ([Uniswap support](https://support.uniswap.org/hc/en-us/articles/33827089858701-Adding-liquidity-to-Uniswap-v4-with-a-hook)).
  Third-party aggregators (1inch, CoW, Paraswap, Matcha) index v4 hooked pools **that
  their solvers can simulate safely**; UniswapX fillers integrate hooks when
  profitable.
- Implication for this design specifically: LP yield depends on benign flow paying
  the base fee. A pool that only receives toxic flow and solver fills does not
  deliver the APR-uplift story. The quoter/routing integration is the PMF gate.
  **Update 2026-07-17: the technical half is done** — the pool is provably
  quotable by Uniswap's standard `V4Quoter` with no hook-specific integration
  (benign and auction-open toxic quotes match execution exactly; stale oracle
  reverts cleanly), and a non-reverting `quotable(key)` health view was added for
  indexers. See [`routing_integration.md`](routing_integration.md) for the quote
  semantics and the distribution playbook; what remains is outreach (allowlist,
  aggregator simulation sets), not code.
- Realistic customer segments, in order:
  1. **Uniswap Foundation ecosystem** (UHI 10 → Hook Design Lab → grants + Security
     Fund). UF committed [$26M in grants during 2025](https://www.coindesk.com/business/2026/04/01/uniswap-foundation-held-usd85-8m-at-year-end-committed-usd26m-in-grants-during-2025);
     the Hook Design Lab exists to take hooks from idea to mainnet with GTM help.
     The public-good/no-protocol-fee framing fits this channel exactly.
  2. **Token issuers / DAO protocol-owned liquidity** — they choose the hook at pool
     creation, bring their own liquidity, and care about LP economics (this is who
     Arrakis Pro targets).
  3. Passive retail LPs — hardest to reach; do not build GTM around them.
  4. ALM vault managers (Gamma/Steer/Arrakis) — unlikely adopters; they build their own.

## 4. Design-level findings to resolve

1. **Chainlink deviation threshold vs. trigger gap — RESOLVED 2026-07-17
   (downgraded from red flag to documented scope limit).** Verified that the
   backtest reference series is exported from **on-chain `AnswerUpdated` / OCR
   `NewTransmission` logs of the real mainnet Chainlink feeds**
   (`research/lvr/export/export_historical_replay_data.py`), so the trigger and
   selectivity results already embed feed granularity, and Base's fresher feeds
   (0.15% ETH/USD vs 0.5% mainnet) make the backtest conservative for the
   deployment target. Remaining true limitation: dislocations below the feed's
   deviation threshold are invisible and unrecapturable by construction. Full
   analysis and deployment rules: [`oracle_granularity.md`](oracle_granularity.md).
2. **L2 sequencer uptime feed — DONE 2026-07-17.** `ChainlinkReferenceOracle` now
   takes an optional sequencer uptime feed + grace period; `latestPriceWad`
   reverts (`SequencerDown` / `SequencerGracePeriodNotOver`) so swaps and previews
   fail closed, per the standard pattern
   ([Chainlink docs](https://docs.chain.link/data-feeds/l2-sequencer-feeds)).
   Wired through `DeployPool` env vars; covered by unit tests.
3. **Solver economics — QUANTIFIED 2026-07-17.** Gas-aware table added
   ([`reports/solver_economics_gas_aware.md`](../reports/solver_economics_gas_aware.md)):
   measured fill gas is 231k, break-even gas prices are 1.9–2.9 gwei for the
   WETH/USDC and LINK/WETH payout scale, all four pools clear with margin at
   Base-typical gas, and mainnet is viable only for the larger pools in quiet
   regimes. Competitor benchmark stands: Angstrom L2 needs **no solver ecosystem
   at all**; the counter-argument (permissionless fills by existing arb bots,
   works where priority ordering fails) is now pinned by
   `test_auction_permissionless_strangerPokesAndStrangerFills` and documented in
   [`solver_bot.md`](solver_bot.md#permissionless-surface).
4. **Paper strengthening** — connect `concessionGrowthWadPerSec` tuning to the
   closed-form loss-versus-fair results in [arXiv 2406.00113](https://arxiv.org/abs/2406.00113).
5. **Methodology corrections — DONE 2026-07-24** ([`methodology_limitations.md`](methodology_limitations.md)).
   Two material fixes changed the honest headline: (a) the `99.9%` recapture was
   measured against the hook's own Chainlink oracle; re-measured against a faster
   Binance truth, Chainlink lags the CEX by `~21 bps` median and **true recapture
   is ~78%** on mainnet feeds (higher on Base). (b) The recommended cell was tuned
   at zero gas; a gas-aware re-tune shows concession *growth* is irrelevant and
   *start* concession is the LP↔filler dial. Both feed the pitch honesty and the
   production parameter choice.

## 5. UHI 10 fit

- Theme: "**The Fair Flow Frontier: MEV protection and sustainable low-fee
  liquidity**" — with "protocol-native MEV auction hooks where LPs recapture a
  percentage of extracted value" listed as an example build direction
  ([Atrium](https://x.com/AtriumAcademy/status/2047359898237952183)). This design is
  dead-center on theme.
- Schedule: Hookathon **Aug 17 – Sep 3, 2026**, Demo Day **Sep 11, 2026**
  ([Atrium Academy](https://atrium.academy/uniswap)). Prize pools have run
  $25–40k across a theme track + general track + sponsor track.
- Post-hookathon path with precedent: Hook Design Lab grantees (EulerSwap, Aegis,
  Renzo Dynamo) and UHI alumni with venture outcomes (Semantic Layer, Tenor,
  Flaunch).

## 6. Recommended validation experiments (cheap, before going all-in)

1. Resolve the Chainlink-granularity question with a replay that uses actual Base
   feed rounds as the reference series; re-derive the trigger/selectivity numbers.
2. Ask the routing question directly: what does it take to get on the Uniswap
   routing API hook allowlist for Base, and what do 1inch/CoW solvers need to
   simulate the pool? (The answer defines the quoter spec.)
3. Apply/track toward the Hook Design Lab and Security Fund — this answers the
   Bunni-scale audit-cost problem and is the natural funding channel for the
   public-good framing.
4. Pitch one token issuer or small DAO POL treasury on a pilot pool instead of
   courting retail LPs; their pool-creation decision is the actual adoption unit.
5. At demo day, lead with the differentiators the landscape supports: fully
   on-chain, ordering-independent (works under Flashblocks where MEV taxes degrade),
   exact replay-validated fee law, anti-deterrence escape property, 0-worsened-window
   selectivity.
