# Concession-Schedule Tuning via Loss-Versus-Fair

This note connects the hook's Dutch-auction concession schedule
(`startConcessionWad`, `concessionGrowthWadPerSec`, `maxConcessionWad`) to the
closed-form results in Milionis, Moallemi & Roughgarden, *Loss-Versus-Fair:
Efficiency of Dutch Auctions on Blockchains*
([arXiv:2406.00113](https://arxiv.org/abs/2406.00113)). It gives the theory frame
the paper draft currently lacks for the empirically chosen
`concession_growth_bps_per_sec = 0.5`, and a first-principles derivation that
lands on the same order of magnitude.

## The LVF model

The paper prices a seller's exponential-decay Dutch auction (`A_t = A_0 e^{-λt}`)
against a GBM asset (volatility `σ`, drift `μ`) with Poisson block arrivals (mean
interblock time `Δt`). With `δ ≡ λ + μ − σ²/2`, the expected fraction of value
ceded to the filling arbitrageur (loss-versus-fair) and the expected time-to-fill
are, for auctions starting at or above fair value:

```
LVF₊ = 1 / (1 + ζ₋),   ζ₋ = (δ/σ²) (√(1 + 2σ²/(δ²Δt)) − 1)
FT(z₀) = z₀/δ + (Δt/2) (1 + √(1 + 2σ²/(δ²Δt)))
```

Two limits matter here:

- **Block-time floor:** no decay-rate choice beats
  `LVF ≥ σ √(Δt/2)` — discreteness alone cedes this much. At the hook's bootstrap
  volatility (5% daily, `σ² = 2.9e-8 s⁻¹`, which is exactly
  `BOOTSTRAP_SIGMA2_PER_SECOND_WAD = 3e10`): **4.2 bps on Ethereum L1 (Δt = 12 s),
  1.7 bps on Base (Δt = 2 s), 0.5 bps under 200 ms flashblocks.** The paper's
  rule of thumb — sub-2 bps LVF at 5% daily volatility needs blocks faster than
  ~2.75 s — is a quantitative argument for the L2-first deployment that
  complements the gas-cost argument in
  [`reports/solver_economics_table.md`](../reports/solver_economics_table.md).
- **Slow-decay limit:** `LVF ≳ δΔt` — decaying faster than one block interval's
  worth of price motion just donates the difference to the block's filler.

## Mapping to the hook's auction

The hook does not auction the asset; it auctions a *discount on the toxic-flow
surcharge*. With gap `z`, surcharge `f* = e^{|z|/2} − 1`, and concession
`c(t) = c₀ + g·t` (capped at `maxConcessionWad`), a fill at elapsed time `τ` splits
the recapturable stale-loss value `f*·V` (notional `V`) as: LP keeps `(1−c(τ))·f*V`,
solver keeps `c(τ)·f*V`. So in LVF terms:

| LVF model | Hook mechanism |
| --- | --- |
| Ask decay rate `λ` | Concession growth `g = concessionGrowthWadPerSec / 1e18` |
| Value sold | Recapturable surplus `f*·V`, not the notional |
| LVF (fraction ceded to arb) | Clearing concession `c(τ_fill)` |
| Time-to-fill `FT` | Trigger-to-fill delay `τ` |
| Uncapped downside | Capped: `c ≤ maxConcessionWad` by construction |

The structural difference is the denominator: a raw Dutch auction cedes
`σ√(Δt/2)` **of the notional**; the fee-space auction cedes `c(τ)` **of the
surcharge**, and never less than the base fee reaches the LP. The October 2025
grid's `99.9%` mean recapture is `c(τ_fill) ≈ 0.1%` in this vocabulary — the
mechanical-ceiling caveat in the README maps to the paper's single-rational-arb,
zero-gas assumption regime. Note this is recapture of the *oracle-visible*
stale-loss; true recapture against a faster CEX reference is ~78% on mainnet
feeds ([`methodology_limitations.md`](methodology_limitations.md)).

## First-principles choice of `g`

With competing solvers, the first profitable fill wins, so the clearing concession
is pinned at the solver cost floor `m̄ = (gas + edge)/(f*V)` regardless of `g`;
the growth rate only sets (a) the **delay** to reach the floor,
`τ ≈ (m̄ − c₀)/g`, during which LPs keep bearing staleness risk at the LVR rate
`≈ (σ²/8)·V`, and (b) the **overshoot** past the floor granted to the filler by
Poisson block arrival, expected `g·Δt` of the surcharge (memorylessness: expected
residual wait after crossing is `Δt`). Minimizing per-fill LP cost

```
C(g) = (σ²/8)·V·(m̄ − c₀)/g  +  g·Δt·f*·V
g*   = √( (σ²/8)(m̄ − c₀) / (Δt·f*) )
```

At the recommended cell's scale (`f* = 5 bps` at the 10 bps trigger, Base
`Δt = 2 s`, 5% daily volatility), `g*` is **0.43–1.9 bps/sec** of the surcharge as
the solver floor `m̄ − c₀` ranges over 0.05%–1% — bracketing the grid-searched
`0.5 bps/sec`. The empirically selected growth rate is consistent with the
LVF-style optimum for Base-like parameters; sensitivity is square-root in every
input, so the choice is robust to the floor being off by an order of magnitude.
At `g = 0.5 bps/sec` the implied trigger-to-fill delay is ~20 s for a 0.1%
solver floor (~100 s at 0.5%), matching the fill delays observed in the live
Base Sepolia demo loop.

## Caveats

- The derivation assumes competitive fills; with a monopolist solver the clearing
  concession rises toward the paper's `LVF₊` and `g` matters less (the solver
  waits regardless), which is why the README reports clear rate and payout scale
  rather than recapture as the informative outputs.
- The paper's `δ > 0` assumption (decay outruns drift) fails in crash regimes;
  this is the theory-side reflection of the Oct 10–11 dislocation windows being
  reported separately in [`reports/lp_apr_uplift.md`](../reports/lp_apr_uplift.md).
- `maxConcessionWad < 1e18` truncates the schedule; in LVF terms it is a hard cap
  on loss-versus-fair at the cost of the time-to-fill tail (deterrence), the
  trade-off already documented in the README's escape-property discussion.

Numbers in this note are reproduced by the snippet in the session log and can be
recomputed with the formulas above; volatility enters only through
`σ² per second`, so re-derive `g*` when configuring pools whose EWMA volatility
differs materially from the 5% daily bootstrap.
