# True Recapture vs a Faster Reference (Chainlink lag correction)

Methodology fix #1 (see [`docs/methodology_limitations.md`](../docs/methodology_limitations.md)).
The headline ~99.9% recapture was measured against the *same* Chainlink series the
hook uses as its oracle. This re-measures stale-loss against a faster **Binance 1m**
truth while keeping Chainlink as the hook oracle, over 24 stratified 2026 windows
(WETH/USDC 0.3%, mainnet feeds), using the study's own `correction_trade` LVR
function.

Reproduce: `python3 -m research.lvr.studies.reference_lag_analysis`
(output `exports/study_recent/reference_lag_summary.json`).

## Headline

| Metric | Value |
| --- | --- |
| Median Chainlink-vs-Binance lag at swap times | **21 bps** |
| p90 lag | ~47–64 bps |
| Oracle-visible fraction of true LVR | **78.3%** |
| Headline recapture (vs Chainlink) | 99.9% |
| **True recapture (vs Binance truth)** | **~78%** |

## Per-month (visible fraction of true LVR)

| Month | windows | swaps | lag median (bps) | lag p90 (bps) | visible % |
| --- | ---: | ---: | ---: | ---: | ---: |
| Jan | 4 | 2,554 | 21.8 | 50.9 | 85.5 |
| Feb | 4 | 2,754 | 26.9 | 58.7 | 76.2 |
| Mar | 4 | 1,786 | 26.4 | 64.5 | 45.6 |
| Apr | 4 | 1,438 | 16.9 | 46.6 | 43.3 |
| May | 4 | 986 | 18.5 | 41.3 | 39.6 |
| Jun | 4 | 1,855 | 18.8 | 46.8 | 44.7 |

The visible fraction collapses in the higher-volatility months: **the hook captures
a smaller share of true LVR precisely when LVR is largest**, because the CEX moves
fastest then and the deviation-triggered Chainlink feed lags most.

## Why the honest number is better than it looks

- **Base feeds are ~3× fresher.** These are mainnet Chainlink feeds (0.5% deviation
  threshold). The Base ETH/USD feed is 0.15%, so a Base deployment's visible
  fraction — and true recapture — is materially higher than 78% (same
  conservative-for-Base direction as [`oracle_granularity.md`](../docs/oracle_granularity.md)).
- **The gap is closable.** A faster reference (Chainlink Data Streams / an
  aggregated oracle) behind the same `IReferenceOracle` interface shrinks the lag;
  the fee law and auction are unchanged.
- **Binance 1m is a conservative truth.** A 1-second reference would show a slightly
  larger lag, so 78% if anything understates the freshness gap — the direction of
  the caveat is honest.

## Recommended framing

Replace "99.9% recapture" with: **"recovers roughly three-quarters of true
CEX-measured LVR on mainnet-granularity feeds — more on Base's tighter feeds — with
a faster oracle as the path to closing the rest."** The 99.9% remains valid only as
"recapture of the oracle-visible stale-loss," and should always carry that qualifier.
