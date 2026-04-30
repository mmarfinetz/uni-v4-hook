# Cross-Pool 24h Stress Artifact

This artifact freezes the April 26, 2026 agent-study stress rerun over blocks
`23,543,615` to `23,550,763`.

## Contents

- `publication_table.csv`: publication-ready numeric table.
- `publication_table.md`: Markdown rendering of the same table.
- `publication_table_metadata.json`: table metadata, including the WETH/USD conversion price.
- `source_summaries/`: copied source summaries used to build the table.
- `reference_inputs/weth_usdc_market_reference_updates.csv`: WETH/USDC reference feed used to USD-normalize WETH-quoted pools.

## Rebuild

```bash
python3 -m script.build_cross_pool_publication_table \
  --summary-csv study_artifacts/cross_pool_24h_2026_04_26/source_summaries/cross_pool_24h_summary.csv \
  --weth-usd-reference study_artifacts/cross_pool_24h_2026_04_26/reference_inputs/weth_usdc_market_reference_updates.csv \
  --weth-usd-start-ts 1760054411 \
  --weth-usd-end-ts 1760140799 \
  --output-csv study_artifacts/cross_pool_24h_2026_04_26/publication_table.csv \
  --output-md study_artifacts/cross_pool_24h_2026_04_26/publication_table.md \
  --output-json study_artifacts/cross_pool_24h_2026_04_26/publication_table_metadata.json
```

## Caveats

- Native quote units are authoritative.
- USD-normalized values convert USDC at `1.0` and WETH at the time-weighted WETH/USDC reference price reported in `publication_table_metadata.json`.
- The run is still an adversarial/informed-repricing stress window. It is not a mixed-flow market-quality test.
- The current Solidity hook remains hook-only; auction results are counterfactual execution-policy results from the simulator.
