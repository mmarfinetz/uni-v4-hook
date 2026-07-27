#!/usr/bin/env bash
# Re-run both RWA studies with a staleness tolerance matched to the feeds'
# 24h heartbeat (90000s = 25h). The 1h default guarantees fail-closed on these
# feeds, so the first pass measured configuration, not asset behaviour.
set -u; cd "$(dirname "$0")/.."
set -a; source .env; set +a
CELL=(--base-fee-bps 5 --max-fee-bps 2500 --alpha-bps 10000
      --auction-start-concession-bps 10 --auction-concession-growth-bps-per-second 0.5
      --auction-max-concession-bps 10000 --max-oracle-age-seconds 90000)
for spec in "study_rwa:paxg_usdc" "study_eurc:eurc_usdc"; do
  s="${spec%%:*}"; g="${spec##*:}"
  for m in 2026_01 2026_02 2026_03 2026_04 2026_05 2026_06; do
    for a in 1 2 3; do
      RPC_MAX_BATCH=20 /opt/homebrew/bin/python3.14 -m script.run_backtest_batch \
        --manifest exports/$s/manifests_strat/${m}_${g}.json \
        --output-dir exports/$s/age25h/${m}_${g} \
        --rpc-url "$MAINNET_RPC_URL" --rpc-cache-dir exports/study_recent/rpc_cache \
        --blocks-per-request 2000 --max-retries 10 "${CELL[@]}" \
        >> exports/$s/age25h_${m}.log 2>&1 && break
      sleep 5
    done
  done
done
echo "=== MATCHED-AGE RERUN DONE @ $(date) ==="
