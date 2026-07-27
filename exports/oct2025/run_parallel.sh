#!/usr/bin/env bash
set -u; cd "$(dirname "$0")/../.."
set -a; source .env; set +a
CELL=(--base-fee-bps 5 --max-fee-bps 2500 --alpha-bps 10000
      --auction-start-concession-bps 10 --auction-concession-growth-bps-per-second 0.5
      --auction-max-concession-bps 10000)
run_pool () {
  local fam=$1
  for a in $(seq 1 10); do
    n=$(ls exports/oct2025/windows 2>/dev/null | grep -c "^${fam%_month}" || true)
    [ "$n" -ge 31 ] && break
    RPC_MAX_BATCH=20 /opt/homebrew/bin/python3.14 -m script.run_backtest_batch \
      --manifest exports/oct2025/split/${fam}.json --output-dir exports/oct2025/windows \
      --rpc-url "$MAINNET_RPC_URL" --rpc-cache-dir exports/study_recent/rpc_cache \
      --blocks-per-request 2000 --max-retries 12 --retry-backoff-seconds 4 \
      "${CELL[@]}" >> exports/oct2025/${fam}.log 2>&1
    sleep 4
  done
  echo "=== $fam done @ $(date '+%H:%M:%S') ==="
}
for fam in weth_usdc_3000_month wbtc_usdc_500_month link_weth_3000_month uni_weth_3000_month; do
  run_pool "$fam" &
done
wait
echo "=== OCT PARALLEL DONE @ $(date) ==="
