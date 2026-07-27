#!/usr/bin/env bash
set -u; cd "$(dirname "$0")/.."
bash exports/study_rwa/run_all.sh
bash exports/study_eurc/run_all.sh
echo "=== BOTH STUDIES DONE @ $(date) ==="
