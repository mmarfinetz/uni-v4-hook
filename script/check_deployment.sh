#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

: "${RPC_URL:?Set RPC_URL}"
: "${HOOK:?Set HOOK}"
: "${POOL_MANAGER:?Set POOL_MANAGER}"
: "${HOOK_OWNER:?Set HOOK_OWNER}"

normalize_address() {
  tr '[:upper:]' '[:lower:]' <<<"$1"
}

expect_address() {
  local label="$1"
  local actual="$2"
  local expected="$3"
  if [[ "$(normalize_address "$actual")" != "$(normalize_address "$expected")" ]]; then
    echo "$label mismatch: expected $expected, got $actual" >&2
    exit 1
  fi
  echo "$label: $actual"
}

if [[ "$(cast code "$HOOK" --rpc-url "$RPC_URL")" == "0x" ]]; then
  echo "no hook bytecode at $HOOK" >&2
  exit 1
fi

expect_address \
  "pool manager" \
  "$(cast call "$HOOK" 'poolManager()(address)' --rpc-url "$RPC_URL")" \
  "$POOL_MANAGER"
expect_address \
  "owner" \
  "$(cast call "$HOOK" 'owner()(address)' --rpc-url "$RPC_URL")" \
  "$HOOK_OWNER"
expect_address \
  "pending owner" \
  "$(cast call "$HOOK" 'pendingOwner()(address)' --rpc-url "$RPC_URL")" \
  "0x0000000000000000000000000000000000000000"

hook_flags="$(python3 - "$HOOK" <<'PY'
import sys
print(hex(int(sys.argv[1], 16) & 0x3FFF))
PY
)"
if [[ "$hook_flags" != "0x880" ]]; then
  echo "invalid hook permission suffix: expected 0x880, got $hook_flags" >&2
  exit 1
fi
echo "hook permission suffix: $hook_flags"

if [[ -n "${ORACLE:-}" ]]; then
  if [[ "$(cast code "$ORACLE" --rpc-url "$RPC_URL")" == "0x" ]]; then
    echo "no oracle bytecode at $ORACLE" >&2
    exit 1
  fi
  [[ -z "${BASE_FEED:-}" ]] || expect_address \
    "oracle base feed" \
    "$(cast call "$ORACLE" 'baseFeed()(address)' --rpc-url "$RPC_URL")" \
    "$BASE_FEED"
  [[ -z "${QUOTE_FEED:-}" ]] || expect_address \
    "oracle quote feed" \
    "$(cast call "$ORACLE" 'quoteFeed()(address)' --rpc-url "$RPC_URL")" \
    "$QUOTE_FEED"
fi

echo "deployment invariants passed"
