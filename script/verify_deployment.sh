#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

: "${CHAIN_ID:?Set CHAIN_ID}"
: "${ETHERSCAN_API_KEY:?Set ETHERSCAN_API_KEY}"
: "${HOOK:?Set HOOK}"
: "${POOL_MANAGER:?Set POOL_MANAGER}"
: "${INITIAL_HOOK_OWNER:?Set INITIAL_HOOK_OWNER to the hook constructor owner}"

hook_constructor_args="$(
  cast abi-encode 'constructor(address,address)' "$POOL_MANAGER" "$INITIAL_HOOK_OWNER"
)"

forge verify-contract \
  "$HOOK" \
  src/OracleAnchoredLVRHook.sol:OracleAnchoredLVRHook \
  --chain "$CHAIN_ID" \
  --constructor-args "$hook_constructor_args" \
  --num-of-optimizations 10000 \
  --via-ir \
  --watch

if [[ -n "${ORACLE:-}" ]]; then
  : "${BASE_FEED:?Set BASE_FEED when ORACLE is set}"
  : "${INVERT_BASE:=false}"
  : "${QUOTE_FEED:=0x0000000000000000000000000000000000000000}"
  : "${INVERT_QUOTE:=false}"
  : "${TOKEN0_DECIMALS:?Set TOKEN0_DECIMALS when ORACLE is set}"
  : "${TOKEN1_DECIMALS:?Set TOKEN1_DECIMALS when ORACLE is set}"
  : "${SEQUENCER_UPTIME_FEED:=0x0000000000000000000000000000000000000000}"
  : "${SEQUENCER_GRACE_PERIOD:=0}"

  oracle_constructor_args="$(
    cast abi-encode \
      'constructor(address,bool,address,bool,uint8,uint8,address,uint256)' \
      "$BASE_FEED" \
      "$INVERT_BASE" \
      "$QUOTE_FEED" \
      "$INVERT_QUOTE" \
      "$TOKEN0_DECIMALS" \
      "$TOKEN1_DECIMALS" \
      "$SEQUENCER_UPTIME_FEED" \
      "$SEQUENCER_GRACE_PERIOD"
  )"

  forge verify-contract \
    "$ORACLE" \
    src/oracles/ChainlinkReferenceOracle.sol:ChainlinkReferenceOracle \
    --chain "$CHAIN_ID" \
    --constructor-args "$oracle_constructor_args" \
    --num-of-optimizations 10000 \
    --via-ir \
    --watch
fi

echo "explorer source verification passed"
