#!/usr/bin/env bash
# Nethermind AuditAgent CLI wrapper.
#
# Runs an AI pre-audit pass over the hook contracts via AuditAgent's REST API
# (docs.auditagent.nethermind.io). Intended as the cheap pass BEFORE a human
# audit, so paid auditor hours go to deep logic rather than lint-level findings.
#
# Requires AUDITAGENT_API_KEY (generate under Profile -> API Keys in the
# AuditAgent dashboard; needs a plan with API access).
#
#   AUDITAGENT_API_KEY=... ./script/audit_agent.sh            # developer scan
#   AUDITAGENT_API_KEY=... ./script/audit_agent.sh auditorScan # deep scan
#
# Results land in audit/ as JSON + PDF.
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] && { set -a; source .env; set +a; }
: "${AUDITAGENT_API_KEY:?Set AUDITAGENT_API_KEY (AuditAgent dashboard -> Profile -> API Keys)}"

QUALITY="${1:-developerScan}"
API="https://api.auditagent.nethermind.io/api/v1/scanner/direct"
OUT=audit; mkdir -p "$OUT"
ZIP="$(mktemp -d)/uni-v4-hook.zip"

# Contracts under review. Interfaces are included so the agent sees the full
# surface; billable LOC is ~733, well under the 12k/scan cap.
SCOPE='["src/OracleAnchoredLVRHook.sol","src/oracles/ChainlinkReferenceOracle.sol","src/interfaces/IReferenceOracle.sol","src/interfaces/IChainlinkAggregatorV3.sol","src/interfaces/IDutchAuctionModule.sol"]'

# Design docs matter: they let the agent check intent against implementation
# (fee law, auction schedule, fail-closed behavior, known limitations).
DOCS='["README.md","docs/design_traceability.md","docs/methodology_limitations.md","docs/oracle_granularity.md"]'

echo "==> packaging (src + docs + dependency sources for import resolution)"
# Ship lib/*/src only (~2.3M) rather than all of lib/ (~74M).
zip -qr "$ZIP" \
  src docs README.md foundry.toml \
  lib/v4-core/src lib/v4-periphery/src lib/forge-std/src \
  lib/v4-core/lib/solmate/src lib/v4-core/lib/openzeppelin-contracts/contracts \
  -x '*/.git/*' '*/node_modules/*' '*/test/*'
echo "    zip: $(du -h "$ZIP" | cut -f1)"

echo "==> submitting scan (quality=$QUALITY)"
RESP=$(curl -sS -X POST "$API/scan-repo-zip" \
  -H "X-API-Key: $AUDITAGENT_API_KEY" \
  -F "payload={\"contracts_in_scope\":$SCOPE,\"docs\":$DOCS,\"scanQuality\":\"$QUALITY\",\"findings_format\":\"pdf_and_json\",\"project_name\":\"OracleAnchoredLVRHook\"}" \
  -F "repo_zip=@$ZIP")

SCAN_ID=$(printf '%s' "$RESP" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("data",{}).get("scan_id",""))' 2>/dev/null || true)
[ -n "$SCAN_ID" ] || { echo "submit failed: $RESP" >&2; exit 1; }
echo "    scan_id: $SCAN_ID"

echo "==> polling for results (deep scans can take a while)"
for i in $(seq 1 180); do
  CODE=$(curl -sS -o "$OUT/findings.json" -w '%{http_code}' \
    -H "X-API-Key: $AUDITAGENT_API_KEY" "$API/result/json/$SCAN_ID")
  if [ "$CODE" = "200" ] && [ -s "$OUT/findings.json" ]; then
    curl -sS -o "$OUT/findings.pdf" -H "X-API-Key: $AUDITAGENT_API_KEY" \
      "$API/result/pdf/$SCAN_ID" || true
    echo "==> done: $OUT/findings.json  $OUT/findings.pdf"
    python3 - "$OUT/findings.json" <<'PY' || true
import json,sys,collections
d=json.load(open(sys.argv[1]))
fs=d if isinstance(d,list) else d.get("findings") or d.get("data",{}).get("findings") or []
if not fs: print("no structured findings array; inspect findings.json"); raise SystemExit
c=collections.Counter((f.get("severity") or f.get("impact") or "unknown").lower() for f in fs)
print(f"\n{len(fs)} findings:", ", ".join(f"{k}={v}" for k,v in c.most_common()))
for f in fs[:10]:
    sev=(f.get("severity") or f.get("impact") or "?").upper()
    print(f"  [{sev}] {(f.get('title') or f.get('name') or '')[:90]}")
PY
    exit 0
  fi
  sleep 10
done
echo "timed out; fetch later with:" >&2
echo "  curl -H 'X-API-Key: \$AUDITAGENT_API_KEY' $API/result/json/$SCAN_ID" >&2
exit 1
