#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

run_slither=false
if [[ "${1:-}" == "--slither" ]]; then
  run_slither=true
elif [[ -n "${1:-}" ]]; then
  echo "usage: $0 [--slither]" >&2
  exit 2
fi

bad_env_files=""
while IFS= read -r path; do
  case "$path" in
    lib/*) continue ;;
    .env|.env.*|*/.env|*/.env.*)
      case "$path" in
        *.example|*.sample|*.template) ;;
        *) bad_env_files+="$path"$'\n' ;;
      esac
      ;;
  esac
done < <(git ls-files)

if [[ -n "$bad_env_files" ]]; then
  echo "tracked secret-bearing environment files are forbidden:" >&2
  printf '%s' "$bad_env_files" >&2
  exit 1
fi

# Print filenames only: a failed check must not echo a credential into CI logs.
secret_pattern="(BEGIN (RSA|EC|DSA|OPENSSH) PRIVATE KEY|AKIA[0-9A-Z]{16}|gh[pousr]_[A-Za-z0-9_]{30,}|(PRIVATE_KEY|DEPLOYER_KEY|MNEMONIC|API_SECRET)[[:space:]]*[:=][[:space:]]*[\"']?0x[0-9a-fA-F]{64})"
if secret_files="$(
  git grep -IlE "$secret_pattern" -- . \
    ':!lib/**' ':!broadcast/**' ':!study_artifacts/**' ':!exports/**' || true
)" && [[ -n "$secret_files" ]]; then
  echo "possible tracked credentials found in:" >&2
  printf '%s\n' "$secret_files" >&2
  exit 1
fi

echo "tracked-secret checks passed"

if [[ "$run_slither" == true ]]; then
  echo "running Slither (high-severity findings fail the gate)"
  python3 -m slither . \
    --filter-paths '(^|/)(lib|test|script)/' \
    --exclude-dependencies \
    --fail-high
fi
