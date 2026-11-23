#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
migration="$script_dir/../migrations/07_setup_api-keys.sh"
work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

mkdir -p "$work_dir/bin"
cat >"$work_dir/bin/bao" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$BAO_CALLS"
case "$*" in
  'secrets list -format=json') printf '%s\n' '{}' ;;
  'kv get '*) printf '%s\n' 'No value found' >&2; exit 2 ;;
  'read -format=json auth/jwt/role/sis-api')
    case "$ROLE_READ_MODE" in
      existing) printf '%s\n' '{"data":{"token_policies":["base","services-event-ledger-jwt-sign-sis-api-rw"]}}' ;;
      already) printf '%s\n' '{"data":{"token_policies":["base","services-api-keys-api-jwt-sign-sis-api-rw"]}}' ;;
      missing) printf '%s\n' 'No value found' >&2; exit 2 ;;
      failed) printf '%s\n' 'permission denied' >&2; exit 2 ;;
    esac ;;
  'write services/api-keys-api/jwt/roles/sis-api -') cat >"$SIS_SECRET_ROLE" ;;
  'write auth/jwt/role/sis-api -') cat >"$SIS_AUTH_ROLE" ;;
  'write '*'-') cat >/dev/null ;;
esac
STUB
chmod +x "$work_dir/bin/bao"
export PATH="$work_dir/bin:$PATH"
export BAO_CALLS="$work_dir/calls.log"
export SIS_SECRET_ROLE="$work_dir/secret-role.json"
export SIS_AUTH_ROLE="$work_dir/auth-role.json"
export DEFAULT_CASSANDRA_PASSWORD=test
export OPENBAO_JWT_ISSUER=https://issuer.example.com

fail() {
  printf 'icms-api-keys-signing: %s\n' "$*" >&2
  exit 1
}

run_migration() {
  export ROLE_READ_MODE="$1"
  : >"$BAO_CALLS"
  rm -f "$SIS_SECRET_ROLE" "$SIS_AUTH_ROLE"
  bash "$migration" >"$work_dir/output.log" 2>&1 || {
    case "$1" in
      failed|missing) ;;
      *) cat "$work_dir/output.log" >&2; fail "migration failed for $1" ;;
    esac
    [[ "$(<"$BAO_CALLS")" == *'read -format=json auth/jwt/role/sis-api'* ]] ||
      fail 'migration stopped before checking the SIS role'
    test ! -e "$SIS_AUTH_ROLE" || fail 'rewrote SIS role after a failed read'
    return
  }
  case "$1" in
    failed|missing) fail 'migration continued without an existing SIS role' ;;
  esac
  test -s "$SIS_SECRET_ROLE" || fail 'missing ICMS signing role'
  test -s "$SIS_AUTH_ROLE" || fail 'missing SIS auth role update'
  jq -e '.claims.azp == "sis-api" and
         .claims.aud == ["sis-api", "s:api-keys-api"] and
         .claims.scopes == ["pdp-evaluate"]' "$SIS_SECRET_ROLE" >/dev/null ||
    fail 'wrong signing role claims'
  [[ "$(<"$BAO_CALLS")" == *'path "services/api-keys-api/jwt/sign/sis-api"'* ]] ||
    fail 'missing signing ACL'
}

run_migration existing
jq -e '.policies == ["base", "services-event-ledger-jwt-sign-sis-api-rw",
                     "services-api-keys-api-jwt-sign-sis-api-rw"]' "$SIS_AUTH_ROLE" >/dev/null ||
  fail 'lost existing SIS policies'
run_migration already
jq -e '.policies == ["base", "services-api-keys-api-jwt-sign-sis-api-rw"]' "$SIS_AUTH_ROLE" >/dev/null ||
  fail 'duplicated existing signing policy'
run_migration missing
run_migration failed

printf '%s\n' 'icms-api-keys-signing: all checks passed'
