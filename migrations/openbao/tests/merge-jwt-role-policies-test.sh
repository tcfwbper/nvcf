#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# The migration helpers require bash even when the shell test runner uses sh.
if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/../migrations/utils/functions.sh"

log_error() { printf '%s\n' "$*" >&2; }

bao() {
    case "${ROLE_READ_MODE}" in
        existing) printf '%s\n' '{"data":{"token_policies":["base","other"]}}' ;;
        empty) printf '%s\n' '{"data":{"token_policies":[]}}' ;;
        missing) printf '%s\n' 'No value found' >&2; return 2 ;;
        failed) printf '%s\n' 'permission denied' >&2; return 2 ;;
        malformed) printf '%s\n' '{"data":{}}' ;;
    esac
}

ROLE_READ_MODE=existing
[[ "$(merge_jwt_role_policies sis-api 'other,new')" == 'base,other,new' ]]
ROLE_READ_MODE=empty
[[ "$(merge_jwt_role_policies sis-api 'new')" == 'new' ]]
ROLE_READ_MODE=missing
[[ "$(merge_jwt_role_policies sis-api 'new')" == 'new' ]]
if merge_jwt_role_policies sis-api 'new' true; then
    printf '%s\n' 'missing required role must stop migration' >&2
    exit 1
fi
ROLE_READ_MODE=failed
if merge_jwt_role_policies sis-api 'new'; then
    printf '%s\n' 'role read failure must stop migration' >&2
    exit 1
fi
ROLE_READ_MODE=malformed
if merge_jwt_role_policies sis-api 'new'; then
    printf '%s\n' 'malformed role must stop migration' >&2
    exit 1
fi
