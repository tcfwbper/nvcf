#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/release_matrix_e2e.sh --candidate DIR --release DIR [--label NAME]

Runs released and candidate Stargate and Pylon binaries together on loopback.
Each directory must contain `stargate` and `pylon`; the candidate directory
must also contain `mock-dynamo`.

For each policy (power-of-n and the pulsar default), one released Pylon and
one candidate Pylon register over reverse raw QUIC tunnels. Stargate then
moves through three phases without restarting the Pylons:

  1. released Stargate        (old consumer, mixed producers)
  2. candidate Stargate       (consumer upgrade)
  3. released Stargate again  (consumer rollback)

Every phase requires both Pylons to register and serve completed streaming
responses. Every request after both Pylons serve must succeed.

The command line uses only options that every supported release accepts, so
the candidate also proves that the released launch options still start it.

Environment:
  MATRIX_PORT_BASE   First loopback port, default 27300. Uses 20 ports.
  MATRIX_WORK_DIR    Logs and configs, kept after the run. Default: a new
                     mktemp directory, removed on success.
  MATRIX_PHASE_TIMEOUT_SECS
                     Seconds for both Pylons to serve in each phase, default 90.

Requires curl.
EOF
}

candidate_dir=""
release_dir=""
label="release"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --candidate) candidate_dir="$2"; shift 2 ;;
        --release) release_dir="$2"; shift 2 ;;
        --label) label="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
done
[[ -n "${candidate_dir}" && -n "${release_dir}" ]] || { usage >&2; exit 2; }
for binary in "${candidate_dir}/stargate" "${candidate_dir}/pylon" "${candidate_dir}/mock-dynamo" \
    "${release_dir}/stargate" "${release_dir}/pylon"; do
    [[ -x "${binary}" ]] || { echo "missing executable: ${binary}" >&2; exit 2; }
done
command -v curl >/dev/null || { echo "curl is required" >&2; exit 2; }

port_base="${MATRIX_PORT_BASE:-27300}"
phase_timeout="${MATRIX_PHASE_TIMEOUT_SECS:-90}"
work_dir="${MATRIX_WORK_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/stargate-release-matrix.XXXXXX")}"
mkdir -p "${work_dir}"

model="compat-model"
steady_requests=24
sg_grpc=$((port_base))
sg_http=$((port_base + 1))
sg_metrics=$((port_base + 2))
sg_discovery=$((port_base + 3))
sg_reverse=$((port_base + 4))
mock_port() { echo $((port_base + 10 + $1)); }
pylon_metrics_port() { echo $((port_base + 15 + $1)); }
pylon_ids=(pylon-release pylon-candidate)
pylon_dirs=("${release_dir}" "${candidate_dir}")

pids=()
stargate_pid=""
cleanup() {
    local pid
    for pid in "${pids[@]+"${pids[@]}"}" ${stargate_pid}; do
        kill "${pid}" 2>/dev/null || true
    done
    for pid in "${pids[@]+"${pids[@]}"}" ${stargate_pid}; do
        wait "${pid}" 2>/dev/null || true
    done
    pids=()
    stargate_pid=""
}
on_exit() {
    local status=$?
    cleanup
    if [[ ${status} -ne 0 ]]; then
        echo "FAIL (${label}): logs kept in ${work_dir}" >&2
    elif [[ -z "${MATRIX_WORK_DIR:-}" ]]; then
        rm -rf "${work_dir}"
    fi
    exit "${status}"
}
trap on_exit EXIT
trap 'exit 130' INT TERM

fail() {
    echo "FAIL (${label}): $*" >&2
    exit 1
}

port_open() {
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

wait_for() {
    local description="$1" deadline=$((SECONDS + 30))
    shift
    until "$@" >/dev/null 2>&1; do
        [[ ${SECONDS} -lt ${deadline} ]] || fail "timed out waiting for ${description}"
        sleep 0.2
    done
}

start() {
    local log="$1"
    shift
    "$@" >>"${log}" 2>&1 &
    pids+=("$!")
}

start_stargate() {
    local binary_dir="$1" log="$2"
    "${binary_dir}/stargate" \
        --stargate-id compat-stargate \
        --listen-addr "127.0.0.1:${sg_grpc}" \
        --http-listen-addr "127.0.0.1:${sg_http}" \
        --model-discovery-listen-addr "127.0.0.1:${sg_discovery}" \
        --advertise-addr "127.0.0.1:${sg_grpc}" \
        --stargate-discovery-dns-name localhost \
        --disable-dns-discovery \
        --metrics-port "${sg_metrics}" \
        --lb-config-path "${run_dir}/lb-config.json" \
        --backend-connectivity reverse \
        --reverse-tunnel-listen-addr "127.0.0.1:${sg_reverse}" \
        --advertised-hostname-template localhost \
        --quic-insecure >>"${log}" 2>&1 &
    stargate_pid=$!
    wait_for "stargate metrics" curl -sf "http://127.0.0.1:${sg_metrics}/metrics"
}

stop_stargate() {
    kill "${stargate_pid}" 2>/dev/null || true
    wait "${stargate_pid}" 2>/dev/null || true
    stargate_pid=""
    local deadline=$((SECONDS + 30)) port
    for port in "${sg_grpc}" "${sg_http}" "${sg_metrics}" "${sg_discovery}"; do
        while port_open "${port}"; do
            [[ ${SECONDS} -lt ${deadline} ]] || fail "Stargate port ${port} stayed open"
            sleep 0.2
        done
    done
}

# Prints the serving Pylon ID, or nothing when the request did not complete.
stream_request() {
    local key="$1" headers="${run_dir}/last-headers.txt" body="${run_dir}/last-body.txt" status
    status=$(curl -s -N --max-time 20 -D "${headers}" -o "${body}" -w '%{http_code}' \
        "http://127.0.0.1:${sg_http}/v1/chat/completions" \
        -H 'content-type: application/json' \
        -H "x-request-id: ${key}" \
        -H "x-model: ${model}" \
        -H 'x-input-tokens: 8' \
        -H "x-cache-affinity-key: ${key}" \
        -d "{\"model\":\"${model}\",\"stream\":true,\"max_tokens\":4,\"messages\":[{\"role\":\"user\",\"content\":\"hello\"}]}") || return 0
    [[ "${status}" == "200" ]] || return 0
    grep -q '^data: \[DONE\]' "${body}" || return 0
    tr -d '\r' <"${headers}" | awk -F': ' 'tolower($1) == "x-inference-server-id" {print $2}'
}

check_phase() {
    local phase="$1" deadline=$((SECONDS + phase_timeout)) index=0 served release_seen=0 candidate_seen=0
    echo "--- ${phase}"
    # Registration and tunnel reconnection can take a few retry periods.
    while [[ ${release_seen} -eq 0 || ${candidate_seen} -eq 0 ]]; do
        [[ ${SECONDS} -lt ${deadline} ]] ||
            fail "${phase}: both Pylons did not serve within ${phase_timeout}s (release=${release_seen} candidate=${candidate_seen})"
        index=$((index + 1))
        served=$(stream_request "${phase}-warm-${index}")
        case "${served}" in
            pylon-release) release_seen=1 ;;
            pylon-candidate) candidate_seen=1 ;;
            "") sleep 0.5 ;;
            *) fail "${phase}: unexpected backend ${served}" ;;
        esac
    done
    release_seen=0
    candidate_seen=0
    for index in $(seq 1 "${steady_requests}"); do
        served=$(stream_request "${phase}-steady-${index}")
        case "${served}" in
            pylon-release) release_seen=$((release_seen + 1)) ;;
            pylon-candidate) candidate_seen=$((candidate_seen + 1)) ;;
            *) fail "${phase}: steady request ${index} did not complete (backend '${served}', body: $(head -c 300 "${run_dir}/last-body.txt"))" ;;
        esac
    done
    # Both Pylons already served in the warm-up loop. Routing weights may send
    # all steady requests to one of them, so only completion is required here.
    echo "    ok: release=${release_seen} candidate=${candidate_seen}"
}

run_policy() {
    local policy="$1" index port
    run_dir="${work_dir}/${policy}"
    mkdir -p "${run_dir}"
    echo "=== ${label}: ${policy}"
    for port in "${sg_grpc}" "${sg_http}" "${sg_metrics}" "${sg_discovery}" \
        "$(mock_port 0)" "$(mock_port 1)" "$(pylon_metrics_port 0)" "$(pylon_metrics_port 1)"; do
        port_open "${port}" && fail "port ${port} is already in use; set MATRIX_PORT_BASE"
    done
    if [[ "${policy}" == power-of-n ]]; then
        printf '{"default": "power-of-n"}\n' >"${run_dir}/lb-config.json"
    else
        printf '{"default": "power-of-n", "models": {"%s": {"algorithm": "%s"}}}\n' \
            "${model}" "${policy}" >"${run_dir}/lb-config.json"
    fi

    for index in 0 1; do
        start "${run_dir}/mock-${index}.log" "${candidate_dir}/mock-dynamo" \
            --http-listen-addr "127.0.0.1:$(mock_port "${index}")" \
            --model-name "${model}" \
            --num-tokens 4 \
            --token-delay-ms 1
    done
    start_stargate "${release_dir}" "${run_dir}/stargate.log"
    for index in 0 1; do
        wait_for "mock-dynamo ${index}" curl -sf "http://127.0.0.1:$(mock_port "${index}")/health"
        start "${run_dir}/${pylon_ids[${index}]}.log" env RUST_LOG=info \
            "${pylon_dirs[${index}]}/pylon" \
            --upstream-http-base-url "http://127.0.0.1:$(mock_port "${index}")" \
            --model-name "${model}" \
            --stargate-address "127.0.0.1:${sg_grpc}" \
            --inference-server-id "${pylon_ids[${index}]}" \
            --cluster-id "cluster-${pylon_ids[${index}]}" \
            --backend-connectivity reverse \
            --quic-insecure \
            --kv-cache-stats-path /kv-cache/stats \
            --min-update-interval-ms 100 \
            --disable-bringup \
            --active-canary-interval-ms=0 \
            --initial-input-tps 1000 \
            --max-engine-concurrency 8 \
            --metrics-host 127.0.0.1 \
            --metrics-port "$(pylon_metrics_port "${index}")"
    done

    check_phase "${policy}: released Stargate"
    stop_stargate
    start_stargate "${candidate_dir}" "${run_dir}/stargate.log"
    check_phase "${policy}: candidate Stargate (upgrade)"
    stop_stargate
    start_stargate "${release_dir}" "${run_dir}/stargate.log"
    check_phase "${policy}: released Stargate (rollback)"
    cleanup
}

run_dir="${work_dir}"
for policy in power-of-n pulsar; do
    run_policy "${policy}"
done
echo "PASS (${label})"
