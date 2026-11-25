<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/server

The REST API of nvsnap-server (under `/api/v1`), its `/metrics`, WebSocket
and OpenAPI endpoints, and the background loops it runs. It reads nodes and
pods from the Kubernetes API, keeps the checkpoint catalog in
[`internal/db`](../db/), and forwards checkpoint and restore requests to the
agent on the pod's node.

## Used by

- [`cmd/nvsnap-server/main.go`](../../cmd/nvsnap-server/main.go): `server.New`,
  `SetUI` and `Run`.
- Clients of the API: the web UI ([`ui/src/api`](../../ui/src/api/)), the
  `nvsnap` CLI, [`examples/checkpoint-restore`](../../examples/checkpoint-restore/),
  the agent (checkpoint registration and L2 `pvc-state`) and
  [`cmd/nvsnap-l2-wait`](../../cmd/nvsnap-l2-wait/).

## Key files

- `server.go`: `Server`, `New`, the router and most handlers.
- `openapi.yaml`, `openapi.go`: the API spec, embedded and served.
- `sources.go`, `lookup.go`: checkpoint registration, peers, lookup and L2
  `pvc-state`.
- `reconciler.go`: adds captures found as ConfigMaps to the catalog.
- `election_release.go`: releases or evicts gated model-election followers.
- `function_status.go`, `restore_history.go`: the `NvsnapFunction` objects
  ([`internal/fnstatus`](../fnstatus/)).
- `demo.go`, `manifests.go`: the UI demo page and its workload catalog
  (`/etc/nvsnap/workloads` or `NVSNAP_WORKLOADS_DIR`).
- `observability_proxy.go`, `ws.go`, `agent_auth.go`: Grafana, Jaeger and
  Prometheus proxy, WebSocket hub, agent bearer token.

## Configuration

Helm `server.enabled` (default `true`); see
[`cmd/nvsnap-server`](../../cmd/nvsnap-server/) for its flags.
