<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/agent

The node agent, run by the `nvsnap-agent` DaemonSet: it captures and restores
GPU workloads on its node (CRIU with gpushare, or the cachedir/rootfs path),
serves its HTTP API to the server, peer agents and injected init containers,
moves captures between nodes and storage tiers, and starts the in-process
admission webhook.

## Used by

- [`cmd/agent`](../../cmd/agent/): `main.go` calls `agent.New` and
  `(*Agent).Run` (`agent.go`).
- Its HTTP routes, registered in `agent.go`, are listed in
  [docs/REFERENCE.md](../../docs/REFERENCE.md).

## Key files

- `agent.go`: `Config`, `New`, `Run`, the HTTP router and the
  `/v1/checkpoint` and `/v1/restore` handlers.
- `checkpoint.go`, `restore.go`, `checkpoint_v2.go`, `restore_v2.go`:
  `(*Agent).Checkpoint`, `(*Agent).Restore` and the criu-v2 dump and restore.
- `criu_group.go`: capture and restore of a multi-pod instance as one group.
- `gpushare.go` and `gpushare_*.go`: GPU state through gpushare.
- `rootfsonly_integration.go`: wires the cachedir/rootfs capture watcher
  ([`../rootfsonly`](../rootfsonly/)).
- `l2_integration.go`, `cascade_fetch.go`, `replication.go`: the L2 volume
  tier, peer-to-peer fetch and object-store replication.
- `webhook_integration.go`: builds and starts [`../webhook`](../webhook/).

Other files group by prefix: `modelvolume_*` and `*model_cache*` (model
volumes, node model cache), `rails_*`, `capture_*`, `restore*`, `pidns_*`.

## Configuration

Flags come from [`cmd/agent/main.go`](../../cmd/agent/main.go), set by the
chart's [values.yaml](../../deploy/helm/nvsnap/values.yaml), for example
`agent.captureMethod` (`criu` or `cachedir`, default `cachedir`) and
`agent.auth.enabled` (default `false`).

See [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md) and
[docs/design/GPUSHARE.md](../../docs/design/GPUSHARE.md);
[`testdata/compat/`](testdata/compat/) holds fixtures from a released agent.
