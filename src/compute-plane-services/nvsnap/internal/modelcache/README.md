<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/modelcache

The on-disk format of the node model cache: downloaded model trees kept on
a node's local disk, one directory per key, each copy an immutable
generation with a manifest of every file. A later pod on that node, or on a
node that fetches the tree from it, seeds its download volume from a
generation instead of downloading. The package comment describes the
layout.

## Used by

- [`internal/agent`](../agent/): `node_model_cache.go` (harvest, `Scan`,
  `Plans`, `AllocEstimate`), `model_cache_flight.go` and
  `model_cache_stream.go` (single-flight download and streaming).
- [`internal/webhook`](../webhook/): `node_model_cache.go` (`Key`).
- [`cmd/nvsnap-helper`](../../cmd/nvsnap-helper/): `modelcache.go`
  (`LatestIn`, `Seed`, `Scan`), which seeds a pod from the cache.

## Key files

- `modelcache.go`: keys, generations, manifests, `Seed`.
- `walk.go`: `Scan`, which describes a tree without reading file contents.
- `live.go`: `ScanLive` and helpers for a tree still being written.
- `plan.go`: per-pod plan, heartbeat and end-marker files in a key directory.

## Configuration

Helm `agent.nodeModelCache.enabled` (default `false`), with `root`,
`maxPercent`, `wait`, `fetchTimeout` and `singleFlight` beside it; passed as
the agent's `--node-model-cache*` flags.

See [docs/design/node-model-cache.md](../../docs/design/node-model-cache.md)
and [docs/design/model-cache-single-flight.md](../../docs/design/model-cache-single-flight.md).
