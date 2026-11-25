<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/objectstore

A small object-store interface (`Bucket`: head, list, get, put by key) for
replicating captures between clusters through a bucket. Google Cloud
Storage is the only implementation; the `s3` provider returns a "not yet
implemented" error and an empty provider disables replication.

## Used by

- [`internal/agent`](../agent/): `replication.go` opens the home bucket
  with `NewBucket`; `replication_pull.go` and `replication_poll.go` use it.

## Key files

- `objectstore.go`: the `Bucket` interface, `ObjectInfo`, `ErrNotFound`.
- `provider.go`: `NewBucket`, which picks the backend by provider name.
- `gcs.go`: the Google Cloud Storage backend.

## Configuration

Helm `agent.replication.objectStore.provider` (default `""`, off),
`homeBucket` and `peerBuckets`. When both `provider` and `homeBucket` are
set, the chart passes them as `NVSNAP_REPLICATION_*` environment variables,
the defaults of the agent's `--replication-*` flags.

See [docs/design/cross-cluster-replication.md](../../docs/design/cross-cluster-replication.md).
