<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/db

The SQLite catalog of nvsnap-server: checkpoints, restores, retention
policies, the audit log, the peers that hold a checkpoint, and the demo
page state. It uses the pure-Go `modernc.org/sqlite` driver, so the server
builds with `CGO_ENABLED=0`.

## Used by

- [`cmd/nvsnap-server/main.go`](../../cmd/nvsnap-server/main.go): `db.Open`.
- [`internal/server`](../server/): every catalog read and write behind the
  REST API.

## Key files

- `db.go`: `DB` and `Open`.
- `migrations.go`: the schema and its versioned migrations.
- `models.go`: the row types.
- `checkpoint.go`, `restore.go`, `retention.go`, `audit.go`, `peers.go`:
  queries per table.
- `dedupe.go`: matching and de-duplicating checkpoint rows by id or hash.

## Configuration

The database path is the server flag `--db-path`; the chart passes
`/data/nvsnap.db`, and `/data` is a PVC when `server.persistence.enabled`
(default `true`).
