<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/fnstatus

Names the function version a capture record belongs to, and projects the
records onto one status per function version. Agents stamp the names on
the records; nvsnap-server keeps one `NvsnapFunction` object per function
version from them. The records stay the source of truth.

## Used by

- [`internal/agent`](../agent/): `record_identity.go` (`Identity`) and
  `verify_abandoned.go` (`AbandonedKey`).
- [`internal/server`](../server/): `function_status.go` (`Project`,
  `ObjectName`, `GPURanks`, `RecordRestoreSeconds`) and
  `restore_history.go`, which follows restores from the pods' group labels
  and restore annotations and records each `Restore` and its outcome.

## Key files

- `fnstatus.go`: record labels, `Identity`, `Status` and `Project`.
- `history.go`: the restore history of a configuration kept in a record.

## Configuration

The `NvsnapFunction` CRD is rendered by the chart's
`nvsnapfunction-crd.yaml` when `server.enabled` is `true` (the default).

See [docs/design/FUNCTION-CACHE-STATUS.md](../../docs/design/FUNCTION-CACHE-STATUS.md).
