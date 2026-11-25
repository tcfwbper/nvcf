<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/modelvolume

The per-model volume: one volume per model URI and tenant scope
(`ScopedURI`; a namespace is its own scope when no tenant label applies),
written once, then immutable and read through read-only views by every
other pod in that scope that needs the model. The package holds the names, labels and annotations of
that lifecycle, the `Provisioner` the agent drives, and the `Reaper` that
removes the PersistentVolumes the flow leaves behind.

## Used by

- [`internal/agent`](../agent/): `modelvolume_controller.go` (claim and PV
  names, staging state, marker files), `l2_integration.go` (`ReaderMode`),
  `agent.go` (the `Provisioner` and the `Reaper` it runs),
  `modelvolume_trust.go` (`State`, `ScopedURI`), `bundle_versions.go`
  (`HelperVolumeName`), and the rank annotations
  (`CacheOrdinalAnnotation`, `CacheGroupSizeAnnotation`) in
  `criu_group.go`, `criu_fallback.go`, `capture_hold.go`,
  `capture_preflight.go`, `grove_replicas.go` and `lifecycle_metrics.go`.
- [`internal/webhook`](../webhook/): `model_volume.go`, `cache_volume.go`,
  `reader_steps.go` and `helper.go` (`Key`, `ScopedURI`, `Harden`,
  `HelperMount`), `mutate.go` (`Provisioner`) and `criu_identity.go`
  (the rank annotations).

## Key files

- `modelvolume.go`: keys, names, labels, annotations, `Provisioner`, and the
  helper and idle binary paths in the node bundle.
- `reaper.go`: `Reaper`: read-only PVs whose claim is gone, abandoned
  primaries, and complete primaries unused for longer than the retention.

## Configuration

Helm `agent.modelVolume.enabled` (default `false`), with `waitDeadline`,
`reapInterval`, `retention` and `tenantLabel` beside it; passed as the
agent's `--model-volume*` flags.

See [docs/design/helm-shared-model-volume.md](../../docs/design/helm-shared-model-volume.md).
