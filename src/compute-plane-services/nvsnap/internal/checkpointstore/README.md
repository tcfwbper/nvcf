<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/checkpointstore

Content-addressed storage for rootfs and cachedir captures. A capture is
named by the hash of its inputs (`HashInput`, `ComputeHash`), so the same
content has the same name on every node. Backends store it on the node
(`Local`), index it cluster-wide in a ConfigMap (`ConfigMapBackend`), and
publish it on a per-capture volume (`PerCapturePVCBackend`, the L2 tier);
`Chain` writes to several at once.

## Used by

- [`internal/agent`](../agent/): `rootfsonly_integration.go` builds the
  backends (`NewLocal`, `NewConfigMapBackend`), `l2_integration.go` the L2
  backend and its promoter (`ResolveStorageProfile`,
  `NewPromoterFromProfile`); many files use `Manifest` and `ShortHash`.
- [`internal/rootfsonly`](../rootfsonly/), [`internal/webhook`](../webhook/),
  [`internal/election`](../election/), [`internal/server`](../server/) and
  [`cmd/agent`](../../cmd/agent/): hashes, manifests and names.

## Key files

- `store.go`: `Store`, `Manifest`, `CaptureSource`, `CaptureFormatVersion`.
- `mounter.go`: the `Backend` interface.
- `hash.go`: `HashInput`, `ComputeHash`, `ShortHash` and object names.
- `local.go`, `cmregistry.go`, `chain.go`: the node, ConfigMap and `Chain`
  backends.
- `percapture_pvc.go`, `mount_holder.go`: the L2 per-capture volume and the
  pod that mounts it for writing.
- `promoter.go`, `promoter_snapshot.go`, `promoter_shared.go`: how a written
  volume becomes readable (snapshot and clone, or shared read-only attach).
- `storage_profile.go`: picks the promoter from the StorageClass
  provisioner.

## Configuration

Helm `agent.l2.storageClass` (default `""`, L2 off) and
`agent.l2.snapshotClass`, passed as `NVSNAP_L2_*` environment variables.

See [docs/design/STORAGE-AGNOSTIC-L2-PROMOTION.md](../../docs/design/STORAGE-AGNOSTIC-L2-PROMOTION.md).
