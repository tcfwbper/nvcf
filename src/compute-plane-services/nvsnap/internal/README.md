<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->
# internal/

Repo-private Go packages (not importable by external modules). The bulk of
nvsnap's logic lives here; `cmd/` binaries wire these together.

## Package map

Each Go package has its own README (`criu/` holds only `mountinfo/`).

**Capture / restore core**

- [`agent/`](agent/): the node agent: capture and restore, gpushare, peer
  fetch, storage tiers, and the wiring of the in-process webhook.
- [`capturehold/`](capturehold/): the readiness gate that keeps a pod out of
  service while it may be captured.
- [`criu/mountinfo/`](criu/mountinfo/): /proc mountinfo parsing for capture
  and restore.
- [`cuda/`](cuda/): finds GPU processes for the agent; its
  `cuda-checkpoint` wrappers are not on the capture path.
- [`rootfsonly/`](rootfsonly/): rootfs and cachedir capture and the warm-pod
  watcher.
- [`treecopy/`](treecopy/): file-tree copier (`sendfile`, or direct I/O in
  faithful mode; parallel files).
- [`tarstream/`](tarstream/): moves a directory tree between agents as a tar
  stream that cannot escape its destination.
- [`hostlibs/`](hostlibs/): per-architecture driver and libc directories.

**Models and admission**

- [`webhook/`](webhook/): the mutating admission webhook.
- [`modelid/`](modelid/): which model a GPU pod will download, and where.
- [`modelvolume/`](modelvolume/): the per-model volume and its reaper.
- [`modelcache/`](modelcache/): the on-disk format of the node model cache.
- [`election/`](election/): one downloader per cache hash, through a
  Kubernetes Lease.
- [`tenant/`](tenant/): the tenant a namespace belongs to, part of every
  content hash.

**Storage / catalog**

- [`checkpointstore/`](checkpointstore/): content-addressed capture storage
  (node, ConfigMap index, per-capture volume) and its promoters.
- [`objectstore/`](objectstore/): bucket interface for cross-cluster
  replication (GCS).
- [`db/`](db/): SQLite catalog of nvsnap-server.
- [`fnstatus/`](fnstatus/): per-function-version status from capture records.

**Control plane / platform**

- [`server/`](server/): REST API of nvsnap-server.
- [`runtime/`](runtime/) (with [`runtime/crio/`](runtime/crio/)) and
  [`containerd/`](containerd/): container-runtime backends.
- [`manifests/`](manifests/): generates and checks the criu-v2 restore
  placeholders in `deploy/k8s/workloads/`.
- [`metrics/`](metrics/), [`tracing/`](tracing/), [`podtimings/`](podtimings/):
  Prometheus metrics, OpenTelemetry, pod cold-versus-restored timings.

## Rules

Anything an external module must import belongs in [`../pkg/`](../pkg/), not
here. Keep package boundaries narrow; don't widen `internal/` to leak a type.
