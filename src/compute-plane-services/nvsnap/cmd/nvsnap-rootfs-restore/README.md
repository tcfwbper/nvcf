<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# cmd/nvsnap-rootfs-restore

`nvsnap-rootfs-restore`, the command the webhook puts in front of a
workload container for a rootfs (non-CRIU) restore. Running as the
container's PID 1, it builds one overlay over the image rootfs and the
captured tree, `pivot_root`s into it, optionally reads the captured tree
into the page cache, then execs the workload's original command.

`NVSNAP_NO_OVERLAY` selects a legacy cachedir mode (no overlay: read
`NVSNAP_PREWARM_DIR` into the page cache, then exec). The webhook no
longer sets it: a cachedir restore keeps the workload's command and uses
`nvsnap-helper` init containers (`nvsnap-seed-cache`, `nvsnap-prewarm`)
instead, and `TestTryL2CacheDir_NoShim_PodCommandUntouched` checks that
none of this binary's environment reaches such a pod.

## Used by

- [`internal/webhook/rootfs_l2_overlay.go`](../../internal/webhook/rootfs_l2_overlay.go),
  which sets the container command and the `NVSNAP_*` environment this
  binary reads.
- Built into the agent image by
  [`docker/agent/Dockerfile.app`](../../docker/agent/Dockerfile.app) and reaches the pod
  through the node bundle mount
  ([`internal/webhook/bundle_mount.go`](../../internal/webhook/bundle_mount.go)).
- Entry point: `main.go:main`.

## Key files

- `main.go`: reads the environment (`NVSNAP_ORIG_COMMAND`,
  `NVSNAP_ORIG_CWD`, `NVSNAP_ROOTFS_VOLUMES`, `NVSNAP_NO_OVERLAY`, ...),
  builds the overlay, pivots and execs.
- `prewarm.go`: the page-cache read of the captured tree before exec.
- `main_test.go`: configuration parsing and helpers.
