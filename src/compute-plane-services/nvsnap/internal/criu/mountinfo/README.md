<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/criu/mountinfo

Parses `/proc/<pid>/mountinfo`: it finds a container's overlay upperdir and
the mount points other than `/`, for capture and for cross-pod restore.

## Used by

- [`internal/agent`](../../agent/): `checkpoint.go` and `rootfs_diff.go`
  (`ProcMountinfoPath`, `ParseMountinfo`, `ResolveOverlayUpperdir`,
  `NonRootMountPoints`); `restore.go` (`NonRootMountPoints`).
- [`internal/rootfsonly`](../../rootfsonly/): `orchestrator.go`
  (`DefaultProcRoot`, `ResolveOverlayUpperdirAtRoot`,
  `NonRootMountPointsAtRoot`).

## Key files

- `mountinfo.go`: the parser and the upperdir and mount-point lookups.

The parent directory `internal/criu/` holds only this package.
