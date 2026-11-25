<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/treecopy

Copies a directory tree file by file, regular files in parallel. In both
modes it keeps symlinks and hardlinks, the access and modification times of
regular files and symlinks, and, best effort (a failure is ignored), the
extended attributes of regular files and directories except
`trusted.overlay.*`. It does not keep directory times or the extended
attributes of symlinks, devices and FIFOs.

By default it copies file data with `sendfile(2)` (a buffered copy where
sendfile is unavailable) and skips what does not belong in a copy of a
live root filesystem (sockets, FIFOs, devices, overlay whiteouts).

Faithful mode re-creates a stored tree: it also keeps owners and modes,
re-creates device nodes and FIFOs with their times, and reads file data with `O_DIRECT`
(`copyDirect`), through the page cache where the filesystem refuses it.

## Used by

- [`cmd/agent/capture_write.go`](../../cmd/agent/capture_write.go): the
  `capture-write` compatibility subcommand, kept for writer Jobs from
  older agents.
- [`internal/agent`](../agent/): `l2_writer.go` and `l2_fetch.go`
  (`NewCopier`, `SetFaithful`).
- [`internal/checkpointstore`](../checkpointstore/): `local.go`.

## Key files

- `copier.go`: `Copier`, `NewCopier` and the walk.
- `faithful.go`: `SetWorkers`, `SetFaithful` and faithful-mode copying.
