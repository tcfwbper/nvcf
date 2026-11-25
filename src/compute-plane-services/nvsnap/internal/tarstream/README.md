<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/tarstream

Moves a directory tree between agents as an uncompressed tar stream:
regular files, directories and relative symlinks only, and nothing that
would land outside the destination directory.

## Used by

- [`internal/agent`](../agent/): `modelvolume_controller.go` (`Write` on the
  sending side, `ExtractFresh` on the receiving side).

## Key files

- `tarstream.go`: `Write` and `ExtractFresh`.
