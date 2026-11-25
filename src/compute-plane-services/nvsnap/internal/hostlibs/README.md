<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/hostlibs

Names the per-architecture library directories of the NVIDIA driver and
of a container's libc, for amd64 and arm64 nodes.

## Used by

- [`internal/agent`](../agent/): `checkpoint.go` (`DriverDir`) and
  `gpu_probe.go` (`Dir`, `DriverDir`).
- [`internal/cuda`](../cuda/): `checkpoint.go` (`Dir`, `DriverDir`).

## Key files

- `hostlibs.go`: `Dir` and `DriverDir`.
