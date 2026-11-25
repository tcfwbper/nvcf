<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/runtime

The container-runtime interface the agent uses: find a pod's containers,
list containers, and resolve a container's rootfs path. Backends register a
factory from their `init`; `New` detects the runtime from the socket that
exists on the node and builds that backend.

## Used by

- [`internal/agent`](../agent/): `agent.go` calls `runtime.New` and uses the
  `Runtime` interface and `ContainerInfo`.
- The backends: [`internal/containerd`](../containerd/) and [`crio/`](crio/),
  which call `RegisterFactory`.

## Key files

- `runtime.go`: the `Runtime` interface, `ContainerInfo`, `ErrNotFound`.
- `factory.go`: `New` and `RegisterFactory`.
- `detect.go`: `Detect`, which uses the socket hint if it exists and
  otherwise probes the well-known socket paths.

## Configuration

The socket hint is the agent flag `--containerd-socket` (default
`/run/containerd/containerd.sock`); Helm `agent.runtime` is informational.
