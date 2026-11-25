<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/containerd

The containerd backend of [`internal/runtime`](../runtime/): it implements
`runtime.Runtime` with the containerd Go client, so the agent can find a
pod's containers and their rootfs on a containerd node.

## Used by

- [`internal/agent`](../agent/): it gets the client through `runtime.New`,
  which this package's `init` registers with `runtime.RegisterFactory`, and
  calls it through the `runtime.Runtime` interface. `agent.go` also keeps
  the `*containerd.Client` in `Agent.containerd`, which nothing reads yet.
  Several agent files import the package only for its `ContainerInfo`
  alias of `runtime.ContainerInfo`.

## Key files

- `client.go`: `New` and the `runtime.Runtime` methods.

## Configuration

None of its own. The agent picks the runtime from the socket it finds
(`runtime.Detect`, starting from the `--containerd-socket` flag); Helm
`agent.runtime` is informational. The CRI-O backend is
[`internal/runtime/crio`](../runtime/crio/).
