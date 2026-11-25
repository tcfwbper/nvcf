<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/runtime/crio

The CRI-O backend of [`internal/runtime`](../): it implements
`runtime.Runtime` over the CRI gRPC API (`k8s.io/cri-api`), and resolves a
container's host rootfs through `/proc/<pid>/root`.

## Used by

- [`internal/agent/agent.go`](../../agent/agent.go), which imports it only
  for its `init` (registration with `runtime.RegisterFactory`).

## Key files

- `client.go`: `New` and the `runtime.Runtime` methods.
