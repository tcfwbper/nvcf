<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/manifests/gen

A small `main` package that writes the criu-v2 restore placeholders in
[`deploy/k8s/workloads/`](../../../deploy/k8s/workloads/) from their source
manifests, using [`internal/manifests`](../).

## Used by

- Developers: `go run ./internal/manifests/gen` (write) or
  `go run ./internal/manifests/gen -check` (report drift, write nothing).
  Nothing else invokes it.
- Entry point: `main.go:main`.

## Key files

- `main.go`: walks the workload manifests, renders each placeholder with
  `manifests.RenderRestore`, and writes or checks it.
