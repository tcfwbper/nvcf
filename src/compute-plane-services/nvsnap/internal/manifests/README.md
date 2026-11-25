<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/manifests

Derives the criu-v2 restore placeholder manifests in
[`deploy/k8s/workloads/`](../../deploy/k8s/workloads/) from their source
manifests. Build and test time only; no shipped binary imports it.

## Used by

- [`gen/`](gen/): `go run ./internal/manifests/gen` writes the placeholders;
  `-check` reports drift without writing. Only a person runs it: no test,
  Makefile target or workflow does.
- `conformance_test.go`, which reads the manifests in
  `deploy/k8s/workloads/` (Bazel `data`) and checks them.

## Key files

- `restore.go`: `RenderRestore` and `IsCRIUV2Source`.
- `conformance_test.go`: the checks, for example that a restore manifest uses
  its source's image and that multi-GPU sources are not CRIU.

`workloads.yaml` is read by no code or build file here; its header names a
`source.go` and a `go generate` step that do not exist.
