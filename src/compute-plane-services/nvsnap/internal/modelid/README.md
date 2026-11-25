<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/modelid

Answers, for a GPU pod at admission, which model artifact it will download
(as a URI such as `hf://org/repo`, `ngc://...`, `s3://...` or `nim://...`)
and where in the pod the download lands. The webhook uses the answer to
share one model volume or cache entry between pods that name the same
artifact.

## Used by

- [`internal/webhook`](../webhook/): `model_volume.go` (`ResolveWithGroup`)
  and `node_model_cache.go` (`Resolve`).

## Key files

- `modelid.go`: `Resolve`, `Identity` and `Landing`.
- `group.go`: `GroupResolver` and `ResolveWithGroup`, which let a pod
  inherit the identity of its group (for example a LeaderWorkerSet leader
  template, `LWSResolver`).

See [docs/design/helm-shared-model-volume.md](../../docs/design/helm-shared-model-volume.md).
