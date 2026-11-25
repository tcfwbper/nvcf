<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# CRIU test matrix workloads

`chart/` deploys one inference workload in a chosen shape, as an NVCF Helm
function or with plain Helm. `cases/` holds one values file per matrix case.

## Key files

- `chart/values.yaml`: the shape (`kind`: `StatefulSet`, `Deployment`, `Pod`,
  `LeaderWorkerSet` or `DynamoGraphDeployment`; `podsPerInstance`,
  `gpusPerPod`), the engine, image, model and launch style.
- `chart/templates/workload.yaml`: the workload for each `kind`;
  `computedomain.yaml`, `dynamo-deps.yaml` and `service.yaml` add what some
  shapes need.
- `cases/*.yaml`: one case each, for example `sts-tp2.yaml` or
  `vllm-llama-8b.yaml`.
- `create-functions.sh`: creates the chart repository if it is missing,
  then for each selected case pushes the chart with the case's values as
  its defaults and creates an NVCF Helm function, plus a container function
  for single-pod Deployment cases that set no `runAsUser` and no
  `function.container: false`. `MODE=helm` skips the container functions
  and `MODE=container` the per-case chart push and Helm functions (default
  `both`). Its header lists the
  environment it reads (`ORG`, `NGC_CLI_API_KEY`, `CASES`, `ARCH`,
  `DRY_RUN`, ...).

## Used by

- Run by hand. The chart is listed as a test fixture in the repository's
  `tools/docs-version-sync/chart-image-exclusions.yaml`.
