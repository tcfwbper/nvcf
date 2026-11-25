<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# cmd/nvsnap-helper

`nvsnap-helper`, a static binary that runs the steps the webhook adds to a
workload pod. It comes from the node's nvsnap bundle, mounted read-only into
the pod, so a step runs in the workload's own image without a shell.

## Used by

- Init containers and commands written by the webhook
  ([`internal/webhook/helper.go`](../../internal/webhook/helper.go)) and by
  [`internal/modelvolume`](../../internal/modelvolume/).
- Built into the agent image by
  [`docker/agent/Dockerfile.app`](../../docker/agent/Dockerfile.app).
- Entry point: `main.go:main`, which dispatches on the first argument:
  `skip`, `wait`, `write`, `seed`, `model-cache`, `prewarm`,
  `await-pid-floor`.

## Key files

- `main.go`: subcommand dispatch and the `skip`, `wait`, `write`,
  `seed` and `prewarm` steps.
- `modelcache.go`: `model-cache`, which seeds a pod's download volume from
  the node model cache ([`internal/modelcache`](../../internal/modelcache/))
  or runs the pod's own download.
- `pidfloor.go`: `await-pid-floor`, which holds a CRIU restore placeholder
  until the agent has raised the pod's next pid.

## See also

- [docs/design/node-model-cache.md](../../docs/design/node-model-cache.md)
- [docs/design/TOOLS-OUTSIDE-THE-WORKLOAD.md](../../docs/design/TOOLS-OUTSIDE-THE-WORKLOAD.md)
