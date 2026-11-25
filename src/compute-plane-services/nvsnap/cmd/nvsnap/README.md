<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# cmd/nvsnap

The `nvsnap` command-line client. `pods` reads pods from the Kubernetes API
directly, `version` prints the CLI version, and the other subcommands call
nvsnap-server over HTTP (`--server`, default `http://localhost:8080`).
`ps`, `checkpoint`, `restore` and `status` call routes that
[`internal/server`](../../internal/server/) does not register
(`/api/v1/processes...`, `/api/v1/checkpoints/{id}/restore`,
`/api/v1/status`; the server has `/api/v1/health`). `checkpoints` reaches
the server but decodes snake_case fields (`size_bytes`, `created_at`),
while the server returns `db.Checkpoint`'s camelCase ones
(`checkpointSize`, `createdAt`).

## Used by

- Operators, by hand. Bazel builds it (`go_binary` `nvsnap` in
  `BUILD.bazel`); no Dockerfile, Makefile target or chart ships it.
- Entry point: `main.go:main` (cobra root command).

## Key files

- `main.go`: the `ps`, `checkpoint`, `restore`, `checkpoints`,
  `delete`, `status` and `version` subcommands.
- `pods.go`: `nvsnap pods`, which classifies pods as cold-start or
  restored with [`internal/podtimings`](../../internal/podtimings/).

## See also

- The server it talks to: [`../nvsnap-server/`](../nvsnap-server/).
