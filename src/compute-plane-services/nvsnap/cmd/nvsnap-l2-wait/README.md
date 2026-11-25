<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# cmd/nvsnap-l2-wait

`nvsnap-l2-wait`, an init container the webhook adds to an election
follower whose capture is still waiting to be promoted. It polls
nvsnap-server's `GET /api/v1/checkpoints/by-hash/{hash}/pvc-state` until
the capture's L2 volume reaches a terminal state, then exits: 0 when it is
ready, 1 when it failed, 2 on timeout, 3 on a configuration error. Any
non-zero exit is an init failure that kubelet handles by the pod's
`restartPolicy`; codes 1 and 2 differ only in what they log.

## Used by

- [`internal/webhook`](../../internal/webhook/): `buildL2WaitContainer` in
  `l2_mount.go`, added only to election followers by `election.go` when
  an image is set
  (agent flag `--webhook-l2-wait-image`, Helm `agent.l2.waitImage`).
- Built by [`docker/nvsnap-l2-wait/Dockerfile`](../../docker/nvsnap-l2-wait/Dockerfile)
  (`ci/build-image.sh l2-wait`).
- Entry point: `main.go:main`.

## Key files

- `main.go`: flags, each with an environment fallback
  (`--server-url`/`NVSNAP_SERVER_URL`, `--hash`/`NVSNAP_CHECKPOINT_HASH`,
  `--timeout`/`NVSNAP_WAIT_TIMEOUT`, ...), the poll loop and the exit codes
  (documented in its package comment).
- `main_test.go`: the poll loop against a stub server.
