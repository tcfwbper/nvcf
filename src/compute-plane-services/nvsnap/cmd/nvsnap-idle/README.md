<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# cmd/nvsnap-idle

`nvsnap-idle`, a static, single-threaded C program that keeps a container
alive until it is stopped. The webhook sets it as the command of a CRIU
restore placeholder's container, which holds the pod until the agent
restores the captured processes into it. Model staging uses it too, as
the hold container of a block-storage download Job.

## Used by

- [`internal/webhook/criu_restore.go`](../../internal/webhook/criu_restore.go)
  writes its path (`IdleBin`, from the node bundle) as the container
  command.
- [`internal/modelvolume/modelvolume.go`](../../internal/modelvolume/modelvolume.go)
  (`EnsureDownloadJob`) runs it in the hold container that keeps a
  block-storage download Job's pod running until the agent has copied the
  staged tree.
- Built with `gcc -static` in
  [`docker/agent/Dockerfile.app`](../../docker/agent/Dockerfile.app).
- Compiled and run by the tests here and by the agent's cold-start tests
  (`internal/agent/cold_start_test.go`).

## Key files

- [`c/nvsnap-idle.c`](c/nvsnap-idle.c): the program. With no arguments it
  idles, one short-lived child at a time; on SIGTERM it forwards the signal
  to the container and may hold while a teardown hold file exists. With
  `--cold-start MARKER -- COMMAND` it runs COMMAND when MARKER exists.
  The header comment describes both modes.
- `idle_test.go`: builds the program and tests it.
- `BUILD.bazel`: exports `c/nvsnap-idle.c` for the agent's tests.
