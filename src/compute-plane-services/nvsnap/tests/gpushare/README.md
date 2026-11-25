<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# tests/gpushare

Tests of the gpushare libraries and `nvsnap-gpu-suspend`
([`docker/agent/gpushare`](../../docker/agent/gpushare/)).

## Used by

- [`scripts/check-gpushare-build.sh`](../../scripts/check-gpushare-build.sh)
  compiles the C tests here with warnings as errors; the `nvsnap gpushare`
  GitHub workflow runs that script and the host tests in [`host/`](host/).
- The C tests themselves need a GPU node and are run by hand.

## Key files

- `Makefile`: builds every `test_*.c`.
- `test_*.c`: GPU tests, one behaviour each, for example
  `test_release_rollback.c` (a failed suspend leaves the workload running)
  or `test_checkpoint_nccl.c` (multi-rank checkpoint and restore). Each
  takes the path of `nvsnap-gpu-suspend`, some also a mode or scratch
  directory. The `Run` line in each file's header comment gives its full
  invocation, including whether it runs under
  `LD_PRELOAD=libnvsnap_gpushare.so` (`test_checkpoint_nccl.c` needs it
  only with its `suspend` argument).

## Subdirectories

- [`host/`](host/): Python tests that need no GPU.
- [`k8s/`](k8s/): manifests and scripts for trying CRIU and
  `nvsnap-gpu-suspend` on vLLM pods in a cluster.

See [docs/design/GPUSHARE.md](../../docs/design/GPUSHARE.md).
