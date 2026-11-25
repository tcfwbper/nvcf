<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# tests/

Tests that are not Go unit tests. Go tests live next to their packages
(`*_test.go`); the cluster test scripts are in [`scripts/`](../scripts/).

## Subdirectories

- [`gpushare/`](gpushare/): tests of the gpushare libraries and
  `nvsnap-gpu-suspend` (GPU, host-only and on-cluster).
- [`criu-matrix/`](criu-matrix/): a Helm chart and one values file per case
  for deploying inference workloads in many shapes, as NVCF functions or
  with plain Helm.

## Files here

`cuda_va_preserve_test.py`, `cuda_va_preserve_stress.py` and
`cuda_va_pytorch_test.py` are standalone GPU probes of the CUDA virtual
memory API (save device memory, free it, map it again at the same address).
They call `libcuda` through `ctypes` and use no nvsnap code.
`cuda_va_pytorch_test.py` also needs PyTorch, and its multi-GPU case uses
up to four GPUs (it skips with fewer than two). Each is run with
`python3 tests/<file>` from `src/compute-plane-services/nvsnap`. No script,
workflow or build file runs them.
