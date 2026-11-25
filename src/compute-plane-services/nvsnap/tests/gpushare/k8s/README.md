<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# tests/gpushare/k8s

Manifests and scripts for trying CRIU and `nvsnap-gpu-suspend` on vLLM pods
in a cluster, run by hand. No workflow or other script calls them.

## Key files

- `vllm-tp2.yaml`: a vLLM pod with tensor parallelism 2 for running
  `nvsnap-gpu-suspend` against a real workload.
- `vllm-criu.yaml`: a vLLM pod that can be CRIU-dumped and restored into a
  new pod.
- `criu-build.yaml`: builds CRIU into the node-local checkpoint directory.
- `vllm_criu_bench.sh`: a full CRIU checkpoint and restore of a vLLM model,
  with timings, on one node by default. `RESTORE_NODE` restores on another
  node, with `CKPT_PVC` putting `/ckpt` on a claim that node can read.
- `vllm_ckpt_cycle.sh`: a checkpoint and restore cycle against a running
  vLLM pod.
- `dump_evict.py`, `vllm_query.py`: evict a dump from the page cache;
  send a fixed completion request.

Image pull secrets are described in
[docs/operate/PULL-SECRETS.md](../../../docs/operate/PULL-SECRETS.md).
