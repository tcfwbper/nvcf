<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->
# cmd/

Entry points (`main` packages). One directory per binary.

## Binaries

- [`agent/`](agent/): `nvsnap-agent`, the node DaemonSet. It also serves
  the mutating admission webhook and runs as a helper subcommand
  (`pidns-dump-exec`, `pidns-restore-exec`, `placeholder-files`, and
  `capture-write`, kept for writer Jobs from older agents).
- [`nvsnap-server/`](nvsnap-server/): REST API, web UI and checkpoint catalog.
- [`nvsnap/`](nvsnap/): the `nvsnap` CLI.
- [`nvsnap-rootfs-restore/`](nvsnap-rootfs-restore/): the command the webhook
  puts in front of a workload for a rootfs restore: builds the overlay,
  pivots into it and execs the workload. Its `NVSNAP_NO_OVERLAY` mode is a
  legacy cachedir path the webhook no longer sets.
- [`nvsnap-mount-prep/`](nvsnap-mount-prep/): init container that asks the
  agent to prepare restore mounts and waits for them.
- [`nvsnap-l2-wait/`](nvsnap-l2-wait/): init container that waits until a
  capture's L2 volume is ready.
- [`nvsnap-helper/`](nvsnap-helper/): static helper that runs the steps the
  webhook adds to a pod (`skip`, `wait`, `write`, `seed`, `model-cache`,
  `prewarm`, `await-pid-floor`) in any image.
- [`nvsnap-idle/`](nvsnap-idle/): static C program that keeps a CRIU restore
  placeholder's container alive until the agent restores into it.

## Rules

Keep `main` thin — wiring and flag parsing only. Logic lives in
[`../internal/`](../internal/) (private) or [`../pkg/`](../pkg/) (importable).
