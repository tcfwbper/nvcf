<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/rootfsonly

The capture side of the rootfs and cachedir paths, which save a pod's files
but no process state. A watcher on each agent sees warm pods labelled
`nvsnap.io/capture` on its node; the orchestrator copies the pod's overlay
upperdir (or its cache directory) and its classified data volumes into a
[`checkpointstore`](../checkpointstore/) backend under the pod's content
hash.

## Used by

- [`internal/agent/rootfsonly_integration.go`](../agent/rootfsonly_integration.go):
  builds the `Capturer`, `Watcher` and `HashInputComposer`
  (`NewPIDResolver`, ...).
- [`internal/agent`](../agent/) and [`internal/webhook`](../webhook/): helpers
  such as `IsPodReady` and `IsModelWorkload`.

## Key files

- `watcher.go`: `Watcher`, the per-node pod informer, and
  `DefaultCaptureLabel`.
- `orchestrator.go`: `Capturer` and `Capture`, which copy the tree.
- `composer.go`: `HashInputComposer`, which derives a pod's hash input.
- `classify.go`: which volumes are captured.
- `enumerate.go`: the directories worth mounting at restore.
- `pidresolver.go`: finds the workload process through its cgroup.

## Configuration

The chart always passes `--rootfs-capture`; Helm `agent.podCacheDir`
(default `/opt/nvsnap`) selects cachedir mode, and `agent.captureMethod`
(default `cachedir`) the default capture path.

See [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md).
