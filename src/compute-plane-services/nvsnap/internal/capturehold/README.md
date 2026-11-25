<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/capturehold

Helpers for the readiness gate `nvsnap.io/capture-settled` (`Condition`),
which keeps a pod that nvsnap may CRIU-capture out of service until the
capture is over or does not apply. The package only reads pod conditions;
the webhook adds the gate and the agent sets it.

## Used by

- [`internal/webhook`](../webhook/): `capture_hold.go` adds the gate
  (`HasGate`).
- [`internal/agent`](../agent/): `capture_hold.go` sets it (`Held`, `Warm`,
  `Settled`, `Get`, `WarmSince`); `modelvolume_controller.go` reads it.
- [`internal/rootfsonly`](../rootfsonly/): `watcher.go` uses `Ready`.

## Key files

- `capturehold.go`: the condition name and the predicates on a pod.

## Configuration

Helm `webhook.captureHold` (default `true`), passed to the agent as
`--webhook-capture-hold`.
