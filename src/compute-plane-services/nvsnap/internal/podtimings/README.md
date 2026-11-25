<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/podtimings

Classifies pods as cold-start or restored (by the `nvsnap.io/restore-from`
annotation) and computes their start-up latency from pod timestamps, for
display as a table or JSON.

## Used by

- [`cmd/nvsnap`](../../cmd/nvsnap/): `pods.go` (`nvsnap pods`) calls
  `Compute`, `RenderTable` and `RenderJSON`.

## Key files

- `podtimings.go`: classification and per-pod timings (`Compute`).
- `summary.go`: overall and per-function-version statistics.
- `render.go`: table and JSON output.
