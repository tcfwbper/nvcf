<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->
# pkg/

Importable Go packages — the public surface other modules may depend on.
Everything else is repo-private under [`../internal/`](../internal/).

Nothing in this repository imports these packages, and no binary links them.

## Package map

- [`discovery/`](discovery/) — container information (rootfs, spec) resolved
  through the containerd client.
- [`introspection/`](introspection/) — container introspection via `/proc`:
  mounts, namespaces and rootfs.
- [`metadata/`](metadata/) — checkpoint metadata types shared across producers
  and consumers.

## Rules

Keep the API stable and minimal: these are the packages meant for import. If something is
only used inside nvsnap, put it in `internal/` instead.
