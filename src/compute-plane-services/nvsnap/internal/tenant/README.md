<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/tenant

Names the tenant a namespace belongs to. A capture or cache is shared only
within one tenant, so the tenant is part of every content hash.
`Resolver.Scope` returns `tenant:<label>=<value>` from a configured
namespace label, or `namespace:<ns>` when the label is unset, absent or
unreadable.

## Used by

- [`internal/agent`](../agent/): `agent.go` builds the `Resolver`
  (`bindTenants`, also called from `node_model_cache.go`) and calls `Scope`
  (`tenantScope`).

## Key files

- `tenant.go`: `Resolver` and `Scope`.

## Configuration

Helm `agent.tenantLabel` (default `""`: every namespace is its own tenant),
passed as `--tenant-label`.
