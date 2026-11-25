<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/tracing

OpenTelemetry set-up for nvsnap binaries. `Init` installs a tracer provider
that exports spans over OTLP/gRPC when `OTEL_EXPORTER_OTLP_ENDPOINT` is set,
and drops them when it is not. `Tracer` returns the named tracer callers
start spans from.

## Used by

- `Init`: [`cmd/nvsnap-server/main.go`](../../cmd/nvsnap-server/main.go) and
  [`internal/agent/agent.go`](../agent/agent.go).
- `Tracer`: [`internal/agent`](../agent/), [`internal/checkpointstore`](../checkpointstore/),
  [`internal/rootfsonly`](../rootfsonly/), [`internal/treecopy`](../treecopy/)
  and [`internal/webhook`](../webhook/).

## Key files

- `tracing.go`: `Init` and `Tracer`.

## Configuration

The chart sets `OTEL_EXPORTER_OTLP_ENDPOINT` on the agent and server when
`jaeger.enabled` is `true` (default `false`).
