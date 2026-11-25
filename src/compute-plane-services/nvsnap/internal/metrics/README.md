<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/metrics

Prometheus metrics for the agent, the webhook and nvsnap-server: the
collectors, the `/metrics` handler, and helpers that keep label values
bounded.

## Used by

- [`internal/agent`](../agent/): `agent.go` serves `/metrics` (`Handler`);
  capture, restore and model-cache code records outcomes and phases
  (`ObserveCapture`, `ObserveCapturePhase`, `CountModelCacheOp`, ...).
- [`internal/webhook`](../webhook/): admission decisions (`CountAdmission`,
  `AdmitCold`, `AdmitRestore`).
- [`internal/server`](../server/): `server.go` (`RegisterServer`,
  `InstrumentRoute`, `Handler`) and WebSocket connection counts.

## Key files

- `metrics.go`: collectors, `RegisterAgent`, `RegisterServer`, `Handler`,
  `InstrumentRoute`.
- `lifecycle.go`: capture and restore outcome, phase and failure-reason
  metrics.
- `first_sight.go`: creates a labelled series at zero and holds its first
  observation until Prometheus has scraped the zero.

## Configuration

Scraping and alerting are set by the chart: `agent.metrics.serviceMonitor`,
`agent.metrics.prometheusRule` (both `enabled: auto`) and
`agent.metrics.dashboard.enabled` (default `false`).

See [docs/operate/METRICS.md](../../docs/operate/METRICS.md).
