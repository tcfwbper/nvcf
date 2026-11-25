<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# ui/src/api

The web UI's data layer: an HTTP client for nvsnap-server's `/api/v1`, the
WebSocket connection, and React hooks that the pages call.

## Used by

- The pages in [`../pages/`](../pages/).
- [`../components/Layout.tsx`](../components/Layout.tsx) polls
  `/api/v1/observability` through `fetchObservabilityTargets` for its
  sidebar links.
- It calls the routes registered in
  [`internal/server/server.go`](../../../internal/server/server.go). The
  `/blobstore/stats` and `/blobstore/captures` calls (used by the Blobstore
  page) have no route there.

## Key files

- `client.ts`: request functions and response types, base URL `/api/v1`.
- `hooks.ts`: React Query hooks over `client.ts` (`useNodes`,
  `useAuditLog`, ...).
- `websocket.ts`: a single WebSocket connection with reconnect and topic
  subscriptions.
- `ws-hooks.ts`: hooks over WebSocket topics (`useDemoStateWS`,
  `useDemoPodsWS`, `useDemoLogsWS`).
