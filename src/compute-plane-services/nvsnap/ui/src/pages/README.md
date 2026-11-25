<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# ui/src/pages

One React component per route of the web UI, wired in
[`../App.tsx`](../App.tsx). The pages read their data through
[`../api/`](../api/): the hooks, and in `Demo.tsx` also `client.ts`
functions directly. Two requests bypass `../api/` and call the server with
Axios: `CheckpointDetail.tsx` (`/api/v1/checkpoints/:id/files`) and
`Demo.tsx` (`/api/v1/demo/pods`).

## Key files

- `Demo.tsx` (`/demo`, the start page): deploy a demo workload, run
  inference, checkpoint, restore and scale out, through `/api/v1/demo/*`.
- `Dashboard.tsx` (`/dashboard`), `Checkpoints.tsx` (`/checkpoints`),
  `CheckpointDetail.tsx` (`/checkpoints/:id`), `Restores.tsx` (`/restores`),
  `Nodes.tsx` (`/nodes`), `Policies.tsx` (`/policies`, retention policies).
- `Events.tsx` (`/events`): the audit log. `Settings.tsx` (`/settings`):
  server health and recent audit entries.
- `Clusters.tsx` (`/clusters`): built from the node list.
- `Blobstore.tsx` (`/blobstore`): calls `/api/v1/blobstore/*`, which
  nvsnap-server does not serve.
