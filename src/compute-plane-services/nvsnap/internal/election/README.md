<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/election

Picks one downloader per cache hash when many model workers start at once.
The webhook elects through a Kubernetes Lease create, which only one
admission per hash wins; the winner becomes the capture source and the
others become gated pods that nvsnap-server releases later.

## Used by

- [`internal/webhook`](../webhook/): `election.go` (`Elect`, `ArchPin`) and
  `PodIdentity` in several files.
- [`internal/server`](../server/): `election_release.go` (`LeaseName`,
  `ResolveLeader`) removes the gates or evicts the followers.
- [`internal/agent`](../agent/): `l2_integration.go` builds the
  `LeaseElector` and `NodeArchScope` that the webhook
  (`webhook_integration.go`) and the capture watcher
  (`rootfsonly_integration.go`) use; `companion_tools.go` calls `Admits`.

## Key files

- `election.go`: the Lease election, labels and annotations.
- `scope.go`: the compute-capability node selector that keeps a hash's pods
  on one GPU architecture.

## Configuration

Helm `agent.election.enabled` (default `false`) and
`agent.election.deadline` (default `60m`), passed as `--election` and
`--election-deadline`.

See [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md).
