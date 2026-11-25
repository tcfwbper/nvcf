# Compatibility fixtures from nvsnap v0.3.2

Fixtures recorded from the released agent, tag
`src/compute-plane-services/nvsnap/v0.3.2` (commit 027b8fc890dd, 2026-11-23),
by a test run in that tree. Do not regenerate them from a newer agent.

- `driver-checkpoint-request.json`: the request that agent's group checkpoint
  driver posts to a member for a leave-running capture (`groupCheckpoint`,
  first member). It has no `gpushareSyncOpen`.
- `member-fabric-state.json`: the fabric state that agent's member serves to
  a driver (`fabricState`). It has no `holdsOpen` and no `resumed`.
