# Working on nvsnap

Rules for anyone changing this tree, people and coding agents alike. The
root `AGENTS.md` applies too: commit messages, pull requests and public
content hygiene.

## Build and test

From `src/compute-plane-services/nvsnap`:

```bash
go build ./...
make test
go test -race ./internal/agent/ ./internal/webhook/
golangci-lint run --new-from-rev origin/main ./...
make check-docs
```

From the repository root, after adding or removing Go files:

```bash
bazel run //:gazelle -- src/compute-plane-services/nvsnap
bazel test //src/compute-plane-services/nvsnap/internal/agent:agent_test
```

`internal/agent/rbac_test.go` checks that the agent's ClusterRole in
`deploy/helm/nvsnap/templates/agent-rbac.yaml` allows every typed client call
the agent makes. Add the rule in the same change as the call.

## Capture and restore changes

These rules apply to any change that touches how an instance is captured,
admitted, held, frozen, dumped, restored or released: `internal/agent`
(checkpoint, restore, group, freeze, hold, companion, gpushare, CRIU files),
`internal/webhook` (placeholder admission), `docker/agent/gpushare`, and the
agent RBAC.

### Invariants

Every such change keeps these. Cite them by number in reviews.

1. Nothing a running engine depends on stops before the engine does.
   Companions (etcd, NATS, frontends) are held only once every GPU rank is
   suspended (`docs/design/STOP-THE-WORLD-CAPTURE.md`). An engine that loses
   a lease during a capture is saved while shutting down, and its restore
   finishes the shutdown.
2. One restore per pod. A member, companions included, is restored by
   exactly one group restore. Two leaders never share a member.
3. A capture restores only into an instance of the same shape: the same
   ranks, companions and replica count. Identical pod specs do not make two
   instances the same shape.
4. Admission does not depend on the order in which pods are deleted or
   recreated, or on API server latency. Test the bad order.
5. Fail closed. When the agent cannot decide (an API read fails, a member is
   missing, a count is unknown), the pod starts cold. A cold start is slow; a
   wrong restore is an outage.
6. Restored means serving. A restore returning does not prove it. The
   agent's automatic check after a capture only watches the restored pods
   stay ready; release validation (below) checks that the restored engine
   answers requests and keeps its leases.
7. Every hold, freeze, lock and claim is released or expires on every path:
   success, abort, timeout, cancellation and agent restart.

### Before coding

- Write the failing test first. The pull request shows it failing on the
  code before the change.
- Test the wiring, not only the helper. A helper test alone does not prove
  the change is used: add a wiring test that drives the production call
  site, and show it fails when that call site is removed or reordered. Show
  that a concurrency test fails without the lock, claim or precondition it
  is meant to prove.
- When a change alters what counts as an instance or a member, or the order
  of capture or restore steps, update the design doc under `docs/design/`
  first and get it reviewed.

### Adversarial review

Required before a capture or restore change is marked ready:

1. A reviewer who did not write the change (another agent, model or person)
   reviews it adversarially. Give the reviewer the diff, the goal and the
   invariants above, not your own conclusions.
2. Ask the reviewer to break the change, invariant by invariant: bad
   deletion orders, partial failures, timeouts, concurrent leaders, several
   replicas sharing companions, tens of ranks, slow or restarting agents,
   and clusters without optional APIs.
3. Fix each confirmed finding with a test, or answer it in the pull request
   with the reason it does not apply.
4. Summarize the review and its outcome in the pull request description.

### Validation before release

Unit tests do not prove capture and restore. Before releasing a build that
changes either:

- Run the group capture and restore conformance scenarios on a multi-node
  GPU cluster, including the lease scenario: an engine that stops when its
  companion stops answering. A fix for a regression shows the scenario
  failing on the build before the fix and passing on the fix.
- Capture a multi-node instance of a production-sized model, then run at
  least five restore cycles in place and one restore into a new instance.
  Each cycle ends with zero restarts after a soak and a correct answer.
- Cite the exact build with every result. A result from another build does
  not validate this one.
- In a bug fix, name the change that introduced the bug.
