# Repository Scripts

The `scripts/` directory contains supported repository checks, local workflow
helpers, and their Python unit tests.

## Manual Python Test Suite

Run all Python support-script tests with:

```bash
python3 -m unittest discover -s scripts -p 'test_*.py'
```

This suite is intentionally manual. Individual repository gates run the focused
test modules they own, such as the coverage-policy tests in
`scripts/check_coverage_quality.sh`.

Use the discovery command after changing Python support scripts, their tests,
Buildkite quality checks, Kubernetes integration helpers, benchmark profiling
helpers, or PR reporting utilities.

## Tilt Lifecycle

Use the Make targets rather than calling `tilt up` directly:

```bash
make tilt-up-kind
make tilt-up-docker-desktop
make tilt-up
```

The generic `make tilt-up` target snapshots the current kubectl context before
starting Tilt, so teardown remains pinned to the same cluster.

Run the CI integration suite through the same lifecycle wrapper:

```bash
python3 scripts/run_tilt.py ci --context kind-kind --timeout 30m
```

The runner supports `up` and `ci`. When Tilt exits successfully, fails, or is
interrupted, it invokes `tilt down --delete-namespaces`, preserving compatible
context, namespace, Tiltfile, and Tiltfile arguments, then removes the matching
CoreDNS rewrite. This removes the Kubernetes resources and cluster-wide DNS
side effect owned by that Tilt instance. The selected context is also applied
to kubectl calls made by the local integration and manifest-rendering helpers.

## PR Checks

The Bazel workflow runs protobuf compatibility in a separate path-selected job.
Its result is required when selected, while other build rows run independently.
Runtime compatibility tests use frozen released fixtures in the Stargate row.

Run the support-script tests after changing repository tooling:

```bash
python3 -m unittest discover -s scripts -p 'test_*.py'
```

## Protocol compatibility

With Buf on `PATH`, run `scripts/check_buf_breaking.sh origin/main` from this
directory's parent. It checks the Stargate registration and gateway client
protobuf APIs against the supplied Git
base with strict `FILE` rules and no deletion exceptions. Candidate Buf
configuration cannot disable the check. CI supplies the pull request or
merge queue base, or the previous commit for a push. CI also checks only
`llm_gateway.proto` in `nvcf-core` and the client copy in `llm-api-gateway` against that base. The adjacent `nvcf.proto` is outside
this gate. Historical gateway releases have no release matrix in this gate;
existing drift between copies is not reconciled by these checks.

For relevant changes targeting the default branch, Stargate schemas are compared
against every supported stable Stargate release, including backports and the
oldest release. Maintenance branches keep target-base checks without comparing
their older schemas against fields introduced in newer releases. Releases are
selected by UTC publication date from the upstream GitHub release inventory.
The rolling support window is 60 days. Older versions
still deployed or needed for rollback must remain in
`compatibility_retained_releases.json`; remove them only after verified migration
and closure of the rollback window. Missing inventory or retained-release
metadata fails CI. The selected tag, publication date, and cutoff appear in the
job log, along with the full supported tag list.
`scripts/check_supported_release_schemas.sh` selects the releases and runs these
checks for both the Bazel workflow and the release compatibility workflow.

`cargo test --locked -p stargate --test compatibility -- --test-threads=1`
decodes released stats values, then re-encodes them with the current client for
gRPC registration and HTTP/QUIC streaming.
The frozen v0.8.0 and v0.25.0 fixtures run locally and in CI. There is no
release-service dependency or generated payload passed to the runtime test.
It checks power-of-n, both Pulsar defaults, and their explicit mean-based modes
with old-only, mixed, and new-only backends, then restores the older backend
and withdraws the newer one. Both backends must serve requests in the mixed
state. An absent aggregate maximum falls back to the positive finite last mean.
Cluster aggregation treats missing or invalid backend maxima as absent. This
also covers current Pylons that have not established a maximum.

This checks released stats values with current registration envelopes and
transports. Restoring an older backend is a producer replacement scenario.

## Released Contract Fixtures

Frozen fixtures from the oldest supported release (v0.19.2) run as unit tests:

```bash
cargo test --locked -p stargate-proto -p stargate-protocol --lib released
cargo test --locked -p pylon -p stargate --bins released_cli
```

- `crates/proto/src/released_wire_fixtures.rs`: a Pylon registration, a
  registration ACK, and a `WatchStargates` response. Each must decode with
  every value intact and re-encode to the released bytes.
- `crates/protocol/src/released_contract_fixtures.rs`: reverse tunnel
  handshakes and raw QUIC header, body, and trailer frames must decode.
  Released `quic.capnp` fields must keep their struct, name, ordinal, and type.
  Released tunnel header names, retry reasons, and paths must stay defined, and
  the required tunnel request headers must not change.
- `released_cli_options_remain_accepted` in each binary: every released
  command-line option and environment variable must still be accepted. Keep a
  removed option as a hidden option that logs a deprecation warning.

Do not regenerate fixtures from the candidate. Generate a newer release's
fixtures by building that tag and encoding the same values; keep the older
fixtures until that release leaves the window and no retained release needs it.

## Release Compatibility Workflow

`.github/workflows/stargate-release-compatibility.yml` runs weekly, on manual
dispatch, and on pull requests to `main` that change the protobuf or tunnel
schemas, the Stargate or Pylon command line, the retained-release list, or this
tooling. It:

1. Checks schemas against every supported release and writes the selection to
   the job summary. Releases leaving the window within 14 days appear as
   warnings. Add any that are still deployed or needed for rollback to
   `compatibility_retained_releases.json`.
2. Selects the release matrix with `scripts/select_matrix_releases.py`: the
   first supported release for each distinct `stargate.proto`, `quic.capnp`,
   and `tunnel_contract.rs` combination, plus the newest supported release.
   Backports with their own contract are included.
3. Builds each selected release from its own tag with
   `scripts/build_release_binaries.sh`, cached by release commit, and runs
   `scripts/release_matrix_e2e.sh` against candidate binaries.

The matrix script starts one released and one candidate Pylon behind mock-dynamo
backends over reverse raw QUIC tunnels. For power-of-n and the Pulsar default,
it runs released Stargate, upgrades to candidate Stargate, then rolls back to
released Stargate. Each phase requires both Pylons to serve completed streaming
responses. The same launch options start both Pylon versions.

```bash
scripts/build_release_binaries.sh src/libraries/rust/stargate/vX.Y.Z /tmp/release-bin
scripts/release_matrix_e2e.sh --candidate /tmp/candidate-bin --release /tmp/release-bin
```

The matrix does not cover the `http3` or WebTransport tunnel transports, direct
backend connectivity, worker authentication, router relays, or production
rolling upgrades.

## Last-Cluster End-to-End Check

`scripts/e2e-last-cluster.sh` runs the LLM API gateway, Stargate, two Pylons,
and two mock-dynamo backends on loopback and checks that
`last_cluster_affinity` keeps a returning session on its last cluster after
overflow.

```bash
scripts/e2e-last-cluster.sh
```

Run `scripts/e2e-last-cluster.sh --help` for the scenario, options, and
environment variables.
