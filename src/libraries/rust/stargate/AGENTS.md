# Stargate agent guide

This guidance covers the Rust workspace, including `stargate-k8s-router` and
the shared `stargate-forwarding` crate.

## Assumed deployment invariants

- `stargate-k8s-router` supports `raw-quic` and `webtransport` tunnel traffic.
  Plain `http3` tunnel traffic uses an L4 path and does not pass through this router.
- Raw QUIC request streams are bidirectional. During drain, prioritizing
  bidirectional acceptance over unidirectional acceptance is intentional.
- WebTransport uses a separate session bridge. Do not infer a plain HTTP/3
  stream fairness requirement for the raw QUIC relay from that implementation.

For router transport changes, inspect `crates/stargate-k8s-router/src/quic.rs`
and `crates/stargate-forwarding/src/lib.rs`. The
[transport guide](docs/tunnel-transports.md) describes available protocol
implementations and their routing paths.

## Build and verification

Run commands from this directory. For router and relay Rust changes:

```sh
cargo build --locked -p stargate-k8s-router
cargo test --locked -p stargate-k8s-router -p stargate-forwarding -- --test-threads=1
cargo clippy --locked -p stargate-k8s-router -p stargate-forwarding --all-targets -- -D warnings
cargo fmt --all -- --check
```

For other Rust changes, select the affected workspace packages with `-p`.
For documentation changes, check whitespace and link targets.

## Compatibility contract

Support every stable release published in the past 60 days, using UTC release
publication timestamps. For relevant changes targeting the default branch, CI
checks schemas against every supported release from the upstream GitHub release
inventory, including later backports and the oldest by publication date. Add
older versions still deployed or needed for rollback to `compatibility_retained_releases.json` before
they age out. Entries do not expire automatically; remove one only after verified
migration and closure of its rollback window. Missing release metadata fails CI.
The weekly release compatibility workflow lists releases leaving the window
within 14 days; check deployment inventory for each before it ages out.

Stargate, Pylon, registration relays, and gateways upgrade independently.
Preserve registration, discovery, tunnel, HTTP, stats, and configuration
contracts in both upgrade and rollback directions. Do not add exceptions to
the protobuf breaking-change check. Deprecate fields and RPCs while keeping
them functional; use a separately versioned protocol for incompatible changes.
Keep released command-line options and environment variables accepted. When
an option stops having an effect, keep it as a hidden option that logs a
deprecation warning.

An additive protobuf change can still break routing. Missing new telemetry
must retain the existing policy's behavior. Gate new requirements behind an
explicit opt-in or negotiated capability, with a tested legacy mode. Verify
that relays preserve the new fields before enabling a dependent policy.

Run these from this directory, with Buf installed:

```sh
scripts/check_buf_breaking.sh origin/main
python3 -m unittest discover -s scripts -p 'test_check_buf_breaking.py'
python3 -m unittest discover -s scripts -p 'test_select_*.py'
cargo test --locked -p stargate --test compatibility -- --test-threads=1
cargo test --locked -p stargate-proto -p stargate-protocol --lib released
cargo test --locked -p pylon -p stargate --bins released_cli
```

To run released binaries against the candidate, build both and run the matrix:

```sh
scripts/build_release_binaries.sh src/libraries/rust/stargate/vX.Y.Z /tmp/release-bin
cargo build --locked --release -p stargate -p pylon -p mock-dynamo --bins
mkdir -p /tmp/candidate-bin
cp target/release/{stargate,pylon,mock-dynamo} /tmp/candidate-bin/
scripts/release_matrix_e2e.sh --candidate /tmp/candidate-bin --release /tmp/release-bin
```

For maintenance work, use that maintenance branch instead of `origin/main`.

CI checks Stargate schemas against the target base on all branches. The full
release matrix runs only when targeting the default branch: an older maintenance
branch need not contain optional fields added in newer releases.
Gateway server and client copies are checked against the target base only;
historical gateway release coverage needs separate qualification.
The Bazel routing test uses frozen released stats fixtures on every branch.
Compatibility checks run separately from subtree detection and build jobs;
unrelated docs and Java changes do not contact release services. Extend the
compatibility test when changing a contract. Cover old-only, new-only, and mixed backends, absent optional stats,
and streaming completion. Require both backends to serve in the mixed state.
The test decodes released stats and re-encodes registrations with the current
client. Released registration and discovery frames must decode and re-encode
unchanged. Released tunnel frames must decode, and released tunnel schema
fields, tunnel strings, and command-line options must stay defined. The release
matrix runs released and candidate binaries together and rolls Stargate back;
relays need separate tests. Preserve released fixtures. Document the supported
policy modes, upgrade order, and how existing function launch
specifications and running Pylons receive an image update. An agent or chart
upgrade alone does not prove that running function Pylons changed.

## Mandatory Pull Request validation

Before marking a Pull Request ready or merging, run at least one local, AWS,
or CI mock integration test for every meaningful Stargate change. This covers
Stargate, router/relay, Pylon, mock engines, authentication, dependencies, image
pins, related charts, deployment settings, benchmark logic, and CI changes
that affect how these components build or run. Pure documentation and formatting
changes are exempt; explain the exemption in the Pull Request.

- Build the affected binaries or images from the candidate commit. Send real
  requests through the changed path with mock inference backends. Check
  registration/readiness, response correctness, completion, and streaming when
  applicable. Exercise the affected transport and routing policies.
- Choose a relevant loopback integration test such as
  `scripts/e2e-last-cluster.sh`, a local k3d deployment, an isolated AWS mock
  deployment, or a CI job that runs equivalent request traffic. See the
  [development stack](../../../../deploy/stacks/stargate-dev/README.md) for
  deployment, streaming checks, and benchmark campaigns.
- Unit tests, chart rendering, mocked kubectl, microbenchmarks, and readiness
  checks alone do not satisfy this integration gate. A CI job must exercise
  the affected runtime path; a green build alone does not count.
- Follow the root local QA safety rules. Give runs unique resources and
  artifacts. Serialize campaigns that share backends or mutate deployment
  state; separate output directories do not isolate caches, queues, or resets.
- Record the tested commit, commands, environment, image digests, policies,
  workload, acceptance results, and retained artifact links in the Pull Request.
  Retest after changes that alter the validated behavior. Blocked or failed
  validation keeps the Pull Request unready and blocks merge.

## Performance regression gate

For changes that can affect request handling, routing, transport, caching,
concurrency, scheduling, resource limits, or performance measurements, compare
the merge-base baseline and candidate on the same test environment before
marking the Pull Request ready or merging.

Scope the comparison before running it:

- Compare baseline and candidate image digests and runtime settings for each
  component on the measured path. When both match, record them and skip the
  comparison for that component; identical builds measure only noise.
- Benchmark only the components that differ. For example, a change limited to
  charts and the auth fixture needs the integration gate plus an auth RPC
  benchmark, not a routing comparison.
- Select the policies and workload profiles that exercise the changed path.
  Do not run the full policy and profile matrix by default.

Run the comparison:

- Use release builds and identical hardware, resource allocation, topology,
  mock-engine settings, policy, load-generator version, workload/seed, offered
  load, concurrency, cache state, warmup, and run duration. Change only the
  component or setting under test. Exclude unrelated traffic and overlapping
  campaigns. Local and AWS measurements must be compared within their own
  environments.
- Run at least three measured repetitions per revision for each selected
  profile, alternate revision order, and report individual results and
  variation. Use representative sustained load. A short smoke run alone is
  insufficient.
- Compare successful throughput, p95/p99 time to first token and end-to-end
  latency, failures/timeouts/retries, and CPU/memory where relevant. Include
  admission failures and cancellations, not only completed-request latency.
- Retain raw baseline and candidate reports and a comparison table. Algorithm
  comparisons within one build do not establish revision-level performance.
  Explain unavailable metrics and collect another observable for that risk.

Judge the results:

- Treat a candidate metric as worse only when it falls outside the baseline
  range across repetitions and moves in the same direction in every pair.
  Report smaller or mixed differences as within run-to-run variation.
- Policies that sample backends without a fixed seed, such as PowerOfN, choose
  different backends for the same workload on each run. Note this in the
  report; their tail variation alone does not establish a regression.
- Investigate any result that meets the worse-result rule. Any reproducible
  degradation blocks merge. For an inconclusive profile, run at most one
  additional reversed-order pair, then report the result and ask the user to
  decide instead of collecting more runs.
- An intentional tradeoff requires explicit user approval with the measured
  impact before merge. Mock results support the tested workload, not
  production capacity claims.

## Routing methods

`routing_methods.json` lists every load-balancing algorithm name, alias, and
routing-expression parameter. When you add, rename, or remove one, update that
file and run `cargo test --locked -p stargate --lib routing_method`. A failure
names the other code, docs, and configs to update.

The function API in `src/control-plane-services/cloud-functions/nvcf-core`
stores `routingMethod` after a format check only. Its
`LlmRoutingMethodValidatorTest` reads the manifest and fails if that check
would reject a listed name, alias, or parameter. Run it from the repository
root:

```sh
bazel test //src/control-plane-services/cloud-functions/nvcf-core:tests \
  --test_arg=--exclude-classname='^(?!com\.nvidia\.nvcf\.rest\.function\.management\.dto\.LlmRoutingMethodValidatorTest$).*$'
```

## LLM routing BDD

The `@llm-function-type` scenarios in the single- and multi-cluster Helmfile
features cover LLM routing and request handling. See `tests/bdd/README.md`
to run them.

## Code style

Use Rust 2024 and the workspace formatter and Clippy rules. Match existing
crate structure and use `tracing` for structured logs. Keep deployment
assumptions explicit when reviewing transport behavior.
