# Inventory and documentation tooling

This Go module owns stack inventories, umbrella manifests and documentation
synchronization. Reuse the existing image-reference parser and inventory
extraction when adding image sources.

Run from the repository root:

```sh
go test -C tools/docs-version-sync ./...
go vet -C tools/docs-version-sync ./...
NVCF_IMAGE_CATALOG_E2E=1 go test -C tools/docs-version-sync -run '^(TestChartImageCoverage|TestChartImageCoverageReportsEachFailure|TestImageCatalogRepositoriesReuseNamesAcrossCharts)$' -count=1
NVCF_IMAGE_CATALOG_E2E=1 go test -C tools/docs-version-sync -run '^TestNATSAndOpenBaoChartImageGuards$' -count=1
```

`TestChartImageCoverageReportsEachFailure` runs the same evaluation on a
fixture repository, one chart per rule.

`TestChartImageCoverage` checks every tracked `Chart.yaml`. A chart either
declares `thirdPartyImages` or `ownedImages` in its `values.yaml` and passes
the chart image checks, or carries an entry in `chart-image-exclusions.yaml`.
Charts do not choose an image source. For an indexed chart the test requires:

- Every indexed image `registry` and `repository` in the chart `values.yaml` is
  written as `""`, and a `repository` never contains a registry host. Tag and
  digest may be prefilled.
- The chart's own templates contain no literal registry host in an image
  reference and no `default` with a literal registry or repository.
- A render with only the defaults fails with an error that names an indexed
  `registry` or `repository` path. Emptying any indexed `registry` or
  `repository` fails with an error that names its values path. Wrappers add
  this guard for images that an upstream subchart renders.
- Every CI values scenario in `tools/ci/helm-chart-values.yaml` supplies
  `registry` and `repository` for every indexed image. Each scenario renders
  only indexed images, and ordinary overrides layered on it reach every image.

The test downloads remote dependencies into test-owned directories. It does
not create a cluster or verify registry publication. Keep the chart probe and
override helpers test-only; installation uses native Helm values.

A chart that declares an image index cannot be listed as `test-fixture` or
`no-runtime-image`, so do not commit an indexed fixture chart. Build it in a
temporary tree with `writeChartFixtureTree`, as the matrix tests do.

When a pending-fix chart is fixed, remove its exclusion entry in the same Pull
Request. Image value shapes that differ from `registry`/`repository`/`tag`/
`digest` belong in `imageValueExceptions` in `chart_image_exceptions_test.go`.

## Indirect image convention

The scanner reads every `image` field. Other image references are visible
only through these registered patterns:

- `--<name>-image=<ref>` arguments
- the `AGENT_INJECT_VAULT_IMAGE` environment variable
- the `OTEL_COLLECTOR_IMAGE_REPO` and `OTEL_COLLECTOR_IMAGE_TAG` pair
- `BYOO_OTEL_COLLECTOR_CONTAINER` inside the base64 environment overrides
- `release-artifact-*-image` keys

Avoid indirect images. When one is necessary, use a registered pattern. A
change that introduces a new indirect pattern adds it to the scanner in
`resolved_inventory.go` in the same Pull Request. An image in any other form,
such as a ConfigMap string, is invisible to the chart image checks.
