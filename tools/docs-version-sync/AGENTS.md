# Inventory and documentation tooling

This Go module owns stack inventories, umbrella manifests and documentation
synchronization. Reuse the existing image-reference parser and inventory
extraction when adding image sources.

Run from the repository root:

```sh
go test -C tools/docs-version-sync ./...
go vet -C tools/docs-version-sync ./...
NVCF_IMAGE_CATALOG_E2E=1 go test -C tools/docs-version-sync -run '^(TestChartImageCoverage|TestChartImageCoverageReportsEachFailure|TestImageCatalogRepositoriesReuseNamesAcrossCharts)$' -count=1
```

`TestChartImageCoverageReportsEachFailure` runs the same evaluation on a
fixture repository, one chart per failure path.

`TestChartImageCoverage` checks every tracked `Chart.yaml`. A chart either
declares `thirdPartyImages` or `ownedImages` in its `values.yaml` and passes
image coverage, or carries an entry in `chart-image-exclusions.yaml`. It
renders defaults, the CI values scenarios in `tools/ci/helm-chart-values.yaml`
and ordinary Helm overrides. It downloads remote dependencies of covered
charts into test-owned directories. It does not create a cluster or verify
registry publication. Keep the chart probe and override helpers test-only;
installation uses native Helm values.

A chart that declares an image index cannot be listed as `test-fixture` or
`no-runtime-image`, so do not commit an indexed fixture chart. Build it in a
temporary tree with `writeChartFixtureTree`, as the matrix tests do.

When a pending-fix chart is fixed, remove its exclusion entry in the same Pull
Request. Image value shapes that differ from `registry`/`repository`/`tag`/
`digest` belong in `imageValueFormats` in `chart_image_formats_test.go`.

## Indirect images

On this maintenance branch the scanner reads only `image` fields. Indirect
image references, such as `--<name>-image=<ref>` arguments or the
`AGENT_INJECT_VAULT_IMAGE` environment variable, are not scanned. The covered
Cassandra chart uses none. Charts that need indirect scanning stay
`pending-fix` here.
