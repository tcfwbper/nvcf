# NATS wrapper chart

Do not choose an image source in this chart. Image `registry`,
`repository` and `fullImageName`, and `nats.global.image.registry`, belong in
the stack `base.yaml` or a values file; the chart is pending-fix until its
prefilled sources move there and the wrapper guards them. The catalog index
is for CI; Helm consumes the referenced settings directly. Preserve server,
reloader and box overrides and auth-callout secrets.

Run from the repository root. With a fresh Helm configuration, first register
the repository from `Chart.lock` and download the chart dependency:

```sh
helm repo add nats https://nats-io.github.io/k8s/helm/charts/
make -C deploy/helm/nats deps
make -C deploy/helm/nats test
NVCF_IMAGE_CATALOG_E2E=1 go test -C tools/docs-version-sync -run '^TestChartImageCoverage$' -count=1
NVCF_IMAGE_CATALOG_E2E=1 go test -C tools/docs-version-sync -run '^TestNATSAndOpenBaoChartImageGuards$' -count=1
```

The coverage test checks every tracked chart, including this one, with the
CI values scenario `tools/ci/helm-validate-values/nats.yaml`. The guards test
covers native NATS `fullImageName` and digest overrides. Both render without
a cluster.
Follow root QA guidance before live installation.
