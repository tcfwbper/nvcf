# NVCF 1.1.1

NVCF 1.1.1 is a patch release. It combines the following stack releases:

| Stack | Version |
| --- | --- |
| Self-Managed | 1.1.9 |
| Compute Plane | 1.1.1 |
| Observability | 1.1.0 |

## What is new

- Fixes the emSign CA issue (CVE-2026-111943).
- Adds the optional worker callback proxy for multi-cluster installs. It is off by default.
- Moves OpenBao to a versioned JWT plugin lifecycle.
- Includes high availability resilience improvements (HA phase 2).
- Rebuilds component images with Go 1.26.10 and the patched CA bundle.

## Download and install

Download the attached `nvcf-1.1.1.tar.gz`, `manifest.json`, and
`release-assets.json`. The asset index records SHA256 digests. Use the packaged
umbrella archive rather than GitHub's automatically generated source archives.

Extract into a new persistent directory and retain the sibling directories
under `deploy/stacks/`. Use their existing Helmfile/Make installation entrypoints
with separate environment files, secrets, and explicit Kubernetes contexts.
The recorded toolchain is Helm 3.21.4 and Helmfile 1.1.9.

## Upgrade to NVCF 1.1.1

NVCF 1.0.4 is the upgrade stop for every customer on a 1.0.x release.

- NVCF 1.0.1, 1.0.2 or 1.0.3: upgrade to NVCF 1.0.4 first, then use the
  1.0.4 to 1.1.1 guide.
- NVCF 1.0.4: use the
  [1.0.4 to 1.1.1 upgrade guide](https://github.com/dsx-ai-factory/nvcf/blob/517ebf322e217adfb274a34ea5c5ed03ff74b34e/docs/overview/release-notes/1.0.4-to-1.1.1-upgrade.md).
- NVCF 1.1.0: use the
  [1.1.0 to 1.1.1 upgrade guide](https://github.com/dsx-ai-factory/nvcf/blob/517ebf322e217adfb274a34ea5c5ed03ff74b34e/docs/overview/release-notes/1.1.0-to-1.1.1-upgrade.md).

Do not upgrade from a 1.0.x release other than 1.0.4 straight to 1.1.1. Both
guides were validated on single-cluster and multi-cluster AWS deployments.
Preserve backups and follow the documented rollback limits; replacing the
archive does not reverse database migrations.

## Observability on multi-cluster AWS

Observability is supported on a single cluster and on the control-plane cluster
of a split deployment. Collecting compute-plane metrics across clusters in a
multi-cluster AWS deployment is experimental and has not been qualified.

## Documentation

The maintained 1.1.1 documentation edition is activated separately. Reviewed
documentation corrections can be published under the same 1.1.1 edition without
changing release assets.
