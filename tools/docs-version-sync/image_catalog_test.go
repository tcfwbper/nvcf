// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// imageCatalogFixture builds a snapshot whose chart declares the nats and
// cert-manager dependencies, with a synthetic cert-manager appVersion.
func imageCatalogFixture(t *testing.T, values string) imageChartSnapshot {
	t.Helper()
	var tree map[string]any
	if err := yaml.Unmarshal([]byte(values), &tree); err != nil {
		t.Fatal(err)
	}
	return imageChartSnapshot{
		Values:       tree,
		AppVersions:  map[string]string{"cert-manager": "v8.2.1"},
		Dependencies: map[string]bool{"nats": true, "cert-manager": true},
	}
}

var fixtureDigest = "sha256:" + strings.Repeat("a", 64)

// TestImageFormatResolution exercises each format table entry's precedence.
func TestImageFormatResolution(t *testing.T) {
	for _, test := range []struct {
		name   string
		values string
		want   string
	}{
		{
			name:   "standard registry, repository and tag",
			values: "ownedImages: {server: server.image}\nserver:\n  image: {registry: registry.example:5000, repository: team/server, tag: v2}\n",
			want:   "registry.example:5000/team/server:v2",
		},
		{
			name:   "standard tag and digest",
			values: "ownedImages: {server: server.image}\nserver:\n  image: {registry: r.example, repository: server, tag: v2, digest: " + fixtureDigest + "}\n",
			want:   "r.example/server:v2@" + fixtureDigest,
		},
		{
			name:   "nats full reference wins",
			values: "thirdPartyImages: {server: nats.container.image}\nnats:\n  container:\n    image: {registry: stale.example, repository: ignored, tag: stale, digest: " + fixtureDigest + ", fullImageName: selected.example/nats:custom}\n",
			want:   "selected.example/nats:custom",
		},
		{
			name:   "nats digest replaces tag",
			values: "thirdPartyImages: {server: nats.container.image}\nnats:\n  container:\n    image: {registry: selected.example, repository: nats, tag: stale, digest: " + fixtureDigest + "}\n",
			want:   "selected.example/nats@" + fixtureDigest,
		},
		{
			name:   "cert-manager inherits appVersion",
			values: "thirdPartyImages: {controller: cert-manager.image}\ncert-manager:\n  imageRegistry: quay.io\n  imageNamespace: jetstack\n  image: {name: controller, tag: \"\"}\n",
			want:   "quay.io/jetstack/controller:v8.2.1",
		},
		{
			name:   "cert-manager tag and digest together",
			values: "thirdPartyImages: {controller: cert-manager.image}\ncert-manager:\n  imageRegistry: selected.example\n  imageNamespace: team\n  image: {name: controller, tag: fixture, digest: " + fixtureDigest + "}\n",
			want:   "selected.example/team/controller:fixture@" + fixtureDigest,
		},
		{
			name:   "cert-manager explicit repository with empty shared registry",
			values: "thirdPartyImages: {controller: cert-manager.image}\ncert-manager:\n  imageRegistry: \"\"\n  image: {name: controller, repository: override.example/specific/image}\n",
			want:   "override.example/specific/image:v8.2.1",
		},
		{
			name:   "cert-manager image.registry prefixes the repository",
			values: "thirdPartyImages: {controller: cert-manager.image}\ncert-manager:\n  imageRegistry: quay.io\n  image: {registry: legacy.example, repository: team/controller, tag: v1}\n",
			want:   "legacy.example/team/controller:v1",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			images, err := resolveChartImageCatalog(imageCatalogFixture(t, test.values))
			if err != nil {
				t.Fatal(err)
			}
			if len(images) != 1 || images[0].Reference != test.want {
				t.Fatalf("got %+v, want %s", images, test.want)
			}
		})
	}
}

// TestImageFormatOverridesRoundTrip merges each format's override fields over
// a fixture default the way Helm coalesces an ordinary values file (fields the
// override omits keep their defaults) and requires the format to resolve the
// target, so reading and writing a format cannot drift apart. Defaults that
// set a digest or a full reference must not survive the override: a kept
// digest would render "<tag>@<new>@<old>", and a kept NATS fullImageName or
// digest would still take precedence.
func TestImageFormatOverridesRoundTrip(t *testing.T) {
	oldDigest := "sha256:" + strings.Repeat("c", 64)
	for _, test := range []struct {
		name, format, path, defaults string
	}{
		{"standard tag", "standard", "server.image", "server:\n  image: {registry: r.example, repository: server, tag: v1}\n"},
		{"standard digest only", "standard", "server.image", "server:\n  image: {registry: r.example, repository: server, digest: " + oldDigest + "}\n"},
		{"standard tag and digest", "standard", "server.image", "server:\n  image: {registry: r.example, repository: server, tag: v1, digest: " + oldDigest + "}\n"},
		{"nats tag", "nats", "nats.container.image", "nats:\n  container:\n    image: {registry: r.example, repository: nats, tag: v1}\n"},
		{"nats digest", "nats", "nats.container.image", "nats:\n  container:\n    image: {registry: r.example, repository: nats, tag: v1, digest: " + oldDigest + "}\n"},
		{"nats full reference", "nats", "nats.container.image", "nats:\n  container:\n    image: {registry: r.example, repository: nats, tag: v1, fullImageName: r.example/nats:old}\n"},
		{"cert-manager inherited tag", "cert-manager", "cert-manager.image", "cert-manager:\n  imageRegistry: quay.io\n  image: {name: controller}\n"},
		{"cert-manager tag and digest", "cert-manager", "cert-manager.image", "cert-manager:\n  imageRegistry: quay.io\n  image: {name: controller, registry: legacy.example, tag: v1, digest: " + oldDigest + "}\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := imageCatalogFixture(t, "thirdPartyImages: {image: "+test.path+"}\n"+test.defaults)
			images, err := resolveChartImageCatalog(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if images[0].Format.name != test.format {
				t.Fatalf("path %s selected format %s", test.path, images[0].Format.name)
			}
			node, err := imageCatalogValue(snapshot.Values, test.path)
			if err != nil {
				t.Fatal(err)
			}
			for key, value := range images[0].Format.override("mirror.example:5000", "renamed/image", "v2", fixtureDigest) {
				node[key] = value
			}
			overridden, err := resolveChartImageCatalog(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if want := "mirror.example:5000/renamed/image:v2@" + fixtureDigest; overridden[0].Reference != want {
				t.Fatalf("override resolved %s, want %s", overridden[0].Reference, want)
			}
		})
	}
}

// TestImageFormatRequiresDeclaredDependency fails a table key the chart does
// not declare, instead of silently using the standard format.
func TestImageFormatRequiresDeclaredDependency(t *testing.T) {
	snapshot := imageCatalogFixture(t, "thirdPartyImages: {server: nats.container.image}\nnats:\n  container:\n    image: {registry: r.example, repository: nats, tag: v1}\n")
	snapshot.Dependencies = map[string]bool{"nats-renamed": true}
	if _, err := resolveChartImageCatalog(snapshot); err == nil || !strings.Contains(err.Error(), "declared nats dependency") {
		t.Fatalf("undeclared format key accepted: %v", err)
	}
	// An ordinary resolution error on an entry that sorts first ("aa") must
	// not mask the key error on "server".
	snapshot.Values["thirdPartyImages"] = map[string]any{"server": "nats.container.image", "aa": "broken.image"}
	if _, err := resolveChartImageCatalog(snapshot); err == nil || !strings.Contains(err.Error(), "declared nats dependency") {
		t.Fatalf("format key error masked: %v", err)
	}
	for key := range imageValueFormats {
		if _, err := imageFormatFor(key+".image", map[string]bool{key: true}); err != nil {
			t.Errorf("declared %s rejected: %v", key, err)
		}
	}
}

// TestImageFormatKeyErrorWinsOverDeclarationErrors keeps a malformed index
// entry from masking an undeclared format key on a pending-fix chart. Each
// case pairs the key error on "server" with a declaration error that sorts
// or is walked first, and requires the chart's final outcome to fail the run
// rather than count as an expected failure.
func TestImageFormatKeyErrorWinsOverDeclarationErrors(t *testing.T) {
	const natsImage = "nats:\n  container:\n    image: {registry: r.example, repository: nats, tag: v1}\n"
	for _, test := range []struct{ name, index string }{
		{"empty path", "thirdPartyImages: {aa: \"\", server: nats.container.image}\n"},
		{"non-string path", "thirdPartyImages: {aa: [x], server: nats.container.image}\n"},
		{"duplicate identity", "ownedImages: {server: nats.container.image}\nthirdPartyImages: {server: nats.container.image}\n"},
		{"category is not a map", "thirdPartyImages: [x]\nownedImages: {server: nats.container.image}\n"},
		{"unresolvable value", "thirdPartyImages: {aa: missing.image, server: nats.container.image}\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := imageCatalogFixture(t, test.index+natsImage)
			snapshot.Dependencies = map[string]bool{}
			_, err := resolveChartImageCatalog(snapshot)
			var keyError *imageFormatKeyError
			if !errors.As(err, &keyError) {
				t.Fatalf("format key error masked by %v", err)
			}
			check := &chartCheck{chart: "c"}
			check.catalogFailure(err.Error(), err, "", "")
			findings, expected := pendingFixOutcome(check, chartImageExclusion{Reason: reasonPendingFix})
			if expected != "" || len(findings) != 1 || findings[0].Category != findingInvalidEntry {
				t.Fatalf("pending-fix outcome: findings %+v, expected label %q", findings, expected)
			}
		})
	}
}

// TestImageFormatWrapperVisibility requires each format's defaults in the wrapper values.
func TestImageFormatWrapperVisibility(t *testing.T) {
	for _, test := range []struct {
		name, values, path string
		visible            bool
	}{
		{"standard complete", "server: {image: {registry: r.example, repository: server, tag: v1}}", "server.image", true},
		{"standard without registry", "server: {image: {repository: server, tag: v1}}", "server.image", false},
		{"nats full reference", "nats: {container: {image: {fullImageName: r.example/nats:1}}}", "nats.container.image", true},
		{"nats without repository", "nats: {container: {image: {registry: r.example, tag: v1}}}", "nats.container.image", false},
		{"cert-manager shared registry and name", "cert-manager: {imageRegistry: quay.io, image: {name: controller}}", "cert-manager.image", true},
		{"cert-manager name inherited from subchart", "cert-manager: {imageRegistry: quay.io, image: {tag: v1}}", "cert-manager.image", false},
		{"cert-manager registry inherited from subchart", "cert-manager: {image: {name: controller}}", "cert-manager.image", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := imageCatalogFixture(t, test.values)
			format, err := imageFormatFor(test.path, snapshot.Dependencies)
			if err != nil {
				t.Fatal(err)
			}
			parent, node, err := imageCatalogNodes(snapshot.Values, test.path)
			if err != nil {
				t.Fatal(err)
			}
			if err := format.visible(parent, node); (err == nil) != test.visible {
				t.Fatalf("visible=%v, err=%v", test.visible, err)
			}
		})
	}
}

// TestImageCatalogRejectsInvalidDeclarations requires complete, uniquely indexed images.
func TestImageCatalogRejectsInvalidDeclarations(t *testing.T) {
	for _, input := range []string{
		"{}",
		"thirdPartyImages: []",
		"thirdPartyImages: {server: missing.image}",
		"thirdPartyImages: {server: server.image}\nserver: {image: broken}",
		"thirdPartyImages: {server: server.image}\nserver: {image: {repository: nginx, tag: latest}}",
		"thirdPartyImages: {server: server.image}\nserver: {image: {registry: r.example, repository: nginx}}",
		"thirdPartyImages: {server: server.image}\nownedImages: {server: server.image}\nserver: {image: {repository: nginx, tag: v1}}",
		"thirdPartyImages: {server: nats.container.image}\nnats:\n  container:\n    image: {registry: source.example, repository: nats, tag: fixture, fullImageName: missing-tag}\n",
	} {
		if _, err := resolveChartImageCatalog(imageCatalogFixture(t, input)); err == nil {
			t.Errorf("accepted invalid declaration %s", input)
		}
	}
}

// TestImageCatalogCoverageIncludesIndirectImages catches undeclared solver and injected-agent images.
func TestImageCatalogCoverageIncludesIndirectImages(t *testing.T) {
	catalog := []catalogImage{{Reference: "mirror.example/server:v1"}}
	for _, manifest := range []string{
		"spec:\n  containers:\n    - image: mirror.example/unknown:v1\n",
		"spec:\n  containers:\n    - image: mirror.example/server:v1\n      args: [--acme-http01-solver-image=mirror.example/solver:v1]\n",
		"spec:\n  containers:\n    - image: mirror.example/server:v1\n      env:\n        - name: AGENT_INJECT_VAULT_IMAGE\n          value: mirror.example/agent:v1\n",
	} {
		if err := checkChartImageCoverage(catalog, []byte(manifest)); err == nil {
			t.Errorf("accepted undeclared effective image in %s", manifest)
		}
	}
	if err := checkChartImageCoverage(catalog, []byte("spec:\n  containers:\n    - image: mirror.example/server:v1\n")); err != nil {
		t.Fatal(err)
	}
	catalog = append(catalog, catalogImage{Reference: "mirror.example/agent:v1"})
	if err := checkChartImageCoverage(catalog, []byte("spec:\n  env:\n    - name: AGENT_INJECT_VAULT_IMAGE\n      value: mirror.example/agent:v1\n")); err != nil {
		t.Fatal(err)
	}
}

// TestNATSAndOpenBaoChartImageGuards covers chart-owned behaviour that the
// generic coverage test does not: native NATS image override forms, and the
// OpenBao guards against client version skew and an untagged auto-unseal
// sidecar. Generic coverage and override checks live in TestChartImageCoverage.
func TestNATSAndOpenBaoChartImageGuards(t *testing.T) {
	if os.Getenv("NVCF_IMAGE_CATALOG_E2E") != "1" {
		t.Skip("set NVCF_IMAGE_CATALOG_E2E=1 to render the NATS and OpenBao charts with Helm")
	}
	isolateHelmConfiguration(t)
	t.Run("nats native overrides", func(t *testing.T) {
		chart := prepareGuardChart(t, "nats")
		for _, fields := range []map[string]any{
			{"fullImageName": "explicit.example/customer/box:custom"},
			{"registry": "explicit.example", "repository": "customer/box", "digest": "sha256:" + strings.Repeat("b", 64)},
		} {
			values := make(map[string]any)
			setImageValues(values, "nats.natsBox.container.image", fields)
			rendered := renderGuardChart(t, chart, values)
			want := "explicit.example/customer/box@" + fmt.Sprint(fields["digest"])
			if full, ok := fields["fullImageName"].(string); ok {
				want = full
			}
			actual, err := extractResolvedImages(rendered.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, image := range actual {
				found = found || image.Reference == want
			}
			if !found {
				t.Fatalf("native NATS image override %s did not reach the box workload", want)
			}
		}
	})
	t.Run("openbao guards", func(t *testing.T) {
		chart := prepareGuardChart(t, "openbao/helm")
		defaults, err := renderImageChart(chart, nil)
		if err != nil {
			t.Fatal(err)
		}
		catalog, err := resolveChartImageCatalog(defaults)
		if err != nil {
			t.Fatal(err)
		}
		// The guards run on top of a full mirror override, as an operator would.
		mirrorPath := filepath.Join(t.TempDir(), "mirror.yaml")
		if _, err := writeImageOverrides(catalog, mirrorOverride, mirrorPath); err != nil {
			t.Fatal(err)
		}
		for _, image := range []string{
			"",
			"mirror.example/openbao",
			"mirror.example:5000/openbao",
			"mirror.example:5000/openbao:",
			"mirror.example:5000/openbao@sha256:" + strings.Repeat("a", 64),
		} {
			t.Run("untagged sidecar "+image, func(t *testing.T) {
				path := writeGuardValues(t, fmt.Sprintf("openbao:\n  server:\n    extraContainers:\n      - name: auto-unseal-sidecar\n        image: %q\n", image))
				_, err := renderImageChart(chart, []string{mirrorPath, path})
				if err == nil || !strings.Contains(err.Error(), "auto-unseal sidecar image requires an explicit tag") {
					t.Fatalf("sidecar image %q did not fail the explicit-tag guard: %v", image, err)
				}
			})
		}
		for _, skew := range []string{
			"openbao:\n  injector:\n    agentImage:\n      tag: incompatible\n",
			"openbao:\n  server:\n    extraContainers:\n      - name: auto-unseal-sidecar\n        image: mirror.example:5000/server:incompatible\n",
		} {
			_, err := renderImageChart(chart, []string{mirrorPath, writeGuardValues(t, skew)})
			if err == nil || !strings.Contains(err.Error(), "OpenBao image tag mismatch") {
				t.Fatalf("OpenBao client version skew was accepted: %v", err)
			}
		}
	})
}

func prepareGuardChart(t *testing.T, chart string) string {
	t.Helper()
	prepared, err := prepareImageChart(filepath.Join("..", "..", "deploy", "helm", chart), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return prepared
}

func renderGuardChart(t *testing.T, chart string, values map[string]any) imageChartSnapshot {
	t.Helper()
	encoded, err := yaml.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := renderImageChart(chart, []string{writeGuardValues(t, string(encoded))})
	if err != nil {
		t.Fatal(err)
	}
	return rendered
}

func writeGuardValues(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "values.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
