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
		Dependencies: map[string]string{"nats": "nats", "cert-manager": "cert-manager"},
	}
}

var fixtureDigest = "sha256:" + strings.Repeat("a", 64)

// TestImageReferenceHost pins the one rule that tells a registry host from a
// repository path.
func TestImageReferenceHost(t *testing.T) {
	for _, test := range []struct {
		reference, host string
	}{
		{"docker.io/library/nats", "docker.io"},
		{"registry.example:5000/team/server", "registry.example:5000"},
		{"localhost/x", "localhost"},
		{"localhost:5000/x", "localhost:5000"},
		{"library/nats", ""},
		{"nvidia/nvcf/cassandra", ""},
		// A single segment is an image name, even with a dot in it.
		{"nats.io", ""},
		{"", ""},
	} {
		host, found := imageReferenceHost(test.reference)
		if host != test.host || found != (test.host != "") {
			t.Errorf("%q: host %q (found %v), want %q", test.reference, host, found, test.host)
		}
	}
}

// TestImageValueExceptionResolution exercises the convention and each
// exception row through catalog resolution.
func TestImageValueExceptionResolution(t *testing.T) {
	for _, test := range []struct {
		name   string
		values string
		want   string
	}{
		{
			name:   "convention registry, repository and tag",
			values: "ownedImages: {server: server.image}\nserver:\n  image: {registry: registry.example:5000, repository: team/server, tag: v2}\n",
			want:   "registry.example:5000/team/server:v2",
		},
		{
			name:   "convention tag and digest",
			values: "ownedImages: {server: server.image}\nserver:\n  image: {registry: r.example, repository: server, tag: v2, digest: " + fixtureDigest + "}\n",
			want:   "r.example/server:v2@" + fixtureDigest,
		},
		{
			name:   "nats digest replaces tag",
			values: "thirdPartyImages: {server: nats.container.image}\nnats:\n  container:\n    image: {registry: selected.example, repository: nats, tag: stale, digest: " + fixtureDigest + "}\n",
			want:   "selected.example/nats@" + fixtureDigest,
		},
		{
			name:   "cert-manager inherits appVersion",
			values: "thirdPartyImages: {controller: cert-manager.image}\ncert-manager:\n  image: {registry: r.example, repository: jetstack/controller, tag: \"\"}\n",
			want:   "r.example/jetstack/controller:v8.2.1",
		},
		{
			name:   "cert-manager tag and digest together",
			values: "thirdPartyImages: {controller: cert-manager.image}\ncert-manager:\n  image: {registry: r.example, repository: team/controller, tag: fixture, digest: " + fixtureDigest + "}\n",
			want:   "r.example/team/controller:fixture@" + fixtureDigest,
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

// TestImageValueExceptionOverridesRoundTrip merges each row's override fields
// over a fixture default the way Helm coalesces an ordinary values file
// (fields the override omits keep their defaults) and requires the result to
// resolve to the target, so reading and writing an image cannot drift apart.
// A default digest must not survive: it would render "<tag>@<new>@<old>", and
// a kept NATS digest would still replace the tag.
func TestImageValueExceptionOverridesRoundTrip(t *testing.T) {
	oldDigest := "sha256:" + strings.Repeat("c", 64)
	for _, test := range []struct {
		name, exception, path, defaults string
	}{
		{"convention tag", "convention", "server.image", "server:\n  image: {registry: r.example, repository: server, tag: v1}\n"},
		{"convention digest only", "convention", "server.image", "server:\n  image: {registry: r.example, repository: server, digest: " + oldDigest + "}\n"},
		{"convention tag and digest", "convention", "server.image", "server:\n  image: {registry: r.example, repository: server, tag: v1, digest: " + oldDigest + "}\n"},
		{"nats tag", "nats", "nats.container.image", "nats:\n  container:\n    image: {registry: r.example, repository: nats, tag: v1}\n"},
		{"nats digest", "nats", "nats.container.image", "nats:\n  container:\n    image: {registry: r.example, repository: nats, tag: v1, digest: " + oldDigest + "}\n"},
		{"cert-manager inherited tag", "cert-manager", "cert-manager.image", "cert-manager:\n  image: {registry: r.example, repository: controller}\n"},
		{"cert-manager tag and digest", "cert-manager", "cert-manager.image", "cert-manager:\n  image: {registry: r.example, repository: controller, tag: v1, digest: " + oldDigest + "}\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := imageCatalogFixture(t, "thirdPartyImages: {image: "+test.path+"}\n"+test.defaults)
			images, err := resolveChartImageCatalog(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if images[0].Exception.name != test.exception {
				t.Fatalf("path %s selected %s", test.path, images[0].Exception.name)
			}
			node, err := imageCatalogValue(snapshot.Values, test.path)
			if err != nil {
				t.Fatal(err)
			}
			for key, value := range images[0].Exception.override("mirror.example:5000", "renamed/image", "v2", fixtureDigest) {
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

// TestImageValueExceptionOverrideClearsSourceFields keeps a scenario's NATS
// fullImageName from winning over an override.
func TestImageValueExceptionOverrideClearsSourceFields(t *testing.T) {
	fields := imageValueExceptions["nats"].override("m.example", "x", "v1", fixtureDigest)
	if value, ok := fields["fullImageName"]; !ok || value != "" {
		t.Fatalf("nats override fields %v must clear fullImageName", fields)
	}
}

// TestImageExceptionRequiresDeclaredDependency fails a table key the chart
// does not declare, instead of silently using the convention.
func TestImageExceptionRequiresDeclaredDependency(t *testing.T) {
	snapshot := imageCatalogFixture(t, "thirdPartyImages: {server: nats.container.image}\nnats:\n  container:\n    image: {registry: r.example, repository: nats, tag: v1}\n")
	snapshot.Dependencies = map[string]string{"nats-renamed": "nats-renamed"}
	if _, err := resolveChartImageCatalog(snapshot); err == nil || !strings.Contains(err.Error(), "declared nats dependency") {
		t.Fatalf("undeclared exception key accepted: %v", err)
	}
	// An ordinary resolution error on an entry that sorts first ("aa") must
	// not mask the key error on "server".
	snapshot.Values["thirdPartyImages"] = map[string]any{"server": "nats.container.image", "aa": "broken.image"}
	if _, err := resolveChartImageCatalog(snapshot); err == nil || !strings.Contains(err.Error(), "declared nats dependency") {
		t.Fatalf("exception key error masked: %v", err)
	}
	for key := range imageValueExceptions {
		if _, err := imageExceptionFor(key+".image", map[string]string{key: key}); err != nil {
			t.Errorf("declared %s rejected: %v", key, err)
		}
	}
}

// TestImageExceptionFollowsDependencyAlias looks an exception up by the chart
// a values root belongs to, so an alias neither drops the exception nor
// borrows one from an unrelated chart.
func TestImageExceptionFollowsDependencyAlias(t *testing.T) {
	for _, test := range []struct {
		name, path   string
		dependencies map[string]string
		exception    string
		keyError     bool
	}{
		// Values for an aliased dependency live under the alias.
		{"aliased cert-manager", "cm.image", map[string]string{"cm": "cert-manager"}, "cert-manager", false},
		// Values under the chart name never reach an aliased dependency.
		{"chart name of an aliased dependency", "nats.container.image", map[string]string{"messaging": "nats"}, "", true},
		// An alias that reuses a table key names a different chart.
		{"alias reusing a table key", "nats.image", map[string]string{"nats": "other"}, "convention", false},
	} {
		exception, err := imageExceptionFor(test.path, test.dependencies)
		var keyError *imageExceptionKeyError
		if test.keyError {
			if !errors.As(err, &keyError) {
				t.Errorf("%s: err %v, want an exception key error", test.name, err)
			}
			continue
		}
		if err != nil || exception.name != test.exception {
			t.Errorf("%s: exception %q, err %v, want %q", test.name, exception.name, err, test.exception)
		}
	}
}

// TestImageIndexErrorsFailPendingFix keeps a malformed index from counting as
// an expected pending-fix failure, and keeps a malformed entry from masking an
// undeclared exception key.
func TestImageIndexErrorsFailPendingFix(t *testing.T) {
	const natsImage = "nats:\n  container:\n    image: {registry: r.example, repository: nats, tag: v1}\n"
	for _, test := range []struct {
		name, index string
		keyError    bool
	}{
		{"empty path beside key error", "thirdPartyImages: {aa: \"\", server: nats.container.image}\n", true},
		{"non-string path beside key error", "thirdPartyImages: {aa: [x], server: nats.container.image}\n", true},
		// thirdPartyImages is read first, so only the duplicate names nats.
		{"duplicate identity beside key error", "thirdPartyImages: {server: server.image}\nownedImages: {server: nats.container.image}\n", true},
		{"null category beside key error", "thirdPartyImages:\nownedImages: {server: nats.container.image}\n", true},
		{"null category", "thirdPartyImages:\nownedImages: {server: server.image}\n", false},
		{"category is not a map beside key error", "thirdPartyImages: [x]\nownedImages: {server: nats.container.image}\n", true},
		{"empty path", "thirdPartyImages: {aa: \"\"}\n", false},
		{"empty index", "thirdPartyImages: {}\n", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := imageCatalogFixture(t, test.index+natsImage)
			snapshot.Dependencies = map[string]string{}
			_, err := resolveChartImageCatalog(snapshot)
			var keyError *imageExceptionKeyError
			if errors.As(err, &keyError) != test.keyError {
				t.Fatalf("error %v, want key error %v", err, test.keyError)
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

// TestImageCatalogKeepsChartIndex resolves a scenario against the index from
// the chart's own values.yaml. A scenario that rewrites ownedImages must not
// move the checks to a path the chart does not declare.
func TestImageCatalogKeepsChartIndex(t *testing.T) {
	index, err := readImageIndex(map[string]any{"ownedImages": map[string]any{"server": "server.image"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := imageCatalogFixture(t, "ownedImages: {server: other.image}\n"+
		"server: {image: {registry: r.example, repository: team/server, tag: v1}}\n"+
		"other: {image: {registry: r.example, repository: team/other, tag: v1}}\n")
	resolved, err := resolveImageCatalog(snapshot, index)
	if err != nil {
		t.Fatal(err)
	}
	if len(resolved) != 1 || resolved[0].Path != "server.image" || resolved[0].Reference != "r.example/team/server:v1" {
		t.Fatalf("resolved %+v, want server.image at r.example/team/server:v1", resolved)
	}
	if index[0].Reference != "" {
		t.Fatalf("resolving changed the chart index: %+v", index)
	}
}

// TestImageOverridesKeepCaseVariantIdentitiesDistinct gives identities that
// differ only by case their own expected reference, so one image cannot pass
// the override check with another image's reference.
func TestImageOverridesKeepCaseVariantIdentitiesDistinct(t *testing.T) {
	catalog := []catalogImage{
		{ID: "API", Path: "upper.image", Reference: "r.example/api:v1"},
		{ID: "api", Path: "lower.image", Reference: "r.example/api:v1"},
		{ID: "api-1", Path: "suffixed.image", Reference: "r.example/api:v1"},
	}
	want, err := writeImageOverrides(catalog, mirrorOverride, filepath.Join(t.TempDir(), "mirror.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]string)
	for id, reference := range want {
		if other, ok := seen[reference]; ok {
			t.Fatalf("%s and %s share the expected override %s", id, other, reference)
		}
		seen[reference] = id
	}
	if len(seen) != len(catalog) {
		t.Fatalf("want %v, one reference per identity", want)
	}
}

// TestRenderRejectedSeparatesExecutionErrors counts only a command that ran
// and exited nonzero as a rejection; a command that cannot run or is killed
// by a signal is an execution error, never evidence that a guard fired.
func TestRenderRejectedSeparatesExecutionErrors(t *testing.T) {
	if _, err := imageCatalogCommand("sh", "-c", "echo set x.image.registry >&2; exit 1"); !renderRejected(err) {
		t.Fatalf("nonzero exit not a rejection: %v", err)
	}
	if _, err := imageCatalogCommand(filepath.Join(t.TempDir(), "missing-helm")); err == nil || renderRejected(err) {
		t.Fatalf("missing command counted as a rejection: %v", err)
	}
	// An OOM kill ends Helm with a signal before it decides anything.
	if _, err := imageCatalogCommand("sh", "-c", "echo set x.image.registry >&2; kill -KILL $$"); err == nil || renderRejected(err) {
		t.Fatalf("signal termination counted as a rejection: %v", err)
	}
}

// TestImageCatalogRejectsUnresolvableImages requires every indexed path of a
// rendered scenario to resolve to a complete reference.
func TestImageCatalogRejectsUnresolvableImages(t *testing.T) {
	for _, input := range []string{
		"thirdPartyImages: {server: missing.image}",
		"thirdPartyImages: {server: server.image}\nserver: {image: broken}",
		"thirdPartyImages: {server: server.image}\nserver: {image: {repository: nginx, tag: latest}}",
		"thirdPartyImages: {server: server.image}\nserver: {image: {registry: r.example, repository: nginx}}",
	} {
		if _, err := resolveChartImageCatalog(imageCatalogFixture(t, input)); err == nil {
			t.Errorf("accepted unresolvable image %s", input)
		}
	}
}

// TestImageCatalogCoverageIncludesIndirectImages catches undeclared solver and injected-agent images.
func TestImageCatalogCoverageIncludesIndirectImages(t *testing.T) {
	catalog := []catalogImage{{Reference: "mirror.example/server:v1"}}
	for _, test := range []struct {
		manifest, undeclared string
	}{
		{"spec:\n  containers:\n    - image: mirror.example/unknown:v1\n", "mirror.example/unknown:v1"},
		{"spec:\n  containers:\n    - image: mirror.example/server:v1\n      args: [--acme-http01-solver-image=mirror.example/solver:v1]\n", "mirror.example/solver:v1"},
		{"spec:\n  containers:\n    - image: mirror.example/server:v1\n      env:\n        - name: AGENT_INJECT_VAULT_IMAGE\n          value: mirror.example/agent:v1\n", "mirror.example/agent:v1"},
		{"spec:\n  containers:\n    - image: mirror.example/server:v1\n", ""},
	} {
		missing, err := undeclaredImages(catalog, []byte(test.manifest))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Join(missing, ",") != test.undeclared {
			t.Errorf("undeclared %v in %s, want %q", missing, test.manifest, test.undeclared)
		}
	}
	catalog = append(catalog, catalogImage{Reference: "mirror.example/agent:v1"})
	missing, err := undeclaredImages(catalog, []byte("spec:\n  env:\n    - name: AGENT_INJECT_VAULT_IMAGE\n      value: mirror.example/agent:v1\n"))
	if err != nil || len(missing) != 0 {
		t.Fatalf("declared agent image reported: %v, %v", missing, err)
	}
}

// TestNATSAndOpenBaoChartImageGuards covers chart-owned behaviour that the
// generic checks do not: native NATS image override forms, and the OpenBao
// guards against client version skew and an untagged auto-unseal sidecar.
// Each render starts from the chart's CI values scenario, which supplies the
// image sources. Generic checks live in TestChartImageCoverage.
func TestNATSAndOpenBaoChartImageGuards(t *testing.T) {
	if os.Getenv("NVCF_IMAGE_CATALOG_E2E") != "1" {
		t.Skip("set NVCF_IMAGE_CATALOG_E2E=1 to render the NATS and OpenBao charts with Helm")
	}
	isolateHelmConfiguration(t)
	t.Run("nats native overrides", func(t *testing.T) {
		chart, scenario := prepareGuardChart(t, "nats")
		for _, fields := range []map[string]any{
			{"fullImageName": "explicit.example/customer/box:custom"},
			{"registry": "explicit.example", "repository": "customer/box", "digest": "sha256:" + strings.Repeat("b", 64)},
		} {
			values := make(map[string]any)
			setImageValue(values, "nats.natsBox.container.image", fields)
			rendered := renderGuardChart(t, chart, scenario, values)
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
		chart, scenario := prepareGuardChart(t, "openbao/helm")
		base, err := renderImageChart(chart, []string{scenario})
		if err != nil {
			t.Fatal(err)
		}
		catalog, err := resolveChartImageCatalog(base)
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
				_, err := renderImageChart(chart, []string{scenario, mirrorPath, path})
				if err == nil || !strings.Contains(err.Error(), "auto-unseal sidecar image requires an explicit tag") {
					t.Fatalf("sidecar image %q did not fail the explicit-tag guard: %v", image, err)
				}
			})
		}
		for _, skew := range []string{
			"openbao:\n  injector:\n    agentImage:\n      tag: incompatible\n",
			"openbao:\n  server:\n    extraContainers:\n      - name: auto-unseal-sidecar\n        image: mirror.example:5000/server:incompatible\n",
		} {
			_, err := renderImageChart(chart, []string{scenario, mirrorPath, writeGuardValues(t, skew)})
			if err == nil || !strings.Contains(err.Error(), "OpenBao image tag mismatch") {
				t.Fatalf("OpenBao client version skew was accepted: %v", err)
			}
		}
	})
}

// prepareGuardChart prepares a chart under deploy/helm and returns it with
// its first CI values scenario.
func prepareGuardChart(t *testing.T, chart string) (string, string) {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	charts, err := discoverTrackedCharts(repoRoot)
	if err != nil {
		t.Fatal(err)
	}
	tracked := make(map[string]bool)
	for _, name := range charts {
		tracked[name] = true
	}
	scenarios, err := loadChartScenarios(repoRoot, tracked)
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join("deploy", "helm", chart)
	if len(scenarios[directory]) == 0 {
		t.Fatalf("%s has no CI values scenario in %s", directory, chartValuesMapFile)
	}
	prepared, err := prepareImageChart(filepath.Join(repoRoot, directory), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return prepared, scenarios[directory][0]
}

func renderGuardChart(t *testing.T, chart, scenario string, values map[string]any) imageChartSnapshot {
	t.Helper()
	encoded, err := yaml.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := renderImageChart(chart, []string{scenario, writeGuardValues(t, string(encoded))})
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
