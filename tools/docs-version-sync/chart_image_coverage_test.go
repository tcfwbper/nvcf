// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestChartImageCoverage requires every tracked chart to pass image coverage
// or to carry a valid entry in chart-image-exclusions.yaml. It evaluates every
// chart before failing and prints one summary.
func TestChartImageCoverage(t *testing.T) {
	if os.Getenv("NVCF_IMAGE_CATALOG_E2E") != "1" {
		t.Skip("set NVCF_IMAGE_CATALOG_E2E=1 to render the repository's charts with Helm")
	}
	isolateHelmConfiguration(t)
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	report, err := evaluateChartImageCoverage(repoRoot, chartImageExclusionsFile, t.TempDir)
	if err != nil {
		t.Fatal(err)
	}
	// Printed rather than logged so skips and expected failures are visible
	// on a passing run without -v.
	fmt.Print(report.Summary())
	if len(report.Findings) > 0 {
		t.Fatalf("%d chart image coverage failures; see the FAIL lines above", len(report.Findings))
	}
}

// isolateHelmConfiguration keeps repository registrations and downloaded
// dependencies in test-owned directories.
func isolateHelmConfiguration(t *testing.T) {
	t.Helper()
	directory := t.TempDir()
	t.Setenv("HELM_REPOSITORY_CONFIG", filepath.Join(directory, "repositories.yaml"))
	t.Setenv("HELM_REPOSITORY_CACHE", filepath.Join(directory, "cache"))
	t.Setenv("HELM_REGISTRY_CONFIG", filepath.Join(directory, "registry.json"))
}

// evaluateChartImageCoverage validates the matrix, then classifies and checks
// every tracked chart. workspace returns a fresh temporary directory per chart.
func evaluateChartImageCoverage(repoRoot, exclusionsPath string, workspace func() string) (*chartCoverageReport, error) {
	charts, err := discoverTrackedCharts(repoRoot)
	if err != nil {
		return nil, err
	}
	entries, err := loadChartImageExclusions(exclusionsPath)
	if err != nil {
		return nil, err
	}
	report := &chartCoverageReport{Skipped: make(map[string][]string)}
	tracked := make(map[string]bool)
	indexed := make(map[string]bool)
	for _, chart := range charts {
		tracked[chart] = true
		if indexed[chart], err = chartHasImageIndex(filepath.Join(repoRoot, chart)); err != nil {
			return nil, err
		}
	}
	scenarios, err := loadChartScenarios(repoRoot, tracked)
	if err != nil {
		return nil, err
	}
	invalid := make(map[string]bool)
	for _, chart := range sortedKeys(entries) {
		if err := validateChartImageExclusion(repoRoot, chart, entries[chart], tracked, indexed[chart]); err != nil {
			invalid[chart] = true
			report.add(chartFinding{Category: findingInvalidEntry, Chart: chart, Problem: err.Error(),
				Expected: "a valid " + chartImageExclusionsFile + " entry", Fix: "correct or remove the entry"})
		}
	}
	for _, chart := range charts {
		if invalid[chart] {
			continue
		}
		var entry *chartImageExclusion
		if value, ok := entries[chart]; ok {
			entry = &value
		}
		action := classifyChart(indexed[chart], entry)
		check := &chartCheck{chart: chart, source: filepath.Join(repoRoot, chart), scenarios: scenarios[chart]}
		if action != actionExpectedFailure && action != actionSkip {
			check.workspace = workspace()
		}
		switch action {
		case actionCheck:
			checkIndexedChart(check)
			report.add(check.findings...)
			if len(check.findings) == 0 {
				report.Covered = append(report.Covered, chart)
			}
		case actionCheckExpectFailure:
			checkIndexedChart(check)
			findings, expected := pendingFixOutcome(check, *entry)
			report.add(findings...)
			if expected != "" {
				report.Expected = append(report.Expected, expected)
			}
		case actionExpectedFailure:
			report.Expected = append(report.Expected, expectedFailureLabel(chart, *entry, "no image index"))
		case actionSkip:
			report.Skipped[entry.Reason] = append(report.Skipped[entry.Reason], chart)
		case actionConfirmNoImage:
			confirmNoRuntimeImage(check)
			report.add(check.findings...)
		case actionUnclassified:
			diagnoseUnclassified(check)
			report.add(check.findings...)
		}
	}
	return report, nil
}

// pendingFixOutcome applies expected-failure strictness to an indexed
// pending-fix chart: only rule (coverage) findings are expected. It returns
// the findings that fail the run, or the expected-failure label for the
// summary. A render, dependency, index or exception-key error still fails,
// and a clean pass means the entry is stale.
func pendingFixOutcome(check *chartCheck, entry chartImageExclusion) ([]chartFinding, string) {
	var hard []chartFinding
	for _, finding := range check.findings {
		if finding.Category != findingCoverage {
			hard = append(hard, finding)
		}
	}
	if len(hard) > 0 {
		return hard, ""
	}
	if len(check.findings) == 0 {
		return []chartFinding{{Category: findingPendingFixed, Chart: check.chart, Problem: "chart now passes image coverage",
			Expected: "no " + reasonPendingFix + " entry for a covered chart", Fix: "remove the entry from " + chartImageExclusionsFile}}, ""
	}
	return nil, expectedFailureLabel(check.chart, entry, fmt.Sprintf("%d coverage findings", len(check.findings)))
}

func expectedFailureLabel(chart string, entry chartImageExclusion, detail string) string {
	parts := []string{detail}
	if entry.Issue != "" {
		parts = append(parts, entry.Issue)
	}
	return chart + " (" + strings.Join(parts, ", ") + ")"
}

// TestChartImageFindingFormat pins the one-line failure format.
func TestChartImageFindingFormat(t *testing.T) {
	finding := chartFinding{Category: findingCoverage, Chart: "deploy/helm/x", Image: "server", ValuesPath: "x.server.image.registry",
		Problem: `prefilled image source "nvcr.io"`, Expected: `""`, Fix: "remove it"}
	if got, want := finding.String(), `FAIL deploy/helm/x server x.server.image.registry: prefilled image source "nvcr.io"; expected ""; fix: remove it`; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	finding.Image, finding.ValuesPath = "", ""
	if got := finding.String(); !strings.HasPrefix(got, "FAIL deploy/helm/x: prefilled") {
		t.Fatalf("chart-level finding: %q", got)
	}
}

// TestChartImageSummaryListsDowngrades requires skips and expected failures in
// the summary, with counts by failure category.
func TestChartImageSummaryListsDowngrades(t *testing.T) {
	report := &chartCoverageReport{
		Findings: []chartFinding{{Category: findingUncovered, Chart: "new"}, {Category: findingRenderError, Chart: "broken"}},
		Skipped:  map[string][]string{reasonTestFixture: {"examples/a"}},
		Expected: []string{expectedFailureLabel("deploy/helm/p", chartImageExclusion{Reason: reasonPendingFix, Issue: "https://github.com/dsx-ai-factory/nvcf/issues/7"}, "no image index")},
		Covered:  []string{"deploy/helm/nats"},
	}
	summary := report.Summary()
	for _, want := range []string{
		"1 covered, 2 failures, uncovered: 1, render error: 1",
		"skipped (test-fixture): examples/a",
		"expected failures (pending-fix, 1): deploy/helm/p (no image index, https://github.com/dsx-ai-factory/nvcf/issues/7)",
	} {
		if !strings.Contains(summary, want) {
			t.Errorf("summary missing %q:\n%s", want, summary)
		}
	}
}

// TestPendingFixOutcome keeps render errors fatal and flags a stale entry.
func TestPendingFixOutcome(t *testing.T) {
	entry := chartImageExclusion{Reason: reasonPendingFix}
	for _, test := range []struct {
		name     string
		findings []chartFinding
		want     string
		expected bool
	}{
		{"coverage failure is expected", []chartFinding{{Category: findingCoverage}}, "", true},
		{"render error fails", []chartFinding{{Category: findingCoverage}, {Category: findingRenderError}}, findingRenderError, false},
		{"index or exception key error fails", []chartFinding{{Category: findingCoverage}, {Category: findingInvalidEntry}}, findingInvalidEntry, false},
		{"incomplete scenario fails", []chartFinding{{Category: findingCoverage}, {Category: findingScenario}}, findingScenario, false},
		{"passing chart is stale", nil, findingPendingFixed, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, label := pendingFixOutcome(&chartCheck{chart: "c", findings: test.findings}, entry)
			if test.want == "" && len(got) != 0 || test.want != "" && (len(got) != 1 || got[0].Category != test.want) {
				t.Fatalf("findings %+v, want category %q", got, test.want)
			}
			if (label != "") != test.expected {
				t.Fatalf("expected-failure label %q", label)
			}
		})
	}
}

// TestChartScenarioMapRejectsUnmatchablePaths keeps the Go reader of the chart
// values map from silently dropping a scenario that check-helm-charts renders.
func TestChartScenarioMapRejectsUnmatchablePaths(t *testing.T) {
	tracked := map[string]bool{"deploy/helm/x": true}
	for _, test := range []struct {
		name, entry, want string
	}{
		{"canonical entry", "{chart: deploy/helm/x, values: x.yaml}", ""},
		{"leading dot", "{chart: ./deploy/helm/x, values: x.yaml}", "canonical"},
		{"trailing slash", "{chart: deploy/helm/x/, values: x.yaml}", "canonical"},
		{"parent escape", "{chart: ../x, values: x.yaml}", "canonical"},
		{"untracked chart", "{chart: deploy/helm/y, values: x.yaml}", "no tracked Chart.yaml"},
		{"missing values", "{chart: deploy/helm/x}", "must be a .yaml file name"},
		{"values path", "{chart: deploy/helm/x, values: sub/x.yaml}", "must be a .yaml file name"},
		{"values parent", "{chart: deploy/helm/x, values: ..}", "must be a .yaml file name"},
		{"misspelled field", "{chart: deploy/helm/x, value: x.yaml}", "not found"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := writeChartFixtureTree(t, map[string]string{chartValuesMapFile: "scenarios:\n  - " + test.entry + "\n"})
			scenarios, err := loadChartScenarios(root, tracked)
			if test.want == "" {
				if err != nil || len(scenarios["deploy/helm/x"]) != 1 {
					t.Fatalf("scenarios %v, err %v", scenarios, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("got %v, want error containing %q", err, test.want)
			}
		})
	}
}

// coverageFixtureChart writes a chart whose Pod renders the given container
// image expressions (Helm template text). With no images it renders a
// ConfigMap, plus a sidecar Pod when .Values.sidecar is set.
func coverageFixtureChart(name, values string, images ...string) map[string]string {
	pod := "apiVersion: v1\nkind: Pod\nmetadata:\n  name: " + name + "\nspec:\n  containers:\n"
	for index, image := range images {
		pod += fmt.Sprintf("    - name: c%d\n      image: %s\n", index, image)
	}
	if len(images) == 0 {
		pod = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: " + name + "\n" +
			"{{- if .Values.sidecar }}\n---\napiVersion: v1\nkind: Pod\nmetadata:\n  name: " + name + "\nspec:\n" +
			"  containers:\n    - name: sidecar\n      image: {{ .Values.sidecar }}\n{{- end }}\n"
	}
	dir := "charts/" + name + "/"
	return map[string]string{
		dir + "Chart.yaml":            "apiVersion: v2\nname: " + name + "\nversion: 0.1.0\n",
		dir + "values.yaml":           values,
		dir + "templates/object.yaml": pod,
	}
}

// Fixture image values and templates. The guarded template names the values
// path in each required message, as every chart must.
const (
	fixtureIndexed   = "ownedImages: {server: server.image}\nserver:\n  image: {registry: \"\", repository: \"\", tag: v1}\n"
	fixturePrefilled = "ownedImages: {server: server.image}\nserver:\n  image: {registry: r.example, repository: team/server, tag: v1}\n"
	fixtureGuarded   = `{{ required "set server.image.registry" .Values.server.image.registry }}/` +
		`{{ required "set server.image.repository" .Values.server.image.repository }}:{{ .Values.server.image.tag }}`
	fixtureUnguarded = "{{ .Values.server.image.registry }}/{{ .Values.server.image.repository }}:{{ .Values.server.image.tag }}"
	fixtureScenario  = "server:\n  image: {registry: r.example, repository: team/server}\n"
)

// TestChartImageCoverageReportsEachFailure drives the whole evaluation on a
// fixture repository, so each rule is pinned by a fixture built for it rather
// than by the real charts, which are all pending-fix until their image
// sources move to the stack.
func TestChartImageCoverageReportsEachFailure(t *testing.T) {
	if os.Getenv("NVCF_IMAGE_CATALOG_E2E") != "1" {
		t.Skip("set NVCF_IMAGE_CATALOG_E2E=1 to render fixture charts with Helm")
	}
	isolateHelmConfiguration(t)
	scenarios := map[string]string{
		"covered": fixtureScenario, "wrapped": "sub:\n  image: {registry: r.example, repository: team/sub}\n",
		"prefilled-registry": fixtureScenario, "prefilled-repository": fixtureScenario, "repository-host": fixtureScenario,
		"unguarded": fixtureScenario, "unnamed-guard": fixtureScenario, "template-host": fixtureScenario,
		"unrelated-guard":  fixtureScenario + "mode: server\n",
		"template-default": fixtureScenario, "hidden": fixtureScenario + "hidden: r.example/team/server:v1\n",
		"undeclared": fixtureScenario + "sidecar: r.example/sidecar:v1\n", "scenario-gap": "server:\n  image: {registry: r.example}\n",
		"format-key":      "nats:\n  image: {registry: r.example, repository: nats}\n",
		"pending-passing": fixtureScenario, "pending-failing": fixtureScenario,
		"no-image": "sidecar: r.example/sidecar:v1\n", "pending-gap": "server:\n  image: {registry: r.example}\n",
	}
	files := map[string]string{
		chartImageExclusionsFile: "charts/no-image: {reason: no-runtime-image}\n" +
			"charts/pending-passing: {reason: pending-fix}\n" +
			"charts/pending-failing: {reason: pending-fix, issue: https://github.com/dsx-ai-factory/nvcf/issues/3532}\n" +
			"charts/pending-static: {reason: pending-fix}\n" +
			"charts/pending-gap: {reason: pending-fix}\n" +
			"charts/format-key: {reason: pending-fix}\n" +
			"charts/fixture: {reason: test-fixture}\n" +
			"charts/wrapped/charts/sub: {reason: vendored-subchart}\n" +
			"charts/no-image-ok: {reason: no-runtime-image}\n" +
			"charts/gone: {reason: test-fixture}\n",
	}
	mapping := "scenarios:\n"
	for _, chart := range sortedKeys(scenarios) {
		mapping += fmt.Sprintf("  - {chart: charts/%s, values: %s.yaml}\n", chart, chart)
		files[chartValuesDir+"/"+chart+".yaml"] = scenarios[chart]
	}
	files[chartValuesMapFile] = mapping
	wrapped := coverageFixtureChart("wrapped", "ownedImages: {sub: sub.image}\nsub:\n  image: {registry: \"\", repository: \"\"}\n")
	// The subchart renders the image; only the wrapper guards it.
	wrapped["charts/wrapped/Chart.yaml"] += "dependencies:\n  - {name: sub, version: 0.1.0, repository: file://charts/sub}\n"
	wrapped["charts/wrapped/templates/object.yaml"] = `{{- $_ := required "set sub.image.registry" .Values.sub.image.registry -}}` + "\n" +
		`{{- $_ := required "set sub.image.repository" .Values.sub.image.repository -}}` + "\n"
	wrapped["charts/wrapped/charts/sub/Chart.yaml"] = "apiVersion: v2\nname: sub\nversion: 0.1.0\n"
	wrapped["charts/wrapped/charts/sub/values.yaml"] = "image: {registry: \"\", repository: \"\", tag: v2}\n"
	wrapped["charts/wrapped/charts/sub/templates/pod.yaml"] = "apiVersion: v1\nkind: Pod\nmetadata:\n  name: sub\nspec:\n  containers:\n" +
		"    - name: sub\n      image: {{ .Values.image.registry }}/{{ .Values.image.repository }}:{{ .Values.image.tag }}\n"
	// The defaults fail on a required value unrelated to the image source,
	// which the scenario supplies, before any image guard runs; every
	// per-image guard is named.
	unrelatedGuard := coverageFixtureChart("unrelated-guard", fixtureIndexed+"mode: \"\"\n", fixtureGuarded)
	unrelatedGuard["charts/unrelated-guard/templates/object.yaml"] = `{{- $_ := required "set mode" .Values.mode -}}` + "\n" +
		unrelatedGuard["charts/unrelated-guard/templates/object.yaml"]
	for _, chart := range []map[string]string{
		coverageFixtureChart("covered", fixtureIndexed, fixtureGuarded),
		wrapped,
		coverageFixtureChart("prefilled-registry", "ownedImages: {server: server.image}\nserver:\n  image: {registry: r.example, repository: \"\", tag: v1}\n", fixtureGuarded),
		coverageFixtureChart("prefilled-repository", "ownedImages: {server: server.image}\nserver:\n  image: {registry: \"\", repository: team/server, tag: v1}\n", fixtureGuarded),
		coverageFixtureChart("repository-host", "ownedImages: {server: server.image}\nserver:\n  image: {registry: \"\", repository: docker.io/team/server, tag: v1}\n", fixtureGuarded),
		coverageFixtureChart("unguarded", fixtureIndexed, fixtureUnguarded),
		coverageFixtureChart("unnamed-guard", fixtureIndexed,
			`{{ required "image source missing" .Values.server.image.registry }}/{{ required "image source missing" .Values.server.image.repository }}:{{ .Values.server.image.tag }}`),
		unrelatedGuard,
		coverageFixtureChart("template-host", fixtureIndexed, fixtureGuarded, "docker.io/library/busybox:1"),
		coverageFixtureChart("template-default", fixtureIndexed,
			`{{ .Values.server.image.registry | default "r.example" }}/{{ required "set server.image.repository" .Values.server.image.repository }}:{{ .Values.server.image.tag }}`),
		// The scenario renders the indexed reference a second time, unindexed:
		// it passes the scenario, and stays behind once the mirror override
		// moves the indexed value.
		coverageFixtureChart("hidden", fixtureIndexed+"hidden: \"\"\n", fixtureGuarded, "{{ .Values.hidden }}"),
		coverageFixtureChart("undeclared", fixtureIndexed+"sidecar: \"\"\n", fixtureGuarded, "{{ .Values.sidecar }}"),
		coverageFixtureChart("no-scenario", fixtureIndexed, fixtureGuarded),
		coverageFixtureChart("scenario-gap", fixtureIndexed, fixtureGuarded),
		// pending-fix with ordinary findings too: the undeclared exception
		// key must still fail the run.
		coverageFixtureChart("format-key", "thirdPartyImages: {server: nats.image}\nnats:\n  image: {registry: r.example, repository: nats, tag: v1}\n",
			"{{ .Values.nats.image.registry }}/{{ .Values.nats.image.repository }}:{{ .Values.nats.image.tag }}"),
		coverageFixtureChart("no-image", "sidecar: \"\"\n"),
		coverageFixtureChart("unlisted", "sidecar: \"\"\n"),
		// A correct no-runtime-image entry produces no finding.
		coverageFixtureChart("no-image-ok", "sidecar: \"\"\n"),
		coverageFixtureChart("pending-passing", fixtureIndexed, fixtureGuarded),
		coverageFixtureChart("pending-failing", fixturePrefilled, fixtureUnguarded),
		// pending-fix does not excuse an incomplete scenario: without a
		// complete scenario the override and guard checks cannot run.
		coverageFixtureChart("pending-gap", fixtureIndexed, fixtureGuarded),
		// Never rendered: the template would fail.
		coverageFixtureChart("pending-static", "{}\n", "{{ fail \"must not render\" }}"),
		coverageFixtureChart("fixture", "{}\n", "{{ fail \"must not render\" }}"),
	} {
		for name, content := range chart {
			files[name] = content
		}
	}
	root := writeChartFixtureTree(t, files)
	for _, args := range [][]string{{"init", "-q"}, {"add", "."}} {
		if _, err := imageCatalogCommand("git", append([]string{"-C", root}, args...)...); err != nil {
			t.Fatal(err)
		}
	}
	report, err := evaluateChartImageCoverage(root, filepath.Join(root, chartImageExclusionsFile), t.TempDir)
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]map[string]bool)
	problems := make(map[string][]string)
	for _, finding := range report.Findings {
		if got[finding.Chart] == nil {
			got[finding.Chart] = make(map[string]bool)
		}
		got[finding.Chart][finding.Category] = true
		problems[finding.Chart] = append(problems[finding.Chart], finding.ValuesPath+": "+finding.Problem)
	}
	// Each fixture names the check that must report it, so another check
	// producing the same category cannot stand in for a broken one.
	want := map[string]struct{ category, problem string }{
		"charts/prefilled-registry":   {findingCoverage, `server.image.registry: prefilled image source "r.example"`},
		"charts/prefilled-repository": {findingCoverage, `server.image.repository: prefilled image source "team/server"`},
		"charts/repository-host":      {findingCoverage, "server.image.repository: repository \"docker.io/team/server\" contains registry host docker.io"},
		"charts/unguarded":            {findingCoverage, ": defaults render without an image source"},
		"charts/unnamed-guard":        {findingCoverage, "server.image.registry: scenario unnamed-guard.yaml with an empty registry fails without naming it"},
		"charts/unrelated-guard":      {findingCoverage, ": defaults render fails without naming an image source path"},
		"charts/template-host":        {findingCoverage, "templates/object.yaml:10: image reference docker.io/library/busybox:1 starts with literal registry host docker.io"},
		"charts/template-default":     {findingCoverage, `templates/object.yaml:8: default "r.example" supplies an image source`},
		"charts/hidden":               {findingCoverage, "mirror override renders undeclared image r.example/team/server:v1"},
		"charts/undeclared":           {findingCoverage, "scenario undeclared.yaml renders undeclared image r.example/sidecar:v1"},
		"charts/no-scenario":          {findingScenario, "indexed chart has no CI values scenario"},
		"charts/pending-gap":          {findingScenario, "server.image.repository: scenario pending-gap.yaml does not supply repository"},
		"charts/scenario-gap":         {findingScenario, "server.image.repository: scenario scenario-gap.yaml does not supply repository"},
		"charts/format-key":           {findingInvalidEntry, "declared nats dependency"},
		"charts/no-image":             {findingInvalidEntry, "listed as no-runtime-image but scenario no-image.yaml renders"},
		"charts/unlisted":             {findingUncovered, "defaults render no image"},
		"charts/pending-passing":      {findingPendingFixed, "now passes image coverage"},
		"charts/gone":                 {findingInvalidEntry, "no tracked Chart.yaml"},
	}
	for chart, expectation := range want {
		if !got[chart][expectation.category] || len(got[chart]) != 1 {
			t.Errorf("%s: categories %v, want only %q", chart, got[chart], expectation.category)
		}
		if !strings.Contains(strings.Join(problems[chart], "\n"), expectation.problem) {
			t.Errorf("%s: problems %q, want one containing %q", chart, problems[chart], expectation.problem)
		}
	}
	if len(got) != len(want) {
		t.Errorf("unexpected findings:\n%s", report.Summary())
	}
	if strings.Join(report.Covered, ",") != "charts/covered,charts/wrapped" {
		t.Errorf("covered %v", report.Covered)
	}
	expected := strings.Join(report.Expected, "\n")
	for _, chart := range []string{"charts/pending-failing", "issues/3532", "charts/pending-static (no image index)"} {
		if !strings.Contains(expected, chart) {
			t.Errorf("expected failures missing %s: %v", chart, report.Expected)
		}
	}
	if strings.Join(report.Skipped[reasonTestFixture], ",") != "charts/fixture" {
		t.Errorf("skipped %v", report.Skipped)
	}
}

// TestImageCatalogRepositoriesReuseNamesAcrossCharts registers two charts'
// dependency repositories in one Helm config, as a coverage run does. Both
// charts name their first repository image-catalog-0, at different URLs; the
// second chart must not fail with "repository name already exists", which
// would be a false render error. Local HTTP servers stand in for the remote
// repositories.
func TestImageCatalogRepositoriesReuseNamesAcrossCharts(t *testing.T) {
	if os.Getenv("NVCF_IMAGE_CATALOG_E2E") != "1" {
		t.Skip("set NVCF_IMAGE_CATALOG_E2E=1 to register repositories with Helm")
	}
	isolateHelmConfiguration(t)
	var urls []string
	for range 2 {
		directory := t.TempDir()
		if err := os.WriteFile(filepath.Join(directory, "index.yaml"), []byte("apiVersion: v1\nentries: {}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(http.FileServer(http.Dir(directory)))
		t.Cleanup(server.Close)
		urls = append(urls, server.URL)
	}
	for _, url := range urls {
		if err := registerImageCatalogRepositories([]string{url}); err != nil {
			t.Fatalf("register %s: %v", url, err)
		}
	}
	listed, err := imageCatalogCommand("helm", "repo", "list")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(listed), urls[1]) {
		t.Fatalf("image-catalog-0 does not point at the latest chart's repository:\n%s", listed)
	}
}
