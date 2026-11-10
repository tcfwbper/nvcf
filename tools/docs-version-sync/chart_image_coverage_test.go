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
// pending-fix chart: only coverage findings are expected. It returns the
// findings that fail the run, or the expected-failure label for the summary.
// A render, dependency or format-key error still fails, and a clean pass
// means the entry is stale.
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
	finding := chartFinding{Category: findingCoverage, Chart: "deploy/helm/x", Image: "server", ValuesPath: "x.server.image",
		Problem: "default not visible", Expected: "nvcr.io/x:1", Fix: "write it"}
	if got, want := finding.String(), "FAIL deploy/helm/x server x.server.image: default not visible; expected nvcr.io/x:1; fix: write it"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	finding.Image, finding.ValuesPath = "", ""
	if got := finding.String(); !strings.HasPrefix(got, "FAIL deploy/helm/x: default") {
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
		{"format key error fails", []chartFinding{{Category: findingCoverage}, {Category: findingInvalidEntry}}, findingInvalidEntry, false},
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
// image expressions (Helm template text).
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

// TestChartImageCoverageReportsEachFailure drives the whole evaluation on a
// fixture repository, so each rejection path is pinned by a committed test
// rather than by the real charts, which currently exercise only the passing
// and expected-failure paths.
func TestChartImageCoverageReportsEachFailure(t *testing.T) {
	if os.Getenv("NVCF_IMAGE_CATALOG_E2E") != "1" {
		t.Skip("set NVCF_IMAGE_CATALOG_E2E=1 to render fixture charts with Helm")
	}
	isolateHelmConfiguration(t)
	const indexed = "ownedImages: {server: server.image}\nserver:\n  image: {registry: r.example, repository: team/server, tag: v1}\n"
	const fromValues = "{{ .Values.server.image.registry }}/{{ .Values.server.image.repository }}:{{ .Values.server.image.tag }}"
	files := map[string]string{
		chartValuesMapFile:                        "scenarios:\n  - {chart: charts/no-image, values: no-image-sidecar.yaml}\n",
		chartValuesDir + "/no-image-sidecar.yaml": "sidecar: r.example/sidecar:v1\n",
		chartImageExclusionsFile: "charts/no-image: {reason: no-runtime-image}\n" +
			"charts/pending-passing: {reason: pending-fix}\n" +
			"charts/pending-failing: {reason: pending-fix, issue: https://github.com/dsx-ai-factory/nvcf/issues/3185}\n" +
			"charts/pending-static: {reason: pending-fix}\n" +
			"charts/format-key: {reason: pending-fix}\n" +
			"charts/fixture: {reason: test-fixture}\n" +
			"charts/no-image-ok: {reason: no-runtime-image}\n" +
			"charts/gone: {reason: test-fixture}\n",
	}
	for _, chart := range []map[string]string{
		coverageFixtureChart("covered", indexed, fromValues),
		// The same reference hard-coded: passes on defaults, caught once the
		// mirror override moves the indexed value.
		coverageFixtureChart("hidden", indexed, fromValues, "r.example/team/server:v1"),
		coverageFixtureChart("undeclared", indexed, fromValues, "r.example/sidecar:v1"),
		// The template supplies the registry, so the wrapper default is not visible.
		coverageFixtureChart("invisible", "ownedImages: {server: server.image}\nserver:\n  image: {repository: team/server, tag: v1}\n",
			`{{ .Values.server.image.registry | default "r.example" }}/{{ .Values.server.image.repository }}:{{ .Values.server.image.tag }}`),
		// pending-fix with an ordinary coverage error too: the undeclared
		// format key must still fail the run.
		coverageFixtureChart("format-key", "thirdPartyImages: {server: nats.image, aa: missing.image}\nnats:\n  image: {registry: r.example, repository: nats, tag: v1}\n",
			"{{ .Values.nats.image.registry }}/{{ .Values.nats.image.repository }}:{{ .Values.nats.image.tag }}"),
		coverageFixtureChart("no-image", "sidecar: \"\"\n"),
		coverageFixtureChart("unlisted", "sidecar: \"\"\n"),
		// A correct no-runtime-image entry produces no finding.
		coverageFixtureChart("no-image-ok", "sidecar: \"\"\n"),
		coverageFixtureChart("pending-passing", indexed, fromValues),
		coverageFixtureChart("pending-failing", indexed, fromValues, "r.example/team/server:v1"),
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
		problems[finding.Chart] = append(problems[finding.Chart], finding.Problem)
	}
	// Each fixture names the check that must report it, so another check
	// producing the same category cannot stand in for a broken one.
	want := map[string]struct{ category, problem string }{
		"charts/hidden":          {findingCoverage, "mirror override renders undeclared image r.example/team/server:v1"},
		"charts/undeclared":      {findingCoverage, "defaults renders undeclared image r.example/sidecar:v1"},
		"charts/invisible":       {findingCoverage, "default not visible in wrapper values"},
		"charts/format-key":      {findingInvalidEntry, "declared nats dependency"},
		"charts/no-image":        {findingInvalidEntry, "listed as no-runtime-image but scenario no-image-sidecar.yaml renders"},
		"charts/unlisted":        {findingUncovered, "defaults render no image"},
		"charts/pending-passing": {findingPendingFixed, "now passes image coverage"},
		"charts/gone":            {findingInvalidEntry, "no tracked Chart.yaml"},
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
	if strings.Join(report.Covered, ",") != "charts/covered" {
		t.Errorf("covered %v", report.Covered)
	}
	expected := strings.Join(report.Expected, "\n")
	for _, chart := range []string{"charts/pending-failing", "issues/3185", "charts/pending-static (no image index)"} {
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
