// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Finding categories, counted separately in the summary.
const (
	findingUncovered    = "uncovered"
	findingPendingFixed = "pending-fix now passing"
	findingInvalidEntry = "stale or invalid entry"
	findingRenderError  = "render error"
	findingCoverage     = "coverage"
)

var findingCategories = []string{findingUncovered, findingPendingFixed, findingInvalidEntry, findingRenderError, findingCoverage}

// chartFinding is one line of the failure report.
type chartFinding struct {
	Category   string
	Chart      string
	Image      string
	ValuesPath string
	Problem    string
	Expected   string
	Fix        string
}

// String renders "FAIL <chart> [<image id>] [<values path>]: <problem>; expected <x>; fix: <y>".
func (f chartFinding) String() string {
	subject := f.Chart
	for _, part := range []string{f.Image, f.ValuesPath} {
		if part != "" {
			subject += " " + part
		}
	}
	return fmt.Sprintf("FAIL %s: %s; expected %s; fix: %s", subject, f.Problem, f.Expected, f.Fix)
}

// chartCoverageReport aggregates every chart before the test fails, so one
// run lists every problem.
type chartCoverageReport struct {
	Findings []chartFinding
	// Skipped maps a reason to the chart directories skipped for it.
	Skipped map[string][]string
	// Expected lists pending-fix charts that failed as expected, with their
	// issue when present.
	Expected []string
	Covered  []string
}

func (r *chartCoverageReport) add(findings ...chartFinding) {
	r.Findings = append(r.Findings, findings...)
}

// Summary counts failures by category and lists every deliberate downgrade
// (skips and expected failures), so they are visible without verbose logs.
func (r *chartCoverageReport) Summary() string {
	var out strings.Builder
	counts := make(map[string]int)
	for _, finding := range r.Findings {
		counts[finding.Category]++
		fmt.Fprintln(&out, finding)
	}
	fmt.Fprintf(&out, "chart image coverage: %d covered, %d failures", len(r.Covered), len(r.Findings))
	for _, category := range findingCategories {
		if counts[category] > 0 {
			fmt.Fprintf(&out, ", %s: %d", category, counts[category])
		}
	}
	fmt.Fprintln(&out)
	for _, reason := range sortedKeys(r.Skipped) {
		fmt.Fprintf(&out, "skipped (%s): %s\n", reason, strings.Join(r.Skipped[reason], ", "))
	}
	if len(r.Expected) > 0 {
		fmt.Fprintf(&out, "expected failures (%s, %d): %s\n", reasonPendingFix, len(r.Expected), strings.Join(r.Expected, ", "))
	}
	return out.String()
}

// discoverTrackedCharts returns the directory of every tracked Chart.yaml,
// relative to the repository root.
func discoverTrackedCharts(repoRoot string) ([]string, error) {
	raw, err := imageCatalogCommand("git", "-C", repoRoot, "ls-files", "--", "Chart.yaml", "**/Chart.yaml")
	if err != nil {
		return nil, err
	}
	var charts []string
	for _, file := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if filepath.Base(file) == "Chart.yaml" {
			charts = append(charts, filepath.Dir(file))
		}
	}
	sort.Strings(charts)
	if len(charts) == 0 {
		return nil, fmt.Errorf("no tracked Chart.yaml under %s", repoRoot)
	}
	return charts, nil
}

const chartValuesMapFile = "tools/ci/helm-chart-values.yaml"
const chartValuesDir = "tools/ci/helm-validate-values"

// loadChartScenarios reads the CI values scenarios per chart from the chart
// values map, which check-helm-charts also reads and wires to its files.
// Chart keys are compared with git ls-files output, so a non-canonical path
// such as "./deploy/x" or "deploy/x/" is rejected rather than silently never
// matching a chart (check-helm-charts would still render it).
func loadChartScenarios(repoRoot string, tracked map[string]bool) (map[string][]string, error) {
	raw, err := os.ReadFile(filepath.Join(repoRoot, chartValuesMapFile))
	if err != nil {
		return nil, err
	}
	var document struct {
		Scenarios []struct {
			Chart  string `yaml:"chart"`
			Values string `yaml:"values"`
		} `yaml:"scenarios"`
	}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("parse %s: %w", chartValuesMapFile, err)
	}
	scenarios := make(map[string][]string)
	for index, entry := range document.Scenarios {
		switch {
		case entry.Chart == "" || entry.Chart != filepath.Clean(entry.Chart) || filepath.IsAbs(entry.Chart) || strings.HasPrefix(entry.Chart, ".."):
			return nil, fmt.Errorf("%s entry %d: chart %q must be a canonical repo-root-relative directory", chartValuesMapFile, index+1, entry.Chart)
		case entry.Values != filepath.Base(entry.Values) || !strings.HasSuffix(entry.Values, ".yaml"):
			return nil, fmt.Errorf("%s entry %d: values %q must be a .yaml file name in %s", chartValuesMapFile, index+1, entry.Values, chartValuesDir)
		case !tracked[entry.Chart]:
			return nil, fmt.Errorf("%s entry %d: chart %q has no tracked Chart.yaml", chartValuesMapFile, index+1, entry.Chart)
		}
		scenarios[entry.Chart] = append(scenarios[entry.Chart], filepath.Join(repoRoot, chartValuesDir, entry.Values))
	}
	return scenarios, nil
}

// prepareImageChart copies a chart into a temporary directory and builds its
// dependencies there, so the source tree is never changed.
//
// External access: charts with remote dependencies (the NATS HTTPS repository,
// the OpenBao OCI registry) are fetched here, as the Helm CI job already does
// for check-helm-charts. Callers isolate HELM_REPOSITORY_CONFIG,
// HELM_REPOSITORY_CACHE and HELM_REGISTRY_CONFIG. A network failure surfaces as
// a render error and fails the run.
func prepareImageChart(source, workspace string) (string, error) {
	chart := filepath.Join(workspace, "chart")
	if err := copyResolvedInventoryStack(source, chart); err != nil {
		return "", fmt.Errorf("copy chart: %w", err)
	}
	needsBuild, httpsRepositories, err := chartDependencyFetches(chart)
	if err != nil || !needsBuild {
		return chart, err
	}
	if err := registerImageCatalogRepositories(httpsRepositories); err != nil {
		return "", err
	}
	if _, err := imageCatalogCommand("helm", "dependency", "build", "--skip-refresh", chart); err != nil {
		return "", err
	}
	return chart, nil
}

// registerImageCatalogRepositories registers one chart's HTTPS dependency
// repositories as image-catalog-<index> in the run's shared Helm config.
// --force-update lets a later chart reuse a name at a different URL: without
// it Helm fails with "repository name already exists", a false render error.
// Overwriting is safe because charts are prepared one at a time and each
// builds its dependencies right after registering them.
func registerImageCatalogRepositories(repositories []string) error {
	for index, repository := range repositories {
		if _, err := imageCatalogCommand("helm", "repo", "add", "--force-update", fmt.Sprintf("image-catalog-%d", index), repository); err != nil {
			return err
		}
	}
	return nil
}

// chartDependencyFetches reports whether a chart needs helm dependency build,
// and which HTTPS repositories to register for it. A file:// dependency that
// is already vendored as charts/<dir> renders without a build.
func chartDependencyFetches(chart string) (bool, []string, error) {
	dependencies, err := readChartDependencies(chart)
	if err != nil {
		return false, nil, err
	}
	needsBuild := false
	var https []string
	for _, dependency := range dependencies {
		local, isFile := strings.CutPrefix(dependency.Repository, "file://")
		if isFile {
			if _, err := os.Stat(filepath.Join(chart, local, "Chart.yaml")); err == nil && strings.HasPrefix(filepath.Clean(local), "charts/") {
				continue
			}
		}
		needsBuild = true
		if strings.HasPrefix(dependency.Repository, "https://") {
			https = append(https, dependency.Repository)
		}
	}
	return needsBuild, https, nil
}

// undeclaredImages returns rendered images, direct or through a registered
// indirect pattern, that the catalog does not declare.
func undeclaredImages(catalog []catalogImage, manifest []byte) ([]string, error) {
	declared := make(map[string]bool)
	for _, image := range catalog {
		declared[image.Reference] = true
	}
	actual, err := extractResolvedImages(manifest)
	if err != nil {
		return nil, err
	}
	var missing []string
	for _, image := range actual {
		if !declared[image.Reference] {
			missing = append(missing, image.Reference)
		}
	}
	return missing, nil
}

// checkChartImageCoverage uses the release scanner so indirect image settings
// are subject to the same declaration check as Pod and hook images.
func checkChartImageCoverage(catalog []catalogImage, manifest []byte) error {
	missing, err := undeclaredImages(catalog, manifest)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return fmt.Errorf("rendered images %s are absent from the chart image catalog", strings.Join(missing, ", "))
	}
	return nil
}

// imageOverrideTarget is where an override check moves every indexed image.
type imageOverrideTarget struct {
	Registry string
	// Repository prefixes the lower-cased image identity.
	Repository string
	Digest     string
}

var (
	mirrorOverride   = imageOverrideTarget{Registry: "mirror.example:5000", Repository: "renamed", Digest: "sha256:" + strings.Repeat("a", 64)}
	explicitOverride = imageOverrideTarget{Registry: "explicit.example", Repository: "customer", Digest: "sha256:" + strings.Repeat("b", 64)}
)

// writeImageOverrides writes an ordinary values file that moves every image to
// the target in its chart's own override shape, and returns the reference
// each image identity must then render.
func writeImageOverrides(catalog []catalogImage, target imageOverrideTarget, path string) (map[string]string, error) {
	values := make(map[string]any)
	want := make(map[string]string)
	for _, image := range catalog {
		parsed, err := parseResolvedImageReference(image.Reference)
		if err != nil {
			return nil, err
		}
		version := parsed.Version
		if version == "" {
			// A digest-only default still gets a tag, so every format's
			// tag and digest fields are exercised.
			version = "override"
		}
		repository := target.Repository + "/" + strings.ToLower(image.ID)
		setImageValues(values, image.Path, image.Format.override(target.Registry, repository, version, target.Digest))
		want[image.ID] = target.Registry + "/" + repository + ":" + version + "@" + target.Digest
	}
	encoded, err := yaml.Marshal(values)
	if err != nil {
		return nil, err
	}
	return want, os.WriteFile(path, encoded, 0o600)
}

// setImageValues places override fields at a dotted path. Helm merges them
// with the chart defaults when the file is rendered.
func setImageValues(values map[string]any, path string, fields map[string]any) {
	keys := strings.Split(path, ".")
	for _, key := range keys[:len(keys)-1] {
		next, ok := values[key].(map[string]any)
		if !ok {
			next = make(map[string]any)
			values[key] = next
		}
		values = next
	}
	values[keys[len(keys)-1]] = fields
}
