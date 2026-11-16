// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	fixIndexImage = "index the image value path in thirdPartyImages or ownedImages, or render it from an indexed image " +
		"through a registered indirect pattern (tools/docs-version-sync/AGENTS.md)"
	fixRender = "make the chart render with only its defaults and its CI values scenarios"
)

// chartCheck runs the image checks for one chart against a temporary copy.
type chartCheck struct {
	chart     string
	source    string
	workspace string
	scenarios []string
	findings  []chartFinding
}

func (c *chartCheck) fail(category, image, path, problem, expected, fix string) {
	c.findings = append(c.findings, chartFinding{Category: category, Chart: c.chart, Image: image, ValuesPath: path,
		Problem: problem, Expected: expected, Fix: fix})
}

// catalogFailure records an unresolvable catalog. A format table key the
// chart does not declare is a configuration error that fails even a
// pending-fix chart; anything else is a coverage finding.
func (c *chartCheck) catalogFailure(problem string, err error, expected, fix string) {
	var keyError *imageFormatKeyError
	if errors.As(err, &keyError) {
		c.fail(findingInvalidEntry, "", "", problem, "every format table key to name a declared dependency",
			"declare the dependency in Chart.yaml or rename the index path root")
		return
	}
	c.fail(findingCoverage, "", "", problem, expected, fix)
}

func (c *chartCheck) renderError(scenario string, err error) {
	c.fail(findingRenderError, "", "", fmt.Sprintf("render %s failed: %v", scenario, err), "a successful helm render", fixRender)
}

// render renders the prepared chart with values files.
func (c *chartCheck) render(chart, scenario string, values ...string) (imageChartSnapshot, bool) {
	snapshot, err := renderImageChart(chart, values)
	if err != nil {
		c.renderError(scenario, err)
		return imageChartSnapshot{}, false
	}
	return snapshot, true
}

func (c *chartCheck) prepare() (string, bool) {
	chart, err := prepareImageChart(c.source, c.workspace)
	if err != nil {
		c.fail(findingRenderError, "", "", fmt.Sprintf("dependency build failed: %v", err), "chart dependencies to build",
			"check Chart.yaml dependencies and registry access")
		return "", false
	}
	return chart, true
}

// checkIndexedChart runs the full check for a chart that declares an image
// index. Findings are coverage-class unless the chart fails to render.
func checkIndexedChart(c *chartCheck) {
	chart, ok := c.prepare()
	if !ok {
		return
	}
	defaults, ok := c.render(chart, "defaults")
	if !ok {
		return
	}
	catalog, err := resolveChartImageCatalog(defaults)
	if err != nil {
		c.catalogFailure(err.Error(), err, "every indexed path to resolve to a complete image reference",
			"write registry, repository and tag (or digest) for the image in the wrapper values.yaml")
		return
	}
	c.checkWrapperDefaults(chart, defaults, catalog)
	c.checkDeclared("defaults", catalog, defaults.Manifest)
	for _, scenario := range c.scenarios {
		c.checkScenario(chart, scenario)
	}
	c.checkOverrides(chart, catalog)
}

// checkWrapperDefaults requires every indexed image to be written in the
// wrapper values.yaml and to equal the effective image, so an operator never
// has to discover a subchart default to mirror it.
func (c *chartCheck) checkWrapperDefaults(chart string, defaults imageChartSnapshot, catalog []catalogImage) {
	raw, err := os.ReadFile(filepath.Join(chart, "values.yaml"))
	if err != nil {
		c.renderError("wrapper values.yaml", err)
		return
	}
	wrapper := imageChartSnapshot{AppVersions: defaults.AppVersions, Dependencies: defaults.Dependencies}
	if err := yaml.Unmarshal(raw, &wrapper.Values); err != nil {
		c.renderError("wrapper values.yaml", err)
		return
	}
	for _, image := range catalog {
		parent, node, err := imageCatalogNodes(wrapper.Values, image.Path)
		if err == nil {
			err = image.Format.visible(parent, node)
		}
		if err != nil {
			c.fail(findingCoverage, image.ID, image.Path, "default not visible in wrapper values: "+err.Error(),
				image.Reference, "write the image registry and repository in the wrapper values.yaml")
			continue
		}
		if written := image.Format.resolve(parent, node, defaults.AppVersions[strings.Split(image.Path, ".")[0]]); written != image.Reference {
			c.fail(findingCoverage, image.ID, image.Path, fmt.Sprintf("wrapper default %s differs from the rendered value", written),
				image.Reference, "make the wrapper values.yaml default match what the chart renders")
		}
	}
}

// checkDeclared fails each rendered image the catalog does not declare.
func (c *chartCheck) checkDeclared(scenario string, catalog []catalogImage, manifest []byte) {
	missing, err := undeclaredImages(catalog, manifest)
	if err != nil {
		c.renderError(scenario, fmt.Errorf("scan images: %w", err))
		return
	}
	for _, reference := range missing {
		c.fail(findingCoverage, "", "", fmt.Sprintf("%s renders undeclared image %s", scenario, reference),
			"every rendered image to be indexed", fixIndexImage)
	}
}

// checkScenario renders one CI values scenario and resolves the catalog from
// the same values, so scenarios that point images elsewhere stay consistent.
func (c *chartCheck) checkScenario(chart, scenario string) {
	name := "scenario " + filepath.Base(scenario)
	snapshot, ok := c.render(chart, name, scenario)
	if !ok {
		return
	}
	catalog, err := resolveChartImageCatalog(snapshot)
	if err != nil {
		c.catalogFailure(fmt.Sprintf("%s: %v", name, err), err, "every indexed path to resolve",
			"keep indexed image values complete in the scenario")
		return
	}
	c.checkDeclared(name, catalog, snapshot.Manifest)
}

// checkOverrides moves every image with an ordinary values file, then applies
// an explicit customer file last. Run on the defaults only, to avoid
// combinatorial growth across scenarios. An image hard-coded outside the
// index stays at its old reference and is caught as undeclared.
func (c *chartCheck) checkOverrides(chart string, catalog []catalogImage) {
	mirrorPath := filepath.Join(c.workspace, "mirror-overrides.yaml")
	explicitPath := filepath.Join(c.workspace, "explicit-overrides.yaml")
	steps := []struct {
		name   string
		target imageOverrideTarget
		path   string
	}{
		{"mirror override", mirrorOverride, mirrorPath},
		{"explicit customer override after the mirror file", explicitOverride, explicitPath},
	}
	var files []string
	for _, step := range steps {
		want, err := writeImageOverrides(catalog, step.target, step.path)
		if err != nil {
			c.renderError(step.name, err)
			return
		}
		files = append(files, step.path)
		snapshot, ok := c.render(chart, step.name, files...)
		if !ok {
			return
		}
		c.checkOverrideReached(step.name, snapshot, want)
	}
}

// checkOverrideReached requires each override to change the resolved value
// and to reach a rendered workload or registered indirect image setting.
func (c *chartCheck) checkOverrideReached(step string, snapshot imageChartSnapshot, want map[string]string) {
	resolved, err := resolveChartImageCatalog(snapshot)
	if err != nil {
		c.catalogFailure(fmt.Sprintf("%s: %v", step, err), err, "every indexed path to resolve after an override",
			"keep the image value shape overridable with ordinary values")
		return
	}
	rendered, err := extractResolvedImages(snapshot.Manifest)
	if err != nil {
		c.renderError(step, fmt.Errorf("scan images: %w", err))
		return
	}
	present := make(map[string]bool)
	for _, image := range rendered {
		present[image.Reference] = true
	}
	for _, image := range resolved {
		switch {
		case image.Reference != want[image.ID]:
			c.fail(findingCoverage, image.ID, image.Path, fmt.Sprintf("%s did not take effect, resolved %s", step, image.Reference),
				want[image.ID], "render the image from its indexed values without precedence over ordinary overrides")
		case !present[image.Reference]:
			c.fail(findingCoverage, image.ID, image.Path, step+" did not reach the rendered manifest",
				want[image.ID], "render the indexed value in a workload image field or a registered indirect pattern")
		}
	}
	c.checkDeclared(step, resolved, snapshot.Manifest)
}

// confirmNoRuntimeImage renders the defaults and every scenario; any image or
// render failure means the no-runtime-image entry is wrong.
func confirmNoRuntimeImage(c *chartCheck) {
	chart, ok := c.prepare()
	if !ok {
		return
	}
	renders := append([]string{""}, c.scenarios...)
	for _, scenario := range renders {
		name, values := "defaults", []string(nil)
		if scenario != "" {
			name, values = "scenario "+filepath.Base(scenario), []string{scenario}
		}
		snapshot, ok := c.render(chart, name, values...)
		if !ok {
			continue
		}
		images, err := extractResolvedImages(snapshot.Manifest)
		if err != nil {
			c.renderError(name, fmt.Errorf("scan images: %w", err))
			continue
		}
		for _, image := range images {
			c.fail(findingInvalidEntry, "", "", fmt.Sprintf("listed as %s but %s renders %s", reasonNoRuntimeImage, name, image.Reference),
				"no rendered image", "declare an image index and remove the entry, or list the chart as pending-fix")
		}
	}
}

// diagnoseUnclassified renders the defaults once to shape the message for a
// chart that is neither indexed nor listed. The finding is the same either
// way; the index, not the render, is the declaration of runtime images.
func diagnoseUnclassified(c *chartCheck) {
	const expected = "an image index in values.yaml or an entry in tools/docs-version-sync/" + chartImageExclusionsFile
	problem, fix := "chart has no image index and no exclusion entry", "declare thirdPartyImages or ownedImages in values.yaml"
	chart, err := prepareImageChart(c.source, c.workspace)
	var snapshot imageChartSnapshot
	if err == nil {
		snapshot, err = renderImageChart(chart, nil)
	}
	var images []parsedImageReference
	if err == nil {
		images, err = extractResolvedImages(snapshot.Manifest)
	}
	switch {
	case err != nil:
		problem += fmt.Sprintf(" (defaults do not render: %v)", err)
		fix += ", or classify the chart in the exclusion matrix"
	case len(images) > 0:
		var references []string
		for _, image := range images {
			references = append(references, image.Reference)
		}
		problem += "; defaults render " + strings.Join(references, ", ")
	default:
		problem += "; defaults render no image"
		fix = fmt.Sprintf("add \"%s: {reason: %s}\" and a CI values scenario that enables its optional resources", c.chart, reasonNoRuntimeImage)
	}
	c.fail(findingUncovered, "", "", problem, expected, fix)
}
