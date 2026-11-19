// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	fixIndexImage = "index the image value path in thirdPartyImages or ownedImages, or render it from an indexed image " +
		"through a registered indirect pattern (tools/docs-version-sync/AGENTS.md)"
	fixRender   = "make the chart render with each of its CI values scenarios"
	fixScenario = "supply a non-empty registry and repository for every indexed image in each CI values scenario of the chart (" +
		chartValuesMapFile + ")"
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

// catalogFailure records an unusable image index or catalog. A malformed
// index or an exception key the chart does not declare is a configuration
// error that fails even a pending-fix chart; anything else is a rule finding.
func (c *chartCheck) catalogFailure(problem string, err error, expected, fix string) {
	var keyError *imageExceptionKeyError
	var indexError *imageIndexError
	switch {
	case errors.As(err, &keyError):
		c.fail(findingInvalidEntry, "", "", problem, "every image value exception key to name a declared dependency",
			"declare the dependency in Chart.yaml or rename the index path root")
	case errors.As(err, &indexError):
		c.fail(findingInvalidEntry, "", "", problem, "a valid thirdPartyImages or ownedImages index",
			"map each image identity once to the dotted values path of its image object")
	default:
		c.fail(findingCoverage, "", "", problem, expected, fix)
	}
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

// checkIndexedChart runs every check for a chart that declares an image
// index. The chart's own values.yaml and templates are checked statically for
// an implicit image source; the renders then prove that the chart rejects a
// missing source and that each CI values scenario renders only indexed,
// overridable images.
func checkIndexedChart(c *chartCheck) {
	chart, ok := c.prepare()
	if !ok {
		return
	}
	values, catalog, ok := c.readIndex(chart)
	if !ok {
		return
	}
	c.checkChartSources(values, catalog)
	c.checkTemplateSources(chart)
	c.checkDefaultsRejected(chart, catalog)
	if len(c.scenarios) == 0 {
		c.fail(findingScenario, "", "", "indexed chart has no CI values scenario",
			"at least one scenario in "+chartValuesMapFile, fixScenario)
	}
	for _, scenario := range c.scenarios {
		c.checkScenario(chart, scenario, catalog)
	}
}

// readIndex reads the chart's own values.yaml and its image index. Image
// identity comes from the index, never from a render of the defaults.
func (c *chartCheck) readIndex(chart string) (map[string]any, []catalogImage, bool) {
	dependencies, err := chartDependencyRoots(chart)
	if err != nil {
		c.renderError("Chart.yaml", err)
		return nil, nil, false
	}
	raw, err := os.ReadFile(filepath.Join(chart, "values.yaml"))
	if err != nil {
		c.renderError("values.yaml", err)
		return nil, nil, false
	}
	var values map[string]any
	if err := yaml.Unmarshal(raw, &values); err != nil {
		c.renderError("values.yaml", err)
		return nil, nil, false
	}
	catalog, err := readImageIndex(values, dependencies)
	if err != nil {
		c.catalogFailure(err.Error(), err, "", "")
		return nil, nil, false
	}
	return values, catalog, true
}

// checkDefaultsRejected requires a render with only the chart defaults to
// fail on an image source guard: Helm must reject the render with an error
// that names an indexed registry or repository path. A rejection for another
// reason, such as an unrelated required value, does not show that the chart
// refuses a missing source. A render that could not run is a render error.
// Whether every image is guarded is proven image by image in checkGuards.
func (c *chartCheck) checkDefaultsRejected(chart string, catalog []catalogImage) {
	const expected = "the defaults render to fail with a guard naming an indexed registry or repository path"
	_, err := renderImageChart(chart, nil)
	switch {
	case err == nil:
		c.fail(findingCoverage, "", "", "defaults render without an image source", expected,
			"guard every indexed registry and repository with required, in the wrapper for subchart images")
	case !renderRejected(err):
		c.renderError("defaults", err)
	case !guardNamesSourcePath(err.Error(), catalog):
		c.fail(findingCoverage, "", "", "defaults render fails without naming an image source path: "+firstLine(err.Error()), expected,
			"guard the indexed registry and repository before other required values, and name the path in the message")
	}
}

// guardNamesSourcePath reports whether a render error names the registry or
// repository path of any indexed image.
func guardNamesSourcePath(message string, catalog []catalogImage) bool {
	for _, image := range catalog {
		for _, field := range imageSourceFields {
			if guardNamesPath(message, image.Path+"."+field) {
				return true
			}
		}
	}
	return false
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

// checkScenario checks one CI values scenario: it supplies every image
// source, renders only indexed images, carries ordinary overrides to every
// image, and fails with a named guard when a source is emptied.
func (c *chartCheck) checkScenario(chart, scenario string, catalog []catalogImage) {
	name := "scenario " + filepath.Base(scenario)
	if !c.checkScenarioSources(name, scenario, catalog) {
		// Rendering would only report the chart's guard for the gap.
		return
	}
	snapshot, ok := c.render(chart, name, scenario)
	if !ok {
		return
	}
	resolved, err := resolveImageCatalog(snapshot, catalog)
	if err != nil {
		c.catalogFailure(fmt.Sprintf("%s: %v", name, err), err, "every indexed path to resolve to a complete image reference", fixScenario)
		return
	}
	c.checkDeclared(name, resolved, snapshot.Manifest)
	c.checkOverrides(chart, name, scenario, catalog, resolved)
	c.checkGuards(chart, name, scenario, resolved)
}

// checkScenarioSources requires the scenario file itself to supply registry
// and repository for every indexed image, so its render never depends on a
// prefilled chart source. The host rule does not apply: scenarios are test
// data and may spell a source any way the chart accepts. It reports whether
// the scenario is complete.
func (c *chartCheck) checkScenarioSources(name, scenario string, catalog []catalogImage) bool {
	raw, err := os.ReadFile(scenario)
	if err != nil {
		c.renderError(name, err)
		return false
	}
	var values map[string]any
	if err := yaml.Unmarshal(raw, &values); err != nil {
		c.renderError(name, err)
		return false
	}
	complete := true
	for _, image := range catalog {
		node, _ := imageCatalogValue(values, image.Path)
		for _, field := range imageSourceFields {
			if imageCatalogString(node, field) == "" {
				c.fail(findingScenario, image.ID, image.Path+"."+field, name+" does not supply "+field,
					"a non-empty "+field, fixScenario)
				complete = false
			}
		}
	}
	return complete
}

// checkOverrides moves every image with an ordinary values file layered on
// the scenario, then applies an explicit customer file last. An image
// hard-coded outside the index stays at its old reference and is caught as
// undeclared.
func (c *chartCheck) checkOverrides(chart, name, scenario string, index, catalog []catalogImage) {
	stem := strings.TrimSuffix(filepath.Base(scenario), ".yaml")
	steps := []struct {
		name   string
		target imageOverrideTarget
		path   string
	}{
		{name + " + mirror override", mirrorOverride, filepath.Join(c.workspace, stem+"-mirror.yaml")},
		{name + " + explicit customer override after the mirror file", explicitOverride, filepath.Join(c.workspace, stem+"-explicit.yaml")},
	}
	files := []string{scenario}
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
		c.checkOverrideReached(step.name, snapshot, index, want)
	}
}

// checkOverrideReached requires each override to change the resolved value
// and to reach a rendered workload or registered indirect image setting.
func (c *chartCheck) checkOverrideReached(step string, snapshot imageChartSnapshot, index []catalogImage, want map[string]string) {
	resolved, err := resolveImageCatalog(snapshot, index)
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

// checkGuards empties one source field of one image at a time on top of the
// scenario. The render must fail, and the error must name the values path to
// set. A failing defaults render alone does not prove this: a chart that
// guards only its first image would pass it while rendering the next image
// with an implicit source.
func (c *chartCheck) checkGuards(chart, name, scenario string, catalog []catalogImage) {
	for _, image := range catalog {
		for _, field := range imageSourceFields {
			path := image.Path + "." + field
			values := make(map[string]any)
			setImageValue(values, path, "")
			encoded, err := yaml.Marshal(values)
			if err != nil {
				c.renderError(name, err)
				return
			}
			blank := filepath.Join(c.workspace, "guard-"+strings.ReplaceAll(path, ".", "-")+".yaml")
			if err := os.WriteFile(blank, encoded, 0o600); err != nil {
				c.renderError(name, err)
				return
			}
			_, err = renderImageChart(chart, []string{scenario, blank})
			switch {
			case err != nil && !renderRejected(err):
				c.renderError(fmt.Sprintf("%s with an empty %s", name, path), err)
			case err == nil:
				c.fail(findingCoverage, image.ID, path, fmt.Sprintf("%s renders with an empty %s", name, field),
					"the render to fail with an error naming "+path,
					"guard "+path+" with required, in the wrapper for subchart images")
			case !guardNamesPath(err.Error(), path):
				c.fail(findingCoverage, image.ID, path, fmt.Sprintf("%s with an empty %s fails without naming it: %s", name, field, firstLine(err.Error())),
					"the error to name "+path, "name "+path+" in the required message")
			}
		}
	}
}

// guardNamesPath reports whether a render error names the values path itself,
// not a longer path that contains it. A hyphen continues a path token, as in
// a values key such as cert-manager, and Helm's .Values. prefix is accepted.
//
//	"set x.image.registry."            names x.image.registry
//	"set .Values.x.image.registry"     names x.image.registry
//	"set x.image.registryMirror"       does not
//	"set x.image.registry.sub"         does not
//	"set x.image.registry-backup"      does not
//	"set .Values.y.x.image.registry"   does not
func guardNamesPath(message, path string) bool {
	return regexp.MustCompile(`(^|[^\w.-])(\.Values\.)?` + regexp.QuoteMeta(path) + `($|[^\w.-]|\.($|[^\w-]))`).MatchString(message)
}

func firstLine(text string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(text), "\n")
	return line
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
