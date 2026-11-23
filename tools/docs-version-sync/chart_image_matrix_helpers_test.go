// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const chartImageExclusionsFile = "chart-image-exclusions.yaml"

// Chart exclusion reasons, with reasonPendingFix and reasonDevOnly (a chart
// only an unreleased stack deploys, shared with the stack matrix). Every
// tracked chart that is not covered by an image index carries exactly one of
// them in chart-image-exclusions.yaml.
const (
	// reasonTestFixture: testdata, validator test charts and samples that no
	// NVCF stack deploys.
	reasonTestFixture = "test-fixture"
	// reasonNoRuntimeImage: a chart that renders no image in its defaults or
	// any CI values scenario.
	reasonNoRuntimeImage = "no-runtime-image"
	// reasonVendoredSubchart: an upstream chart vendored under a parent that
	// is checked instead.
	reasonVendoredSubchart = "vendored-subchart"
)

var chartImageReasons = map[string]bool{
	reasonPendingFix: true, reasonTestFixture: true, reasonNoRuntimeImage: true, reasonVendoredSubchart: true, reasonDevOnly: true,
}

// validateChartImageExclusion checks one matrix entry. charts holds every
// tracked chart directory, indexed reports whether the chart declares an
// image index, and stacks says which stacks deploy it.
//
// Rejected, with the reason:
//   - reason "skip": not in the enum, so nothing says how the chart is handled.
//   - a chart directory with no tracked Chart.yaml: the entry is stale.
//   - issue on a test-fixture: only pending work has a tracking issue.
//   - an indexed chart under test-fixture: an index declares runtime images,
//     so the chart must be checked, not skipped. A dev-only chart may be
//     indexed: no released stack ships it.
//   - dev-only for a chart a released stack deploys: see validateDevOnlyChart.
func validateChartImageExclusion(repoRoot, chart string, entry imageExclusion, charts map[string]bool, indexed bool, stacks chartStacks) error {
	if !chartImageReasons[entry.Reason] {
		return fmt.Errorf("unknown reason %q; use one of %s", entry.Reason, strings.Join(sortedKeys(chartImageReasons), ", "))
	}
	if !charts[chart] {
		return fmt.Errorf("no tracked Chart.yaml in this directory; remove the stale entry")
	}
	if entry.Issue != "" && entry.Reason != reasonPendingFix {
		return fmt.Errorf("issue is allowed only on %s entries", reasonPendingFix)
	}
	if entry.Reason == reasonDevOnly {
		return validateDevOnlyChart(chart, stacks)
	}
	if indexed && entry.Reason != reasonPendingFix {
		return fmt.Errorf("chart declares an image index but is listed as %s; remove the entry so the chart is checked", entry.Reason)
	}
	if entry.Reason == reasonVendoredSubchart {
		return validateVendoredSubchart(repoRoot, chart)
	}
	return nil
}

// validateDevOnlyChart allows dev-only only for a chart that no released
// stack deploys, resolved from the released stacks' Helmfile states. Example:
// stargate-dev's charts/stargate-dev-auth passes; once self-managed deploys
// it, or once stargate-dev gains release-inventory.yaml and is built as a
// released stack, the entry fails and the chart is checked.
//
// Where the chart lives is deliberately not checked: a path rule would break
// when a chart moves or is referenced from elsewhere, and it would not prove
// that a dev-only stack deploys the chart either. CI cannot build the
// dev-only stacks (stargate-dev reads a protected values file), so whether
// one of them deploys the chart is not verified. Nor is whether users install
// the chart directly: review keeps dev-only off a chart that ships on its own.
func validateDevOnlyChart(chart string, stacks chartStacks) error {
	if deployers := stacks.deployedBy[chart]; len(deployers) > 0 {
		return fmt.Errorf("chart is deployed by released stacks %s; remove the %s entry so the chart is checked",
			strings.Join(deployers, ", "), reasonDevOnly)
	}
	return nil
}

// validateVendoredSubchart requires a subchart path: <parent>/charts/<name>
// where <parent> is itself a chart. A charts/<name> directory whose parent is
// not a chart, such as a stack's local deploy/stacks/<stack>/charts/<name>, is
// deployed on its own and must be checked as a standalone chart.
func validateVendoredSubchart(repoRoot, chart string) error {
	notSubchart := fmt.Errorf("not a subchart; check as a standalone chart")
	parent := filepath.Dir(filepath.Dir(chart))
	if filepath.Base(filepath.Dir(chart)) != "charts" || parent == "." {
		return notSubchart
	}
	if _, err := os.Stat(filepath.Join(repoRoot, parent, "Chart.yaml")); err != nil {
		return notSubchart
	}
	return nil
}

// chartHasImageIndex reports whether the chart's own values.yaml declares
// thirdPartyImages or ownedImages. The index is the declaration of runtime
// images; classification never depends on what a render happens to produce.
func chartHasImageIndex(chartDir string) (bool, error) {
	raw, err := os.ReadFile(filepath.Join(chartDir, "values.yaml"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var values map[string]any
	if err := yaml.Unmarshal(raw, &values); err != nil {
		return false, fmt.Errorf("parse %s/values.yaml: %w", chartDir, err)
	}
	for _, category := range imageIndexCategories {
		if _, ok := values[category]; ok {
			return true, nil
		}
	}
	return false, nil
}

// chartAction is what the coverage test does with one chart.
type chartAction int

const (
	// actionCheck: indexed and not excluded; any finding fails.
	actionCheck chartAction = iota
	// actionCheckExpectFailure: indexed pending-fix; coverage findings are
	// expected and a clean pass means the entry is stale.
	actionCheckExpectFailure
	// actionExpectedFailure: pending-fix without an index; decided statically
	// so unremediated charts never fetch dependencies.
	actionExpectedFailure
	// actionSkip: test-fixture, vendored-subchart or dev-only, after entry
	// validation.
	actionSkip
	// actionConfirmNoImage: render defaults and every scenario; any image fails.
	actionConfirmNoImage
	// actionUnclassified: neither indexed nor listed; fails with a diagnostic.
	actionUnclassified
)

// classifyChart picks the action for a chart whose matrix entry, if any, is
// already valid (so an indexed chart can only be listed as pending-fix).
func classifyChart(indexed bool, entry *imageExclusion) chartAction {
	switch {
	case entry == nil && indexed:
		return actionCheck
	case entry == nil:
		return actionUnclassified
	case entry.Reason == reasonPendingFix && indexed:
		return actionCheckExpectFailure
	case entry.Reason == reasonPendingFix:
		return actionExpectedFailure
	case entry.Reason == reasonNoRuntimeImage:
		return actionConfirmNoImage
	default:
		return actionSkip
	}
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
