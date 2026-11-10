// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const chartImageExclusionsFile = "chart-image-exclusions.yaml"

// Exclusion reasons. Every tracked chart that is not covered by an image
// index carries exactly one of them in chart-image-exclusions.yaml.
const (
	// pendingFix: an unremediated chart. It is expected to fail coverage until
	// its own fix lands and removes the entry.
	reasonPendingFix = "pending-fix"
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
	reasonPendingFix: true, reasonTestFixture: true, reasonNoRuntimeImage: true, reasonVendoredSubchart: true,
}

// chartImageExclusion is one matrix entry, keyed by repo-root-relative chart directory.
type chartImageExclusion struct {
	Reason string `yaml:"reason"`
	// Issue is a free-form note, such as a tracking link. It is not validated.
	Issue string `yaml:"issue,omitempty"`
}

// loadChartImageExclusions decodes the matrix strictly, so a misspelled field
// fails instead of silently dropping an issue link or reason.
func loadChartImageExclusions(path string) (map[string]chartImageExclusion, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	entries := make(map[string]chartImageExclusion)
	if err := decoder.Decode(&entries); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return entries, nil
}

// validateChartImageExclusion checks one matrix entry. charts holds every
// tracked chart directory, and indexed reports whether the chart declares an
// image index.
//
// Rejected, with the reason:
//   - reason "skip": not in the enum, so nothing says how the chart is handled.
//   - a chart directory with no tracked Chart.yaml: the entry is stale.
//   - issue on a test-fixture: only pending work has a tracking issue.
//   - an indexed chart under test-fixture: an index declares runtime images,
//     so the chart must be checked, not skipped.
func validateChartImageExclusion(repoRoot, chart string, entry chartImageExclusion, charts map[string]bool, indexed bool) error {
	if !chartImageReasons[entry.Reason] {
		return fmt.Errorf("unknown reason %q; use one of %s", entry.Reason, strings.Join(sortedKeys(chartImageReasons), ", "))
	}
	if !charts[chart] {
		return fmt.Errorf("no tracked Chart.yaml in this directory; remove the stale entry")
	}
	if entry.Issue != "" && entry.Reason != reasonPendingFix {
		return fmt.Errorf("issue is allowed only on %s entries", reasonPendingFix)
	}
	if indexed && entry.Reason != reasonPendingFix {
		return fmt.Errorf("chart declares an image index but is listed as %s; remove the entry so the chart is checked", entry.Reason)
	}
	if entry.Reason == reasonVendoredSubchart {
		return validateVendoredSubchart(repoRoot, chart)
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
	// actionSkip: test-fixture or vendored-subchart, after entry validation.
	actionSkip
	// actionConfirmNoImage: render defaults and every scenario; any image fails.
	actionConfirmNoImage
	// actionUnclassified: neither indexed nor listed; fails with a diagnostic.
	actionUnclassified
)

// classifyChart picks the action for a chart whose matrix entry, if any, is
// already valid (so an indexed chart can only be listed as pending-fix).
func classifyChart(indexed bool, entry *chartImageExclusion) chartAction {
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
