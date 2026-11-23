// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Chart classes. The class decides who supplies a chart's image source.
const (
	// classStackDeployed: a released stack deploys the chart and supplies its
	// image source from the stack's base.yaml, so the chart leaves registry
	// and repository empty.
	classStackDeployed = "stack-deployed"
	// classStandalone: no released stack deploys the chart. It is installed
	// directly with Helm, so its values.yaml is the outermost configuration
	// and must prefill a working registry and repository (issue #3652).
	classStandalone = "standalone"
)

// chartStacks is what the chart gate knows about stacks: which released
// stacks deploy each tracked chart.
type chartStacks struct {
	// deployedBy maps a tracked chart directory to the released stacks whose
	// states declare a release of it.
	deployedBy map[string][]string
}

func (s chartStacks) class(chart string) string {
	if len(s.deployedBy[chart]) > 0 {
		return classStackDeployed
	}
	return classStandalone
}

// chartSubject labels a chart with its class in findings and summary lines,
// such as "deploy/helm/nvstor (standalone)".
func chartSubject(chart, class string) string {
	return chart + " (" + class + ")"
}

// loadChartStacks resolves the charts every released stack deploys. A state
// that cannot be built or a release whose chart cannot be located is returned
// as a finding: classifying its charts as standalone would wrongly allow them
// to prefill.
//
// directories holds every tracked chart directory.
func loadChartStacks(repoRoot string, directories []string, workspace func() string) (chartStacks, []imageGateFinding, error) {
	tracked, err := discoverTrackedStacks(repoRoot)
	if err != nil {
		return chartStacks{}, nil, err
	}
	deployedBy, findings, err := releasedStackCharts(repoRoot, tracked, directories, workspace)
	return chartStacks{deployedBy: deployedBy}, findings, err
}

// releasedStackCharts maps each tracked chart directory to the released
// stacks that declare a release of it. It builds every state the stack image
// source check renders (stackRenderStates) and locates each release's chart
// as that check does, without rendering or pulling charts.
//
// directories holds every tracked chart directory, the same set the chart
// gate checks; a located chart outside it is an error.
//
// A release counts whether or not it is enabled or installed: once an
// operator enables it, the stack still supplies its image source.
//
// Known limit, shared with the stack image source check: states are built
// with the probe environment only, so a release that a state declares only
// in another environment is not seen, and its chart is classified
// standalone. Remove this note when classification builds every environment.
func releasedStackCharts(repoRoot string, tracked trackedStacks, directories []string, workspace func() string) (map[string][]string, []imageGateFinding, error) {
	charts, err := trackedChartsByName(repoRoot)
	if err != nil {
		return nil, nil, err
	}
	isChart := make(map[string]bool)
	for _, directory := range directories {
		isChart[directory] = true
	}
	space, err := newStackWorkspace(repoRoot, tracked, charts, workspace)
	if err != nil {
		return nil, nil, err
	}
	if err := writeProbeEnvironment(space.copyRoot, "", tracked.stacks, nil); err != nil {
		return nil, nil, err
	}
	deployedBy := make(map[string][]string)
	var findings []imageGateFinding
	for _, stack := range tracked.stacks {
		// Released means having release-inventory.yaml. A stack without one
		// is dev-only or reported by the stack image source check.
		if !tracked.hasFile(stack, stackInventoryFile) {
			continue
		}
		// stackRenderStates also reports inventory problems, such as a state
		// it does not list. The stack check reports them too; they are kept
		// here on purpose, because the charts of a state that is not built
		// would otherwise be classified standalone.
		check := &stackCheck{stack: stack}
		for _, state := range stackRenderStates(repoRoot, stack, tracked, check) {
			declared, err := space.declaredCharts(state, isChart)
			if err != nil {
				check.fail(findingStackConfig, "", "", state.Path, strings.Join(strings.Fields(err.Error()), " "),
					"a state whose releases resolve to their charts", "fix the state; the chart classes depend on it")
				continue
			}
			for _, chart := range declared {
				if !slices.Contains(deployedBy[chart], stack) {
					deployedBy[chart] = append(deployedBy[chart], stack)
				}
			}
		}
		findings = append(findings, check.findings...)
	}
	return deployedBy, findings, nil
}

// declaredCharts returns the tracked chart directory of every release the
// state and its included states declare. Releases of untrackedStackCharts and
// of third-party repositories have no tracked chart to classify. isChart holds
// every tracked chart directory: a local chart path outside it, such as a
// typo, is an error rather than a key that classifies nothing.
func (w *stackWorkspace) declaredCharts(state stackRenderState, isChart map[string]bool) ([]string, error) {
	built, err := w.helmfile(state, "build")
	if err != nil {
		return nil, fmt.Errorf("build %s: %w", state.Path, err)
	}
	documents, err := parseHelmfileBuild(built)
	if err != nil {
		return nil, err
	}
	var charts []string
	for _, document := range documents {
		for _, release := range document.Releases {
			location, err := w.locateChart(document, release.Chart)
			var untracked untrackedChartError
			switch {
			case errors.As(err, &untracked):
				continue
			case err != nil:
				return nil, fmt.Errorf("release %s/%s: %w", release.Namespace, release.Name, err)
			case location.Tracked != "" && !isChart[location.Tracked]:
				return nil, fmt.Errorf("release %s/%s: chart %s is not a tracked chart directory", release.Namespace, release.Name, release.Chart)
			case location.Tracked != "":
				charts = append(charts, location.Tracked)
			}
		}
	}
	return charts, nil
}
