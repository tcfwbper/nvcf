// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	stackImageExclusionsFile = "stack-image-exclusions.yaml"
	stacksRoot               = "deploy/stacks"
	stackInventoryFile       = "release-inventory.yaml"
	stackBaseValuesFile      = "environments/base.yaml"
)

// reasonDevOnly marks an unreleased development stack. It is skipped, and it
// is eligible only while the stack has no release-inventory.yaml: a stack
// that starts releasing must supply its image sources from base.yaml. For
// example, stargate-dev reads one environment per region and supplies real
// images from a protected values file outside the repository.
const reasonDevOnly = "dev-only"

var stackImageReasons = map[string]bool{reasonPendingFix: true, reasonDevOnly: true}

// Stack finding categories, counted separately in the summary.
const (
	// findingImageSource: a rule finding. It is the only category a
	// pending-fix stack is expected to produce.
	findingImageSource = "image source"
	// findingStackConfig: the stack cannot be checked as configured, such as
	// a released stack without a release inventory, a state the inventory
	// does not render, or an included Helmfile outside deploy/stacks. The
	// chart gate also reports it for a released stack state whose charts
	// cannot be resolved, so their classes are unknown.
	findingStackConfig = "stack configuration"
)

var stackFindingCategories = []string{findingPendingFixed, findingInvalidEntry, findingRenderError, findingStackConfig, findingImageSource}

// stackImageGate names the stack matrix in its report and its pending-fix
// messages.
var stackImageGate = imageGateMatrix{File: stackImageExclusionsFile, Noun: "stack", Check: "image sources"}

func newStackImageReport() *imageGateReport {
	return &imageGateReport{Name: "stack image sources", Categories: stackFindingCategories, Skipped: make(map[string][]string)}
}

// stackExtraStates lists Helmfile states a stack deploys that its release
// inventory does not render, with the overrides to render them. The
// self-managed observability include deploys the observability stack, which
// publishes its own inventory, so the self-managed inventory leaves it out;
// the gate renders it to check what self-managed deploys by default. Remove an
// entry when its state moves out of the stack or into the inventory.
var stackExtraStates = map[string][]string{
	"deploy/stacks/self-managed/helmfile.d/00-observability-infrastructure.yaml.gotmpl": {"observability.profile=all"},
}

// untrackedStackCharts lists charts that a stack deploys from its own chart
// repository but whose source this repository does not track. The probe
// cannot render them: the published charts need registry credentials that
// pull request CI does not have. Their releases are skipped and listed in the
// summary with the reason given here. Remove an entry when the chart's source
// lands in this repository; the check fails until then. Any other untracked
// chart fails the run.
var untrackedStackCharts = map[string]string{
	"helm-nvcf-state-metrics": "chart source is not tracked in this repository",
}

const skippedUntrackedChart = "untracked chart"

// trackedStacks maps each stack directory to its tracked files, relative to
// the repository root. A stack is a deploy/stacks/<name> directory with a
// helmfile.d state or a helmfile.yaml.gotmpl.
type trackedStacks struct {
	files  map[string][]string
	stacks []string
}

func discoverTrackedStacks(repoRoot string) (trackedStacks, error) {
	raw, err := imageCatalogCommand("git", "-C", repoRoot, "ls-files", "--", stacksRoot)
	if err != nil {
		return trackedStacks{}, err
	}
	tracked := trackedStacks{files: make(map[string][]string)}
	isStack := make(map[string]bool)
	for _, file := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		rest, ok := strings.CutPrefix(file, stacksRoot+"/")
		name, inside, found := strings.Cut(rest, "/")
		if !ok || !found {
			continue
		}
		stack := stacksRoot + "/" + name
		tracked.files[stack] = append(tracked.files[stack], file)
		if strings.HasPrefix(inside, "helmfile.d/") || inside == "helmfile.yaml.gotmpl" {
			isStack[stack] = true
		}
	}
	tracked.stacks = sortedKeys(isStack)
	if len(tracked.stacks) == 0 {
		return trackedStacks{}, fmt.Errorf("no Helmfile stack under %s in %s", stacksRoot, repoRoot)
	}
	return tracked, nil
}

// stackStates returns the states of each tracked helmfile.d file or
// helmfile.yaml.gotmpl, in file order.
func (t trackedStacks) stackStates(stack string) []string {
	var states []string
	for _, file := range t.files[stack] {
		inside := strings.TrimPrefix(file, stack+"/")
		if (path.Dir(inside) == "helmfile.d" && strings.HasSuffix(inside, ".gotmpl")) || inside == "helmfile.yaml.gotmpl" {
			states = append(states, file)
		}
	}
	sort.Strings(states)
	return states
}

func (t trackedStacks) hasFile(stack, name string) bool {
	for _, file := range t.files[stack] {
		if file == stack+"/"+name {
			return true
		}
	}
	return false
}

// validateStackImageExclusion checks one matrix entry.
//
// Rejected, with the reason:
//   - reason "skip": not in the enum, so nothing says how the stack is handled.
//   - a directory that is not a tracked stack: the entry is stale.
//   - issue on a dev-only entry: only pending work has a tracking issue.
//   - dev-only for a stack with release-inventory.yaml: a released stack must
//     be checked.
func validateStackImageExclusion(stack string, entry imageExclusion, tracked trackedStacks, stacks map[string]bool) error {
	if !stackImageReasons[entry.Reason] {
		return fmt.Errorf("unknown reason %q; use one of %s", entry.Reason, strings.Join(sortedKeys(stackImageReasons), ", "))
	}
	if !stacks[stack] {
		return fmt.Errorf("no tracked Helmfile stack in this directory; remove the stale entry")
	}
	if entry.Issue != "" && entry.Reason != reasonPendingFix {
		return fmt.Errorf("issue is allowed only on %s entries", reasonPendingFix)
	}
	if entry.Reason == reasonDevOnly && tracked.hasFile(stack, stackInventoryFile) {
		return fmt.Errorf("stack has %s, so it is released; remove the %s entry so the stack is checked", stackInventoryFile, reasonDevOnly)
	}
	return nil
}

// stackRenderState is one Helmfile state to render, with its overrides.
type stackRenderState struct {
	Path      string
	Overrides []string
}

// stackRenderStates returns every state the stack deploys: the release
// inventory states with their overrides, and the stackExtraStates entries. A
// tracked state in neither list is reported, so a new state cannot go
// unchecked.
//
// The inventory's sourceCharts.renderValues are not applied: they are a
// release-time workaround for older published charts and would hide what the
// stack itself supplies. Remove this note if renderValues goes away.
func stackRenderStates(repoRoot, stack string, tracked trackedStacks, check *stackCheck) []stackRenderState {
	if !tracked.hasFile(stack, stackInventoryFile) {
		check.fail(findingStackConfig, "", "", "", "released stack has no "+stackInventoryFile,
			"a release inventory that lists the states the stack deploys",
			"add "+stackInventoryFile+", or list the stack as "+reasonDevOnly+" in "+stackImageExclusionsFile)
		return nil
	}
	config, err := loadResolvedInventoryConfig(repoRoot, filepath.Join(repoRoot, stack, stackInventoryFile))
	if err != nil {
		check.fail(findingStackConfig, "", "", "", err.Error(), "a valid "+stackInventoryFile, "correct "+stack+"/"+stackInventoryFile)
		return nil
	}
	listed := make(map[string][]string)
	for _, state := range config.States {
		listed[state.Path] = append(append([]string{}, state.BaseOverrides...), state.FullOverrides...)
	}
	for state, overrides := range stackExtraStates {
		if strings.HasPrefix(state, stack+"/") {
			listed[state] = overrides
		}
	}
	var states []stackRenderState
	for _, state := range tracked.stackStates(stack) {
		overrides, ok := listed[state]
		if !ok {
			check.fail(findingStackConfig, "", "", state, "state is not rendered by the image source check",
				"every state listed in "+stackInventoryFile, "list the state in "+stack+"/"+stackInventoryFile)
			continue
		}
		states = append(states, stackRenderState{Path: state, Overrides: overrides})
		delete(listed, state)
	}
	for _, state := range sortedKeys(listed) {
		check.fail(findingStackConfig, "", "", state, "listed state is not a tracked Helmfile state of the stack",
			"states under "+stack+"/helmfile.d", "correct the state path")
	}
	return states
}

// readStackBaseValues reads the stack's base.yaml. It returns an error that
// wraps os.ErrNotExist when the stack has none.
func readStackBaseValues(repoRoot, stack string) (map[string]any, error) {
	raw, err := os.ReadFile(filepath.Join(repoRoot, stack, stackBaseValuesFile))
	if err != nil {
		return nil, err
	}
	values := make(map[string]any)
	if err := yaml.Unmarshal(raw, &values); err != nil {
		return nil, fmt.Errorf("parse %s/%s: %w", stack, stackBaseValuesFile, err)
	}
	return values, nil
}
