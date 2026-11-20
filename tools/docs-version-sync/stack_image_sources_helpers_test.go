// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

const (
	fixStackSource = "declare the image registry and repository in base.yaml and pass them through the release values"
	fixStackTag    = "pass the base.yaml value through to the release values without a literal"
)

// stackCheck collects one stack's findings.
type stackCheck struct {
	stack    string
	findings []imageGateFinding
	// untracked lists "<stack> <release> (<chart>: <reason>)" for each
	// skipped release whose chart is listed in untrackedStackCharts.
	untracked []string
}

func (c *stackCheck) fail(category, release, image, path, problem, expected, fix string) {
	c.findings = append(c.findings, imageGateFinding{Category: category, Subject: c.stack, Release: release, Image: image,
		ValuesPath: path, Problem: problem, Expected: expected, Fix: fix})
}

// stackRun is what a stack's own render left failing, keyed by failureKey.
type stackRun struct {
	failing map[string]bool
}

// includedFinding is a finding about a release or value that an included
// stack declares, seen while rendering its parent.
type includedFinding struct {
	stack, key string
	finding    imageGateFinding
}

// stackFindings routes each finding of one stack's render. A finding about a
// release or value of the stack itself is recorded at once. A finding about
// an included stack becomes a candidate that mergeIncludedFindings keeps only
// when the included stack does not fail the same way on its own. Example:
// self-managed renders observability's victoria-metrics with a chart default
// image; observability reports that itself, so self-managed drops it. A
// victoria-metrics tag that self-managed fails to forward is self-managed's
// own finding.
type stackFindings struct {
	check    *stackCheck
	run      stackRun
	included []includedFinding
}

func (f *stackFindings) record(owner, key string, finding imageGateFinding) {
	if owner == f.check.stack || owner == "" {
		finding.Subject = f.check.stack
		f.run.failing[key] = true
		f.check.findings = append(f.check.findings, finding)
		return
	}
	finding.Subject = f.check.stack
	finding.Problem += " through included stack " + owner
	finding.Fix = "forward the image values of " + owner + " from the including state"
	f.included = append(f.included, includedFinding{stack: owner, key: key, finding: finding})
}

// mergeIncludedFindings adds to each parent the included-stack candidates
// that the included stack's own run does not report. An included stack that
// was not run (dev-only or an invalid entry) reports nothing, so the parent
// keeps every candidate and still reports what it deploys.
func mergeIncludedFindings(checks map[string]*stackCheck, runs map[string]stackRun, included []includedFinding) {
	for _, candidate := range included {
		if !runs[candidate.stack].failing[candidate.key] {
			parent := checks[candidate.finding.Subject]
			parent.findings = append(parent.findings, candidate.finding)
		}
	}
}

func failureKey(release, detail string) string {
	return release + "\x00" + detail
}

// evaluateStackImageSources validates the matrix, then checks every tracked
// stack. workspace returns a fresh temporary directory per call.
func evaluateStackImageSources(repoRoot, exclusionsPath string, workspace func() string) (*imageGateReport, error) {
	tracked, err := discoverTrackedStacks(repoRoot)
	if err != nil {
		return nil, err
	}
	entries, err := loadImageExclusions(exclusionsPath)
	if err != nil {
		return nil, err
	}
	charts, err := trackedChartsByName(repoRoot)
	if err != nil {
		return nil, err
	}
	report := newStackImageReport()
	isStack := make(map[string]bool)
	for _, stack := range tracked.stacks {
		isStack[stack] = true
	}
	invalid := make(map[string]bool)
	for _, stack := range sortedKeys(entries) {
		if err := validateStackImageExclusion(stack, entries[stack], tracked, isStack); err != nil {
			invalid[stack] = true
			report.add(imageGateFinding{Category: findingInvalidEntry, Subject: stack, Problem: err.Error(),
				Expected: "a valid " + stackImageExclusionsFile + " entry", Fix: "correct or remove the entry"})
		}
	}
	for _, chart := range sortedKeys(untrackedStackCharts) {
		if directory := charts[chart]; directory != "" {
			report.add(imageGateFinding{Category: findingStackConfig, Subject: directory, Problem: "chart " + chart + " is tracked but listed in untrackedStackCharts",
				Expected: "only charts whose source is not tracked", Fix: "remove " + chart + " from untrackedStackCharts"})
		}
	}
	checks := make(map[string]*stackCheck)
	runs := make(map[string]stackRun)
	var included []includedFinding
	for _, stack := range tracked.stacks {
		if invalid[stack] {
			continue
		}
		if entry, ok := entries[stack]; ok && entry.Reason == reasonDevOnly {
			report.Skipped[reasonDevOnly] = append(report.Skipped[reasonDevOnly], stack)
			continue
		}
		check, run, viaParent, err := checkStackImageSources(repoRoot, stack, tracked, charts, workspace)
		if err != nil {
			return nil, err
		}
		checks[stack], runs[stack] = check, run
		included = append(included, viaParent...)
	}
	mergeIncludedFindings(checks, runs, included)
	for _, stack := range sortedKeys(checks) {
		check := checks[stack]
		if len(check.untracked) > 0 {
			report.Skipped[skippedUntrackedChart] = append(report.Skipped[skippedUntrackedChart], check.untracked...)
		}
		entry, listed := entries[stack]
		if !listed {
			report.add(check.findings...)
			if len(check.findings) == 0 {
				report.Covered = append(report.Covered, stack)
			}
			continue
		}
		findings, label := stackImageGate.pendingFixOutcome(stack, check.findings, entry, findingImageSource)
		report.add(findings...)
		if label != "" {
			report.Expected = append(report.Expected, label)
			report.ExpectedFindings = append(report.ExpectedFindings, check.findings...)
		}
	}
	return report, nil
}

// checkStackImageSources checks one stack: the static template scan, the
// base.yaml image values, and a probe render of every state. It returns the
// findings about images an included stack declares separately, for the
// caller to keep only those the included stack does not report itself.
func checkStackImageSources(repoRoot, stack string, tracked trackedStacks, charts map[string]string, workspace func() string) (*stackCheck, stackRun, []includedFinding, error) {
	check := &stackCheck{stack: stack}
	run := stackRun{failing: make(map[string]bool)}
	if err := scanStackTemplates(repoRoot, stack, tracked, check); err != nil {
		return nil, run, nil, err
	}
	// The release inventory and states are checked first: a configuration
	// error fails the run even when base.yaml is missing too.
	states := stackRenderStates(repoRoot, stack, tracked, check)
	base, err := readStackBaseValues(repoRoot, stack)
	if errors.Is(err, os.ErrNotExist) {
		check.fail(findingImageSource, "", "", "", "stack has no "+stackBaseValuesFile,
			"a base.yaml that declares every image registry and repository", "add "+stackBaseValuesFile)
		return check, run, nil, nil
	}
	if err != nil {
		return nil, run, nil, err
	}
	leaves := probeLeaves(stack, base, check)
	if len(states) == 0 {
		return check, run, nil, nil
	}
	space, err := newStackWorkspace(repoRoot, tracked, charts, workspace)
	if err != nil {
		return nil, run, nil, err
	}
	includes, includedLeaves, err := discoverIncludedStacks(space, stack, tracked.stacks, states, leaves, check)
	if err != nil {
		return nil, run, nil, err
	}
	includedLeaves = withoutSharedPaths(includedLeaves, leaves)
	all := append(append([]probeLeaf{}, leaves...), includedLeaves...)
	if err := writeProbeEnvironment(space.copyRoot, stack, tracked.stacks, all); err != nil {
		return nil, run, nil, err
	}
	sentinels := indexProbeLeaves(all)
	findings := &stackFindings{check: check, run: run}
	// Images are kept per state: a tag forwarded in one state must not
	// satisfy the same leaf in another state that replaces it.
	var images [][]parsedImageReference
	for _, state := range states {
		documents, releases, err := space.renderState(state)
		if err != nil {
			check.fail(findingRenderError, "", "", state.Path, strings.Join(strings.Fields(err.Error()), " "), "a state that renders with the probe environment",
				"fix the state or the probe render inputs")
			continue
		}
		checkForwardedKeys(space, stack, documents, includes, includedLeaves, check)
		var stateImages []parsedImageReference
		for _, release := range releases {
			if release.Untracked != "" {
				check.untracked = append(check.untracked, release.Stack+" "+release.Name+" ("+release.Untracked+": "+untrackedStackCharts[release.Untracked]+")")
				continue
			}
			stateImages = append(stateImages, checkRenderedRelease(release, sentinels, findings)...)
		}
		images = append(images, stateImages)
	}
	for _, leaf := range unreachedVersionLeaves(all, images) {
		findings.record(leaf.Stack, failureKey("", leaf.Path), imageGateFinding{Category: findingImageSource, ValuesPath: leaf.Path,
			Problem:  leaf.Field + " set in base.yaml does not reach its rendered image",
			Expected: "the base.yaml " + leaf.Field + " on the image rendered from " + leaf.Path[:strings.LastIndex(leaf.Path, ".")], Fix: fixStackTag})
	}
	return check, run, findings.included, nil
}

// withoutSharedPaths drops the included stack's leaves at a values path the
// parent also declares, such as global.image.registry in both base.yaml
// files. The parent's value is what it forwards, so only the parent's
// sentinel may count for that path.
func withoutSharedPaths(included, own []probeLeaf) []probeLeaf {
	paths := make(map[string]bool)
	for _, leaf := range own {
		paths[leaf.Path] = true
	}
	var kept []probeLeaf
	for _, leaf := range included {
		if !paths[leaf.Path] {
			kept = append(kept, leaf)
		}
	}
	return kept
}

// unreachedVersionLeaves returns each tag and digest leaf whose value does not
// reach its image. images holds the rendered images of each state:
//   - A leaf beside a registry or repository sentinel, such as nats.image.tag
//     next to nats.image.registry, must appear on an image that carries one of
//     those sentinels. When no rendered image carries them, the image is not
//     deployed in the rendered profiles (a disabled feature) and nothing is
//     checked: its registry and repository are not reported either.
//   - A tag-only leaf, such as cassandra.migrations.image.tag whose registry
//     comes from global.image, must appear on some rendered image.
//
// The sibling rule holds in each state: when two states render the same
// image, one forwarding the tag does not excuse the other replacing it.
//
// One carrier with the sentinel is enough: images that share a registry, such
// as a container and a sidecar both reading nats.image.registry, may set their
// own tags.
func unreachedVersionLeaves(leaves []probeLeaf, images [][]parsedImageReference) []probeLeaf {
	var unreached []probeLeaf
	for _, leaf := range leaves {
		if leaf.Field != "tag" && leaf.Field != "digest" {
			continue
		}
		siblings := sourceSiblings(leaf, leaves)
		if !versionReached(leaf, siblings, images) {
			unreached = append(unreached, leaf)
		}
	}
	return unreached
}

// sourceSiblings returns the registry and repository leaves in the same image
// map as a tag or digest leaf.
func sourceSiblings(leaf probeLeaf, leaves []probeLeaf) []probeLeaf {
	parent := leaf.Path[:strings.LastIndex(leaf.Path, ".")]
	var siblings []probeLeaf
	for _, other := range leaves {
		if other.Stack == leaf.Stack && (other.Field == "registry" || other.Field == "repository") && other.Path == parent+"."+other.Field {
			siblings = append(siblings, other)
		}
	}
	return siblings
}

// versionReached applies the two rules above. A leaf with siblings is checked
// in every state that renders a carrier; a tag-only leaf needs one image in
// any state.
func versionReached(leaf probeLeaf, siblings []probeLeaf, images [][]parsedImageReference) bool {
	carries := func(image parsedImageReference) bool {
		return (leaf.Field == "tag" && image.Version == leaf.Sentinel) || (leaf.Field == "digest" && image.Digest == leaf.Sentinel)
	}
	if len(siblings) == 0 {
		for _, state := range images {
			if slices.ContainsFunc(state, carries) {
				return true
			}
		}
		return false
	}
	for _, state := range images {
		carriers, reached := 0, false
		for _, image := range state {
			if carriesSentinel(image, siblings) {
				carriers++
				reached = reached || carries(image)
			}
		}
		if carriers > 0 && !reached {
			return false
		}
	}
	return true
}

// carriesSentinel reports whether a rendered image's repository (registry host
// and path) holds a registry or repository sentinel of the given leaves.
func carriesSentinel(image parsedImageReference, leaves []probeLeaf) bool {
	for _, leaf := range leaves {
		if leaf.Field == "registry" && strings.HasPrefix(image.Repository, leaf.Sentinel+"/") ||
			leaf.Field == "repository" && strings.Contains("/"+image.Repository+"/", "/"+leaf.Sentinel+"/") {
			return true
		}
	}
	return false
}

// scanStackTemplates reports every literal default on an image field in the
// stack's tracked .gotmpl files.
func scanStackTemplates(repoRoot, stack string, tracked trackedStacks, check *stackCheck) error {
	for _, file := range tracked.files[stack] {
		if !strings.HasSuffix(file, ".gotmpl") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(repoRoot, file))
		if err != nil {
			return err
		}
		for _, source := range templateDefaultSources(blankTemplateComments(string(raw)), stackDefaultForms) {
			check.fail(findingImageSource, "", "", fmt.Sprintf("%s:%d", strings.TrimPrefix(file, stack+"/"), source.line), source.problem,
				"no literal default on an image field in a stack template", "declare the value in base.yaml and pass it through without a default")
		}
	}
	return nil
}

// discoverIncludedStacks finds the stacks each state includes. It renders the
// states once with the stack's own sentinels and returns the included
// stacks' base.yaml leaves, which the parent must forward.
func discoverIncludedStacks(space *stackWorkspace, stack string, allStacks []string, states []stackRenderState, leaves []probeLeaf, check *stackCheck) (map[string]bool, []probeLeaf, error) {
	if err := writeProbeEnvironment(space.copyRoot, stack, allStacks, leaves); err != nil {
		return nil, nil, err
	}
	isStack := make(map[string]bool)
	for _, other := range allStacks {
		isStack[other] = true
	}
	stacks := make(map[string]bool)
	for _, state := range states {
		built, err := space.helmfile(state, "build")
		if err != nil {
			// renderState runs the same build and parse and reports the
			// failure with its context.
			continue
		}
		documents, err := parseHelmfileBuild(built)
		if err != nil {
			continue
		}
		for _, document := range documents {
			if space.stackOf(document.Source) != stack {
				continue
			}
			included, err := includedStates(document)
			if err != nil {
				check.fail(findingStackConfig, "", "", state.Path, err.Error(), "readable included values", "fix the helmfiles values of the state")
				continue
			}
			for path := range included {
				other := space.stackOf(path)
				switch {
				case !isStack[other]:
					check.fail(findingStackConfig, "", "", state.Path, "includes Helmfile "+path+" outside a tracked stack under "+stacksRoot,
						"included states that belong to a stack", "move the included state into a stack under "+stacksRoot)
				case other != stack:
					stacks[other] = true
				}
			}
		}
	}
	var included []probeLeaf
	for _, other := range sortedKeys(stacks) {
		base, err := readStackBaseValues(space.repoRoot, other)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		// The included stack reports its own base.yaml form findings.
		included = append(included, probeLeaves(other, base, &stackCheck{stack: other})...)
	}
	return stacks, included, nil
}

// checkForwardedKeys requires the including state to forward every image
// value of an included stack, so a layered environment of the parent can
// override it. Worked example: self-managed forwards collector, so
// collector.image.tag reaches observability; a new top-level kubeRbacProxy
// key would not.
func checkForwardedKeys(space *stackWorkspace, stack string, documents []builtState, includes map[string]bool, leaves []probeLeaf, check *stackCheck) {
	for _, document := range documents {
		if space.stackOf(document.Source) != stack {
			continue
		}
		// discoverIncludedStacks already reported an unreadable values file.
		included, err := includedStates(document)
		if err != nil {
			continue
		}
		for _, path := range sortedKeys(included) {
			forwarded, other := included[path], space.stackOf(path)
			if !includes[other] {
				continue
			}
			for _, leaf := range leaves {
				top, _, _ := strings.Cut(leaf.Path, ".")
				if leaf.Stack == other && !forwarded[top] {
					relative, _ := filepath.Rel(space.copyRoot, document.Source)
					check.fail(findingImageSource, "", "", leaf.Path, "image value of included stack "+other+" is not forwarded",
						"the including state forwards "+top, "forward "+top+" in the helmfiles values of "+filepath.ToSlash(relative))
				}
			}
		}
	}
}

// checkRenderedRelease reports each rendered image whose registry and
// repository do not carry probe sentinels, and returns the rendered images.
func checkRenderedRelease(release renderedRelease, sentinels probeSentinels, findings *stackFindings) []parsedImageReference {
	if release.Err != nil {
		category, problem := findingRenderError, strings.Join(strings.Fields(release.Err.Error()), " ")
		// Matched before flattening, so a guard and a path on separate lines
		// of one error cannot combine.
		if guardPath.MatchString(release.Err.Error()) {
			category, problem = findingImageSource, "chart image guard rejects the probe values: "+problem
		}
		// The category is part of the key: a guard rejection in the
		// included stack must not hide an unrelated render error here.
		findings.record(release.Stack, failureKey(release.Name, "render:"+category), imageGateFinding{Category: category,
			Release: release.Name, Problem: problem, Expected: "a release that renders with the base.yaml image sources", Fix: fixStackSource})
		return nil
	}
	images, err := extractResolvedImages(release.Snapshot.Manifest)
	if err != nil {
		findings.record(release.Stack, failureKey(release.Name, "images"), imageGateFinding{Category: findingRenderError,
			Release: release.Name, Problem: err.Error(), Expected: "resolvable rendered images", Fix: fixStackSource})
		return nil
	}
	for _, image := range images {
		if sentinels.sourcedFromProbe(image.Repository) {
			continue
		}
		path, reason := imageValuesPath(release, image)
		problem := "image does not take its registry and repository from base.yaml"
		if path == "" {
			problem += "; chart default, values path unknown (" + reason + ")"
		}
		findings.record(release.Stack, failureKey(release.Name, image.Reference), imageGateFinding{Category: findingImageSource,
			Release: release.Name, Image: image.Reference, ValuesPath: path, Problem: problem,
			Expected: "registry and repository from " + release.Stack + "/" + stackBaseValuesFile, Fix: fixStackSource})
	}
	return images
}

// imageValuesPath names the values path that produced a rendered image, or
// says why it cannot:
//  1. the chart's image index entry that resolves to the image;
//  2. otherwise the computed release values leaf that holds its reference,
//     repository or registry, which finds a literal a stack template wrote,
//     such as grove.image.repository;
//  3. otherwise "" with the reason: the chart has no image index, or its
//     index does not name the image.
func imageValuesPath(release renderedRelease, image parsedImageReference) (string, string) {
	reason := "chart has no image index"
	if release.Indexed {
		reason = "the chart image index does not name it"
		// An unresolvable index only loses the index path here; the chart
		// gate reports an index error in the chart defaults itself, so the
		// search below still runs.
		if catalog, err := resolveChartImageCatalog(release.Snapshot); err == nil {
			for _, entry := range catalog {
				if entry.Reference == image.Reference {
					return entry.Path, ""
				}
			}
		}
	}
	host, path, _ := strings.Cut(image.Repository, "/")
	for _, candidate := range []string{image.Reference, image.Repository, path, host} {
		if found := findValuesLeaf("", release.Snapshot.Values, candidate); found != "" {
			return found, ""
		}
	}
	return "", reason
}

func findValuesLeaf(prefix string, values map[string]any, want string) string {
	for _, key := range sortedKeys(values) {
		path := strings.TrimPrefix(prefix+"."+key, ".")
		switch value := values[key].(type) {
		case string:
			if value == want {
				return path
			}
		case map[string]any:
			if found := findValuesLeaf(path, value, want); found != "" {
				return found
			}
		}
	}
	return ""
}
