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

// stackWorkspace is a copy of every tracked stack file, rendered with the
// probe environment. The copy keeps untracked stack files, such as a
// developer's environments/local.yaml, out of the render, and it keeps the
// sibling stacks that an included Helmfile reaches. Charts are copied from
// the working tree by prepareImageChart, as the chart gate does.
//
// It does not reuse the release inventory renderer: that renderer writes an
// inventory environment that sets global.image, which would replace the
// sentinels, and it needs registry credentials to pull published charts.
type stackWorkspace struct {
	repoRoot string
	copyRoot string
	env      []string
	runner   execResolvedInventoryCommandRunner
	// charts maps a chart name to its tracked directory, or to "" when
	// several tracked charts share the name.
	charts    map[string]string
	workspace func() string
}

func newStackWorkspace(repoRoot string, tracked trackedStacks, charts map[string]string, workspace func() string) (*stackWorkspace, error) {
	// helmfile build prints resolved paths, so the copy root is resolved too
	// (a temporary directory may sit behind a symbolic link).
	copyRoot, err := filepath.EvalSymlinks(workspace())
	if err != nil {
		return nil, err
	}
	for _, stack := range sortedKeys(tracked.files) {
		for _, file := range tracked.files[stack] {
			raw, err := os.ReadFile(filepath.Join(repoRoot, file))
			if err != nil {
				return nil, err
			}
			target := filepath.Join(copyRoot, file)
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return nil, err
			}
			if err := os.WriteFile(target, raw, 0o600); err != nil {
				return nil, err
			}
		}
	}
	// The self-managed states load secrets/<HELMFILE_ENV>-secrets.yaml with
	// requiredEnv, so the probe writes an empty one; it holds no image value.
	renderEnv, err := writeStackRenderInputs(copyRoot, probeEnvironment)
	if err != nil {
		return nil, err
	}
	renderEnv["HELMFILE_CACHE_HOME"] = filepath.Join(copyRoot, ".helmfile-cache")
	return &stackWorkspace{
		repoRoot:  repoRoot,
		copyRoot:  copyRoot,
		env:       setResolvedInventoryEnvironment(os.Environ(), renderEnv),
		charts:    charts,
		workspace: workspace,
	}, nil
}

// trackedChartsByName maps each tracked chart's Chart.yaml name to its
// directory. A name that several charts share maps to "", which resolving a
// release reports instead of guessing.
func trackedChartsByName(repoRoot string) (map[string]string, error) {
	directories, err := discoverTrackedCharts(repoRoot)
	if err != nil {
		return nil, err
	}
	charts := make(map[string]string)
	for _, directory := range directories {
		raw, err := os.ReadFile(filepath.Join(repoRoot, directory, "Chart.yaml"))
		if err != nil {
			return nil, err
		}
		var metadata struct {
			Name string `yaml:"name"`
		}
		if err := yaml.Unmarshal(raw, &metadata); err != nil {
			return nil, fmt.Errorf("parse %s/Chart.yaml: %w", directory, err)
		}
		if _, seen := charts[metadata.Name]; seen {
			charts[metadata.Name] = ""
			continue
		}
		charts[metadata.Name] = directory
	}
	return charts, nil
}

func (w *stackWorkspace) helmfile(state stackRenderState, command ...string) ([]byte, error) {
	stateFile := filepath.Join(w.copyRoot, state.Path)
	return runResolvedInventoryHelmfile(w.runner, filepath.Dir(stateFile), w.env, stateFile, state.Overrides, command...)
}

// builtState is one document of helmfile build output: a state file and the
// states it includes are printed as separate documents.
type builtState struct {
	// Source is the absolute path of the state file in the copy.
	Source       string
	Repositories []helmfileRepository `yaml:"repositories"`
	Releases     []builtRelease       `yaml:"releases"`
	Helmfiles    []struct {
		Path string `yaml:"path"`
		// Values holds inline maps and values file paths, in order.
		Values []any `yaml:"values"`
	} `yaml:"helmfiles"`
}

type builtRelease struct {
	Name      string `yaml:"name"`
	Namespace string `yaml:"namespace"`
	Chart     string `yaml:"chart"`
	Version   string `yaml:"version"`
	// Set and SetString are applied by Helmfile as helm flags, after the
	// values files; write-values does not include them.
	Set       []builtSetValue `yaml:"set"`
	SetString []builtSetValue `yaml:"setString"`
}

// builtSetValue is one release set entry: a value, a list of values, or a
// file.
type builtSetValue struct {
	Name   string   `yaml:"name"`
	Value  string   `yaml:"value"`
	Values []string `yaml:"values"`
	File   string   `yaml:"file"`
}

// helmSetFlags returns the helm template flags for a release's set and
// setString entries, in Helmfile's order.
func (r builtRelease) helmSetFlags() []string {
	var flags []string
	add := func(flag string, entries []builtSetValue) {
		for _, entry := range entries {
			switch {
			case entry.File != "":
				flags = append(flags, "--set-file", entry.Name+"="+entry.File)
			case len(entry.Values) > 0:
				flags = append(flags, flag, entry.Name+"={"+strings.Join(entry.Values, ",")+"}")
			default:
				flags = append(flags, flag, entry.Name+"="+entry.Value)
			}
		}
	}
	add("--set", r.Set)
	add("--set-string", r.SetString)
	return flags
}

var (
	builtStateSource  = regexp.MustCompile(`(?m)^#\s+Source:\s+(\S+)\s*$`)
	documentSeparator = regexp.MustCompile(`(?m)^---\s*$`)
)

func parseHelmfileBuild(raw []byte) ([]builtState, error) {
	var states []builtState
	for _, document := range documentSeparator.Split(string(raw), -1) {
		match := builtStateSource.FindStringSubmatch(document)
		if match == nil {
			continue
		}
		state := builtState{Source: match[1]}
		if err := yaml.Unmarshal([]byte(document), &state); err != nil {
			return nil, fmt.Errorf("parse built state %s: %w", match[1], err)
		}
		states = append(states, state)
	}
	if len(states) == 0 {
		return nil, fmt.Errorf("helmfile build printed no state")
	}
	return states, nil
}

// stackOf returns the stack directory, relative to the repository root, that
// owns a path in the copy, or "" for a path outside deploy/stacks.
func (w *stackWorkspace) stackOf(path string) string {
	relative, err := filepath.Rel(w.copyRoot, path)
	if err != nil {
		return ""
	}
	parts := strings.SplitN(filepath.ToSlash(relative), "/", 4)
	if len(parts) < 4 || parts[0]+"/"+parts[1] != stacksRoot {
		return ""
	}
	return stacksRoot + "/" + parts[2]
}

// renderedRelease is one enabled release rendered with the probe values.
type renderedRelease struct {
	// Stack owns the state document that declares the release.
	Stack    string
	Name     string
	Indexed  bool
	Snapshot imageChartSnapshot
	Err      error
	// Untracked names the chart when it is listed in untrackedStackCharts;
	// such a release is skipped, not rendered.
	Untracked string
}

// renderState builds, lists and renders every enabled release of a state,
// including the releases of included states.
func (w *stackWorkspace) renderState(state stackRenderState) ([]builtState, []renderedRelease, error) {
	built, err := w.helmfile(state, "build")
	if err != nil {
		return nil, nil, fmt.Errorf("build %s: %w", state.Path, err)
	}
	documents, err := parseHelmfileBuild(built)
	if err != nil {
		return nil, nil, err
	}
	listed, err := w.helmfile(state, "list", "--output", "json")
	if err != nil {
		return nil, nil, fmt.Errorf("list %s: %w", state.Path, err)
	}
	releases, err := decodeHelmfileReleaseList(listed)
	if err != nil {
		return nil, nil, fmt.Errorf("decode release list of %s: %w", state.Path, err)
	}
	valuesDir := w.workspace()
	if _, err := w.helmfile(state, "write-values", "--output-file-template",
		filepath.Join(valuesDir, releaseValuesFile("{{ .Release.Namespace }}", "{{ .Release.Name }}"))); err != nil {
		return nil, nil, fmt.Errorf("write values of %s: %w", state.Path, err)
	}
	var rendered []renderedRelease
	for _, release := range releases {
		// write-values skips a release that is disabled or marked
		// installed: false, and the stack does not deploy it.
		if !release.Enabled || !release.Installed {
			continue
		}
		document, declared := findBuiltRelease(documents, release)
		if document == nil {
			return nil, nil, fmt.Errorf("release %s/%s of %s is missing from helmfile build", release.Namespace, release.Name, state.Path)
		}
		result := renderedRelease{Stack: w.stackOf(document.Source), Name: release.Name}
		chart, tracked, err := w.resolveChart(*document, declared.Chart, declared.Version)
		var untracked untrackedChartError
		if errors.As(err, &untracked) {
			result.Untracked = string(untracked)
			rendered = append(rendered, result)
			continue
		}
		if err == nil && tracked {
			// Only a tracked chart can declare an image index; an upstream
			// chart's values.yaml is not read as one.
			result.Indexed, err = chartHasImageIndex(chart)
		}
		if err == nil {
			values := filepath.Join(valuesDir, releaseValuesFile(release.Namespace, release.Name))
			// A Helm test hook runs only on helm test and is not deployed;
			// the release inventory renders with --skip-tests too.
			flags := append(declared.helmSetFlags(), "--skip-tests")
			identity := imageRelease{Name: release.Name, Namespace: release.Namespace, Flags: flags}
			result.Snapshot, err = renderImageChartAs(chart, identity, []string{values})
		}
		result.Err = err
		rendered = append(rendered, result)
	}
	return documents, rendered, nil
}

// releaseValuesFile names a release's written values file. Namespaces and
// release names cannot contain "_", so the name is unique per release.
func releaseValuesFile(namespace, name string) string {
	return namespace + "_" + name + ".yaml"
}

func findBuiltRelease(documents []builtState, release helmfileRelease) (*builtState, builtRelease) {
	for index := range documents {
		for _, declared := range documents[index].Releases {
			if declared.Name == release.Name && declared.Namespace == release.Namespace {
				return &documents[index], declared
			}
		}
	}
	return nil, builtRelease{}
}

// resolveChart returns a local chart directory to render a release from, and
// whether it is a tracked chart:
//   - a relative chart path, from the tracked chart it names;
//   - a chart from the stack's own repository (probeChartRepositoryURL), from
//     the tracked chart of the same name at HEAD;
//   - any other repository chart, pulled at the version the state pins.
//
// HEAD rather than the pinned published version: pulling the stack's own
// published charts needs registry credentials that pull request CI does not
// have, and HEAD is what the next pin bump ships. Revisit if CI gains read
// access to the published chart repository.
//
// External access: third-party charts are pulled from their public
// repositories, as the chart coverage check already pulls remote
// dependencies in the same CI job. A network failure surfaces as a render
// error and fails the run.
func (w *stackWorkspace) resolveChart(document builtState, chart, version string) (string, bool, error) {
	if strings.HasPrefix(chart, ".") || filepath.IsAbs(chart) {
		absolute := chart
		if !filepath.IsAbs(chart) {
			absolute = filepath.Join(filepath.Dir(document.Source), chart)
		}
		relative, err := filepath.Rel(w.copyRoot, absolute)
		if err != nil || strings.HasPrefix(relative, "..") {
			return "", false, fmt.Errorf("chart %s is outside the repository", chart)
		}
		directory, err := prepareImageChart(filepath.Join(w.repoRoot, relative), w.workspace())
		return directory, true, err
	}
	alias, name, found := strings.Cut(chart, "/")
	if !found {
		return "", false, fmt.Errorf("chart %s names no repository", chart)
	}
	var repository *helmfileRepository
	for index := range document.Repositories {
		if document.Repositories[index].Name == alias {
			repository = &document.Repositories[index]
		}
	}
	if repository == nil {
		return "", false, fmt.Errorf("chart %s uses undeclared repository %s", chart, alias)
	}
	if repository.URL == probeChartRepositoryURL {
		directory, ok := w.charts[name]
		switch {
		case !ok && untrackedStackCharts[name] != "":
			return "", false, untrackedChartError(name)
		case !ok:
			return "", false, fmt.Errorf("chart %s matches no tracked Chart.yaml name", chart)
		case directory == "":
			return "", false, fmt.Errorf("chart %s matches several tracked Chart.yaml names", chart)
		}
		prepared, err := prepareImageChart(filepath.Join(w.repoRoot, directory), w.workspace())
		return prepared, true, err
	}
	pulled, err := pullChart(*repository, name, version, w.workspace())
	return pulled, false, err
}

// untrackedChartError names a chart listed in untrackedStackCharts.
type untrackedChartError string

func (e untrackedChartError) Error() string {
	return "chart " + string(e) + " is listed in untrackedStackCharts"
}

func pullChart(repository helmfileRepository, name, version, workspace string) (string, error) {
	args := []string{"pull", name, "--repo", repository.URL}
	if repository.OCI {
		args = []string{"pull", "oci://" + strings.TrimSuffix(repository.URL, "/") + "/" + name}
	}
	if version != "" {
		args = append(args, "--version", version)
	}
	args = append(args, "--untar", "--untardir", workspace)
	if _, err := imageCatalogCommand("helm", args...); err != nil {
		return "", err
	}
	entries, err := os.ReadDir(workspace)
	if err != nil {
		return "", err
	}
	if len(entries) != 1 || !entries[0].IsDir() {
		return "", fmt.Errorf("helm pull %s left %d entries, want one chart directory", name, len(entries))
	}
	return filepath.Join(workspace, entries[0].Name()), nil
}

// includedStates returns the states a state document includes, each with the
// top-level keys of the values it forwards. A values entry is an inline map
// or a values file path relative to the including state; a file is read for
// its keys.
func includedStates(document builtState) (map[string]map[string]bool, error) {
	included := make(map[string]map[string]bool)
	for _, include := range document.Helmfiles {
		path := include.Path
		if !filepath.IsAbs(path) {
			path = filepath.Join(filepath.Dir(document.Source), path)
		}
		forwarded := make(map[string]bool)
		for _, entry := range include.Values {
			values, err := includedValues(document, entry)
			if err != nil {
				return nil, err
			}
			for key := range values {
				forwarded[key] = true
			}
		}
		included[filepath.Clean(path)] = forwarded
	}
	return included, nil
}

func includedValues(document builtState, entry any) (map[string]any, error) {
	switch value := entry.(type) {
	case map[string]any:
		return value, nil
	case string:
		file := value
		if !filepath.IsAbs(file) {
			file = filepath.Join(filepath.Dir(document.Source), file)
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("read included values file of %s: %w", document.Source, err)
		}
		values := make(map[string]any)
		if err := yaml.Unmarshal(raw, &values); err != nil {
			return nil, fmt.Errorf("parse included values file %s: %w", file, err)
		}
		return values, nil
	default:
		return nil, fmt.Errorf("included values entry of %s is %T, want a map or a file path", document.Source, entry)
	}
}

// guardPath matches a chart template guard (required or fail, which Helm
// reports as "execution error at (<file>): <message>") whose message names a
// values path ending in registry or repository, such as
// "execution error at (app/templates/pod.yaml:8:16): set image.registry".
// Such a render error is an image source finding, not a crash: the chart
// gate (tracked in #3532) makes charts reject an empty source, so a stack that
// does not supply one stops there. A template crash that mentions a path,
// such as "at <.Values.image.registry>: nil pointer evaluating", is not a
// guard and stays a render error. Remove this rule if charts stop guarding
// their image sources.
var guardPath = regexp.MustCompile(`execution error at \([^)]*\):[^\n]*?\b[\w-]+(?:\.[\w-]+)*\.(?:registry|repository)\b`)
