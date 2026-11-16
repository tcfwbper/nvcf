// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// catalogImage connects a stable image identity to its effective chart values.
type catalogImage struct {
	ID        string
	Path      string
	Ownership string
	Reference string
	Format    imageValueFormat
}

// imageChartSnapshot contains Helm-coalesced values, dependency versions and
// the dependency names and aliases declared in the chart's Chart.yaml.
type imageChartSnapshot struct {
	Values       map[string]any
	AppVersions  map[string]string
	Dependencies map[string]bool
	Manifest     []byte
}

// imageIndexCategories are the values keys that declare a chart's runtime images.
var imageIndexCategories = []string{"thirdPartyImages", "ownedImages"}

const imageCatalogProbeName = "nvcf-image-catalog-probe.yaml"

const imageCatalogProbe = "{{- $versions := dict \"\" .Chart.AppVersion -}}\n" +
	"{{- range $name, $chart := .Subcharts -}}\n" +
	"{{- $_ := set $versions $name $chart.Chart.AppVersion -}}\n{{- end }}\n" +
	"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: nvcf-image-catalog-probe\n" +
	"data:\n  values: {{ toJson .Values | quote }}\n" +
	"  appVersions: {{ toJson $versions | quote }}\n"

// renderImageChart probes computed values in a temporary chart copy, without
// installing resources or changing the source chart.
func renderImageChart(chart string, values []string) (imageChartSnapshot, error) {
	directory, err := os.MkdirTemp("", "nvcf-image-catalog-")
	if err != nil {
		return imageChartSnapshot{}, err
	}
	defer os.RemoveAll(directory)
	copyPath := filepath.Join(directory, "chart")
	if err := copyResolvedInventoryStack(chart, copyPath); err != nil {
		return imageChartSnapshot{}, fmt.Errorf("copy image chart: %w", err)
	}
	probePath := filepath.Join(copyPath, "templates", imageCatalogProbeName)
	if _, err := os.Stat(probePath); !os.IsNotExist(err) {
		return imageChartSnapshot{}, fmt.Errorf("chart reserves %s", probePath)
	}
	if err := os.MkdirAll(filepath.Dir(probePath), 0o700); err != nil {
		return imageChartSnapshot{}, err
	}
	if err := os.WriteFile(probePath, []byte(imageCatalogProbe), 0o600); err != nil {
		return imageChartSnapshot{}, err
	}
	args := []string{"template", "image-catalog", copyPath, "--namespace", "image-catalog"}
	for _, value := range values {
		args = append(args, "-f", value)
	}
	raw, err := imageCatalogCommand("helm", args...)
	if err != nil {
		return imageChartSnapshot{}, err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	for {
		var document map[string]any
		if err := decoder.Decode(&document); err != nil {
			return imageChartSnapshot{}, fmt.Errorf("image catalog probe missing from Helm output: %w", err)
		}
		metadata, _ := document["metadata"].(map[string]any)
		if document["kind"] != "ConfigMap" || metadata["name"] != "nvcf-image-catalog-probe" {
			continue
		}
		data, _ := document["data"].(map[string]any)
		dependencies, err := chartDependencyNames(chart)
		if err != nil {
			return imageChartSnapshot{}, err
		}
		snapshot := imageChartSnapshot{Manifest: raw, Dependencies: dependencies}
		if err := json.Unmarshal([]byte(fmt.Sprint(data["values"])), &snapshot.Values); err != nil {
			return imageChartSnapshot{}, fmt.Errorf("decode computed image values: %w", err)
		}
		if err := json.Unmarshal([]byte(fmt.Sprint(data["appVersions"])), &snapshot.AppVersions); err != nil {
			return imageChartSnapshot{}, fmt.Errorf("decode image app versions: %w", err)
		}
		return snapshot, nil
	}
}

// imageCatalogCommand bounds external tool execution and attaches stderr to failures.
func imageCatalogCommand(name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, name, args...)
	if name == "git" {
		command.Env = gitCommandEnvironment()
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("%s failed: %w: %s", name, err, strings.TrimSpace(stderr.String()))
	}
	return output, nil
}

// chartDependency is one dependencies entry of a Chart.yaml.
type chartDependency struct {
	Name       string `yaml:"name"`
	Alias      string `yaml:"alias"`
	Repository string `yaml:"repository"`
}

// readChartDependencies reads the dependencies a chart declares. Format
// lookup and dependency builds both read them here.
func readChartDependencies(chart string) ([]chartDependency, error) {
	raw, err := os.ReadFile(filepath.Join(chart, "Chart.yaml"))
	if err != nil {
		return nil, err
	}
	var metadata struct {
		Dependencies []chartDependency `yaml:"dependencies"`
	}
	if err := yaml.Unmarshal(raw, &metadata); err != nil {
		return nil, fmt.Errorf("parse %s/Chart.yaml: %w", chart, err)
	}
	return metadata.Dependencies, nil
}

// chartDependencyNames returns the names and aliases a chart declares as
// dependencies; image formats are keyed by them.
func chartDependencyNames(chart string) (map[string]bool, error) {
	dependencies, err := readChartDependencies(chart)
	if err != nil {
		return nil, err
	}
	names := make(map[string]bool)
	for _, dependency := range dependencies {
		names[dependency.Name] = true
		if dependency.Alias != "" {
			names[dependency.Alias] = true
		}
	}
	return names, nil
}

// gitCommandEnvironment drops repository-selecting variables, so "git -C dir"
// acts on dir. A git hook exports GIT_DIR and GIT_INDEX_FILE; inherited, they
// would make the fixture test stage files into the caller's repository.
func gitCommandEnvironment() []string {
	// Non-nil even if empty: a nil Env would inherit the parent environment.
	environment := []string{}
	for _, variable := range os.Environ() {
		name, _, _ := strings.Cut(variable, "=")
		switch name {
		case "GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY", "GIT_COMMON_DIR", "GIT_PREFIX":
			continue
		}
		environment = append(environment, variable)
	}
	return environment
}

// resolveChartImageCatalog validates indexed value paths and returns stable,
// sorted image identities using the same parser as release inventories.
//
// An undeclared format key always wins over every other index error. The
// first pass walks entries in sorted order and checks the format key of every
// usable path, recording a malformed entry (non-map category, empty or
// non-string path, duplicate identity) instead of returning on it; values are
// resolved only after the whole index passes. Otherwise a pending-fix chart
// with {aa: "", server: nats.image} and no nats dependency would report the
// empty path as an expected coverage failure and hide the key error.
func resolveChartImageCatalog(snapshot imageChartSnapshot) ([]catalogImage, error) {
	var declared []catalogImage
	var declarationError error
	identities := make(map[string]bool)
	for _, category := range imageIndexCategories {
		entries, ok := snapshot.Values[category].(map[string]any)
		if snapshot.Values[category] != nil && !ok {
			declarationError = cmp.Or(declarationError, fmt.Errorf("%s must be a map of image value paths", category))
			continue
		}
		for _, id := range sortedKeys(entries) {
			path, ok := entries[id].(string)
			if !ok || path == "" {
				declarationError = cmp.Or(declarationError, fmt.Errorf("%s.%s must be a unique identity referencing existing image values", category, id))
				continue
			}
			if _, err := imageFormatFor(path, snapshot.Dependencies); err != nil {
				return nil, fmt.Errorf("%s.%s (%s): %w", category, id, path, err)
			}
			if identities[id] {
				declarationError = cmp.Or(declarationError, fmt.Errorf("%s.%s must be a unique identity referencing existing image values", category, id))
				continue
			}
			identities[id] = true
			declared = append(declared, catalogImage{ID: id, Path: path, Ownership: category})
		}
	}
	if declarationError != nil {
		return nil, declarationError
	}
	if len(declared) == 0 {
		return nil, fmt.Errorf("chart must declare thirdPartyImages or ownedImages")
	}
	images := make([]catalogImage, 0, len(declared))
	for _, entry := range declared {
		image, err := resolveCatalogImage(snapshot, entry.Path)
		if err != nil {
			return nil, fmt.Errorf("%s.%s (%s): %w", entry.Ownership, entry.ID, entry.Path, err)
		}
		image.ID, image.Ownership = entry.ID, entry.Ownership
		images = append(images, image)
	}
	sort.Slice(images, func(i, j int) bool { return images[i].ID < images[j].ID })
	return images, nil
}

// imageCatalogValue follows a dotted values path and requires an image object at its leaf.
func imageCatalogValue(values map[string]any, path string) (map[string]any, error) {
	var node any = values
	for _, key := range strings.Split(path, ".") {
		object, ok := node.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid image values path %q", path)
		}
		node, ok = object[key]
		if !ok || key == "" {
			return nil, fmt.Errorf("missing image values path %q", path)
		}
	}
	object, ok := node.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("image values path %q must identify an object", path)
	}
	return object, nil
}

// imageCatalogNodes returns the dependency values root and the image object for a path.
func imageCatalogNodes(values map[string]any, path string) (map[string]any, map[string]any, error) {
	node, err := imageCatalogValue(values, path)
	if err != nil {
		return nil, nil, err
	}
	root, _, _ := strings.Cut(path, ".")
	parent, _ := values[root].(map[string]any)
	return parent, node, nil
}

// imageCatalogString treats missing or non-string optional image fields as unset.
func imageCatalogString(node map[string]any, key string) string {
	value, _ := node[key].(string)
	return value
}

// resolveCatalogImage resolves one index path with its chart's image format.
func resolveCatalogImage(snapshot imageChartSnapshot, path string) (catalogImage, error) {
	format, err := imageFormatFor(path, snapshot.Dependencies)
	if err != nil {
		return catalogImage{}, err
	}
	parent, node, err := imageCatalogNodes(snapshot.Values, path)
	if err != nil {
		return catalogImage{}, err
	}
	root, _, _ := strings.Cut(path, ".")
	reference := format.resolve(parent, node, snapshot.AppVersions[root])
	if _, err := parseResolvedImageReference(reference); err != nil {
		return catalogImage{}, err
	}
	return catalogImage{Path: path, Reference: reference, Format: format}, nil
}
