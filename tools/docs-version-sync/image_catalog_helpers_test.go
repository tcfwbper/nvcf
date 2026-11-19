// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
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
	Exception imageValueException
}

// imageChartSnapshot contains Helm-coalesced values, dependency versions and
// the dependency values roots declared in the chart's Chart.yaml, each mapped
// to its chart name.
type imageChartSnapshot struct {
	Values       map[string]any
	AppVersions  map[string]string
	Dependencies map[string]string
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
		dependencies, err := chartDependencyRoots(chart)
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

// imageCatalogCommand bounds external tool execution and attaches stderr to
// failures. A command that ran to completion and exited nonzero returns a
// commandRejectedError. A timeout, a command killed by a signal (an OOM kill,
// for example) or a command that could not start does not: none of them is
// Helm deciding to reject the chart.
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
		err = fmt.Errorf("%s failed: %w: %s", name, err, strings.TrimSpace(stderr.String()))
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.Exited() && ctx.Err() == nil {
			return nil, &commandRejectedError{err}
		}
		return nil, err
	}
	return output, nil
}

// commandRejectedError is a command that ran and exited nonzero, such as a
// helm render that a required guard rejects. Any other render failure is an
// execution error and never evidence that a guard fired.
type commandRejectedError struct{ err error }

func (e *commandRejectedError) Error() string { return e.err.Error() }
func (e *commandRejectedError) Unwrap() error { return e.err }

// renderRejected reports whether a render error is Helm rejecting the chart,
// as opposed to a timeout or a setup failure of the probe.
func renderRejected(err error) bool {
	var rejected *commandRejectedError
	return errors.As(err, &rejected)
}

// chartDependency is one dependencies entry of a Chart.yaml.
type chartDependency struct {
	Name       string `yaml:"name"`
	Alias      string `yaml:"alias"`
	Repository string `yaml:"repository"`
}

// readChartDependencies reads the dependencies a chart declares. Exception
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

// chartDependencyRoots maps the values root of each dependency a chart
// declares (its alias, or its name when it has no alias) to its chart name;
// image value exceptions are looked up by that name.
func chartDependencyRoots(chart string) (map[string]string, error) {
	dependencies, err := readChartDependencies(chart)
	if err != nil {
		return nil, err
	}
	roots := make(map[string]string)
	for _, dependency := range dependencies {
		roots[cmp.Or(dependency.Alias, dependency.Name)] = dependency.Name
	}
	return roots, nil
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

// imageIndexError is a malformed image index declaration: a non-map
// category, an empty or non-string path, or a duplicate identity. Like an
// undeclared exception key, it is a configuration error that fails even a
// pending-fix chart.
type imageIndexError struct{ message string }

func (e *imageIndexError) Error() string { return e.message }

// readImageIndex returns the declared images of a values tree, sorted by
// identity, without resolving their values.
//
// An undeclared exception key always wins over every other index error. The
// walk checks the key of every usable path and records a malformed entry
// instead of returning on it. Otherwise a pending-fix chart with
// {aa: "", server: nats.image} and no nats dependency would report only the
// empty path and hide the key error.
func readImageIndex(values map[string]any, dependencies map[string]string) ([]catalogImage, error) {
	var declared []catalogImage
	var declarationError error
	identities := make(map[string]bool)
	for _, category := range imageIndexCategories {
		raw, present := values[category]
		entries, ok := raw.(map[string]any)
		if present && !ok {
			declarationError = cmp.Or(declarationError, error(&imageIndexError{category + " must be a map of image value paths"}))
			continue
		}
		for _, id := range sortedKeys(entries) {
			path, ok := entries[id].(string)
			if ok && path != "" {
				exception, err := imageExceptionFor(path, dependencies)
				if err != nil {
					return nil, fmt.Errorf("%s.%s (%s): %w", category, id, path, err)
				}
				if !identities[id] {
					identities[id] = true
					declared = append(declared, catalogImage{ID: id, Path: path, Ownership: category, Exception: exception})
					continue
				}
			}
			declarationError = cmp.Or(declarationError, error(&imageIndexError{
				fmt.Sprintf("%s.%s must be a unique identity referencing image values", category, id)}))
		}
	}
	if declarationError != nil {
		return nil, declarationError
	}
	if len(declared) == 0 {
		return nil, &imageIndexError{"chart must declare thirdPartyImages or ownedImages"}
	}
	sort.Slice(declared, func(i, j int) bool { return declared[i].ID < declared[j].ID })
	return declared, nil
}

// resolveChartImageCatalog resolves every indexed image of a rendered values
// snapshot to the reference the chart renders, using the same parser as
// release inventories.
func resolveChartImageCatalog(snapshot imageChartSnapshot) ([]catalogImage, error) {
	images, err := readImageIndex(snapshot.Values, snapshot.Dependencies)
	if err != nil {
		return nil, err
	}
	return resolveImageCatalog(snapshot, images)
}

// resolveImageCatalog resolves a given index against a rendered snapshot. The
// chart checks pass the index from the chart's own values.yaml, so a scenario
// or override that rewrites thirdPartyImages or ownedImages cannot move a
// check away from the paths the chart declares; only image settings are read
// from the snapshot.
func resolveImageCatalog(snapshot imageChartSnapshot, index []catalogImage) ([]catalogImage, error) {
	images := append([]catalogImage(nil), index...)
	for index, image := range images {
		node, err := imageCatalogValue(snapshot.Values, image.Path)
		if err != nil {
			return nil, fmt.Errorf("%s.%s (%s): %w", image.Ownership, image.ID, image.Path, err)
		}
		root, _, _ := strings.Cut(image.Path, ".")
		images[index].Reference = image.Exception.resolve(node, snapshot.AppVersions[root])
		if _, err := parseResolvedImageReference(images[index].Reference); err != nil {
			return nil, fmt.Errorf("%s.%s (%s): %w", image.Ownership, image.ID, image.Path, err)
		}
	}
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

// imageCatalogString treats missing or non-string optional image fields as unset.
func imageCatalogString(node map[string]any, key string) string {
	value, _ := node[key].(string)
	return value
}
