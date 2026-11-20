// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// probeEnvironment is the layered environment the probe renders with. It
// carries a sentinel for every image value in base.yaml, so one render proves
// both that an image reads a key base.yaml declares and that a layered
// environment override reaches the rendered image.
const probeEnvironment = "probe"

// probeChartRepositoryURL is the chart source the probe sets in
// global.helm.sources, so a release from the stack's own chart repository is
// recognized by its repository URL and rendered from the tracked chart of the
// same name. It is an HTTPS URL: Helmfile pulls OCI charts even for build and
// list, and the probe never contacts this repository.
const probeChartRepositoryURL = "https://charts.probe.invalid/in-repo"

var probeChartSource = map[string]any{"url": probeChartRepositoryURL, "registry": "charts.probe.invalid", "repository": "in-repo"}

// imageValueFields are the image fields that the probe sets.
var imageValueFields = []string{"registry", "repository", "tag", "digest"}

// probeLeaf is one image value in a stack's base.yaml and the sentinel the
// probe sets for it.
type probeLeaf struct {
	// Stack owns the base.yaml the value is declared in.
	Stack string
	// Path is the dotted values path, such as "global.image.registry".
	Path     string
	Field    string
	Sentinel string
}

// isImageMapKey reports whether a base.yaml map holds image fields: its key is
// image, ends with Image, or the map is global.image. Worked example:
// global.helm.sources.registry is a chart source, not an image, so it gets no
// sentinel, and an image that reads it lacks a sentinel and is reported.
func isImageMapKey(key string) bool {
	return key == "image" || strings.HasSuffix(key, "Image")
}

// probeLeaves returns a sentinel leaf for every non-empty image registry,
// repository, tag and digest in a stack's base.yaml, plus a finding for each
// image field written in a form the stack rules reject.
//
// A single image string, such as collector: {image: ghcr.io/x/collector:1},
// gets no sentinel and no finding here: an image that reads it carries no
// sentinel, so the render check reports it.
func probeLeaves(stack string, base map[string]any, check *stackCheck) []probeLeaf {
	var leaves []probeLeaf
	var walk func(prefix string, values map[string]any)
	walk = func(prefix string, values map[string]any) {
		for _, key := range sortedKeys(values) {
			path := strings.TrimPrefix(prefix+"."+key, ".")
			value, ok := values[key].(map[string]any)
			if !ok {
				continue
			}
			if isImageMapKey(key) {
				leaves = append(leaves, imageMapLeaves(stack, path, value, check)...)
			}
			walk(path, value)
		}
	}
	walk("", base)
	sort.Slice(leaves, func(i, j int) bool { return leaves[i].Path < leaves[j].Path })
	return leaves
}

func imageMapLeaves(stack, path string, image map[string]any, check *stackCheck) []probeLeaf {
	var leaves []probeLeaf
	for _, field := range imageValueFields {
		leafPath := path + "." + field
		value, ok := imageScalar(image[field])
		if !ok {
			check.fail(findingImageSource, "", "", leafPath, fmt.Sprintf("image %s is a %T, not a scalar", field, image[field]),
				"a string value", "write "+leafPath+" as a string in base.yaml")
			continue
		}
		if value == "" {
			continue
		}
		if field == "repository" {
			if host, found := imageReferenceHost(value); found {
				check.fail(findingImageSource, "", "", leafPath, "repository "+quoteValue(value)+" contains registry host "+host,
					"a repository without a registry host", "move "+host+" to "+path+".registry in base.yaml")
				continue
			}
		}
		leaves = append(leaves, probeLeaf{Stack: stack, Path: leafPath, Field: field, Sentinel: probeSentinel(stack, leafPath, field)})
	}
	return leaves
}

// imageScalar returns an image field as text. YAML reads an unquoted tag such
// as 2 or 3.2 as a number, which is still a configured tag; a map or list is
// not a usable value.
func imageScalar(value any) (string, bool) {
	switch value := value.(type) {
	case nil:
		return "", true
	case string:
		return value, true
	case int, int64, uint64, float64, bool:
		return fmt.Sprint(value), true
	default:
		return "", false
	}
}

// probeSentinel is deterministic per stack and values path, so an included
// stack's image renders the same reference standalone and through its parent.
// The tag is a valid semantic version in case a chart compares versions.
func probeSentinel(stack, path, field string) string {
	sum := sha256.Sum256([]byte(stack + ":" + path))
	digest := hex.EncodeToString(sum[:])
	short := digest[:8]
	switch field {
	case "registry":
		return "r" + short + ".probe.invalid"
	case "repository":
		return "probe/r" + short
	case "tag":
		return "0.0.0-probe." + short
	default:
		return "sha256:" + digest
	}
}

func quoteValue(value string) string {
	return `"` + value + `"`
}

// probeSentinels indexes leaves by sentinel value per field.
type probeSentinels map[string]map[string]probeLeaf

func indexProbeLeaves(leaves []probeLeaf) probeSentinels {
	index := make(probeSentinels)
	for _, leaf := range leaves {
		if index[leaf.Field] == nil {
			index[leaf.Field] = make(map[string]probeLeaf)
		}
		index[leaf.Field][leaf.Sentinel] = leaf
	}
	return index
}

// sourcedFromProbe reports whether a rendered repository (registry host and
// path, as parsed from the image reference) starts with a registry sentinel
// followed by a repository sentinel, such as
// "r1a2b3c4d.probe.invalid/probe/r1a2b3c4d/nvcf-api".
func (s probeSentinels) sourcedFromProbe(repository string) bool {
	host, rest, found := strings.Cut(repository, "/")
	if !found {
		return false
	}
	if _, ok := s["registry"][host]; !ok {
		return false
	}
	segments := strings.SplitN(rest, "/", 3)
	if len(segments) < 2 {
		return false
	}
	_, ok := s["repository"][segments[0]+"/"+segments[1]]
	return ok
}

// writeProbeEnvironment writes environments/probe.yaml for every stack in the
// copied tree: the shared render values for all, plus the given sentinels for
// the stack under test. A stack included by the one under test then receives
// its sentinels only through the parent's forwarded values.
func writeProbeEnvironment(copyRoot, stack string, stacks []string, leaves []probeLeaf) error {
	for _, other := range stacks {
		values := stackRenderValues(probeEnvironment)
		values["global"].(map[string]any)["helm"] = map[string]any{"sources": probeChartSource}
		if other == stack {
			for _, leaf := range leaves {
				setImageValue(values, leaf.Path, leaf.Sentinel)
			}
		}
		encoded, err := yaml.Marshal(values)
		if err != nil {
			return err
		}
		target := filepath.Join(copyRoot, other, "environments", probeEnvironment+".yaml")
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(target, encoded, 0o600); err != nil {
			return err
		}
	}
	return nil
}
