// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"
)

// TestStackImageExclusionValidation pins each rejected matrix entry.
func TestStackImageExclusionValidation(t *testing.T) {
	tracked := trackedStacks{
		files: map[string][]string{
			"deploy/stacks/released": {"deploy/stacks/released/helmfile.d/01.yaml.gotmpl", "deploy/stacks/released/" + stackInventoryFile},
			"deploy/stacks/dev":      {"deploy/stacks/dev/helmfile.yaml.gotmpl"},
		},
		stacks: []string{"deploy/stacks/dev", "deploy/stacks/released"},
	}
	stacks := map[string]bool{"deploy/stacks/dev": true, "deploy/stacks/released": true}
	for _, test := range []struct {
		name, stack string
		entry       imageExclusion
		want        string
	}{
		{"pending-fix with issue", "deploy/stacks/released", imageExclusion{Reason: reasonPendingFix, Issue: "x"}, ""},
		{"dev-only without inventory", "deploy/stacks/dev", imageExclusion{Reason: reasonDevOnly}, ""},
		{"unknown reason", "deploy/stacks/dev", imageExclusion{Reason: "skip"}, "unknown reason"},
		{"stale stack", "deploy/stacks/gone", imageExclusion{Reason: reasonPendingFix}, "no tracked Helmfile stack"},
		{"issue on dev-only", "deploy/stacks/dev", imageExclusion{Reason: reasonDevOnly, Issue: "x"}, "issue is allowed only"},
		{"dev-only with inventory", "deploy/stacks/released", imageExclusion{Reason: reasonDevOnly}, "so it is released"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateStackImageExclusion(test.stack, test.entry, tracked, stacks)
			if test.want == "" && err != nil || test.want != "" && (err == nil || !strings.Contains(err.Error(), test.want)) {
				t.Fatalf("got %v, want %q", err, test.want)
			}
		})
	}
}

// TestUnreachedVersionLeaves checks a tag against the image its sibling
// registry or repository renders, and skips an image that is not rendered.
func TestUnreachedVersionLeaves(t *testing.T) {
	leaf := func(path, field string) probeLeaf {
		return probeLeaf{Stack: "s", Path: path, Field: field, Sentinel: probeSentinel("s", path, field)}
	}
	natsRegistry, natsRepository, natsTag := leaf("nats.image.registry", "registry"), leaf("nats.image.repository", "repository"), leaf("nats.image.tag", "tag")
	natsDigest := leaf("nats.image.digest", "digest")
	offRegistry, offTag := leaf("off.image.registry", "registry"), leaf("off.image.tag", "tag")
	migrationsTag := leaf("migrations.image.tag", "tag")
	leaves := []probeLeaf{natsRegistry, natsRepository, natsTag, natsDigest, offRegistry, offTag, migrationsTag}
	image := func(repository, version string) parsedImageReference {
		return parsedImageReference{Repository: repository, Version: version, Digest: natsDigest.Sentinel}
	}
	nats := natsRegistry.Sentinel + "/" + natsRepository.Sentinel
	for _, test := range []struct {
		name   string
		states [][]parsedImageReference
		want   string
	}{
		{"every tag reaches its image", [][]parsedImageReference{{image(nats, natsTag.Sentinel), image("r/x", migrationsTag.Sentinel)}}, ""},
		// The nats tag appears only on another image: its own image lost it.
		{"tag on the wrong image", [][]parsedImageReference{{image(nats, "v1"), image("r/x", natsTag.Sentinel), image("r/y", migrationsTag.Sentinel)}}, "nats.image.tag"},
		{"tag-only leaf reaches no image", [][]parsedImageReference{{image(nats, natsTag.Sentinel)}}, "migrations.image.tag"},
		{"digest misses its image", [][]parsedImageReference{{{Repository: nats, Version: natsTag.Sentinel}, image("r/y", migrationsTag.Sentinel)}}, "nats.image.digest"},
		// One state forwards the nats tag and another replaces it.
		{"tag lost in one state", [][]parsedImageReference{{image(nats, natsTag.Sentinel), image("r/x", migrationsTag.Sentinel)}, {image(nats, "v1")}}, "nats.image.tag"},
		// A tag-only leaf may appear in any state.
		{"tag-only leaf in another state", [][]parsedImageReference{{image(nats, natsTag.Sentinel)}, {image("r/x", migrationsTag.Sentinel)}}, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			var got []string
			for _, leaf := range unreachedVersionLeaves(leaves, test.states) {
				got = append(got, leaf.Path)
			}
			// off.image is never rendered, so its tag is never reported.
			if strings.Join(got, ",") != test.want {
				t.Fatalf("unreached %v, want %q", got, test.want)
			}
		})
	}
}
