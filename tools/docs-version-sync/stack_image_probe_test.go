// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestProbeLeavesSelectImageValues pins which base.yaml values get a sentinel
// and which image value forms are reported.
func TestProbeLeavesSelectImageValues(t *testing.T) {
	base := "global:\n" +
		"  helm: {sources: {registry: nvcr.io, repository: team}}\n" +
		"  image: {registry: nvcr.io, repository: team}\n" +
		"collector:\n  image: ghcr.io/x/collector:1\n" +
		"operator:\n  collectorImage: {repository: ghcr.io/x/collector, tag: \"1\"}\n" +
		"nats:\n  image: {registry: docker.io, repository: \"\", tag: v2, digest: \"\"}\n" +
		"fluent:\n  image: {tag: 3.2}\n" +
		"bad:\n  image: {registry: [docker.io]}\n"
	var values map[string]any
	if err := yaml.Unmarshal([]byte(base), &values); err != nil {
		t.Fatal(err)
	}
	check := &stackCheck{stack: "deploy/stacks/s"}
	var paths []string
	for _, leaf := range probeLeaves("deploy/stacks/s", values, check) {
		paths = append(paths, leaf.Path)
	}
	// global.helm.sources is a chart source, empty values are not set, a
	// single image string and a repository with a host get no sentinel, and
	// the numeric fluent tag gets one.
	if got, want := strings.Join(paths, ","), "fluent.image.tag,global.image.registry,global.image.repository,nats.image.registry,nats.image.tag,operator.collectorImage.tag"; got != want {
		t.Fatalf("leaves %s, want %s", got, want)
	}
	var problems []string
	for _, finding := range check.findings {
		problems = append(problems, finding.ValuesPath+": "+finding.Problem)
	}
	// The single image string is reported by the render check, not here.
	if strings.Contains(strings.Join(problems, "\n"), "collector.image:") {
		t.Errorf("findings %q, want none for collector.image", problems)
	}
	for _, want := range []string{
		`operator.collectorImage.repository: repository "ghcr.io/x/collector" contains registry host ghcr.io`,
		// A numeric tag is still a tag; a list is not a value.
		`bad.image.registry: image registry is a []interface {}, not a scalar`} {
		if !strings.Contains(strings.Join(problems, "\n"), want) {
			t.Errorf("findings %q, want %q", problems, want)
		}
	}
}

// TestProbeSentinelsMatchRenderedRepositories accepts a repository built from
// a registry sentinel and a repository sentinel, with or without a suffix.
func TestProbeSentinelsMatchRenderedRepositories(t *testing.T) {
	registry := probeLeaf{Path: "global.image.registry", Field: "registry", Sentinel: probeSentinel("s", "global.image.registry", "registry")}
	repository := probeLeaf{Path: "global.image.repository", Field: "repository", Sentinel: probeSentinel("s", "global.image.repository", "repository")}
	sentinels := indexProbeLeaves([]probeLeaf{registry, repository})
	for _, test := range []struct {
		repository string
		want       bool
	}{
		{registry.Sentinel + "/" + repository.Sentinel, true},
		{registry.Sentinel + "/" + repository.Sentinel + "/nvcf-api", true},
		{"docker.io/" + repository.Sentinel + "/app", false},
		{registry.Sentinel + "/team/app", false},
		{"victoriametrics/victoria-metrics", false},
	} {
		if got := sentinels.sourcedFromProbe(test.repository); got != test.want {
			t.Errorf("%s: sourced %v, want %v", test.repository, got, test.want)
		}
	}
	if tag := probeSentinel("s", "app.image.tag", "tag"); !strings.HasPrefix(tag, "0.0.0-probe.") {
		t.Errorf("tag sentinel %s is not a semantic version", tag)
	}
	if digest := probeSentinel("s", "app.image.digest", "digest"); !imageDigestRe.MatchString(digest) {
		t.Errorf("digest sentinel %s is not a valid digest", digest)
	}
}

// TestGuardPathNamesImageSources tells a chart image guard from other render
// errors.
func TestGuardPathNamesImageSources(t *testing.T) {
	for _, test := range []struct {
		message string
		guard   bool
	}{
		{"execution error at (app/templates/pod.yaml:8:16): set image.registry", true},
		{"Error: execution error at (cassandra/templates/sts.yaml:3:5): cassandra.image.repository is required", true},
		// A template crash that names a path is not a guard.
		{`template: app/templates/pod.yaml:8:16: executing "app/templates/pod.yaml" at <.Values.image.registry>: nil pointer evaluating interface {}.registry`, false},
		{"cassandra.image.repository is required", false},
		// A guard and a path on separate lines of one error are not a guard.
		{"execution error at (a.yaml:1:1): failed\nnats.image.registry", false},
		// A guard on another image field is not a source guard.
		{"execution error at (app/templates/pod.yaml:8:16): set image.tag", false},
		{`failed to fetch https://registry-1.docker.io/v2/x: 401`, false},
		{"execution error: unrelated template error", false},
	} {
		if got := guardPath.MatchString(test.message); got != test.guard {
			t.Errorf("%q: guard %v, want %v", test.message, got, test.guard)
		}
	}
}

// TestParseHelmfileBuildKeepsSources reads each printed state with its source
// and included states.
func TestParseHelmfileBuildKeepsSources(t *testing.T) {
	raw := "---\n#  Source: /r/deploy/stacks/o/helmfile.d/01.yaml.gotmpl\n\nreleases:\n  - {name: c, namespace: m, chart: ../charts/c}\n" +
		"---\n#  Source: /r/deploy/stacks/s/helmfile.d/00.yaml.gotmpl\n\nhelmfiles:\n  - path: ../../o/helmfile.d/01.yaml.gotmpl\n    values:\n      - {collector: {image: {tag: x}}}\n"
	states, err := parseHelmfileBuild([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 2 || states[0].Releases[0].Chart != "../charts/c" {
		t.Fatalf("states %+v", states)
	}
	included, err := includedStates(states[1])
	if err != nil {
		t.Fatal(err)
	}
	if forwarded := included["/r/deploy/stacks/o/helmfile.d/01.yaml.gotmpl"]; !forwarded["collector"] || len(forwarded) != 1 {
		t.Fatalf("included %v", included)
	}
}
