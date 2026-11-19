// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestChartSourcesRequireEmptyWrittenFields covers each static source rule on
// a chart's own values.yaml, including the exception rows' extra fields.
func TestChartSourcesRequireEmptyWrittenFields(t *testing.T) {
	for _, test := range []struct {
		name, values, path, problem string
	}{
		{"empty registry and repository", "ownedImages: {server: server.image}\nserver:\n  image: {registry: \"\", repository: \"\", tag: v1}\n", "", ""},
		{"prefilled registry", "ownedImages: {server: server.image}\nserver:\n  image: {registry: r.example, repository: \"\", tag: v1}\n", "server.image.registry", `prefilled image source "r.example"`},
		{"prefilled repository", "ownedImages: {server: server.image}\nserver:\n  image: {registry: \"\", repository: team/server, tag: v1}\n", "server.image.repository", `prefilled image source "team/server"`},
		{"repository with a host", "ownedImages: {server: server.image}\nserver:\n  image: {registry: \"\", repository: docker.io/team/server}\n", "server.image.repository", "contains registry host docker.io"},
		{"registry not written", "ownedImages: {server: server.image}\nserver:\n  image: {repository: \"\", tag: v1}\n", "server.image.registry", "not written"},
		{"null registry", "ownedImages: {server: server.image}\nserver:\n  image: {registry: null, repository: \"\"}\n", "server.image.registry", "not written"},
		{"nats fullImageName prefilled", "thirdPartyImages: {box: nats.box.image}\nnats:\n  global: {image: {registry: \"\"}}\n  box:\n    image: {registry: \"\", repository: \"\", fullImageName: r.example/box:1}\n", "nats.box.image.fullImageName", "prefilled"},
		{"nats global registry prefilled", "thirdPartyImages: {box: nats.box.image}\nnats:\n  global: {image: {registry: r.example}}\n  box:\n    image: {registry: \"\", repository: \"\", fullImageName: \"\"}\n", "nats.global.image.registry", `prefilled image source "r.example"`},
		{"nats global registry not written", "thirdPartyImages: {box: nats.box.image}\nnats:\n  box:\n    image: {registry: \"\", repository: \"\", fullImageName: \"\"}\n", "nats.global.image.registry", "not written"},
		{"cert-manager shared registry prefilled", "thirdPartyImages: {controller: cert-manager.image}\ncert-manager:\n  imageRegistry: quay.io\n  imageNamespace: \"\"\n  image: {registry: \"\", repository: \"\"}\n", "cert-manager.imageRegistry", `prefilled image source "quay.io"`},
		{"cert-manager shared namespace not written", "thirdPartyImages: {controller: cert-manager.image}\ncert-manager:\n  imageRegistry: \"\"\n  image: {registry: \"\", repository: \"\"}\n", "cert-manager.imageNamespace", "not written"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var values map[string]any
			if err := yaml.Unmarshal([]byte(test.values), &values); err != nil {
				t.Fatal(err)
			}
			catalog, err := readImageIndex(values, map[string]string{"nats": "nats", "cert-manager": "cert-manager"})
			if err != nil {
				t.Fatal(err)
			}
			check := &chartCheck{chart: "c"}
			check.checkChartSources(values, catalog)
			if test.problem == "" {
				if len(check.findings) != 0 {
					t.Fatalf("unexpected findings %+v", check.findings)
				}
				return
			}
			if len(check.findings) != 1 || check.findings[0].ValuesPath != test.path ||
				check.findings[0].Category != findingCoverage || !strings.Contains(check.findings[0].Problem, test.problem) {
				t.Fatalf("findings %+v, want one at %s containing %q", check.findings, test.path, test.problem)
			}
		})
	}
}

// TestChartSourcesRejectMissingIndexPath makes an index that points nowhere a
// configuration error, not an expected pending-fix finding.
func TestChartSourcesRejectMissingIndexPath(t *testing.T) {
	values := map[string]any{"ownedImages": map[string]any{"server": "server.image"}}
	catalog, err := readImageIndex(values, nil)
	if err != nil {
		t.Fatal(err)
	}
	check := &chartCheck{chart: "c"}
	check.checkChartSources(values, catalog)
	findings, expected := pendingFixOutcome(check, chartImageExclusion{Reason: reasonPendingFix})
	if expected != "" || len(findings) != 1 || findings[0].Category != findingInvalidEntry {
		t.Fatalf("findings %+v, expected label %q", findings, expected)
	}
}

// TestImplicitTemplateSources pins what the template scan reports.
func TestImplicitTemplateSources(t *testing.T) {
	for _, test := range []struct {
		line, problem string
	}{
		{`image: docker.io/{{ .Values.image.repository }}:{{ .Values.image.tag }}`, "literal registry host docker.io"},
		{`  - image: "nvcr.io/nvidia/x:1"`, "literal registry host nvcr.io"},
		{`        - --acme-http01-solver-image=quay.io/jetstack/solver:v1`, "literal registry host quay.io"},
		{`image: {{ .Values.image.registry | default "nvcr.io" }}/{{ .Values.image.repository }}`, `default "nvcr.io"`},
		{`{{- $registry := .Values.global.imageRegistry | default "quay.io" -}}`, `default "quay.io"`},
		{`{{ default "docker.io" .Values.image.registry }}`, `default "docker.io"`},
		{`image: {{ .Values.image.registry }}/{{ .Values.image.repository }}:{{ .Values.image.tag }}`, ""},
		{`image: "{{ include "openbao.image" (dict "image" .Values.image "name" "x") }}"`, ""},
		{`replicas: {{ .Values.replicas | default 1 }}`, ""},
		{`tag: {{ .Values.image.tag | default .Chart.AppVersion }}`, ""},
		{`{{ .Values.image.registry | default "" }}`, ""},
		{`{{/* .Values.image.registry | default "docker.io" */}}`, ""},
		{"{{ .Values.image.registry | default `nvcr.io` }}", "default \"nvcr.io\""},
		{"{{ default `nvcr.io` .Values.image.registry }}", "default \"nvcr.io\""},
		{`{{ .Values.image.registry | trim | default "nvcr.io" }}`, `default "nvcr.io"`},
		{`{{ default "docker.io" (.Values.global.imageRegistry) }}`, `default "docker.io"`},
		{`imagePullPolicy: {{ .Values.image.pullPolicy | default "IfNotPresent" }}`, ""},
		{`{{ .Values.registryPort | default "5000" }}`, ""},
		// The default applies to the tag, not to a source field.
		{`image: {{ printf "%s/%s:%s" .Values.image.registry .Values.image.repository (.Values.image.tag | default "latest") }}`, ""},
		{`image: {{ .Values.registry }}/nvcr.io`, ""},
		{`containers: [{name: server, image: docker.io/team/server:v1}]`, "image reference docker.io/team/server:v1 starts with literal registry host docker.io"},
		{`containers: [{image: "nvcr.io/x:1", name: server}]`, "literal registry host nvcr.io"},
		{`{"name": "server", "image": "quay.io/x:1"}`, "literal registry host quay.io"},
		{`containers: [{name: server, image: {{ .Values.image.registry }}/x:1}]`, ""},
		{"{{ .Values.image.registry\n  | default \"docker.io\" }}", `default "docker.io"`},
		{"{{/*\nimage: docker.io/team/server:v1\n*/}}", ""},
		{`  # e.g. {image: docker.io/team/server:v1}`, ""},
		{`metadata: {} # example {image: docker.io/team/server:v1}`, ""},
		{`replicas: 1 # was --solver-image=quay.io/jetstack/solver:v1`, ""},
		{`data: '{"image": "docker.io/team/server:v1"}'`, ""},
		{`data: "{\"image\": \"docker.io/team/server:v1\"}"`, ""},
		{`  image: nvcr.io/x:1 # pinned`, "literal registry host nvcr.io"},
		{`metadata: {name: a} # {image: docker.io/x}` + "\n" + `containers: [{image: quay.io/x:1}]`, "literal registry host quay.io"},
		{`args: ['--solver-image=quay.io/jetstack/solver:v1']`, "literal registry host quay.io"},
		{`containers: [{name: "{{ .Values.name }}", image: quay.io/x:1}]`, "literal registry host quay.io"},
		// Quoted scalars hide "#" and "," from YAML, escapes included.
		{`containers: [{name: "a #b", image: quay.io/x:1}]`, "literal registry host quay.io"},
		{`containers: [{name: 'a'' #b', image: quay.io/x:1}]`, "literal registry host quay.io"},
		{`containers: [{name: "a\" #b", image: quay.io/x:1}]`, "literal registry host quay.io"},
		// A quote inside a plain scalar opens nothing, so the comment counts.
		{`note: it's fine # {image: docker.io/x:1}`, ""},
		// "#" starts a comment only after whitespace.
		{`containers: [{name: a#b, image: quay.io/x:1}]`, "literal registry host quay.io"},
		{`- {name: x, image: docker.io/y:1}`, "literal registry host docker.io"},
		// An unquoted flow value stops before the next key.
		{`containers: [{image: app:v1},{image: docker.io/hidden:v1}]`, "image reference docker.io/hidden:v1 starts with literal registry host docker.io"},
		{`containers: [{name: a, image: app:v1}, {name: b, image: quay.io/x:1}]`, "image reference quay.io/x:1 starts with"},
		// "," is a flow indicator only inside a flow collection.
		{`note: one, image: docker.io/x:1`, ""},
		{`note: [a] two, image: docker.io/x:1`, ""},
		// A masked action is blank space, so a flow mapping may follow it.
		{`{{- with .Values.x }}{image: docker.io/x:1}{{ end }}`, "image reference docker.io/x:1 starts with literal registry host docker.io"},
		{`args: ["--solver-image=quay.io/jetstack/solver:v1"]`, "image reference quay.io/jetstack/solver:v1 starts with"},
		{"{{- /* example:\n  {{ .Values.image.registry | default \"nvcr.io\" }}\n*/ -}}", ""},
	} {
		var problems []string
		for _, source := range implicitTemplateSources(test.line) {
			problems = append(problems, source.problem)
		}
		if test.problem == "" {
			if len(problems) != 0 {
				t.Errorf("%s: unexpected %v", test.line, problems)
			}
			continue
		}
		if len(problems) != 1 || !strings.Contains(problems[0], test.problem) {
			t.Errorf("%s: problems %v, want one containing %q", test.line, problems, test.problem)
		}
	}
}

// TestImplicitTemplateSourcesReadBlockScalars skips flow keys inside a block
// scalar, which renders as a string, and keeps reading block keys there as
// the line scan always has. The scalar ends at the first line indented no
// deeper than its key; a line holding only an action does not end it.
func TestImplicitTemplateSourcesReadBlockScalars(t *testing.T) {
	text := "data:\n" +
		"  config.json: |\n" +
		"    {\"image\": \"docker.io/ignored:v1\"}\n" +
		"{{- if .Values.extra }}\n" +
		"    [{image: docker.io/ignored:v2}]\n" +
		"{{- end }}\n" +
		"    image: nvcr.io/kept:v1\n" +
		"spec:\n" +
		"  containers: [{image: quay.io/x:1}]\n"
	var got []string
	for _, source := range implicitTemplateSources(text) {
		got = append(got, fmt.Sprintf("%d: %s", source.line, source.problem))
	}
	want := []string{
		"7: image reference nvcr.io/kept:v1 starts with literal registry host nvcr.io",
		"9: image reference quay.io/x:1 starts with literal registry host quay.io",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("got\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// TestImplicitTemplateSourcesReadMultiLineYAML carries quoted scalars and
// flow collections across lines, and ends a block scalar at the key it
// belongs to rather than at a list dash or a header comment.
func TestImplicitTemplateSourcesReadMultiLineYAML(t *testing.T) {
	for _, test := range []struct {
		name, text string
		lines      []int
	}{
		{"flow mapping spans lines", "x: {name: a,\n  b: c, image: docker.io/x:1}\n", []int{2}},
		{"flow sequence spans lines", "containers: [\n  {name: a},\n  {image: docker.io/x:1}\n]\n", []int{3}},
		{"quoted scalar spans lines", "x: \"multi\n  {image: docker.io/x:1}\"\nnext: [{image: quay.io/y:1}]\n", []int{3}},
		// An argument inside a quoted scalar is still read, as the line scan
		// always did, and its "#" is text, not a comment.
		{"argument in quoted continuation", "x: \"multi\n  b # --solver-image=docker.io/x:1\"\n", []int{2}},
		{"sibling after list-item scalar", "- args: |\n    run\n  containers: [{name: a, image: docker.io/x:1}]\n", []int{3}},
		{"comment after list-item scalar", "- args: |\n    run\n  note: 1 # --solver-image=docker.io/x:1\n", nil},
		{"dash-only scalar", "- |\n  {image: docker.io/ignored:1}\n- [{image: quay.io/x:1}]\n", []int{3}},
		{"chomping header with comment", "a:\n  cfg: |- # text\n    {image: docker.io/ignored:1}\n  other: [{image: quay.io/x:1}]\n", []int{4}},
		{"folded header", "a:\n  cfg: >2\n    {image: docker.io/ignored:1}\n", nil},
		{"no comment inside a scalar", "a: |\n  # --solver-image=docker.io/x:1\n", []int{2}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var lines []int
			for _, source := range implicitTemplateSources(test.text) {
				lines = append(lines, source.line)
			}
			if fmt.Sprint(lines) != fmt.Sprint(test.lines) {
				t.Fatalf("lines %v, want %v in\n%s", lines, test.lines, test.text)
			}
		})
	}
}

// TestImplicitTemplateSourcesReportOriginalLines keeps each finding on the
// line it starts on in the file, after a multi-line comment is blanked and for
// an action that spans lines.
func TestImplicitTemplateSourcesReportOriginalLines(t *testing.T) {
	text := "{{/*\n  image: docker.io/ignored:v1\n*/}}\n" +
		"image: nvcr.io/x:1\n" +
		"{{ .Values.image.registry\n  | default \"quay.io\" }}\n"
	var got []string
	for _, source := range implicitTemplateSources(text) {
		got = append(got, fmt.Sprintf("%d: %s", source.line, source.problem))
	}
	want := []string{
		"4: image reference nvcr.io/x:1 starts with literal registry host nvcr.io",
		`5: default "quay.io" supplies an image source in {{ .Values.image.registry | default "quay.io" }}`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("got\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// TestGuardErrorNamesPath requires the guard error to name the exact values
// path, not a longer path that starts with it.
func TestGuardErrorNamesPath(t *testing.T) {
	for _, test := range []struct {
		message string
		names   bool
	}{
		{"execution error: A valid image registry is required for x.image.registry", true},
		{"execution error: set x.image.registry.", true},
		{"execution error: set x.image.registryMirror", false},
		{"execution error: set x.image.registry.sub", false},
		{"execution error: set x.image.registry-backup", false},
		{"execution error: set backup-x.image.registry", false},
		{"execution error: set x.image.registry, then retry", true},
		{"execution error: set .Values.x.image.registry", true},
		{"execution error: required (.Values.x.image.registry)", true},
		{"execution error: set .Values.y.x.image.registry", false},
		{"execution error: set Values.x.image.registry", false},
		{`execution error: "set x.image.registry."`, true},
		{"execution error: set y.x.image.registry", false},
		{"execution error: image source missing", false},
	} {
		if got := guardNamesPath(test.message, "x.image.registry"); got != test.names {
			t.Errorf("%q: names path %v, want %v", test.message, got, test.names)
		}
	}
}
