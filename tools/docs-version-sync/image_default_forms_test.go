// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"regexp"
	"strings"
	"testing"
)

// TestLiteralDefaultFormExamples keeps every form's worked example caught by
// both gates, since every example defaults a registry to docker.io.
func TestLiteralDefaultFormExamples(t *testing.T) {
	for _, form := range literalDefaultFormTable {
		for gate, forms := range map[string]*regexp.Regexp{"chart": chartDefaultForms, "stack": stackDefaultForms} {
			sources := templateDefaultSources(form.example, forms)
			if len(sources) != 1 || !strings.Contains(sources[0].problem, `default "docker.io"`) {
				t.Errorf("%s gate, form %s: %q reported %v", gate, form.name, form.example, sources)
			}
		}
	}
}

// TestLiteralDefaultFormsByGate pins which field set each gate checks: a
// chart may prefill tag and digest, a stack may not default any image field.
func TestLiteralDefaultFormsByGate(t *testing.T) {
	for _, test := range []struct {
		action       string
		chart, stack bool
	}{
		{`{{ dig "nats" "container" "image" "repository" "" .Values | default "synadia/nats-server" }}`, true, true},
		{`{{- $natsReloaderTag := dig "nats" "reloader" "image" "tag" "" .Values | default "0.25.0" }}`, false, true},
		{`{{ dig "tag" "2.10.21-alpine" $natsImage | quote }}`, false, true},
		{`{{ (.Values.image.tag | default "latest") }}`, false, true},
		{`{{ .Values.image.digest | default "sha256:abc" }}`, false, true},
		{`{{ default "v1" .Values.image.tag }}`, false, true},
		// A parenthesized field before the default.
		{`{{ (.Values.image.tag) | default "v1" }}`, false, true},
		{`{{ (.Values.image.registry) | default "docker.io" }}`, true, true},
		// A function with arguments between the field and the default.
		{`{{ .Values.global.image.registry | trimSuffix "/" | default "docker.io" }}`, true, true},
		{`{{ dig "nats" "image" "registry" "" .Values | trimPrefix "https://" | default "docker.io" }}`, true, true},
		// Not a literal, or an empty literal: nothing is hidden.
		{`{{ .Values.image.tag | default .Chart.AppVersion }}`, false, false},
		{`{{ dig "nats" "image" "tag" "" .Values | quote }}`, false, false},
		{`{{ dig "registry" "" $image | default .Values.global.image.registry }}`, false, false},
		// Not an image field.
		{`{{ .Values.stage | default "dev" }}`, false, false},
		{`{{ dig "nats" "storageSize" "10Gi" .Values }}`, false, false},
		{`{{ .Values.registryPort | default "5000" }}`, false, false},
		// The literal after a field-named key is the next key, not the
		// default: dig's default is the argument before the map.
		{`{{- $oci := dig "victoriaMetrics" "chartRepository" "oci" false .Values }}`, false, false},
	} {
		if got := len(templateDefaultSources(test.action, chartDefaultForms)) == 1; got != test.chart {
			t.Errorf("chart gate %q: reported %v, want %v", test.action, got, test.chart)
		}
		if got := len(templateDefaultSources(test.action, stackDefaultForms)) == 1; got != test.stack {
			t.Errorf("stack gate %q: reported %v, want %v", test.action, got, test.stack)
		}
	}
}
