// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"regexp"
	"strings"
)

// literalDefaultForm is one way a template action can default an image field
// to a literal. pattern holds FIELD where the field alternation goes and
// LITERAL where the non-empty quoted or raw string literal goes, and PIPELINE
// where intermediate pipeline functions go; example is a
// case it catches. Each form is wrong for the same reason: the literal becomes
// a hidden image source that no values file shows or overrides.
type literalDefaultForm struct {
	name, pattern, example string
}

// literalDefaultFormTable is the single home of the literal-default rule for
// both the chart gate and the stack gate. A dotted field matches when its last
// segment ends with the field name (.Values.image.registry, .imageRegistry);
// a dig key matches a quoted key that ends with it ("registry",
// "imageRegistry"). A variable assigned from any form is caught by that form,
// as in the pipe example.
var literalDefaultFormTable = []literalDefaultForm{
	{"pipe", `\.\w*(?:FIELD)\)?\s*PIPELINE\|\s*default\s+LITERAL`,
		`{{- $registry := .Values.image.registry | trimSuffix "/" | default "docker.io" }}`},
	{"prefix", `\bdefault\s+LITERAL\s+\(?\s*[\w.$]*\.\w*(?:FIELD)\b`,
		`{{ default "docker.io" (.Values.image.registry) }}`},
	{"dig key then pipe", `\bdig\s+(?:"[^"]*"\s+)*"[\w-]*(?:FIELD)"\s+(?:""|` + "``" + `)\s+[\w.$]+\s*PIPELINE\|\s*default\s+LITERAL`,
		`{{ dig "nats" "container" "image" "registry" "" .Values | default "docker.io" }}`},
	{"dig default argument", `\bdig\s+(?:"[^"]*"\s+)*"[\w-]*(?:FIELD)"\s+LITERAL\s+[.$(]`,
		`{{ dig "registry" "docker.io" $natsImage | quote }}`},
}

const literalDefaultLiteral = `(?:"([^"]+)"|` + "`([^`]+)`" + `)`

// literalDefaultPipeline matches the functions piped between the field and
// the default, each with optional arguments, such as | trim or
// | trimSuffix "/". Its groups do not capture, so each form keeps exactly two
// literal groups.
const literalDefaultPipeline = `(?:\|\s*\w+(?:\s+(?:"[^"]*"|` + "`[^`]*`" + `|[\w.$-]+))*\s*)*`

// literalDefaultForms compiles every form for the given field names into one
// case-insensitive expression. Each form contributes two capture groups, the
// quoted and the raw literal, so the first non-empty group is the literal.
func literalDefaultForms(fields ...string) *regexp.Regexp {
	alternation := strings.Join(fields, "|")
	patterns := make([]string, 0, len(literalDefaultFormTable))
	for _, form := range literalDefaultFormTable {
		pattern := strings.ReplaceAll(form.pattern, "FIELD", alternation)
		pattern = strings.ReplaceAll(pattern, "PIPELINE", literalDefaultPipeline)
		patterns = append(patterns, strings.ReplaceAll(pattern, "LITERAL", literalDefaultLiteral))
	}
	return regexp.MustCompile("(?i)" + strings.Join(patterns, "|"))
}

var (
	// chartDefaultForms covers registry and repository only: a chart may
	// prefill tag and digest.
	chartDefaultForms = literalDefaultForms("registry", "repository")
	// stackDefaultForms also covers tag and digest: a stack passes every image
	// field through from base.yaml and layered environments.
	stackDefaultForms = literalDefaultForms("registry", "repository", "tag", "digest")
)
