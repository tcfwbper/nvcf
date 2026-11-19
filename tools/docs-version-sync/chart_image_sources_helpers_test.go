// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"cmp"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const fixImplicitSource = "remove the source from the chart; the stack base.yaml or a values file supplies it"

// checkChartSources requires every image source field in the chart's own
// values.yaml to be written as an empty string: registry and repository of
// each indexed image, plus the source fields its image value exception adds.
// Omitting a field is not enough, because a subchart default then supplies
// the source (the vendored cert-manager chart defaults imageRegistry to
// quay.io); an empty string overrides it.
//
//	{registry: "", repository: "", tag: v1}       passes
//	{registry: docker.io, repository: "", ...}    fails: prefilled source
//	{registry: "", repository: docker.io/x, ...}  fails: registry host in repository
//	{repository: "", tag: v1}                     fails: registry not written
func (c *chartCheck) checkChartSources(values map[string]any, catalog []catalogImage) {
	chartFields := make(map[string]bool)
	for _, image := range catalog {
		node, err := imageCatalogValue(values, image.Path)
		if err != nil {
			c.fail(findingInvalidEntry, image.ID, image.Path, fmt.Sprintf("index path not in values.yaml: %v", err),
				"every indexed path to name an image object in the chart's own values.yaml",
				"write the image object, with empty registry and repository, at the indexed path")
			continue
		}
		for _, field := range append(append([]string{}, imageSourceFields...), image.Exception.imageSourceFields...) {
			c.checkSourceField(image.ID, image.Path+"."+field, node, field)
		}
		root, _, _ := strings.Cut(image.Path, ".")
		for _, field := range image.Exception.chartSourceFields {
			chartFields[root+"."+field] = true
		}
	}
	for _, path := range sortedKeys(chartFields) {
		separator := strings.LastIndex(path, ".")
		// A missing parent is a nil map: the field reads as not written.
		parent, _ := imageCatalogValue(values, path[:separator])
		c.checkSourceField("", path, parent, path[separator+1:])
	}
}

func (c *chartCheck) checkSourceField(id, path string, node map[string]any, field string) {
	value, written := node[field].(string)
	host, hasHost := imageReferenceHost(value)
	switch {
	case !written:
		c.fail(findingCoverage, id, path, "image source field not written; a subchart or template default can supply it",
			`""`, fmt.Sprintf(`write %s: "" in values.yaml`, path))
	case value == "":
	case field == "repository" && hasHost:
		c.fail(findingCoverage, id, path, fmt.Sprintf("repository %q contains registry host %s", value, host),
			`""`, fixImplicitSource)
	default:
		c.fail(findingCoverage, id, path, fmt.Sprintf("prefilled image source %q", value), `""`, fixImplicitSource)
	}
}

var (
	// templateAction matches one template action. With (?s) it spans lines,
	// so a default split across lines is still one action.
	templateAction = regexp.MustCompile(`(?s){{.*?}}`)
	// templateComment matches one template comment, which may span lines and
	// contain "}}". Helm renders nothing for it.
	templateComment = regexp.MustCompile(`(?s){{-?\s*/\*.*?\*/\s*-?}}`)
	// sourceDefault matches a non-empty string literal defaulted onto a field
	// that names an image source, such as .Values.image.registry or
	// .imageRegistry, with a quoted or raw string literal:
	//   .registry | default "x"          pipe form
	//   .registry | trim | default "x"   after argument-free functions
	//   default "x" (.registry)          prefix form, optionally parenthesized
	sourceDefault = regexp.MustCompile("(?i)" +
		`\.\w*(?:registry|repository)\s*(?:\|\s*\w+\s*)*\|\s*default\s+(?:"([^"]+)"|` + "`([^`]+)`" + `)` +
		`|\bdefault\s+(?:"([^"]+)"|` + "`([^`]+)`" + `)\s+\(?\s*[\w.$]*\.\w*(?:registry|repository)\b`)
	// imageField captures the value of a block-style image: key or of a
	// --<name>-image= argument.
	imageField = regexp.MustCompile(`(?:^\s*(?:-\s*)?"?image"?\s*:\s*|--[\w-]+-image=)("[^"]*"|'[^']*'|\S+)`)
	// flowImageField captures the value of an image: key after "{" or "," in
	// a flow mapping. An unquoted value stops at ",", "}" or "]", so it never
	// swallows the next key: in "[{image: a:v1},{image: docker.io/b:v1}]"
	// both values are read.
	flowImageField = regexp.MustCompile(`[{,]\s*"?image"?\s*:\s*("[^"]*"|'[^']*'|[^\s,}\]]+)`)
)

// checkTemplateSources scans the chart's own templates/ for an implicit image
// source. Subcharts are not scanned: an upstream subchart is guarded by its
// wrapper, and a vendored one is listed as vendored-subchart.
//
//	image: docker.io/{{ .Values.image.repository }}    fails: literal registry host
//	{{ .Values.image.registry | default "nvcr.io" }}   fails: default source
//	{{ .Values.replicas | default 1 }}                 passes
//
// Template comments and YAML comments are ignored, and a flow-style image key
// counts only in YAML structure, not inside a quoted or block scalar. Known
// limit: a default applied to a variable, placed after a function with
// arguments (| trimSuffix "/" |), or built with printf is not seen, and an
// image: value is read up to the first space on its line. A block-style
// image key or an --<name>-image= argument inside a block scalar is still
// reported. The per-image guard renders still catch a default that fires.
func (c *chartCheck) checkTemplateSources(chart string) {
	root := filepath.Join(chart, "templates")
	var files []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err == nil && entry.Type().IsRegular() && entry.Name() != imageCatalogProbeName {
			files = append(files, path)
		}
		return err
	})
	if err != nil && !os.IsNotExist(err) {
		c.renderError("templates", err)
		return
	}
	sort.Strings(files)
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			c.renderError("templates", err)
			return
		}
		relative, _ := filepath.Rel(chart, file)
		for _, source := range implicitTemplateSources(string(raw)) {
			c.fail(findingCoverage, "", fmt.Sprintf("%s:%d", relative, source.line), source.problem,
				"no implicit image source in chart templates", fixImplicitSource)
		}
	}
}

// templateSource is one implicit image source and the 1-based line it starts on.
type templateSource struct {
	line    int
	problem string
}

// implicitTemplateSources returns each implicit image source in a template
// file, in source order. Comments are blanked first, keeping their line
// breaks, so every reported line is the line in the original file.
func implicitTemplateSources(text string) []templateSource {
	text = blankTemplateComments(text)
	sources := append(templateDefaultSources(text), templateImageFieldSources(text)...)
	sort.SliceStable(sources, func(i, j int) bool { return sources[i].line < sources[j].line })
	return sources
}

// blankTemplateComments replaces each template comment with spaces, keeping
// its line breaks. Helm renders nothing for a comment.
func blankTemplateComments(text string) string {
	return templateComment.ReplaceAllStringFunc(text, func(comment string) string {
		return strings.Map(func(r rune) rune {
			if r == '\n' {
				return r
			}
			return ' '
		}, comment)
	})
}

// templateDefaultSources reports each action, possibly spanning lines, that
// defaults an image source field to a literal. Helm runs an action wherever
// it sits, even inside a YAML comment or a quoted string.
func templateDefaultSources(text string) []templateSource {
	var sources []templateSource
	for _, span := range templateAction.FindAllStringIndex(text, -1) {
		action := text[span[0]:span[1]]
		if match := sourceDefault.FindStringSubmatch(action); match != nil {
			// The first non-empty literal group: quoted or raw, pipe or prefix form.
			literal := cmp.Or(match[1:]...)
			sources = append(sources, templateSource{strings.Count(text[:span[0]], "\n") + 1,
				fmt.Sprintf("default %q supplies an image source in %s", literal, strings.Join(strings.Fields(action), " "))})
		}
	}
	return sources
}

// templateImageFieldSources reports each image field whose value starts with
// a literal registry host. It reads the YAML around each match, with
// template actions masked, so text that renders no image field is skipped:
//
//	image: docker.io/x                            block key: reported
//	containers: [{name: a, image: docker.io/x}]   flow key: reported
//	args: ["--solver-image=docker.io/x"]          argument in a string: reported
//	metadata: {} # e.g. {image: docker.io/x}      comment: skipped
//	data: '{"image": "docker.io/x"}'              quoted scalar: skipped
//	config.json: |                                block scalar: flow keys
//	  {"image": "docker.io/x"}                    in it are skipped
func templateImageFieldSources(text string) []templateSource {
	masked := strings.Split(maskTemplateActions(text), "\n")
	scalarIndent := -1 // column a block scalar's content must exceed, or -1
	var scanner yamlScanner
	var sources []templateSource
	for number, line := range strings.Split(text, "\n") {
		structure := masked[number]
		indent := len(structure) - len(strings.TrimLeft(structure, " "))
		if scalarIndent >= 0 && (strings.Trim(structure, " \t\x01") == "" || indent > scalarIndent) {
			// Block scalar content: no comment and no flow collection. Block
			// keys and arguments are still reported, as the line scan did
			// before flow keys were recognized, so no earlier finding is lost.
			sources = append(sources, imageFieldSources(number+1, line, len(line), nil)...)
			continue
		}
		scalarIndent = -1
		end, flow := scanner.line(structure)
		sources = append(sources, imageFieldSources(number+1, line, end, flow)...)
		if !scanner.open() && blockScalarHeader.MatchString(structure[:end]) {
			scalarIndent = blockScalarIndent(structure, end)
		}
	}
	return sources
}

// imageFieldSources reports the image fields on one line that start before
// end, the start of a YAML comment. A flow key counts only after a "{" or ","
// in flow, the structural flow indicators of the line. Block keys and
// arguments use the line scan's original pattern.
func imageFieldSources(number int, line string, end int, flow map[int]bool) []templateSource {
	var sources []templateSource
	report := func(value string) {
		value = strings.Trim(value, `"'`)
		// Actions become a placeholder segment, so only literal text can
		// start the reference: "nvcr.io/{{ .x }}" -> "nvcr.io/x" has a host,
		// "{{ .registry }}/nvcr.io" -> "x/nvcr.io" does not.
		if host, found := imageReferenceHost(templateAction.ReplaceAllString(value, "x")); found {
			sources = append(sources, templateSource{number,
				fmt.Sprintf("image reference %s starts with literal registry host %s", value, host)})
		}
	}
	for _, match := range imageField.FindAllStringSubmatchIndex(line, -1) {
		if match[0] >= end {
			continue
		}
		value := line[match[2]:match[3]]
		if strings.HasPrefix(line[match[0]:], "--") {
			// An argument inside a flow sequence ends at the first "}", "]"
			// or ",".
			if cut := strings.IndexAny(value, ",}]"); cut >= 0 {
				value = value[:cut]
			}
		}
		report(value)
	}
	for _, match := range flowImageField.FindAllStringSubmatchIndex(line, -1) {
		if match[0] < end && flow[match[0]] {
			report(line[match[2]:match[3]])
		}
	}
	return sources
}

// maskTemplateActions replaces every byte of each action except line breaks
// with \x01, keeping offsets, so quotes, braces and "#" inside an action do
// not read as YAML.
func maskTemplateActions(text string) string {
	return templateAction.ReplaceAllStringFunc(text, func(action string) string {
		masked := []byte(action)
		for i := range masked {
			if masked[i] != '\n' {
				masked[i] = 1
			}
		}
		return string(masked)
	})
}

var (
	// blockScalarHeader matches a line that opens a literal or folded block
	// scalar, such as "config.json: |" or "- >-".
	blockScalarHeader = regexp.MustCompile(`(?:^\s*-|:)\s+[|>][1-9]?[-+]?[1-9]?\s*$`)
	// sequencePrefix matches the indent and sequence dashes before a key.
	sequencePrefix = regexp.MustCompile(`^\s*(?:-\s+)*`)
)

// blockScalarIndent returns the column that a block scalar's content must be
// indented beyond: the key's column for "key: |" (after any "- "), or the
// dash's column for "- |". In a list item "- args: |" the key is at column
// 2, so a "run" line at column 4 is content, while a sibling "image: x" back
// at column 2 ends the scalar and is read as YAML again.
func blockScalarIndent(structure string, end int) int {
	prefix := sequencePrefix.FindString(structure)
	if strings.Contains(structure[len(prefix):end], ":") {
		return len(prefix)
	}
	return strings.LastIndex(prefix, "-")
}

// yamlScanner reads YAML one line at a time with template actions masked.
// It carries an open quoted scalar or flow collection to the next line, so a
// scalar or collection that spans lines is read as one.
type yamlScanner struct {
	quote      byte
	depth      int
	valueStart bool
}

// open reports whether a quoted scalar or flow collection continues past the
// last line read.
func (s *yamlScanner) open() bool { return s.quote != 0 || s.depth > 0 }

// line reads the next line. It returns where a comment starts, or len(line),
// and the offsets of "{" and "," that are flow indicators: outside quoted
// scalars, with "{" opening a value and "," inside a flow collection. A quote
// opens a quoted scalar only where a value starts, so "it's" in a plain
// scalar is not one. A masked action counts as blank, so the "{" in
// "{{- with .x }}{image: y}" still opens a mapping.
//
// Every case that keeps a value start open ends with continue; any other
// byte is plain text and closes it.
func (s *yamlScanner) line(line string) (int, map[int]bool) {
	flow := make(map[int]bool)
	if !s.open() {
		s.valueStart = true
	}
	for i := 0; i < len(line); i++ {
		c := line[i]
		if s.quote != 0 {
			switch {
			case s.quote == '"' && c == '\\':
				i++
			case c == '\'' && s.quote == '\'' && i+1 < len(line) && line[i+1] == '\'':
				i++
			case c == s.quote:
				s.quote = 0
			}
			continue
		}
		blankAfter := i+1 == len(line) || line[i+1] == ' ' || line[i+1] == '\t'
		switch {
		case c == '#' && (i == 0 || line[i-1] == ' ' || line[i-1] == '\t'):
			return i, flow
		case c == ' ' || c == '\t' || c == 1:
			continue
		case (c == '"' || c == '\'') && s.valueStart:
			s.quote = c
		case (c == '{' || c == '[') && (s.valueStart || s.depth > 0):
			s.depth++
			if c == '{' {
				flow[i] = true
			}
			s.valueStart = true
			continue
		case c == ',' && s.depth > 0:
			flow[i] = true
			s.valueStart = true
			continue
		case (c == '}' || c == ']') && s.depth > 0:
			s.depth--
		case c == ':' && (blankAfter || s.depth > 0):
			s.valueStart = true
			continue
		case c == '-' && s.valueStart && blankAfter:
			continue
		}
		s.valueStart = false
	}
	return len(line), flow
}
