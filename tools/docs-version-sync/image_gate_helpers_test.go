// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Finding categories shared by the chart coverage and stack image source
// gates.
const (
	findingPendingFixed = "pending-fix now passing"
	findingInvalidEntry = "stale or invalid entry"
	findingRenderError  = "render error"
)

// reasonPendingFix is the matrix reason, in either gate, for a subject that is
// not yet remediated. It is expected to fail until its own fix lands and
// removes the entry.
const reasonPendingFix = "pending-fix"

// imageGateFinding is one line of a gate's failure report. Subject is a chart
// or stack directory; Release is set only by the stack gate.
type imageGateFinding struct {
	Category   string
	Subject    string
	Release    string
	Image      string
	ValuesPath string
	Problem    string
	Expected   string
	Fix        string
}

// String renders "FAIL <subject> [<release>] [<image>] [<values path>]: <problem>; expected <x>; fix: <y>".
func (f imageGateFinding) String() string {
	return "FAIL " + f.line()
}

func (f imageGateFinding) line() string {
	subject := f.Subject
	for _, part := range []string{f.Release, f.Image, f.ValuesPath} {
		if part != "" {
			subject += " " + part
		}
	}
	return fmt.Sprintf("%s: %s; expected %s; fix: %s", subject, f.Problem, f.Expected, f.Fix)
}

// imageGateReport aggregates every subject before the test fails, so one run
// lists every problem.
type imageGateReport struct {
	// Name starts the summary line, such as "chart image coverage".
	Name string
	// Categories orders the per-category failure counts in the summary.
	Categories []string
	Findings   []imageGateFinding
	// Skipped maps a reason to the subjects skipped for it.
	Skipped map[string][]string
	// Expected lists pending-fix subjects that failed as expected, with their
	// issue when present.
	Expected []string
	// ExpectedFindings are printed one per line, so the detail behind an
	// expected failure is visible on a passing run.
	ExpectedFindings []imageGateFinding
	Covered          []string
}

func (r *imageGateReport) add(findings ...imageGateFinding) {
	r.Findings = append(r.Findings, findings...)
}

// Summary counts failures by category and lists every deliberate downgrade
// (skips and expected failures), so they are visible without verbose logs.
func (r *imageGateReport) Summary() string {
	var out strings.Builder
	counts := make(map[string]int)
	for _, finding := range r.Findings {
		counts[finding.Category]++
		fmt.Fprintln(&out, finding)
	}
	for _, finding := range r.ExpectedFindings {
		fmt.Fprintln(&out, "EXPECTED "+finding.line())
	}
	fmt.Fprintf(&out, "%s: %d covered, %d failures", r.Name, len(r.Covered), len(r.Findings))
	for _, category := range r.Categories {
		if counts[category] > 0 {
			fmt.Fprintf(&out, ", %s: %d", category, counts[category])
		}
	}
	fmt.Fprintln(&out)
	for _, reason := range sortedKeys(r.Skipped) {
		fmt.Fprintf(&out, "skipped (%s): %s\n", reason, strings.Join(r.Skipped[reason], ", "))
	}
	if len(r.Expected) > 0 {
		fmt.Fprintf(&out, "expected failures (%s, %d): %s\n", reasonPendingFix, len(r.Expected), strings.Join(r.Expected, ", "))
	}
	return out.String()
}

// imageExclusion is one matrix entry, keyed by a repo-root-relative chart or
// stack directory.
type imageExclusion struct {
	Reason string `yaml:"reason"`
	// Issue is a free-form note, such as a tracking link. It is not validated.
	Issue string `yaml:"issue,omitempty"`
}

// loadImageExclusions decodes a matrix strictly, so a misspelled field fails
// instead of silently dropping an issue link or reason.
func loadImageExclusions(path string) (map[string]imageExclusion, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	entries := make(map[string]imageExclusion)
	if err := decoder.Decode(&entries); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return entries, nil
}

// imageGateMatrix names a gate's matrix file and its subject in pending-fix
// messages.
type imageGateMatrix struct {
	File string
	// Noun is the subject kind, such as "chart" or "stack".
	Noun string
	// Check is what a subject passes, such as "image coverage".
	Check string
}

// pendingFixOutcome applies expected-failure strictness to a checked
// pending-fix subject: only findings in the expected category are expected.
// It returns the findings that fail the run, or the expected-failure label for
// the summary. Any other finding (a render, dependency, index or entry error)
// still fails, and a clean pass means the entry is stale.
func (m imageGateMatrix) pendingFixOutcome(subject string, findings []imageGateFinding, entry imageExclusion, expected string) ([]imageGateFinding, string) {
	var hard []imageGateFinding
	for _, finding := range findings {
		if finding.Category != expected {
			hard = append(hard, finding)
		}
	}
	if len(hard) > 0 {
		return hard, ""
	}
	if len(findings) == 0 {
		return []imageGateFinding{{Category: findingPendingFixed, Subject: subject, Problem: m.Noun + " now passes " + m.Check,
			Expected: "no " + reasonPendingFix + " entry for a covered " + m.Noun, Fix: "remove the entry from " + m.File}}, ""
	}
	return nil, expectedFailureLabel(subject, entry, fmt.Sprintf("%d %s findings", len(findings), expected))
}

func expectedFailureLabel(subject string, entry imageExclusion, detail string) string {
	parts := []string{detail}
	if entry.Issue != "" {
		parts = append(parts, entry.Issue)
	}
	return subject + " (" + strings.Join(parts, ", ") + ")"
}
