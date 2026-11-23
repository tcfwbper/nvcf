// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeChartFixtureTree writes files into a temporary repository root. The
// tree is built per test rather than committed under testdata: a committed
// indexed fixture chart would itself need a matrix entry, and any entry for an
// indexed chart other than pending-fix is the contradiction under test.
func writeChartFixtureTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for name, content := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

const fixtureChart = "apiVersion: v2\nname: fixture\nversion: 0.1.0\n"

// TestChartImageExclusionValidation covers every matrix rejection and the
// entries each reason accepts.
func TestChartImageExclusionValidation(t *testing.T) {
	root := writeChartFixtureTree(t, map[string]string{
		"parent/Chart.yaml":              fixtureChart + "dependencies:\n  - name: upstream\n    repository: file://charts/sub\n",
		"parent/charts/sub/Chart.yaml":   fixtureChart,
		"parent/charts/loose/Chart.yaml": fixtureChart,
		"orphan/charts/sub/Chart.yaml":   fixtureChart,
		"plain/Chart.yaml":               fixtureChart,
	})
	charts := map[string]bool{"parent": true, "parent/charts/sub": true, "parent/charts/loose": true, "orphan/charts/sub": true, "plain": true,
		"tools/dev": true, "tools/shared": true}
	stacks := chartStacks{deployedBy: map[string][]string{"tools/shared": {"deploy/stacks/a", "deploy/stacks/b"}}}
	issue := "https://github.com/dsx-ai-factory/nvcf/issues/3185"
	for _, test := range []struct {
		name    string
		chart   string
		entry   imageExclusion
		indexed bool
		want    string
	}{
		{name: "pending-fix without issue", chart: "plain", entry: imageExclusion{Reason: reasonPendingFix}},
		{name: "pending-fix with public issue", chart: "plain", entry: imageExclusion{Reason: reasonPendingFix, Issue: issue}},
		{name: "indexed pending-fix", chart: "plain", entry: imageExclusion{Reason: reasonPendingFix}, indexed: true},
		{name: "test-fixture", chart: "plain", entry: imageExclusion{Reason: reasonTestFixture}},
		{name: "no-runtime-image", chart: "plain", entry: imageExclusion{Reason: reasonNoRuntimeImage}},
		{name: "declared vendored subchart", chart: "parent/charts/sub", entry: imageExclusion{Reason: reasonVendoredSubchart}},
		{name: "unknown reason", chart: "plain", entry: imageExclusion{Reason: "skip"}, want: "unknown reason"},
		{name: "missing chart directory", chart: "gone", entry: imageExclusion{Reason: reasonTestFixture}, want: "no tracked Chart.yaml"},
		{name: "issue on test-fixture", chart: "plain", entry: imageExclusion{Reason: reasonTestFixture, Issue: issue}, want: "issue is allowed only on pending-fix"},
		{name: "pending-fix with free-form issue note", chart: "plain", entry: imageExclusion{Reason: reasonPendingFix, Issue: "tracked with the chart owners"}},
		{name: "indexed test-fixture", chart: "plain", entry: imageExclusion{Reason: reasonTestFixture}, indexed: true, want: "declares an image index"},
		{name: "indexed no-runtime-image", chart: "plain", entry: imageExclusion{Reason: reasonNoRuntimeImage}, indexed: true, want: "declares an image index"},
		{name: "subchart not declared as a dependency", chart: "parent/charts/loose", entry: imageExclusion{Reason: reasonVendoredSubchart}},
		{name: "charts directory without a parent chart", chart: "orphan/charts/sub", entry: imageExclusion{Reason: reasonVendoredSubchart}, want: "not a subchart; check as a standalone chart"},
		{name: "standalone chart as subchart", chart: "plain", entry: imageExclusion{Reason: reasonVendoredSubchart}, want: "not a subchart"},
		// dev-only does not depend on where the chart lives.
		{name: "dev-only chart no released stack deploys", chart: "tools/dev", entry: imageExclusion{Reason: reasonDevOnly}},
		{name: "indexed dev-only chart", chart: "tools/dev", entry: imageExclusion{Reason: reasonDevOnly}, indexed: true},
		{name: "dev-only chart released stacks deploy", chart: "tools/shared", entry: imageExclusion{Reason: reasonDevOnly},
			want: "chart is deployed by released stacks deploy/stacks/a, deploy/stacks/b"},
		{name: "issue on dev-only", chart: "tools/dev", entry: imageExclusion{Reason: reasonDevOnly, Issue: issue}, want: "issue is allowed only on pending-fix"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateChartImageExclusion(root, test.chart, test.entry, charts, test.indexed, stacks)
			if test.want == "" {
				if err != nil {
					t.Fatalf("rejected valid entry: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("got %v, want error containing %q", err, test.want)
			}
		})
	}
}

// TestChartImageExclusionsLoadStrictly rejects misspelled fields and duplicate charts.
func TestChartImageExclusionsLoadStrictly(t *testing.T) {
	for name, content := range map[string]string{
		"misspelled field": "plain:\n  reason: pending-fix\n  isue: https://github.com/dsx-ai-factory/nvcf/issues/1\n",
		"duplicate chart":  "plain:\n  reason: pending-fix\nplain:\n  reason: test-fixture\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(writeChartFixtureTree(t, map[string]string{"m.yaml": content}), "m.yaml")
			if _, err := loadImageExclusions(path); err == nil {
				t.Fatal("accepted malformed matrix")
			}
		})
	}
}

// TestChartImageIndexDetection reads only the chart's own values.yaml.
func TestChartImageIndexDetection(t *testing.T) {
	root := writeChartFixtureTree(t, map[string]string{
		"third/values.yaml":  "thirdPartyImages: {}\n",
		"owned/values.yaml":  "ownedImages:\n  server: server.image\n",
		"nested/values.yaml": "server:\n  ownedImages: {}\n",
		"none/Chart.yaml":    fixtureChart,
	})
	for chart, want := range map[string]bool{"third": true, "owned": true, "nested": false, "none": false} {
		got, err := chartHasImageIndex(filepath.Join(root, chart))
		if err != nil || got != want {
			t.Errorf("%s: indexed=%v err=%v, want %v", chart, got, err, want)
		}
	}
}

// TestChartImageClassification follows the agreed order of evaluation.
func TestChartImageClassification(t *testing.T) {
	entry := func(reason string) *imageExclusion { return &imageExclusion{Reason: reason} }
	for _, test := range []struct {
		name    string
		indexed bool
		entry   *imageExclusion
		want    chartAction
	}{
		{"indexed and unlisted is checked", true, nil, actionCheck},
		{"indexed pending-fix is checked and expected to fail", true, entry(reasonPendingFix), actionCheckExpectFailure},
		{"unindexed pending-fix fails statically without a render", false, entry(reasonPendingFix), actionExpectedFailure},
		{"test fixture is skipped", false, entry(reasonTestFixture), actionSkip},
		{"vendored subchart is skipped", false, entry(reasonVendoredSubchart), actionSkip},
		{"no-runtime-image is rendered to confirm", false, entry(reasonNoRuntimeImage), actionConfirmNoImage},
		{"unindexed and unlisted is unclassified", false, nil, actionUnclassified},
	} {
		if got := classifyChart(test.indexed, test.entry); got != test.want {
			t.Errorf("%s: got %d, want %d", test.name, got, test.want)
		}
	}
}
