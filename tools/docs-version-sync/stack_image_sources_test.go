// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestStackImageSources requires every Helmfile stack to take each image
// registry and repository from its base.yaml, or to carry a valid entry in
// stack-image-exclusions.yaml. It evaluates every stack before failing and
// prints one summary.
func TestStackImageSources(t *testing.T) {
	if os.Getenv("NVCF_IMAGE_CATALOG_E2E") != "1" {
		t.Skip("set NVCF_IMAGE_CATALOG_E2E=1 to render the repository's stacks with Helmfile and Helm")
	}
	isolateHelmConfiguration(t)
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	report, err := evaluateStackImageSources(repoRoot, stackImageExclusionsFile, t.TempDir)
	if err != nil {
		t.Fatal(err)
	}
	// Printed rather than logged so skips and expected failures are visible
	// on a passing run without -v.
	fmt.Print(report.Summary())
	if len(report.Findings) > 0 {
		t.Fatalf("%d stack image source failures; see the FAIL lines above", len(report.Findings))
	}
}

// Fixture charts: app guards its image source as the chart gate requires and
// carries a Helm test hook that is not deployed, defaulted prefills one, and
// broken fails for a reason unrelated to images.
var stackFixtureCharts = map[string]string{
	"deploy/helm/app/Chart.yaml":  "apiVersion: v2\nname: helm-app\nversion: 0.1.0\n",
	"deploy/helm/app/values.yaml": "image: {registry: \"\", repository: \"\", tag: v1}\n",
	"deploy/helm/app/templates/pod.yaml": "apiVersion: v1\nkind: Pod\nmetadata:\n  name: app\nspec:\n  containers:\n    - name: app\n" +
		`      image: {{ required "set image.registry" .Values.image.registry }}/{{ required "set image.repository" .Values.image.repository }}:{{ .Values.image.tag }}` + "\n",
	"deploy/helm/app/templates/test.yaml": "apiVersion: v1\nkind: Pod\nmetadata:\n  name: app-test\n  annotations: {helm.sh/hook: test}\nspec:\n  containers:\n" +
		"    - name: check\n      image: busybox:latest\n",
	"deploy/helm/defaulted/Chart.yaml":  "apiVersion: v2\nname: helm-defaulted\nversion: 0.1.0\n",
	"deploy/helm/defaulted/values.yaml": "image: {registry: docker.io, repository: team/defaulted, tag: v1}\n",
	"deploy/helm/defaulted/templates/pod.yaml": "apiVersion: v1\nkind: Pod\nmetadata:\n  name: defaulted\nspec:\n  containers:\n    - name: app\n" +
		"      image: {{ .Values.image.registry }}/{{ .Values.image.repository }}:{{ .Values.image.tag }}\n",
	// workers renders a second Pod only in the workers namespace.
	"deploy/helm/workers/Chart.yaml":  "apiVersion: v2\nname: helm-workers\nversion: 0.1.0\n",
	"deploy/helm/workers/values.yaml": "image: {registry: \"\", repository: \"\", tag: v1}\n",
	"deploy/helm/workers/templates/pod.yaml": "apiVersion: v1\nkind: Pod\nmetadata:\n  name: app\nspec:\n  containers:\n" +
		"    - name: app\n      image: {{ .Values.image.registry }}/{{ .Values.image.repository }}:{{ .Values.image.tag }}\n" +
		"{{- if eq .Release.Namespace \"workers\" }}\n---\napiVersion: v1\nkind: Pod\nmetadata:\n  name: worker\nspec:\n  containers:\n" +
		"    - name: worker\n      image: docker.io/team/worker:v1\n{{- end }}\n",
	// indexed declares its image in an index and prefills it.
	"deploy/helm/indexed/Chart.yaml":  "apiVersion: v2\nname: helm-indexed\nversion: 0.1.0\n",
	"deploy/helm/indexed/values.yaml": "ownedImages: {server: server.image}\nserver:\n  image: {registry: docker.io, repository: team/indexed, tag: v1}\n",
	"deploy/helm/indexed/templates/pod.yaml": "apiVersion: v1\nkind: Pod\nmetadata:\n  name: indexed\nspec:\n  containers:\n    - name: app\n" +
		"      image: {{ .Values.server.image.registry }}/{{ .Values.server.image.repository }}:{{ .Values.server.image.tag }}\n",
	// whole takes its image as one string, as nvcf-otel-collector does.
	"deploy/helm/whole/Chart.yaml":  "apiVersion: v2\nname: helm-whole\nversion: 0.1.0\n",
	"deploy/helm/whole/values.yaml": "image: \"\"\n",
	"deploy/helm/whole/templates/pod.yaml": "apiVersion: v1\nkind: Pod\nmetadata:\n  name: whole\nspec:\n  containers:\n    - name: app\n" +
		"      image: {{ .Values.image }}\n",
	"deploy/helm/broken/Chart.yaml":         "apiVersion: v2\nname: helm-broken\nversion: 0.1.0\n",
	"deploy/helm/broken/templates/pod.yaml": "{{ fail \"unrelated template error\" }}\n",
}

// Fixture stack parts. The state header loads base.yaml and the layered
// environment, as every stack does; the probe is that layered environment.
const (
	fixtureStateHeader = "environments:\n  default:\n    values:\n      - ../environments/base.yaml\n" +
		"      - ../environments/{{ requiredEnv \"HELMFILE_ENV\" }}.yaml\n---\n" +
		"repositories:\n  - name: nvcf\n    url: {{ .Values.global.helm.sources.url }}\n"
	fixtureBase = "global:\n  image: {registry: nvcr.io, repository: team}\n" +
		"app:\n  image: {tag: v2}\n"
	fixtureSourcedValues = "registry: {{ .Values.global.image.registry }}\n" +
		"          repository: {{ .Values.global.image.repository }}/app\n" +
		"          tag: {{ .Values.app.image.tag }}\n"
)

// fixtureRelease returns a release of chart nvcf/<chart> whose image values
// are the given YAML lines.
func fixtureRelease(name, chart, image string) string {
	return "releases:\n  - name: " + name + "\n    namespace: " + name + "\n    chart: nvcf/" + chart + "\n    version: 0.1.0\n" +
		"    values:\n      - image:\n          " + image
}

// fixtureStack returns the files of a released stack with one state.
func fixtureStack(name, base, state string) map[string]string {
	dir := "deploy/stacks/" + name + "/"
	return map[string]string{
		dir + "environments/base.yaml":    base,
		dir + "helmfile.d/01.yaml.gotmpl": fixtureStateHeader + state,
		dir + stackInventoryFile:          "schemaVersion: 1\nstates:\n  - {plane: control-plane, path: " + dir + "helmfile.d/01.yaml.gotmpl}\npublishedChartRepository: https://charts.example.invalid\n",
	}
}

// TestStackImageSourcesReportsEachFailure drives the whole evaluation on a
// fixture repository, one stack per rule, so each rule is pinned by a fixture
// built for it rather than by the real stacks.
func TestStackImageSourcesReportsEachFailure(t *testing.T) {
	if os.Getenv("NVCF_IMAGE_CATALOG_E2E") != "1" {
		t.Skip("set NVCF_IMAGE_CATALOG_E2E=1 to render fixture stacks with Helmfile and Helm")
	}
	isolateHelmConfiguration(t)
	sourced := fixtureRelease("app", "helm-app", fixtureSourcedValues)
	untracked := sortedKeys(untrackedStackCharts)[0]
	// childState is an included stack's state: it loads its own layered
	// environment only when run on its own, as observability does.
	childState := func(release string) string {
		return "environments:\n  default:\n    values:\n      - ../environments/base.yaml\n" +
			"      {{- if not (dig \"included\" false .Values) }}\n      - ../environments/{{ requiredEnv \"HELMFILE_ENV\" }}.yaml\n      {{- end }}\n---\n" +
			"repositories:\n  - name: nvcf\n    url: {{ .Values.global.helm.sources.url }}\n" + release
	}
	childRelease := fixtureRelease("child", "helm-app", "registry: {{ .Values.child.image.registry }}\n"+
		"          repository: {{ .Values.child.image.repository }}\n          tag: {{ .Values.child.image.tag }}\n")
	include := func(stack, forward string) string {
		return "helmfiles:\n  - path: ../../" + stack + "/helmfile.d/01.yaml.gotmpl\n    values:\n      - included: true\n" +
			"      - global:\n          {{- toYaml .Values.global | nindent 10 }}\n" + forward
	}
	// Forwarded as the self-managed stack forwards observability values:
	// only when the parent environment sets the key.
	forward := func(key string) string {
		return "      {{- with (index .Values \"" + key + "\") }}\n      - " + key + ":\n          {{- toYaml . | nindent 10 }}\n      {{- end }}\n"
	}
	childBase := "child:\n  image: {registry: nvcr.io, repository: team/child, tag: v1}\n"
	failingBase := "failing-child:\n  image: {registry: nvcr.io, repository: team/child}\n"
	stacks := []map[string]string{
		fixtureStack("covered", fixtureBase, sourced),
		fixtureStack("template-default", fixtureBase, fixtureRelease("app", "helm-app",
			"registry: {{ dig \"app\" \"image\" \"registry\" \"\" .Values | default \"docker.io\" }}\n          repository: {{ .Values.global.image.repository }}/app\n")),
		fixtureStack("dig-default", fixtureBase, fixtureRelease("app", "helm-app",
			"registry: {{ dig \"registry\" \"docker.io\" (dig \"app\" \"image\" dict .Values) }}\n          repository: {{ .Values.global.image.repository }}/app\n")),
		fixtureStack("no-source", "app: {}\n", fixtureRelease("app", "helm-defaulted", "{}\n")),
		fixtureStack("comment-only", "# app:\n#   image: {registry: docker.io, repository: team/app}\napp: {}\n",
			fixtureRelease("app", "helm-app", "registry: {{ dig \"app\" \"image\" \"registry\" \"\" .Values }}\n          repository: team/app\n")),
		fixtureStack("undeclared-key", "app: {}\n", fixtureRelease("app", "helm-app",
			"registry: {{ dig \"global\" \"image\" \"registry\" \"\" .Values }}\n          repository: {{ dig \"global\" \"image\" \"repository\" \"\" .Values }}/app\n")),
		fixtureStack("empty-key", "global:\n  image: {registry: \"\", repository: team}\n", fixtureRelease("app", "helm-app",
			"registry: {{ .Values.global.image.registry }}\n          repository: {{ .Values.global.image.repository }}/app\n")),
		// The sidecar image is one string in base.yaml, so it gets no
		// sentinel and its rendered image is reported.
		fixtureStack("single-string", fixtureBase+"sidecar:\n  image: nvcr.io/team/sidecar:v1\n",
			sourced+fixtureRelease("sidecar", "helm-whole", "{{ .Values.sidecar.image }}\n")[len("releases:\n"):]),
		fixtureStack("repository-host", "global:\n  image: {registry: nvcr.io, repository: docker.io/team}\napp:\n  image: {tag: v2}\n", sourced),
		// A listed untracked chart is skipped, not rendered or reported.
		fixtureStack("untracked", fixtureBase, sourced+fixtureRelease("skipped", untracked, "{}\n")[len("releases:\n"):]),
		fixtureStack("tag-swallowed", fixtureBase, fixtureRelease("app", "helm-app",
			"registry: {{ .Values.global.image.registry }}\n          repository: {{ .Values.global.image.repository }}/app\n          tag: v1\n")),
		fixtureStack("child", childBase, ""),
		fixtureStack("parent-forwarding", "global: {}\n", include("child", forward("child"))),
		fixtureStack("parent-dropping", "global: {}\n", include("child", "")),
		// Forwards child, then replaces its tag: the override is swallowed
		// only through this parent, so the parent reports it.
		fixtureStack("parent-tag-swallowed", "global: {}\n", include("child", forward("child")+"      - child: {image: {tag: fixed}}\n")),
		// failing-child reports its own chart default image; the parent
		// that forwards everything must not report it again.
		fixtureStack("failing-child", failingBase, ""),
		fixtureStack("parent-of-failing", "global: {}\n", include("failing-child", forward("failing-child"))),
		// dev-child is dev-only, so it is not run: the parent reports its
		// chart default image itself.
		fixtureStack("parent-of-dev", "global: {}\n", include("dev-child", forward("dev-child"))),
		// A release set entry replaces the base.yaml registry after the
		// values files; the probe applies it as Helmfile does.
		fixtureStack("set-override", fixtureBase, sourced+"    set:\n      - name: image.registry\n        value: docker.io\n"),
		// A release marked installed: false is not deployed, so it is not
		// rendered (its chart would fail).
		fixtureStack("not-installed", fixtureBase, sourced+"  - name: skipped\n    namespace: skipped\n    chart: nvcf/helm-broken\n    version: 0.1.0\n    installed: false\n"),
		// The release renders an extra image only in its own namespace.
		fixtureStack("namespace-workload", fixtureBase, fixtureRelease("workers", "helm-workers", fixtureSourcedValues)),
		// The index names the values path of a prefilled image.
		fixtureStack("indexed-default", "app: {}\n", fixtureRelease("app", "helm-indexed", "{}\n")),
		// Parent and included stack both declare global.image with a tag;
		// the parent's forwarded value wins, so both pass.
		fixtureStack("shared-child", "global:\n  image: {registry: nvcr.io, repository: child, tag: v2}\n", ""),
		fixtureStack("shared-parent", "global:\n  image: {registry: nvcr.io, repository: team, tag: v1}\n",
			fixtureRelease("app", "helm-app", "registry: {{ .Values.global.image.registry }}\n          repository: {{ .Values.global.image.repository }}/app\n          tag: {{ .Values.global.image.tag }}\n")+"helmfiles:\n  - path: ../../shared-child/helmfile.d/01.yaml.gotmpl\n    values:\n      - values-included.yaml\n"+
				"      - global:\n          {{- toYaml .Values.global | nindent 10 }}\n"),
		fixtureStack("digest-swallowed", "global:\n  image: {registry: nvcr.io, repository: team}\napp:\n  image: {tag: v2, digest: \"sha256:"+strings.Repeat("c", 64)+"\"}\n", sourced),
		fixtureStack("pending-passing", fixtureBase, sourced),
		fixtureStack("pending-failing", "app: {}\n", fixtureRelease("app", "helm-defaulted", "{}\n")),
		fixtureStack("pending-render", fixtureBase, fixtureRelease("app", "helm-broken", "{}\n")),
		fixtureStack("dev-released", fixtureBase, sourced),
		{"deploy/stacks/dev/helmfile.yaml.gotmpl": "releases: []\n"},
		// pending-fix does not excuse a missing inventory, even without a
		// base.yaml: the configuration error is checked first.
		{"deploy/stacks/pending-no-base/helmfile.d/01.yaml.gotmpl": "releases: []\n"},
		{"deploy/stacks/no-inventory/helmfile.d/01.yaml.gotmpl": fixtureStateHeader + sourced, "deploy/stacks/no-inventory/environments/base.yaml": fixtureBase},
	}
	files := map[string]string{
		stackImageExclusionsFile: "deploy/stacks/pending-passing: {reason: pending-fix}\n" +
			"deploy/stacks/pending-failing: {reason: pending-fix, issue: https://github.com/dsx-ai-factory/nvcf/issues/3297}\n" +
			"deploy/stacks/pending-render: {reason: pending-fix}\n" +
			"deploy/stacks/dev-released: {reason: dev-only}\n" +
			"deploy/stacks/dev: {reason: dev-only}\n" +
			"deploy/stacks/dev-child: {reason: dev-only}\n" +
			"deploy/stacks/pending-no-base: {reason: pending-fix}\n" +
			"deploy/stacks/gone: {reason: pending-fix}\n",
		// The included stacks' own states, which the parents include.
		"deploy/stacks/child/helmfile.d/01.yaml.gotmpl":         childState(childRelease),
		"deploy/stacks/failing-child/helmfile.d/01.yaml.gotmpl": childState(fixtureRelease("failing", "helm-defaulted", "{}\n")),
		"deploy/stacks/dev-child/helmfile.d/01.yaml.gotmpl":     childState(fixtureRelease("dev", "helm-defaulted", "{}\n")),
		"deploy/stacks/dev-child/environments/base.yaml":        "dev-child: {}\n",
		"deploy/stacks/shared-child/helmfile.d/01.yaml.gotmpl": childState(fixtureRelease("shared", "helm-app",
			"registry: {{ .Values.global.image.registry }}\n          repository: {{ .Values.global.image.repository }}\n          tag: {{ .Values.global.image.tag }}\n")),
		// A values file in the include, read for the keys it forwards.
		"deploy/stacks/shared-parent/helmfile.d/values-included.yaml": "included: true\n",
	}
	for name, content := range stackFixtureCharts {
		files[name] = content
	}
	for _, stack := range stacks {
		for name, content := range stack {
			if _, included := files[name]; !included {
				files[name] = content
			}
		}
	}
	// The second state of unlisted-state is missing from its inventory.
	for name, content := range fixtureStack("unlisted-state", fixtureBase, sourced) {
		files[name] = content
	}
	files["deploy/stacks/unlisted-state/helmfile.d/02.yaml.gotmpl"] = fixtureStateHeader + "releases: []\n"
	// stale-state lists a state that is not tracked; bad-inventory has an
	// inventory the release tooling rejects.
	for name, content := range fixtureStack("stale-state", fixtureBase, sourced) {
		files[name] = content
	}
	files["deploy/stacks/stale-state/"+stackInventoryFile] = "schemaVersion: 1\nstates:\n" +
		"  - {plane: control-plane, path: deploy/stacks/stale-state/helmfile.d/01.yaml.gotmpl}\n" +
		"  - {plane: control-plane, path: deploy/stacks/stale-state/helmfile.d/09.yaml.gotmpl}\n" +
		"publishedChartRepository: https://charts.example.invalid\n"
	for name, content := range fixtureStack("bad-inventory", fixtureBase, sourced) {
		files[name] = content
	}
	files["deploy/stacks/bad-inventory/"+stackInventoryFile] = "schemaVersion: 2\n"
	root := writeChartFixtureTree(t, files)
	for _, args := range [][]string{{"init", "-q"}, {"add", "."}} {
		if _, err := imageCatalogCommand("git", append([]string{"-C", root}, args...)...); err != nil {
			t.Fatal(err)
		}
	}
	report, err := evaluateStackImageSources(root, filepath.Join(root, stackImageExclusionsFile), t.TempDir)
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]map[string]bool)
	problems := make(map[string][]string)
	for _, finding := range report.Findings {
		if got[finding.Subject] == nil {
			got[finding.Subject] = make(map[string]bool)
		}
		got[finding.Subject][finding.Category] = true
		problems[finding.Subject] = append(problems[finding.Subject], strings.TrimSpace(finding.Image+" "+finding.ValuesPath)+": "+finding.Problem)
	}
	// Each fixture names the problem its rule must report, so another rule
	// producing the same category cannot stand in for a broken one.
	want := map[string]struct{ category, problem string }{
		"deploy/stacks/template-default":     {findingImageSource, `helmfile.d/01.yaml.gotmpl:17: default "docker.io" supplies an image source`},
		"deploy/stacks/dig-default":          {findingImageSource, `helmfile.d/01.yaml.gotmpl:17: default "docker.io" supplies an image source`},
		"deploy/stacks/no-source":            {findingImageSource, "image.repository: image does not take its registry and repository from base.yaml"},
		"deploy/stacks/comment-only":         {findingImageSource, "chart image guard rejects the probe values"},
		"deploy/stacks/undeclared-key":       {findingImageSource, "chart image guard rejects the probe values"},
		"deploy/stacks/empty-key":            {findingImageSource, "chart image guard rejects the probe values"},
		"deploy/stacks/single-string":        {findingImageSource, "nvcr.io/team/sidecar:v1 image: image does not take its registry and repository from base.yaml"},
		"deploy/stacks/repository-host":      {findingImageSource, `global.image.repository: repository "docker.io/team" contains registry host docker.io`},
		"deploy/stacks/tag-swallowed":        {findingImageSource, "app.image.tag: tag set in base.yaml does not reach its rendered image"},
		"deploy/stacks/parent-dropping":      {findingImageSource, "child.image.registry: image value of included stack deploy/stacks/child is not forwarded"},
		"deploy/stacks/parent-tag-swallowed": {findingImageSource, "child.image.tag: tag set in base.yaml does not reach its rendered image through included stack deploy/stacks/child"},
		"deploy/stacks/failing-child":        {findingImageSource, "image does not take its registry and repository from base.yaml"},
		"deploy/stacks/parent-of-dev":        {findingImageSource, "image does not take its registry and repository from base.yaml through included stack deploy/stacks/dev-child"},
		"deploy/stacks/digest-swallowed":     {findingImageSource, "app.image.digest: digest set in base.yaml does not reach its rendered image"},
		"deploy/stacks/stale-state":          {findingStackConfig, "helmfile.d/09.yaml.gotmpl: listed state is not a tracked Helmfile state"},
		"deploy/stacks/bad-inventory":        {findingStackConfig, "schemaVersion is 2"},
		"deploy/stacks/pending-no-base":      {findingStackConfig, "released stack has no release-inventory.yaml"},
		"deploy/stacks/set-override":         {findingImageSource, "image does not take its registry and repository from base.yaml"},
		"deploy/stacks/namespace-workload":   {findingImageSource, "docker.io/team/worker:v1"},
		"deploy/stacks/indexed-default":      {findingImageSource, "server.image: image does not take its registry and repository"},
		"deploy/stacks/pending-passing":      {findingPendingFixed, "stack now passes image sources"},
		"deploy/stacks/pending-render":       {findingRenderError, "unrelated template error"},
		"deploy/stacks/dev-released":         {findingInvalidEntry, "so it is released"},
		"deploy/stacks/gone":                 {findingInvalidEntry, "no tracked Helmfile stack"},
		"deploy/stacks/no-inventory":         {findingStackConfig, "released stack has no release-inventory.yaml"},
		"deploy/stacks/unlisted-state":       {findingStackConfig, "helmfile.d/02.yaml.gotmpl: state is not rendered"},
	}
	for stack, expectation := range want {
		if !got[stack][expectation.category] || len(got[stack]) != 1 {
			t.Errorf("%s: categories %v, want only %q", stack, got[stack], expectation.category)
		}
		if !strings.Contains(strings.Join(problems[stack], "\n"), expectation.problem) {
			t.Errorf("%s: problems %q, want one containing %q", stack, problems[stack], expectation.problem)
		}
	}
	if len(got) != len(want) {
		t.Errorf("unexpected findings:\n%s", report.Summary())
	}
	// The parent that forwards the included stack's values passes, and the
	// layered probe environment reaches every image of the covered stacks.
	if strings.Join(report.Covered, ",") != "deploy/stacks/child,deploy/stacks/covered,deploy/stacks/not-installed,deploy/stacks/parent-forwarding,deploy/stacks/parent-of-failing,deploy/stacks/shared-child,deploy/stacks/shared-parent,deploy/stacks/untracked" {
		t.Errorf("covered %v\n%s", report.Covered, report.Summary())
	}
	if expected := strings.Join(report.Expected, "\n"); !strings.Contains(expected, "deploy/stacks/pending-failing") || !strings.Contains(expected, "issues/3297") {
		t.Errorf("expected failures %v", report.Expected)
	}
	if want := "deploy/stacks/untracked skipped (" + untracked + ": " + untrackedStackCharts[untracked] + ")"; strings.Join(report.Skipped[skippedUntrackedChart], ",") != want {
		t.Errorf("skipped untracked charts %v, want %s", report.Skipped[skippedUntrackedChart], want)
	}
	if strings.Join(report.Skipped[reasonDevOnly], ",") != "deploy/stacks/dev,deploy/stacks/dev-child" {
		t.Errorf("skipped %v", report.Skipped)
	}
}
