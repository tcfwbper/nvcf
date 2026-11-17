// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"context"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// configuredWorker is a Dynamo vLLM worker in namespace ns, as the operator
// creates it: its Dynamo namespace named after the Kubernetes one, the
// worker hash in DYN_NAMESPACE_WORKER_SUFFIX, a KV-transfer flag, and a
// sidecar without GPUs.
func configuredWorker(ns, suffix string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, GenerateName: "kimi-0-vllmworker-"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: "sidecar", Image: "utils", Env: []corev1.EnvVar{{Name: "LOG", Value: "info"}}},
			{Name: "main", Image: "dynamo-vllm:1", Command: []string{"python3", "-m", "dynamo.vllm"},
				Args: []string{"--model", "kimi", "--kv-transfer-config", `{"kv_role":"kv_both"}`},
				Env: []corev1.EnvVar{
					{Name: "DYN_NAMESPACE", Value: ns + "-kimi"},
					{Name: "DYN_NAMESPACE_WORKER_SUFFIX", Value: suffix},
					{Name: "HF_TOKEN", Value: "hf_secret_literal"},
					{Name: "POD_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}},
				},
				Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("4")}}},
		}},
	}
}

// A later instance of the same configuration runs in a new namespace:
// its process configuration is the same, so it still restores.
func TestProcessConfig_SameAcrossInstances(t *testing.T) {
	a, b := ProcessConfig(configuredWorker("sr-aaa", "6545220e")), ProcessConfig(configuredWorker("sr-bbb", "6545220e"))
	if a == "" || a != b {
		t.Fatalf("instances of one configuration differ:\n%s\n%s", a, b)
	}
	if changed := ProcessConfigChanges(a, b); changed != nil {
		t.Errorf("changes %v, want none", changed)
	}
}

// What differs is named, so the log says why a pod started cold.
func TestProcessConfigChanges_NamesWhatChanged(t *testing.T) {
	// A capture records the checkpointed container's part only.
	recorded := ProcessConfigOf(ProcessConfig(configuredWorker("sr-aaa", "6545220e")), "main")
	for name, tc := range map[string]struct {
		edit func(p *corev1.Pod)
		want []string
	}{
		// The in-place update that kept the configuration hash.
		"worker suffix": {func(p *corev1.Pod) { p.Spec.Containers[1].Env[1].Value = "7215a3f6" },
			[]string{"main: env DYN_NAMESPACE_WORKER_SUFFIX"}},
		"KV-transfer flag": {func(p *corev1.Pod) { p.Spec.Containers[1].Args[3] = `{"kv_role":"kv_producer"}` },
			[]string{"main: args"}},
		"added env": {func(p *corev1.Pod) {
			p.Spec.Containers[1].Env = append(p.Spec.Containers[1].Env, corev1.EnvVar{Name: "DYN_ROUTER_TEMPERATURE", Value: "0.5"})
		}, []string{"main: env DYN_ROUTER_TEMPERATURE (added)"}},
		"removed env": {func(p *corev1.Pod) { p.Spec.Containers[1].Env = p.Spec.Containers[1].Env[:3] },
			[]string{"main: env POD_IP (removed)"}},
		"reference changed": {func(p *corev1.Pod) { p.Spec.Containers[1].Env[3].ValueFrom.FieldRef.FieldPath = "status.hostIP" },
			[]string{"main: env POD_IP"}},
		"image": {func(p *corev1.Pod) { p.Spec.Containers[1].Image = "dynamo-vllm:2" }, []string{"main: image"}},
		// The restored process is the GPU container's; a sidecar starts
		// fresh either way.
		"sidecar only": {func(p *corev1.Pod) { p.Spec.Containers[0].Env[0].Value = "debug" }, nil},
		// Placement, not configuration: replicas of one component differ
		// in it and restore from each other's checkpoints.
		"replica index": {func(p *corev1.Pod) {
			p.Spec.Containers[1].Env = append(p.Spec.Containers[1].Env,
				corev1.EnvVar{Name: "GROVE_PCLQ_POD_INDEX", Value: "1"}, corev1.EnvVar{Name: "LWS_LEADER_ADDRESS", Value: "kimi-1.kimi"})
		}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			p := configuredWorker("sr-bbb", "6545220e")
			tc.edit(p)
			if got := ProcessConfigChanges(recorded, ProcessConfig(p)); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("changes %v, want %v", got, tc.want)
			}
		})
	}
}

// A capture from before configurations were recorded restores as before.
func TestProcessConfigChanges_NoneRecorded(t *testing.T) {
	if got := ProcessConfigChanges("", ProcessConfig(configuredWorker("sr-a", "x"))); got != nil {
		t.Errorf("changes %v, want none", got)
	}
	if got := ProcessConfigChanges("v0 main/args=1", ProcessConfig(configuredWorker("sr-a", "x"))); len(got) != 1 || !strings.Contains(got[0], "format") {
		t.Errorf("another format must not pass as equal: %v", got)
	}
}

// The configuration holds digests, never a value: a pod's literal token
// would otherwise land in its annotations and in the capture record.
func TestProcessConfig_HoldsNoValues(t *testing.T) {
	pc := ProcessConfig(configuredWorker("sr-aaa", "6545220e"))
	for _, v := range []string{"hf_secret_literal", "6545220e", "kimi", "dynamo-vllm"} {
		if strings.Contains(pc, v) {
			t.Errorf("configuration holds %q: %s", v, pc)
		}
	}
}

// The webhook stamps a GPU pod with its configuration as received, before
// its own patches change the env.
func TestMutate_StampsTheProcessConfig(t *testing.T) {
	m, _ := groupMutator(t, CRIUGroup{}, "")
	m.CRIUGroups = nil
	m.CRIUCapture = true
	pod := lwsRank("0")
	want := ProcessConfig(pod)
	patches, err := m.Mutate(context.Background(), pod)
	if err != nil {
		t.Fatal(err)
	}
	if got := applyPatches(t, pod, patches).Annotations[ProcessConfigAnnotation]; got != want || want == "" {
		t.Errorf("stamped %q, want %q", got, want)
	}
	m.CRIUCapture = false
	pod = lwsRank("0")
	patches, _ = m.Mutate(context.Background(), pod)
	if _, ok := applyPatches(t, pod, patches).Annotations[ProcessConfigAnnotation]; ok {
		t.Error("stamped without CRIU capture")
	}
}

// A rank whose configuration differs from its checkpoint's starts cold;
// the same configuration, or a record without one, restores.
func TestCRIUGroupRestore_ProcessConfigChangedStartsCold(t *testing.T) {
	r0, r1 := lwsRank("0"), lwsRank("1")
	recorded := []string{ProcessConfig(r0), ProcessConfig(r1)}
	for name, tc := range map[string]struct {
		recorded []string
		edit     func(p *corev1.Pod)
		restores bool
	}{
		"same configuration":   {recorded, func(*corev1.Pod) {}, true},
		"another namespace":    {recorded, func(p *corev1.Pod) { p.Namespace = "fn2" }, true},
		"changed env":          {recorded, func(p *corev1.Pod) { p.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "DYN_X", Value: "1"}} }, false},
		"changed args":         {recorded, func(p *corev1.Pod) { p.Spec.Containers[0].Args = []string{"serve", "m2"} }, false},
		"record without one":   {nil, func(p *corev1.Pod) { p.Spec.Containers[0].Args = []string{"serve", "m2"} }, true},
		"rank without its own": {[]string{recorded[0], ""}, func(p *corev1.Pod) { p.Spec.Containers[0].Args = []string{"serve", "m2"} }, true},
	} {
		t.Run(name, func(t *testing.T) {
			g := CRIUGroup{Checkpoints: []string{"r0__1", "r1__1"}, Nodes: []string{"n0", "n1"}, ProcessConfigs: tc.recorded}
			m, _ := groupMutator(t, g, "")
			pod := lwsRank("1")
			tc.edit(pod)
			got := m.criuGroupRestorePatches(context.Background(), pod) != nil
			if got != tc.restores {
				t.Errorf("restores = %v, want %v", got, tc.restores)
			}
		})
	}
}

// A single pod's capture record keeps its configuration the same way.
func TestCRIURestore_ProcessConfigChangedStartsCold(t *testing.T) {
	gpuPod := func(args ...string) *corev1.Pod {
		pod := podWithAnnotation("abc123")
		pod.Spec = criuFunctionPod().Spec
		pod.Spec.Containers[1].Args = args
		pod.Spec.Containers[1].Resources.Limits = corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}
		return pod
	}
	man := criuCaptureRecord()
	man.SourcePodMeta[ProcessConfigMeta] = ProcessConfig(gpuPod("serve", "m"))
	m := criuMutator(t, man)
	for _, tc := range []struct {
		args     []string
		restores bool
	}{{[]string{"serve", "m"}, true}, {[]string{"serve", "m", "--max-num-seqs", "8"}, false}} {
		pod := gpuPod(tc.args...)
		patches, err := m.Mutate(context.Background(), pod)
		if err != nil {
			t.Fatal(err)
		}
		if got := applyPatches(t, pod, patches).Annotations[CRIURestoreAnnotation] != ""; got != tc.restores {
			t.Errorf("args %v: restores = %v, want %v", tc.args, got, tc.restores)
		}
	}
}

// The namespace is normalized in env values only: a command naming a word
// that is also the capture's namespace still matches in another one.
func TestProcessConfig_NamespaceWordInTheCommand(t *testing.T) {
	a, b := configuredWorker("dynamo", "x"), configuredWorker("dynamo-prod", "x")
	if c := ProcessConfigChanges(ProcessConfig(a), ProcessConfig(b)); c != nil {
		t.Errorf("changes %v, want none", c)
	}
}

// Past its bound the annotation keeps one digest per container, which
// still tells a changed container from an unchanged one.
func TestProcessConfig_BoundedAnnotation(t *testing.T) {
	big := func(v string) *corev1.Pod {
		p := configuredWorker("sr-a", "x")
		for i := 0; i < 2000; i++ {
			p.Spec.Containers[1].Env = append(p.Spec.Containers[1].Env, corev1.EnvVar{Name: "DYN_SETTING_" + strings.Repeat("X", 10) + string(rune('A'+i%26)) + strings.Repeat("Y", i%7), Value: v})
		}
		return p
	}
	pc := ProcessConfig(big("1"))
	if len(pc) > processConfigAnnotationMax || !strings.Contains(pc, "main/*=") {
		t.Fatalf("annotation of %d bytes: %.200s", len(pc), pc)
	}
	rec := ProcessConfigOf(pc, "main")
	if c := ProcessConfigChanges(rec, ProcessConfig(big("1"))); c != nil {
		t.Errorf("same pod: %v", c)
	}
	if c := ProcessConfigChanges(rec, ProcessConfig(big("2"))); len(c) != 1 || !strings.Contains(c[0], "one digest") {
		t.Errorf("changed pod: %v", c)
	}
}

// An instance's companion (an etcd, a frontend) restores only with the
// process configuration it was captured with, too.
func TestCRIUGroupRestore_CompanionProcessConfig(t *testing.T) {
	frontend := func(temp string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "frontend-x", Namespace: "fn"},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "frontend", Image: "dynamo", Command: []string{"python3", "-m", "dynamo.frontend"},
				Env: []corev1.EnvVar{{Name: "DYN_ROUTER_TEMPERATURE", Value: temp}}}}}}
	}
	rec := ProcessConfigOf(ProcessConfig(frontend("0.0")), "frontend")
	for temp, restores := range map[string]bool{"0.0": true, "0.5": false} {
		g := CRIUGroup{Checkpoints: []string{"r0__1", "f__1"}, Nodes: []string{"n0", "n1"}, Companion: []bool{false, true},
			Containers: []string{"engine", "frontend"}, ProcessConfigs: []string{"", rec}, Slots: []int{1}}
		m, _ := groupMutator(t, g, "")
		if got := m.criuGroupRestorePatches(context.Background(), frontend(temp)) != nil; got != restores {
			t.Errorf("DYN_ROUTER_TEMPERATURE=%s: restores = %v, want %v", temp, got, restores)
		}
	}
}

// Kubernetes expands $(NAME) only from variables declared before it, so
// reordering a variable and one that references it changes the process:
// the restore starts cold. A container whose values reference nothing
// keeps the fingerprint it had (no order item), so existing records still
// match.
func TestProcessConfigChanges_DependentEnvOrder(t *testing.T) {
	withEnv := func(env ...corev1.EnvVar) *corev1.Pod {
		p := configuredWorker("sr-a", "x")
		p.Spec.Containers[1].Env = env
		return p
	}
	base := corev1.EnvVar{Name: "DYN_BASE", Value: "alpha"}
	derived := corev1.EnvVar{Name: "DYN_DERIVED", Value: "$(DYN_BASE)"}
	rec := ProcessConfigOf(ProcessConfig(withEnv(base, derived)), "main")
	c := ProcessConfigChanges(rec, ProcessConfig(withEnv(derived, base)))
	if !reflect.DeepEqual(c, []string{"main: " + processConfigEnvOrder}) {
		t.Errorf("reordered dependent env: changes %v, want the env order", c)
	}
	plain := corev1.EnvVar{Name: "DYN_OTHER", Value: "beta"}
	pc := ProcessConfig(withEnv(base, plain))
	if strings.Contains(pc, processConfigEnvOrder) {
		t.Errorf("an env order without references: %s", pc)
	}
	if c := ProcessConfigChanges(ProcessConfigOf(pc, "main"), ProcessConfig(withEnv(plain, base))); c != nil {
		t.Errorf("reordered independent env: changes %v, want none", c)
	}
}

// A sidecar that grows past the annotation bound compacts every container;
// the unchanged main container still matches its detailed record, and a
// changed one still does not.
func TestProcessConfigChanges_SidecarGrowthCompactsMain(t *testing.T) {
	rec := ProcessConfigOf(ProcessConfig(configuredWorker("sr-a", "x")), "main")
	grown := func(edit func(*corev1.Pod)) string {
		p := configuredWorker("sr-a", "x")
		for i := 0; i < 600; i++ {
			p.Spec.Containers[0].Env = append(p.Spec.Containers[0].Env, corev1.EnvVar{Name: "SIDECAR_SETTING_" + strings.Repeat("Z", 40) + strings.Repeat("Q", i%50) + string(rune('A'+i%26)), Value: "v"})
		}
		if edit != nil {
			edit(p)
		}
		return ProcessConfig(p)
	}
	cur := grown(nil)
	if !strings.Contains(cur, "main/*=") {
		t.Fatalf("main not compacted: %.200s", cur)
	}
	if c := ProcessConfigChanges(rec, cur); c != nil {
		t.Errorf("unchanged main after sidecar growth: changes %v, want none", c)
	}
	if c := ProcessConfigChanges(rec, grown(func(p *corev1.Pod) { p.Spec.Containers[1].Args[1] = "other" })); len(c) != 1 || !strings.Contains(c[0], "one digest") {
		t.Errorf("changed main after sidecar growth: changes %v", c)
	}
}

// A pod that arrives with a process-config stamp for another spec (a
// template that copied an old pod's annotations) is stamped with its own:
// a capture must not record the old configuration for the new process. A
// matching stamp is left as it is.
func TestMutate_ReplacesAStaleSuppliedStamp(t *testing.T) {
	m, _ := groupMutator(t, CRIUGroup{}, "")
	m.CRIUGroups = nil
	m.CRIUCapture = true
	old := ProcessConfig(lwsRank("0"))
	pod := lwsRank("0")
	pod.Spec.Containers[0].Args = append(pod.Spec.Containers[0].Args, "--changed")
	want := ProcessConfig(pod)
	if want == old {
		t.Fatal("the edit did not change the configuration")
	}
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[ProcessConfigAnnotation] = old
	patches, err := m.Mutate(context.Background(), pod)
	if err != nil {
		t.Fatal(err)
	}
	if got := applyPatches(t, pod, patches).Annotations[ProcessConfigAnnotation]; got != want {
		t.Errorf("stamp %q, want the spec's %q", got, want)
	}
	pod = lwsRank("0")
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[ProcessConfigAnnotation] = old
	patches, _ = m.Mutate(context.Background(), pod)
	if got := applyPatches(t, pod, patches).Annotations[ProcessConfigAnnotation]; got != old {
		t.Errorf("matching stamp became %q", got)
	}
}

// Companions that share a cache URI can differ in configuration: the pod
// gets a slot whose checkpoint has its own, and starts cold when none has.
// Before the fix only the first slot was compared, and any slot claimed.
func TestCRIUGroupRestore_CompanionSlotMatchesItsConfig(t *testing.T) {
	frontend := func(temp string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "frontend-x", Namespace: "fn"},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "frontend", Image: "dynamo", Command: []string{"python3", "-m", "dynamo.frontend"},
				Env: []corev1.EnvVar{{Name: "DYN_ROUTER_TEMPERATURE", Value: temp}}}}}}
	}
	rec := func(temp string) string { return ProcessConfigOf(ProcessConfig(frontend(temp)), "frontend") }
	g := CRIUGroup{Checkpoints: []string{"r0__1", "f__1", "f__2"}, Nodes: []string{"n0", "n1", "n2"}, Companion: []bool{false, true, true},
		Containers: []string{"engine", "frontend", "frontend"}, ProcessConfigs: []string{"", rec("0.5"), rec("0.0")}, Slots: []int{1, 2}}
	for temp, want := range map[string]string{"0.0": "2", "0.5": "1", "0.9": ""} {
		m, _ := groupMutator(t, g, "")
		pod := frontend(temp)
		patches := m.criuGroupRestorePatches(context.Background(), pod)
		got := ""
		if patches != nil {
			got = applyPatches(t, pod, patches).Annotations[CRIUGroupOrdinalAnnotation]
		}
		if got != want {
			t.Errorf("DYN_ROUTER_TEMPERATURE=%s: slot %q, want %q", temp, got, want)
		}
	}
}
