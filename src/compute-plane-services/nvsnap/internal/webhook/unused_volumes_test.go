// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"context"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// kimiWorker is a Dynamo vLLM worker as a PodClique runs it: its engine
// container "main" mounts the model emptyDir download-ngc-model fills, and
// a native sidecar and a wait run beside it.
func kimiWorker(i string) *corev1.Pod {
	yes := true
	always := corev1.ContainerRestartPolicyAlways
	empty := corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}
	p := lwsRank(i)
	p.OwnerReferences = []metav1.OwnerReference{{Kind: "PodClique", Name: "w", Controller: &yes}}
	p.Spec.Containers[0].Name = "main"
	p.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "ngc-models", MountPath: "/config/models"}, {Name: "merged-certs", MountPath: "/etc/ssl/certs", ReadOnly: true}}
	p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: "dgd-gang-health", Image: "h"})
	p.Spec.Volumes = []corev1.Volume{{Name: "ngc-models", VolumeSource: empty}, {Name: "merged-certs", VolumeSource: empty}, {Name: "tools", VolumeSource: empty}}
	p.Spec.InitContainers = []corev1.Container{
		{Name: "a-toolbox", Command: []string{"cp"}, VolumeMounts: []corev1.VolumeMount{{Name: "tools", MountPath: "/tools"}}},
		{Name: "fast-merge-certs", Command: []string{"merge"}, VolumeMounts: []corev1.VolumeMount{{Name: "tools", MountPath: "/tools", ReadOnly: true}, {Name: "merged-certs", MountPath: "/merged"}}},
		{Name: "download-ngc-model", Command: []string{"download"}, VolumeMounts: []corev1.VolumeMount{{Name: "ngc-models", MountPath: "/config/models"}}},
		{Name: "wait-for-etcd", Command: []string{"sh", "-c", "until nc etcd 2379; do sleep 1; done"}},
		{Name: "dgd-model-ready-beacon", Command: []string{"serve"}, RestartPolicy: &always},
	}
	return p
}

// The failure this fixes: with no model volume, every group placeholder
// downloaded the whole model again although no restored process used it.
// With the group's capture recording the model volume unused, the
// placeholder skips the download and keeps everything else it ran.
func TestGroupPlaceholderSkipsSetupOfUnusedVolumes(t *testing.T) {
	pod := kimiWorker("0")
	unused := map[string]bool{"ngc-models": true}
	run := placeholderInitsUnused(t, pod, true, unused)
	want := map[string]bool{"a-toolbox": true, "fast-merge-certs": true, "download-ngc-model": false, "wait-for-etcd": false, "dgd-model-ready-beacon": true}
	for n, w := range want {
		if run[n] != w {
			t.Errorf("%s runs = %v, want %v", n, run[n], w)
		}
	}
	// Without the record, as before: the download runs.
	if !placeholderInitsUnused(t, pod, true, nil)["download-ngc-model"] {
		t.Error("without a recorded decision the download must run")
	}
	// A lone placeholder runs everything, whatever the record says.
	if !placeholderInitsUnused(t, pod, false, unused)["download-ngc-model"] {
		t.Error("a lone placeholder must run the download")
	}
	got := applyPatches(t, pod, placeholderInitPatches(pod, true, "main", unused))
	if got.Annotations[SkippedInitsAnnotation] != "download-ngc-model" {
		t.Errorf("annotation = %q", got.Annotations[SkippedInitsAnnotation])
	}
	for _, c := range got.Spec.InitContainers {
		if c.Name == "download-ngc-model" && !strings.Contains(strings.Join(c.Command, " "), "no restored process uses") {
			t.Errorf("skip reason: %v", c.Command)
		}
	}
}

// Another running container that reads the volume keeps its setup: only
// the checkpointed container's processes are restored rather than
// started. So does a volume that is not an emptyDir.
func TestGroupPlaceholderKeepsSetupOthersRead(t *testing.T) {
	pod := kimiWorker("0")
	pod.Spec.Containers[1].VolumeMounts = []corev1.VolumeMount{{Name: "ngc-models", MountPath: "/m", ReadOnly: true}}
	if !placeholderInitsUnused(t, pod, true, map[string]bool{"ngc-models": true})["download-ngc-model"] {
		t.Error("a sidecar reads the model: the download must run")
	}
	pod = kimiWorker("0")
	pod.Spec.Volumes[0].VolumeSource = corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "c"}}
	if !placeholderInitsUnused(t, pod, true, map[string]bool{"ngc-models": true})["download-ngc-model"] {
		t.Error("a PVC is not skipped")
	}
}

func members(pods ...*corev1.Pod) []MemberVolumeUse {
	out := make([]MemberVolumeUse, len(pods))
	for i, p := range pods {
		out[i] = MemberVolumeUse{Pod: p, Container: "main", Used: []string{}, Large: []string{"ngc-models"}}
	}
	return out
}

func TestGroupUnusedVolumes(t *testing.T) {
	got, why := GroupUnusedVolumes(members(kimiWorker("0"), kimiWorker("1")))
	// Only the model: the certificates the engine reads were small enough
	// to copy, so their setup runs, and may be read again after a restore.
	if !reflect.DeepEqual(got, []string{"ngc-models"}) {
		t.Fatalf("got %v (%s)", got, why)
	}
	// The group decides once: a companion that mounts no model (etcd)
	// takes part with nothing to skip.
	etcd := kimiWorker("2")
	etcd.Spec.Containers[0].VolumeMounts = nil
	etcd.Spec.InitContainers = nil
	m := append(members(kimiWorker("0"), kimiWorker("1")), MemberVolumeUse{Pod: etcd, Container: "main", Used: []string{}})
	if got, why := GroupUnusedVolumes(m); !reflect.DeepEqual(got, []string{"ngc-models"}) {
		t.Errorf("with a companion: got %v (%s)", got, why)
	}
	cases := map[string]func(m []MemberVolumeUse){
		"a member's use not known": func(m []MemberVolumeUse) { m[1].Used = nil },
		"a member uses the model":  func(m []MemberVolumeUse) { m[1].Used = []string{"ngc-models"} },
		"nothing too large to copy": func(m []MemberVolumeUse) {
			m[0].Large, m[1].Large = nil, nil
		},
		"a bare pod": func(m []MemberVolumeUse) {
			m[1].Pod.OwnerReferences = nil
		},
		"a Job's pod": func(m []MemberVolumeUse) {
			m[0].Pod.OwnerReferences[0].Kind = "Job"
		},
		"opted out": func(m []MemberVolumeUse) {
			m[0].Pod.Annotations[KeepInitsAnnotation] = "true"
		},
		// All or nothing: one member whose sidecar reads the model would
		// still download it, and hold the group restore back.
		"a member still downloads": func(m []MemberVolumeUse) {
			m[1].Pod.Spec.Containers[1].VolumeMounts = []corev1.VolumeMount{{Name: "ngc-models", MountPath: "/m", ReadOnly: true}}
		},
	}
	for name, mutate := range cases {
		m := members(kimiWorker("0"), kimiWorker("1"))
		mutate(m)
		got, why := GroupUnusedVolumes(m)
		if got != nil || why == "" {
			t.Errorf("%s: got %v, want none with a reason", name, got)
		}
	}
}

func TestVolumesAt(t *testing.T) {
	pod := kimiWorker("0")
	got := VolumesAt(pod, "main", []string{"/", "/dev/shm", "/config/models/", "/etc/ssl/certs", "/config/models"})
	if !reflect.DeepEqual(got, []string{"merged-certs", "ngc-models"}) {
		t.Errorf("got %v", got)
	}
	if got := VolumesAt(pod, "main", nil); got == nil || len(got) != 0 {
		t.Errorf("no mounts used must be known and empty, got %#v", got)
	}
}

// Through admission, a pod that must keep its setup keeps it whatever
// the record says: a Job's (which stops replacing its pods), one nothing
// recreates (which starts cold in place), one opted out.
func TestCRIUGroupRestore_KeepsSetupWhereAFailedRestoreNeedsIt(t *testing.T) {
	g := CRIUGroup{Checkpoints: []string{"r0__1", "r1__1"}, Nodes: []string{"n0", "n1"}, UnusedVolumes: []string{"ngc-models"}}
	for name, mutate := range map[string]func(p *corev1.Pod){
		"Job":       func(p *corev1.Pod) { p.OwnerReferences[0].Kind = "Job" },
		"bare":      func(p *corev1.Pod) { p.OwnerReferences = nil },
		"opted out": func(p *corev1.Pod) { p.Annotations[KeepInitsAnnotation] = "true" },
	} {
		m, _ := groupMutator(t, g, "")
		m.SkipUnusedInits = true
		pod := kimiWorker("1")
		mutate(pod)
		patches, err := m.Mutate(context.Background(), pod)
		if err != nil {
			t.Fatal(err)
		}
		got := applyPatches(t, pod, patches)
		if got.Annotations[CRIURestoreAnnotation] != "r1__1" {
			t.Fatalf("%s: not admitted as a placeholder", name)
		}
		for _, c := range got.Spec.InitContainers {
			if c.Name == "download-ngc-model" && c.Command[0] == HelperBin {
				t.Errorf("%s: download skipped", name)
			}
		}
	}
}

// Through admission: with the flag and a recorded decision, a rank's
// placeholder skips the download; without the flag it does not.
func TestCRIUGroupRestore_SkipsUnusedSetupOnlyWithTheFlag(t *testing.T) {
	g := CRIUGroup{Checkpoints: []string{"r0__1", "r1__1"}, Nodes: []string{"n0", "n1"}, UnusedVolumes: []string{"ngc-models"}}
	for _, flag := range []bool{false, true} {
		m, _ := groupMutator(t, g, "")
		m.SkipUnusedInits = flag
		pod := kimiWorker("1")
		patches, err := m.Mutate(context.Background(), pod)
		if err != nil {
			t.Fatal(err)
		}
		got := applyPatches(t, pod, patches)
		if got.Annotations[CRIURestoreAnnotation] != "r1__1" {
			t.Fatalf("flag %v: not admitted as a placeholder: %v", flag, got.Annotations)
		}
		var download []string
		for _, c := range got.Spec.InitContainers {
			if c.Name == "download-ngc-model" {
				download = c.Command
			}
		}
		skipped := len(download) > 1 && download[0] == HelperBin && download[1] == "skip"
		if skipped != flag {
			t.Errorf("flag %v: download skipped = %v (%v)", flag, skipped, download)
		}
		if (got.Annotations[SkippedInitsAnnotation] != "") != flag {
			t.Errorf("flag %v: annotation %q", flag, got.Annotations[SkippedInitsAnnotation])
		}
	}
}

// A file the engine reads through a subPath of the volume: a placeholder
// that skipped its setup would get a directory kubelet made in its place.
// The download runs, and the group decides nothing to skip.
func TestGroupPlaceholderKeepsSetupForSubPathMounts(t *testing.T) {
	for _, set := range []func(*corev1.VolumeMount){
		func(vm *corev1.VolumeMount) { vm.SubPath = "config.json" },
		func(vm *corev1.VolumeMount) { vm.SubPathExpr = "$(POD_NAME)" },
	} {
		pod := kimiWorker("0")
		vm := corev1.VolumeMount{Name: "ngc-models", MountPath: "/etc/model.json"}
		set(&vm)
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, vm)
		if !placeholderInitsUnused(t, pod, true, map[string]bool{"ngc-models": true})["download-ngc-model"] {
			t.Errorf("%+v: the download must run", vm)
		}
		if got, why := GroupUnusedVolumes(members(pod, kimiWorker("1"))); got != nil || why == "" {
			t.Errorf("%+v: got %v, want none with a reason", vm, got)
		}
	}
}
