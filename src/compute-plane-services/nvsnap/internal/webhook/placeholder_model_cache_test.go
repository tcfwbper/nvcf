// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"context"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// kimiWorkerNGC is kimiWorker with the NGC download init the node model
// cache recognises, in the same slot.
func kimiWorkerNGC(i string) *corev1.Pod {
	p := kimiWorker(i)
	ngc := ngcFunctionPod()
	dl := ngc.Spec.InitContainers[0]
	dl.VolumeMounts = []corev1.VolumeMount{{Name: "ngc-models", MountPath: "/config/models"}}
	for j := range p.Spec.InitContainers {
		if p.Spec.InitContainers[j].Name == "download-ngc-model" {
			p.Spec.InitContainers[j] = dl
		}
	}
	p.Spec.Containers[0].Env = ngc.Spec.Containers[0].Env
	p.Namespace = "sr-fn"
	return p
}

func admitPlaceholder(t *testing.T, g CRIUGroup, skip, cache bool) (*corev1.Pod, *corev1.Pod) {
	t.Helper()
	m, _ := groupMutator(t, g, "")
	m.SkipUnusedInits = skip
	if cache {
		m.NodeModelCache = &NodeModelCache{Root: "/var/lib/nvsnap-model-cache", Wait: 600, Tenants: fixedTenants{"sr-fn": "tenant:nca=a"}}
	}
	pod := kimiWorkerNGC("1")
	patches, err := m.Mutate(context.Background(), pod)
	if err != nil {
		t.Fatal(err)
	}
	got := applyPatches(t, pod, patches)
	if got.Annotations[CRIURestoreAnnotation] != "r1__1" {
		t.Fatalf("not admitted as a placeholder: %v", got.Annotations)
	}
	return pod, got
}

func downloadCommand(p *corev1.Pod) []string {
	if c := initByName(p, "download-ngc-model"); c != nil {
		return c.Command
	}
	return nil
}

func hasVolume(p *corev1.Pod, name string) bool {
	for _, v := range p.Spec.Volumes {
		if v.Name == name {
			return true
		}
	}
	return false
}

// The failure this fixes: a restore placeholder that still ran its model
// download (skipUnusedInits off) pulled the whole model from the registry
// (1.56 TB for one Kimi worker) although the node cache held it, because
// only cold pods were wrapped. Its download now seeds from the node's copy.
func TestGroupPlaceholderDownloadSeedsFromTheNodeCache(t *testing.T) {
	g := CRIUGroup{Checkpoints: []string{"r0__1", "r1__1"}, Nodes: []string{"n0", "n1"}}
	_, got := admitPlaceholder(t, g, false, true)
	_, base := admitPlaceholder(t, g, false, false)
	cmd := downloadCommand(got)
	if len(cmd) < 2 || cmd[0] != HelperBin || cmd[1] != "model-cache" {
		t.Fatalf("download init = %q; want it wrapped by %s model-cache", cmd, HelperBin)
	}
	if !hasVolume(got, ModelCacheVolume) || got.Labels[ModelCacheKeyLabel] == "" {
		t.Fatalf("no cache volume or key label: volumes %+v labels %v", got.Spec.Volumes, got.Labels)
	}
	// The restored container's mounts are those of a placeholder without
	// the cache: it keeps the mounts it was dumped with.
	if !reflect.DeepEqual(got.Spec.Containers, base.Spec.Containers) {
		t.Fatalf("main container mounts changed: %+v", got.Spec.Containers[0].VolumeMounts)
	}
	for _, vm := range got.Spec.Containers[0].VolumeMounts {
		if vm.Name == ModelCacheVolume {
			t.Fatal("the cache is mounted into the restored container")
		}
	}
}

// A download the placeholder skips stays skipped: the cache must not
// bring it back.
func TestGroupPlaceholderSkippedDownloadIsNotWrapped(t *testing.T) {
	g := CRIUGroup{Checkpoints: []string{"r0__1", "r1__1"}, Nodes: []string{"n0", "n1"}, UnusedVolumes: []string{"ngc-models"}}
	_, got := admitPlaceholder(t, g, true, true)
	cmd := downloadCommand(got)
	if len(cmd) < 2 || cmd[0] != HelperBin || cmd[1] != "skip" {
		t.Fatalf("download init = %q; want it skipped", cmd)
	}
	if hasVolume(got, ModelCacheVolume) {
		t.Fatal("a skipped download got the cache volume")
	}
}

// Without the node cache, the placeholder's download is left as it was.
func TestGroupPlaceholderWithoutNodeCacheKeepsTheDownload(t *testing.T) {
	g := CRIUGroup{Checkpoints: []string{"r0__1", "r1__1"}, Nodes: []string{"n0", "n1"}}
	orig, got := admitPlaceholder(t, g, false, false)
	if c, o := downloadCommand(got), downloadCommand(orig); len(c) == 0 || c[0] != o[0] {
		t.Fatalf("download init = %q; want it unchanged (%q)", c, o)
	}
	if hasVolume(got, ModelCacheVolume) {
		t.Fatal("cache volume without a node cache")
	}
}
