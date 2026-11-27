// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/modelvolume"
)

// dynamoWorker is a Dynamo GPU worker pod of controller ctrl, without
// Grove labels (a single-node prefill or decode worker, say).
func dynamoWorker(name string, ctrl types.UID) corev1.Pod {
	yes := true
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "fn", UID: types.UID("uid-" + name),
			OwnerReferences: []metav1.OwnerReference{{Kind: "StatefulSet", Name: string(ctrl), UID: ctrl, Controller: &yes}}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "main",
			Env:  []corev1.EnvVar{{Name: "DYN_NAMESPACE", Value: "dynamo"}},
			Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
				"nvidia.com/gpu": resource.MustParse("4")}},
		}}},
	}
}

// Fail closed: a Dynamo GPU rank without Grove labels shares the
// namespace's etcd, NATS and frontend with any other Dynamo GPU worker of
// another controller there (a prefill beside a decode). Capturing it as a
// group would freeze what the other uses.
func TestSharedCompanionsReasonWithoutGroveLabels(t *testing.T) {
	a := &Agent{}
	prefill := dynamoWorker("prefill-0", "sts-prefill")
	decode := dynamoWorker("decode-0", "sts-decode")
	peer := dynamoWorker("prefill-1", "sts-prefill") // same instance
	why, err := a.sharedCompanionsReason(context.Background(), &prefill, []corev1.Pod{prefill, peer, decode})
	if err != nil || !strings.Contains(why, "decode-0") {
		t.Fatalf("prefill beside a decode: why %q err %v; want refused, naming the other worker", why, err)
	}
	if why, _ := a.sharedCompanionsReason(context.Background(), &prefill, []corev1.Pod{prefill, peer}); why != "" {
		t.Fatalf("one instance alone in its namespace: refused (%q)", why)
	}
	// A worker going away, or a pod with no GPU, is not a sharer.
	gone := decode.DeepCopy()
	now := metav1.Now()
	gone.DeletionTimestamp = &now
	cpu := dynamoWorker("router-0", "sts-router")
	cpu.Spec.Containers[0].Resources = corev1.ResourceRequirements{}
	if why, _ := a.sharedCompanionsReason(context.Background(), &prefill, []corev1.Pod{prefill, *gone, cpu}); why != "" {
		t.Fatalf("a deleted worker and a CPU pod: refused (%q)", why)
	}
}

// A LeaderWorkerSet instance's leader and workers have different
// controllers but one group key: one instance, not two.
func TestSharedCompanionsReasonKeepsAnLWSInstanceTogether(t *testing.T) {
	a := &Agent{}
	leader := dynamoWorker("eng-0", "sts-leader")
	worker := dynamoWorker("eng-0-1", "sts-worker")
	for _, p := range []*corev1.Pod{&leader, &worker} {
		p.Labels = map[string]string{lwsGroupKeyLabel: "g1"}
	}
	if why, _ := a.sharedCompanionsReason(context.Background(), &leader, []corev1.Pod{leader, worker}); why != "" {
		t.Fatalf("one LWS instance refused: %q", why)
	}
	other := dynamoWorker("eng-1", "sts-leader")
	other.Labels = map[string]string{lwsGroupKeyLabel: "g2"}
	if why, _ := a.sharedCompanionsReason(context.Background(), &leader, []corev1.Pod{leader, worker, other}); why == "" {
		t.Fatal("a second LWS group in the namespace is not refused")
	}
}

// Two replicas of a Deployment share their controller but are each an
// instance of one pod: they share the companions.
func TestSharedCompanionsReasonSeparatesDeploymentReplicas(t *testing.T) {
	a := &Agent{}
	r1 := dynamoWorker("dep-a", "rs-1")
	r2 := dynamoWorker("dep-b", "rs-1")
	for _, p := range []*corev1.Pod{&r1, &r2} {
		p.Annotations = map[string]string{modelvolume.CacheOrdinalAnnotation: "0", modelvolume.CacheGroupSizeAnnotation: "1"}
	}
	if why, _ := a.sharedCompanionsReason(context.Background(), &r1, []corev1.Pod{r1, r2}); why == "" {
		t.Fatal("two single-pod replicas sharing companions are not refused")
	}
}

// The checks that pass no pod list (the one just before the instance is
// held, the one before a group restore) read the namespace's pods: a
// Dynamo worker of another instance that started after the capture's first
// list still refuses it.
func TestSharedCompanionsReasonListsPodsWhenGivenNone(t *testing.T) {
	prefill := dynamoWorker("prefill-0", "sts-prefill")
	decode := dynamoWorker("decode-0", "sts-decode")
	a := &Agent{kubeClient: fake.NewSimpleClientset(&prefill, &decode)}
	why, err := a.sharedCompanionsReason(context.Background(), &prefill, nil)
	if err != nil || !strings.Contains(why, "decode-0") {
		t.Fatalf("no pods given, a decode worker in the namespace: why %q err %v; want refused, naming it", why, err)
	}
	alone := &Agent{kubeClient: fake.NewSimpleClientset(&prefill)}
	if why, err := alone.sharedCompanionsReason(context.Background(), &prefill, nil); err != nil || why != "" {
		t.Fatalf("no pods given, alone in its namespace: why %q err %v", why, err)
	}
}

// kubeWorker is dynamoWorker registering through Kubernetes.
func kubeWorker(name string, ctrl types.UID) corev1.Pod {
	p := dynamoWorker(name, ctrl)
	p.Spec.Containers[0].Env = append(p.Spec.Containers[0].Env, corev1.EnvVar{Name: "DYN_DISCOVERY_BACKEND", Value: "kubernetes"})
	return p
}

// A disaggregated deployment that registers through Kubernetes: its
// prefill and decode workers share no etcd or NATS, so each is captured
// and restored alone, unless its capture took companions another worker
// may use.
func TestSharedCompanionsKubernetesDiscovery(t *testing.T) {
	prefill := kubeWorker("prefill-0", "dep-prefill")
	decode := kubeWorker("decode-0", "dep-decode")
	pods := []corev1.Pod{prefill, decode}
	a := &Agent{kubeClient: fake.NewSimpleClientset(&prefill, &decode)}
	if why, err := a.sharedCompanionsReason(context.Background(), &prefill, pods); err != nil || why != "" {
		t.Fatalf("prefill beside a decode, Kubernetes discovery: why %q err %v; want allowed", why, err)
	}
	if why, err := a.kubeSharedCompanions(context.Background(), &prefill, pods, false); err != nil || why != "" {
		t.Fatalf("no companions: why %q err %v; want allowed", why, err)
	}
	why, err := a.kubeSharedCompanions(context.Background(), &prefill, pods, true)
	if err != nil || !strings.Contains(why, "decode-0") {
		t.Fatalf("companions beside a decode: why %q err %v; want refused, naming the other worker", why, err)
	}
	// Listed when not given: a restore checks the namespace as it is now.
	if why, err := a.kubeSharedCompanions(context.Background(), &prefill, nil, true); err != nil || !strings.Contains(why, "decode-0") {
		t.Fatalf("listed pods: why %q err %v; want refused", why, err)
	}
	// Alone in its namespace, companions or not.
	if why, err := a.kubeSharedCompanions(context.Background(), &prefill, []corev1.Pod{prefill}, true); err != nil || why != "" {
		t.Fatalf("alone with companions: why %q err %v; want allowed", why, err)
	}
	// An etcd worker is unchanged: still refused beside another instance.
	etcd := dynamoWorker("prefill-0", "dep-prefill")
	if why, _ := a.sharedCompanionsReason(context.Background(), &etcd, []corev1.Pod{etcd, dynamoWorker("decode-0", "dep-decode")}); why == "" {
		t.Fatal("etcd prefill beside a decode: allowed; want refused")
	}
	if why, _ := a.kubeSharedCompanions(context.Background(), &etcd, []corev1.Pod{etcd, decode}, true); why != "" {
		t.Fatalf("kubeSharedCompanions on an etcd worker: %q; want \"\" (sharedCompanionsReason decides)", why)
	}
}

func TestRecordHasCompanions(t *testing.T) {
	if recordHasCompanions(map[string]string{"checkpoint.0": "x", "size": "1"}) {
		t.Fatal("ranks only: has companions")
	}
	if !recordHasCompanions(map[string]string{"checkpoint.0": "x", "companion.1": "true"}) {
		t.Fatal("companion.1: none")
	}
}
