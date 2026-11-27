// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	k8stesting "k8s.io/client-go/testing"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

func kubeRank(ns, name, uid, ip, discovery string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID(uid)},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Env: []corev1.EnvVar{
			{Name: "DYN_NAMESPACE", Value: "qwen"},
			{Name: "DYN_DISCOVERY_BACKEND", Value: discovery},
		}}}},
		Status: corev1.PodStatus{PodIP: ip},
	}
}

// workerSpec is a DynamoWorkerMetadata spec as Dynamo 1.5 writes it.
func workerSpec(ip string) map[string]any {
	return map[string]any{"data": map[string]any{
		"endpoints": map[string]any{"qwen/backend/generate/1c4c": map[string]any{
			"instance_id": int64(7965170713353809), "namespace": "qwen",
			"transport": map[string]any{"tcp": ip + ":44325/1c4c47d2f1de51/generate"},
		}},
		"event_channels": map[string]any{"kv": map[string]any{
			"transport": map[string]any{"kind": "Zmq", "config": map[string]any{"endpoint": "tcp://" + ip + ":34203"}},
		}},
		"model_cards": map[string]any{"qwen/backend/generate/1c4c": map[string]any{
			"card_json": map[string]any{
				"display_name": "Qwen 1.2.3.4",
				"extra_files":  []any{map[string]any{"path": "http://" + ip + ":9090/v1/metadata/qwen/tokenizer.json"}},
			},
		}},
	}}
}

func workerCR(ns, name, ownerName, ownerUID string, controller bool, spec map[string]any) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "nvidia.com/v1alpha1", "kind": "DynamoWorkerMetadata",
		"metadata": map[string]any{"name": name, "namespace": ns},
		"spec":     spec,
	}}
	u.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: ownerName, UID: types.UID(ownerUID), Controller: &controller}})
	return u
}

func fakeDyn(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{dynamoWorkerMetadata: "DynamoWorkerMetadataList"}, objs...)
}

func TestReplaceIPv4WholeAddressOnly(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"10.0.0.1:80/x", "10.9.9.9:80/x"},
		{"tcp://10.0.0.1:5", "tcp://10.9.9.9:5"},
		{"10.0.0.12:80", "10.0.0.12:80"}, // a longer address
		{"110.0.0.1:80", "110.0.0.1:80"}, // a longer address
		{"10.0.0.1.5", "10.0.0.1.5"},     // not an address
		{"a10.0.0.1", "a10.9.9.9"},       // a letter bounds it
		{"10.0.0.1,10.0.0.1", "10.9.9.9,10.9.9.9"},
	} {
		if got := replaceIPv4(tc.in, "10.0.0.1", "10.9.9.9"); got != tc.want {
			t.Errorf("replaceIPv4(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestIPv4Tokens(t *testing.T) {
	got := ipv4Tokens("tcp://10.0.0.1:5/1.2.3/4.5.6.7.8 and 192.168.1.20")
	want := []string{"10.0.0.1", "192.168.1.20"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestReplacePodIP(t *testing.T) {
	raw, _ := json.Marshal(workerSpec("10.0.0.1"))
	got, err := replacePodIP(raw, "10.0.0.1", "10.9.9.9")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(got)
	s := string(b)
	// The model card's files are fetched from the worker's system port by
	// a frontend that has not loaded the model.
	for _, want := range []string{`"tcp":"10.9.9.9:44325/1c4c47d2f1de51/generate"`, `"endpoint":"tcp://10.9.9.9:34203"`,
		`"path":"http://10.9.9.9:9090/v1/metadata/qwen/tokenizer.json"`, `"display_name":"Qwen 1.2.3.4"`} {
		if !strings.Contains(s, want) {
			t.Errorf("spec lacks %s: %s", want, s)
		}
	}
	// The instance ID survives as the integer it was, not a float.
	if !strings.Contains(s, `"instance_id":7965170713353809`) {
		t.Errorf("instance_id changed: %s", s)
	}
}

func TestRegistrationsOf(t *testing.T) {
	ctx := context.Background()
	pod := kubeRank("a", "worker-x", "u1", "10.0.0.1", "kubernetes")
	a := &Agent{dynClient: fakeDyn(
		workerCR("a", "worker-x", "worker-x", "u1", true, workerSpec("10.0.0.1")),
		workerCR("a", "worker-x-sidecar", "worker-x", "u1", false, workerSpec("10.0.0.1")),
		workerCR("a", "worker-y", "worker-y", "u2", true, workerSpec("10.0.0.2")), // another pod's
	)}
	got, err := a.registrationsOf(ctx, pod)
	if err != nil {
		t.Fatal(err)
	}
	if got.PodIP != "10.0.0.1" || len(got.Objects) != 2 {
		t.Fatalf("got %+v, want the pod's two objects at 10.0.0.1", got)
	}
	suffixes := map[string]bool{}
	for _, o := range got.Objects {
		suffixes[o.Suffix] = o.Controller
	}
	if c, ok := suffixes[""]; !ok || !c {
		t.Errorf("main container's object: %v", suffixes)
	}
	if c, ok := suffixes["-sidecar"]; !ok || c {
		t.Errorf("sidecar's object: %v", suffixes)
	}
}

func TestRegistrationsOfFailsClosed(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		a    *Agent
		pod  *corev1.Pod
		want string
	}{
		{"no client", &Agent{}, kubeRank("a", "w", "u1", "10.0.0.1", "kubernetes"), "no client"},
		{"not registered", &Agent{dynClient: fakeDyn()}, kubeRank("a", "w", "u1", "10.0.0.1", "kubernetes"), "has not registered"},
		{"no address", &Agent{dynClient: fakeDyn()}, kubeRank("a", "w", "u1", "", "kubernetes"), "no IPv4"},
		{"another address", &Agent{dynClient: fakeDyn(workerCR("a", "w", "w", "u1", true, workerSpec("10.0.0.7")))},
			kubeRank("a", "w", "u1", "10.0.0.1", "kubernetes"), "serves at 10.0.0.7"},
		{"a URL at another address", &Agent{dynClient: fakeDyn(workerCR("a", "w", "w", "u1", true, func() map[string]any {
			s := workerSpec("10.0.0.1")
			card := s["data"].(map[string]any)["model_cards"].(map[string]any)["qwen/backend/generate/1c4c"].(map[string]any)["card_json"].(map[string]any)
			card["extra_files"] = []any{map[string]any{"path": "http://10.0.0.8:9090/x"}}
			return s
		}()))}, kubeRank("a", "w", "u1", "10.0.0.1", "kubernetes"), "serves at 10.0.0.8"},
		{"misnamed", &Agent{dynClient: fakeDyn(workerCR("a", "other", "w", "u1", true, workerSpec("10.0.0.1")))},
			kubeRank("a", "w", "u1", "10.0.0.1", "kubernetes"), "not named after it"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.a.registrationsOf(ctx, tc.pod); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestRecordRegistrationsSkipsOtherRanks(t *testing.T) {
	a := &Agent{dynClient: fakeDyn(workerCR("a", "w1", "w1", "u1", true, workerSpec("10.0.0.1")))}
	data, err := a.recordRegistrations(context.Background(), []*corev1.Pod{
		kubeRank("a", "w0", "u0", "10.0.0.0", "etcd"),
		kubeRank("a", "w1", "u1", "10.0.0.1", "kubernetes"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := data[fmt.Sprintf(kubeRegistrationKey, 0)]; ok || len(data) != 1 || data[fmt.Sprintf(kubeRegistrationKey, 1)] == "" {
		t.Errorf("got keys %v, want rank 1's only", data)
	}
}

func TestWriteRegistrations(t *testing.T) {
	ctx := context.Background()
	src := &Agent{dynClient: fakeDyn(workerCR("a", "w-old", "w-old", "u-old", true, workerSpec("10.0.0.1")))}
	record, err := src.recordRegistrations(ctx, []*corev1.Pod{kubeRank("a", "w-old", "u-old", "10.0.0.1", "kubernetes")})
	if err != nil {
		t.Fatal(err)
	}
	dyn := fakeDyn()
	dst := &Agent{dynClient: dyn, kubeClient: k8sfake.NewSimpleClientset()}
	placeholder := kubeRank("b", "w-new", "u-new", "10.0.0.2", "kubernetes")
	if err := dst.writeRegistrations(ctx, record, []*corev1.Pod{placeholder}); err != nil {
		t.Fatal(err)
	}
	got, err := dyn.Resource(dynamoWorkerMetadata).Namespace("b").Get(ctx, "w-new", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	refs := got.GetOwnerReferences()
	if len(refs) != 1 || refs[0].UID != "u-new" || refs[0].Name != "w-new" || refs[0].Controller == nil || !*refs[0].Controller {
		t.Errorf("owner %+v, want the placeholder as controller", refs)
	}
	b, _ := json.Marshal(got.Object["spec"])
	if !strings.Contains(string(b), "10.0.0.2:44325/1c4c47d2f1de51/generate") || !strings.Contains(string(b), "http://10.0.0.2:9090/") || strings.Contains(string(b), "10.0.0.1") {
		t.Errorf("transports not moved to the placeholder's address: %s", b)
	}
	// Driven again: the placeholder's own object is overwritten.
	if err := dst.writeRegistrations(ctx, record, []*corev1.Pod{placeholder}); err != nil {
		t.Errorf("second write: %v", err)
	}
}

func TestWriteRegistrationsFailsClosed(t *testing.T) {
	ctx := context.Background()
	src := &Agent{dynClient: fakeDyn(workerCR("a", "w-old", "w-old", "u-old", true, workerSpec("10.0.0.1")))}
	record, err := src.recordRegistrations(ctx, []*corev1.Pod{kubeRank("a", "w-old", "u-old", "10.0.0.1", "kubernetes")})
	if err != nil {
		t.Fatal(err)
	}
	placeholder := kubeRank("b", "w-new", "u-new", "10.0.0.2", "kubernetes")
	// Another pod's object of that name.
	taken := &Agent{dynClient: fakeDyn(workerCR("b", "w-new", "w-new", "u-other", true, workerSpec("10.0.0.3"))), kubeClient: k8sfake.NewSimpleClientset()}
	if err := taken.writeRegistrations(ctx, record, []*corev1.Pod{placeholder}); err == nil {
		t.Error("overwrote another pod's registration")
	}
	// A Kubernetes rank whose capture kept nothing.
	if err := (&Agent{dynClient: fakeDyn()}).writeRegistrations(ctx, map[string]string{}, []*corev1.Pod{placeholder}); err == nil {
		t.Error("restored a Kubernetes rank without its registration")
	}
	// An etcd rank needs nothing.
	if err := (&Agent{}).writeRegistrations(ctx, map[string]string{}, []*corev1.Pod{kubeRank("b", "e", "u", "10.0.0.4", "etcd")}); err != nil {
		t.Errorf("etcd rank: %v", err)
	}
	// No address yet.
	noIP := kubeRank("b", "w-new", "u-new", "", "kubernetes")
	if err := (&Agent{dynClient: fakeDyn(), kubeClient: k8sfake.NewSimpleClientset()}).writeRegistrations(ctx, record, []*corev1.Pod{noIP}); err == nil {
		t.Error("wrote a registration without the placeholder's address")
	}
}

func TestWriteRegistrationsRefusesASecondReplica(t *testing.T) {
	ctx := context.Background()
	src := &Agent{dynClient: fakeDyn(workerCR("a", "w-old", "w-old", "u-old", true, workerSpec("10.0.0.1")))}
	record, err := src.recordRegistrations(ctx, []*corev1.Pod{kubeRank("a", "w-old", "u-old", "10.0.0.1", "kubernetes")})
	if err != nil {
		t.Fatal(err)
	}
	first := kubeRank("b", "w-1", "u-1", "10.0.0.2", "kubernetes")
	second := kubeRank("b", "w-2", "u-2", "10.0.0.3", "kubernetes")
	other := kubeRank("c", "w-3", "u-3", "10.0.0.4", "kubernetes")
	kc := k8sfake.NewSimpleClientset(first, second, other)
	a := &Agent{dynClient: fakeDyn(), kubeClient: kc}
	if err := a.writeRegistrations(ctx, record, []*corev1.Pod{first}); err != nil {
		t.Fatal(err)
	}
	// Its own again: fine.
	if err := a.writeRegistrations(ctx, record, []*corev1.Pod{first}); err != nil {
		t.Errorf("rewrite of its own: %v", err)
	}
	// Another replica of the same capture in the namespace: refused.
	if err := a.writeRegistrations(ctx, record, []*corev1.Pod{second}); err == nil || !strings.Contains(err.Error(), "7965170713353809") {
		t.Errorf("err = %v, want the shared instance ID refused", err)
	}
	// In another namespace the IDs are free: a frontend lists its own.
	if err := a.writeRegistrations(ctx, record, []*corev1.Pod{other}); err != nil {
		t.Errorf("another namespace: %v", err)
	}
	// The first replica gone: its claim is taken over.
	if err := kc.CoreV1().Pods("b").Delete(ctx, "w-1", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := (&Agent{dynClient: fakeDyn(), kubeClient: kc}).writeRegistrations(ctx, record, []*corev1.Pod{second}); err != nil {
		t.Errorf("after the holder went: %v", err)
	}
}

// Two restores of one capture at once: the claim lets one through, though
// neither sees the other's registration yet.
func TestClaimInstanceIDIsExclusive(t *testing.T) {
	ctx := context.Background()
	p1 := kubeRank("b", "w-1", "u-1", "10.0.0.2", "kubernetes")
	p2 := kubeRank("b", "w-2", "u-2", "10.0.0.3", "kubernetes")
	kc := k8sfake.NewSimpleClientset(p1, p2)
	a := &Agent{kubeClient: kc}
	if err := a.claimInstanceID(ctx, p1, "42"); err != nil {
		t.Fatal(err)
	}
	if err := a.claimInstanceID(ctx, p2, "42"); err == nil {
		t.Error("a second live pod claimed the same instance ID")
	}
	if err := a.claimInstanceID(ctx, p1, "42"); err != nil {
		t.Errorf("its own claim again: %v", err)
	}
}

// The captured pod is deleted as its replacement is admitted; its
// registration outlives it. It must not refuse the replacement's restore.
func TestWriteRegistrationsIgnoresTheTerminatingSource(t *testing.T) {
	ctx := context.Background()
	srcPod := kubeRank("a", "w-old", "u-old", "10.0.0.1", "kubernetes")
	now := metav1.Now()
	srcPod.DeletionTimestamp = &now
	cr := workerCR("a", "w-old", "w-old", "u-old", true, workerSpec("10.0.0.1"))
	record, err := (&Agent{dynClient: fakeDyn(cr)}).recordRegistrations(ctx, []*corev1.Pod{kubeRank("a", "w-old", "u-old", "10.0.0.1", "kubernetes")})
	if err != nil {
		t.Fatal(err)
	}
	ph := kubeRank("a", "w-new", "u-new", "10.0.0.2", "kubernetes")
	a := &Agent{dynClient: fakeDyn(cr), kubeClient: k8sfake.NewSimpleClientset(srcPod, ph)}
	if err := a.writeRegistrations(ctx, record, []*corev1.Pod{ph}); err != nil {
		t.Errorf("refused by the terminating source's registration: %v", err)
	}
	// The source still running (not terminating): refused.
	srcPod.DeletionTimestamp = nil
	b := &Agent{dynClient: fakeDyn(cr), kubeClient: k8sfake.NewSimpleClientset(srcPod, ph)}
	if err := b.writeRegistrations(ctx, record, []*corev1.Pod{ph}); err == nil {
		t.Error("restored beside the running pod that registers the same IDs")
	}
}

func TestDeleteRegistrations(t *testing.T) {
	ctx := context.Background()
	dyn := fakeDyn(workerCR("b", "w", "w", "u", true, workerSpec("10.0.0.2")), workerCR("b", "x", "x", "u2", true, workerSpec("10.0.0.3")))
	if err := (&Agent{dynClient: dyn}).deleteRegistrations(ctx, "b", "u"); err != nil {
		t.Fatal(err)
	}
	l, _ := dyn.Resource(dynamoWorkerMetadata).Namespace("b").List(ctx, metav1.ListOptions{})
	if len(l.Items) != 1 || l.Items[0].GetName() != "x" {
		t.Errorf("left %d objects, want only x", len(l.Items))
	}
}

func TestForeignAddressNamedHost(t *testing.T) {
	spec := workerSpec("10.0.0.1")
	spec["data"].(map[string]any)["endpoints"].(map[string]any)["qwen/backend/generate/1c4c"].(map[string]any)["transport"] =
		map[string]any{"tcp": "worker-0.svc.cluster.local:44325/1c4c/generate"}
	raw, _ := json.Marshal(spec)
	if got := foreignAddress(raw, "10.0.0.1"); got == "" {
		t.Error("a transport naming a host passed: the restore cannot move it")
	}
	raw, _ = json.Marshal(workerSpec("10.0.0.1"))
	if got := foreignAddress(raw, "10.0.0.1"); got != "" {
		t.Errorf("own address flagged: %s", got)
	}
}

func TestReadyOnlyOnceRestored(t *testing.T) {
	p := kubeRank("a", "w", "u", "10.0.0.1", "kubernetes")
	p.Spec.Containers[0].Resources.Limits = corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}
	if err := readyOnlyOnceRestored(p); err != nil {
		t.Errorf("pod mode: %v", err)
	}
	p.Spec.Containers[0].Env = append(p.Spec.Containers[0].Env, corev1.EnvVar{Name: "DYN_KUBE_DISCOVERY_MODE", Value: "container"})
	if err := readyOnlyOnceRestored(p); err == nil {
		t.Error("container mode without a readiness probe passed")
	}
	p.Spec.Containers[0].ReadinessProbe = &corev1.Probe{}
	if err := readyOnlyOnceRestored(p); err != nil {
		t.Errorf("container mode with a probe: %v", err)
	}
}

func TestInstanceIDs(t *testing.T) {
	raw, _ := json.Marshal(workerSpec("10.0.0.1"))
	if got := instanceIDs(raw); len(got) != 1 || got[0] != "7965170713353809" {
		t.Errorf("got %v", got)
	}
}

// A placeholder starting cold in place keeps no restored registration: when
// it cannot be removed, the pod is deleted instead of starting in place.
func TestColdInPlaceAllowed(t *testing.T) {
	ctx := context.Background()
	log := logrus.NewEntry(logrus.New())
	dyn := fakeDyn(workerCR("b", "w", "w", "u", true, workerSpec("10.0.0.2")))
	a := &Agent{dynClient: dyn}
	if !a.coldInPlaceAllowed(ctx, "b", "u", false, log) {
		t.Error("etcd worker: not allowed in place")
	}
	if !a.coldInPlaceAllowed(ctx, "b", "u", true, log) {
		t.Error("registration removed: not allowed in place")
	}
	if l, _ := dyn.Resource(dynamoWorkerMetadata).Namespace("b").List(ctx, metav1.ListOptions{}); len(l.Items) != 0 {
		t.Errorf("registration left: %d", len(l.Items))
	}
	failing := fakeDyn(workerCR("b", "w", "w", "u", true, workerSpec("10.0.0.2")))
	failing.PrependReactor("delete", "dynamoworkermetadatas", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("apiserver unavailable")
	})
	if (&Agent{dynClient: failing}).coldInPlaceAllowed(ctx, "b", "u", true, log) {
		t.Error("registration not removed: allowed in place; want the pod deleted")
	}
}

// Deletes are bound to the listed object's UID.
func TestDeleteRegistrationsPreconditionUID(t *testing.T) {
	ctx := context.Background()
	cr := workerCR("b", "w", "w", "u", true, workerSpec("10.0.0.2"))
	cr.SetUID("obj-1")
	dyn := fakeDyn(cr)
	var got *metav1.Preconditions
	dyn.PrependReactor("delete", "dynamoworkermetadatas", func(act k8stesting.Action) (bool, runtime.Object, error) {
		got = act.(k8stesting.DeleteAction).GetDeleteOptions().Preconditions
		return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "nvidia.com", Resource: "dynamoworkermetadatas"}, "w", errors.New("uid differs"))
	})
	if err := (&Agent{dynClient: dyn}).deleteRegistrations(ctx, "b", "u"); err != nil {
		t.Fatalf("a replaced object (Conflict) is an error: %v", err)
	}
	if got == nil || got.UID == nil || *got.UID != "obj-1" {
		t.Errorf("delete preconditions %+v, want UID obj-1", got)
	}
}

func TestReadyOnlyOnceRestoredEnvFrom(t *testing.T) {
	p := kubeRank("a", "w", "u", "10.0.0.1", "kubernetes")
	p.Spec.Containers[0].Resources.Limits = corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}
	p.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "dyn"}}}}
	if err := readyOnlyOnceRestored(p); err == nil {
		t.Error("envFrom may set container mode: no readiness probe passed")
	}
	p.Spec.Containers[0].Env = append(p.Spec.Containers[0].Env, corev1.EnvVar{Name: "DYN_KUBE_DISCOVERY_MODE", Value: "pod"})
	if err := readyOnlyOnceRestored(p); err != nil {
		t.Errorf("explicit pod mode beside envFrom: %v", err)
	}
}

func TestForeignAddressURLHostWithoutPort(t *testing.T) {
	spec := workerSpec("10.0.0.1")
	spec["data"].(map[string]any)["model_card"] = map[string]any{"tokenizer": "http://worker-old/tokenizer.json"}
	raw, _ := json.Marshal(spec)
	if got := foreignAddress(raw, "10.0.0.1"); got == "" {
		t.Error("a URL naming another host without a port passed")
	}
	spec["data"].(map[string]any)["model_card"] = map[string]any{"tokenizer": "http://10.0.0.1/tokenizer.json"}
	raw, _ = json.Marshal(spec)
	if got := foreignAddress(raw, "10.0.0.1"); got != "" {
		t.Errorf("own address without a port flagged: %s", got)
	}
}
