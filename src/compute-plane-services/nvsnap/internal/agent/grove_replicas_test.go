// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

func groveRank(name, replica, podIndex string) *corev1.Pod {
	const group = "w"
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "fn", UID: types.UID(name + "-uid"), Labels: map[string]string{
		webhook.GroveScalingGroupLabel:        group,
		webhook.GroveScalingGroupReplicaLabel: replica,
		webhook.GroveScalingGroupPodLabel:     podIndex,
	}}}
}

func replicasOf(m map[string]int, err error) func(context.Context, string) (map[string]int, error) {
	return func(context.Context, string) (map[string]int, error) { return m, err }
}

// The worker groups of a scaling group of several replicas share one etcd,
// NATS and frontend, so none of them is restored as a group: on a cluster
// one such deployment had all three replicas admitted as placeholders of
// one capture, and two replicas' restores restored the same companions.
func TestCRIUGroupAdmits_ScalingGroupOfSeveralReplicas(t *testing.T) {
	for _, tc := range []struct {
		name     string
		pod      *corev1.Pod
		replicas func(context.Context, string) (map[string]int, error)
		want     bool
		reason   string
	}{
		{"one replica", groveRank("w-0-ldr", "0", "0"), replicasOf(map[string]int{"w": 1}, nil), true, ""},
		{"first of three replicas", groveRank("w-0-ldr", "0", "0"), replicasOf(map[string]int{"w": 3}, nil), false, "runs 3 Grove"},
		{"third replica, before its scaling group shows it", groveRank("w-2-wkr", "2", "1"), replicasOf(map[string]int{"w": 1}, nil), false, "runs 3 Grove"},
		{"prefill and decode groups of one replica each", groveRank("w-0-ldr", "0", "0"), replicasOf(map[string]int{"w": 1, "prefill": 1}, nil), false, "runs 2 Grove"},
		{"replicas unreadable: fails closed", groveRank("w-0-ldr", "0", "0"), replicasOf(nil, errors.New("forbidden")), false, "read the namespace's Grove"},
		{"scaling group missing: fails closed", groveRank("w-0-ldr", "0", "0"), replicasOf(map[string]int{}, nil), false, "not found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &Agent{kubeClient: fake.NewSimpleClientset(), log: logrus.New(), groveReplicas: tc.replicas}
			got, why := a.criuGroupAdmits(context.Background(), tc.pod, webhook.CRIUGroup{})
			if got != tc.want || !strings.Contains(why, tc.reason) {
				t.Errorf("admits = %v (%q), want %v (%q)", got, why, tc.want, tc.reason)
			}
		})
	}
}

// A companion of a namespace whose scaling group runs several replicas
// starts cold: no replica restores it.
func TestCRIUCompanionAdmits_ScalingGroupOfSeveralReplicas(t *testing.T) {
	etcd := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "etcd-0", Namespace: "fn"}}
	for _, tc := range []struct {
		name     string
		pods     []*corev1.Pod
		replicas func(context.Context, string) (map[string]int, error)
		want     bool
		reason   string
	}{
		{"no Grove in the namespace", nil, replicasOf(map[string]int{}, nil), true, ""},
		{"no Grove on the cluster, read fails", nil, replicasOf(nil, errors.New("no such API")), true, ""},
		{"scaling group of one replica", nil, replicasOf(map[string]int{"w": 1}, nil), true, ""},
		{"scaling group of three replicas", nil, replicasOf(map[string]int{"w": 3}, nil), false, "runs 3 Grove"},
		{"two scaling groups of one replica", nil, replicasOf(map[string]int{"w": 1, "prefill": 1}, nil), false, "runs 2 Grove"},
		{"a second replica's pod is already there", []*corev1.Pod{groveRank("w-1-ldr", "1", "0")}, replicasOf(map[string]int{"w": 1}, nil), false, "runs 2 Grove"},
		{"Grove pods present but the replicas cannot be read", []*corev1.Pod{groveRank("w-0-ldr", "0", "0")}, replicasOf(nil, errors.New("forbidden")), false, "cannot read"},
		{"Grove pods present but no scaling group listed", []*corev1.Pod{groveRank("w-0-ldr", "0", "0")}, replicasOf(map[string]int{}, nil), false, "cannot read"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kc := fake.NewSimpleClientset()
			for _, p := range tc.pods {
				p = p.DeepCopy()
				p.Annotations = map[string]string{webhook.CRIUGroupAnnotation: "g"} // a placeholder: not "running"
				if _, err := kc.CoreV1().Pods("fn").Create(context.Background(), p, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			a := &Agent{kubeClient: kc, log: logrus.New(), groveReplicas: tc.replicas}
			got, why := a.criuCompanionAdmits(context.Background(), etcd, webhook.CRIUGroup{})
			if got != tc.want || !strings.Contains(why, tc.reason) {
				t.Errorf("admits = %v (%q), want %v (%q)", got, why, tc.want, tc.reason)
			}
		})
	}
}

// The replicas are read from the scaling groups, defaulting as the API
// does; a cluster without Grove has none.
func TestGroveScalingGroupReplicas(t *testing.T) {
	pcsg := func(name string, replicas any) *unstructured.Unstructured {
		u := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "grove.io/v1alpha1", "kind": "PodCliqueScalingGroup",
			"metadata": map[string]any{"name": name, "namespace": "fn"}, "spec": map[string]any{}}}
		if replicas != nil {
			u.Object["spec"].(map[string]any)["replicas"] = replicas
		}
		return u
	}
	scheme := runtime.NewScheme()
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{groveScalingGroups: "PodCliqueScalingGroupList"},
		pcsg("a", int64(3)), pcsg("b", nil))
	a := &Agent{dynClient: dyn}
	got, err := a.groveScalingGroupReplicas(context.Background(), "fn")
	if err != nil {
		t.Fatal(err)
	}
	if got["a"] != 3 || got["b"] != 1 || len(got) != 2 {
		t.Errorf("got %v, want a=3 b=1 (the API default)", got)
	}
	if _, err := (&Agent{}).groveScalingGroupReplicas(context.Background(), "fn"); err == nil {
		t.Error("no client read as no scaling groups; it must fail closed")
	}
}

// conditionalPatchClient is a fake clientset whose pod patches honour a
// metadata.resourceVersion precondition, as the API server does (the fake
// tracker ignores it): a stale one is a Conflict.
func conditionalPatchClient(pods ...*corev1.Pod) *fake.Clientset {
	objs := make([]runtime.Object, 0, len(pods))
	for _, p := range pods {
		p = p.DeepCopy()
		p.ResourceVersion = "1"
		objs = append(objs, p)
	}
	kc := fake.NewSimpleClientset(objs...)
	var mu sync.Mutex
	rv := 1
	gvr := corev1.SchemeGroupVersion.WithResource("pods")
	kc.PrependReactor("patch", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		pa := action.(k8stesting.PatchAction)
		var patch struct {
			Metadata struct {
				ResourceVersion string            `json:"resourceVersion"`
				Annotations     map[string]string `json:"annotations"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal(pa.GetPatch(), &patch); err != nil {
			return true, nil, err
		}
		mu.Lock()
		defer mu.Unlock()
		obj, err := kc.Tracker().Get(gvr, pa.GetNamespace(), pa.GetName())
		if err != nil {
			return true, nil, err
		}
		cur := obj.(*corev1.Pod).DeepCopy()
		if want := patch.Metadata.ResourceVersion; want != "" && want != cur.ResourceVersion {
			return true, nil, apierrors.NewConflict(gvr.GroupResource(), cur.Name, fmt.Errorf("resourceVersion %s, not %s", cur.ResourceVersion, want))
		}
		if cur.Annotations == nil {
			cur.Annotations = map[string]string{}
		}
		for k, v := range patch.Metadata.Annotations {
			cur.Annotations[k] = v
		}
		rv++
		cur.ResourceVersion = strconv.Itoa(rv)
		return true, cur, kc.Tracker().Update(gvr, cur, cur.Namespace)
	})
	return kc
}

func claimablePlaceholder(name string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "fn", Annotations: map[string]string{
		webhook.CRIUGroupAnnotation: "g", webhook.CRIUGroupCompanionAnnotation: "true"}}}
}

// Of two leaders restoring the same companions at once, only one may: the
// other does not restore them a second time.
func TestClaimCompanions_OneRestorePerCompanion(t *testing.T) {
	ctx := context.Background()
	leaderA, leaderB := groveRank("w-0-ldr", "0", "0"), groveRank("w-2-ldr", "2", "0")
	etcd, nats := claimablePlaceholder("etcd-0"), claimablePlaceholder("nats-0")
	kc := conditionalPatchClient(leaderA, leaderB, etcd, nats)
	// Both leaders read etcd-0 before either writes its claim: only the
	// resourceVersion precondition keeps the second write from winning too.
	var reads sync.WaitGroup
	reads.Add(2)
	var n atomic.Int32
	claimReadHook = func(pod string) {
		if pod != "etcd-0" || n.Add(1) > 2 {
			return
		}
		reads.Done()
		reads.Wait()
	}
	t.Cleanup(func() { claimReadHook = nil })
	members := []*corev1.Pod{leaderA, etcd, nats}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, l := range []*corev1.Pod{leaderA, leaderB} {
		wg.Add(1)
		go func(i int, l *corev1.Pod) {
			defer wg.Done()
			errs[i] = claimCompanions(ctx, kc, members, l)
		}(i, l)
	}
	wg.Wait()
	won := 0
	for _, err := range errs {
		var c errCompanionClaimed
		switch {
		case err == nil:
			won++
		case !errors.As(err, &c):
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if won != 1 {
		t.Fatalf("%d leaders claimed the companions, want exactly 1 (errors %v)", won, errs)
	}
	winner := leaderA
	if errs[0] != nil {
		winner = leaderB
	}
	for _, n := range []string{"etcd-0", "nats-0"} {
		p, _ := kc.CoreV1().Pods("fn").Get(ctx, n, metav1.GetOptions{})
		if by := p.Annotations[companionClaimAnnotation]; by != string(winner.UID) {
			t.Errorf("%s claimed by %q, want the winner %q", n, by, winner.UID)
		}
	}
	// The winner claiming again (a retried restore) keeps its claim.
	if err := claimCompanions(ctx, kc, members, winner); err != nil {
		t.Errorf("the winner's second claim: %v", err)
	}
}

// A claim left by a leader that is gone does not block its replacement's
// restore.
func TestClaimCompanions_TakesOverAGoneLeadersClaim(t *testing.T) {
	ctx := context.Background()
	etcd := claimablePlaceholder("etcd-0")
	etcd.Annotations[companionClaimAnnotation] = "gone-uid"
	leader := groveRank("w-0-ldr", "0", "0")
	kc := fake.NewSimpleClientset(leader, etcd)
	if err := claimCompanions(ctx, kc, []*corev1.Pod{leader, etcd}, leader); err != nil {
		t.Fatalf("claim over a gone leader's: %v", err)
	}
	p, _ := kc.CoreV1().Pods("fn").Get(ctx, "etcd-0", metav1.GetOptions{})
	if by := p.Annotations[companionClaimAnnotation]; by != string(leader.UID) {
		t.Errorf("claimed by %q, want %q", by, leader.UID)
	}
	// A rank is never claimed.
	l, _ := kc.CoreV1().Pods("fn").Get(ctx, "w-0-ldr", metav1.GetOptions{})
	if _, ok := l.Annotations[companionClaimAnnotation]; ok {
		t.Error("a GPU rank was claimed as a companion")
	}
}

// A companion claimed by another live leader's restore is that restore's:
// ending this leader's restore leaves it alone. A claim by a leader that is
// gone or being deleted does not protect it.
func TestGroupPlaceholders_SparesCompanionsClaimedByAnotherLeader(t *testing.T) {
	now := metav1.Now()
	leader := groveRank("w-0-ldr", "0", "0")
	other := groveRank("w-2-ldr", "2", "0")
	leaving := groveRank("w-1-ldr", "1", "0")
	leaving.DeletionTimestamp, leaving.Finalizers = &now, []string{"t"}
	for _, p := range []*corev1.Pod{leader, other, leaving} {
		p.Annotations = map[string]string{webhook.CRIUGroupAnnotation: "k"}
	}
	claimed := func(name string, by *corev1.Pod) corev1.Pod {
		p := claimablePlaceholder(name)
		p.Annotations[webhook.CRIUGroupAnnotation] = "k"
		if by != nil {
			p.Annotations[companionClaimAnnotation] = string(by.UID)
		}
		return *p
	}
	pods := []corev1.Pod{*leader, *other, *leaving,
		claimed("etcd-0", other), claimed("nats-0", leaving), claimed("front-0", leader), claimed("idle-0", nil)}
	ended := groupPlaceholders(pods, leader, "k", false)
	got := make([]string, 0, len(ended))
	for _, p := range ended {
		got = append(got, p.Name)
	}
	want := "w-0-ldr,nats-0,front-0,idle-0"
	if strings.Join(got, ",") != want {
		t.Errorf("ended %v, want %s (etcd-0 is the live other leader's)", got, want)
	}
}

// The restore of an instance whose namespace runs several Grove replicas
// ends at once as not run: its ranks are deleted and start cold, and the
// record counts a stall. An unreadable count is tried again later instead.
func TestRestoreGroup_SharedCompanions(t *testing.T) {
	for _, tc := range []struct {
		name      string
		replicas  func(context.Context, string) (map[string]int, error)
		wantStall bool
	}{
		{"three replicas", replicasOf(map[string]int{"w": 3}, nil), true},
		{"count unreadable", replicasOf(nil, errors.New("timeout")), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, kc, leader := groupFixture(t)
			c.a.groveReplicas = tc.replicas
			ctx := context.Background()
			for i, name := range []string{"kimi-0", "kimi-1"} {
				p, err := kc.CoreV1().Pods("fn").Get(ctx, name, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				p.Labels = map[string]string{webhook.GroveScalingGroupLabel: "w", webhook.GroveScalingGroupReplicaLabel: "0", webhook.GroveScalingGroupPodLabel: strconv.Itoa(i)}
				if p, err = kc.CoreV1().Pods("fn").Update(ctx, p, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				if i == 0 {
					leader = p
				}
			}
			c.attempted.Store("restore/"+string(leader.UID), struct{}{})
			rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			t0 := time.Now()
			c.restoreGroup(rctx, leader, "k1", 2)
			if d := time.Since(t0); d > time.Second {
				t.Errorf("took %s; the restore must end before waiting for placeholders", d)
			}
			cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, criuGroupConfigMapName("k1"), metav1.GetOptions{})
			_, retried := c.attempted.Load("restore/" + string(leader.UID))
			left := remainingPods(t, kc)
			if tc.wantStall {
				if cm.Data["stalls"] != "1" || len(left) != 1 {
					t.Errorf("record %v, pods %v; want one stall and the ranks deleted", cm.Data, left)
				}
			} else if cm.Data["stalls"] != "" || len(left) != 3 || retried {
				t.Errorf("record %v, pods %v, still attempted %v; want nothing ended and the restore tried again later", cm.Data, left, retried)
			}
		})
	}
}

// The capture of an instance whose namespace runs several Grove replicas
// is skipped, and claims no record; an unreadable count is tried again.
func TestCaptureGroup_SharedCompanions(t *testing.T) {
	saved := groupWarmupWait
	groupWarmupWait = 0
	t.Cleanup(func() { groupWarmupWait = saved })
	for _, tc := range []struct {
		name      string
		replicas  func(context.Context, string) (map[string]int, error)
		wantRetry bool
	}{
		{"three replicas", replicasOf(map[string]int{"w": 3}, nil), false},
		{"count unreadable", replicasOf(nil, errors.New("timeout")), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leader := groveRank("w-0-ldr", "0", "0")
			leader.Annotations = map[string]string{cacheURIAnnotation: "cache://k"}
			kc := fake.NewSimpleClientset(leader)
			a := &Agent{kubeClient: kc, log: logrus.New(), groveReplicas: tc.replicas}
			c := a.criuRestorer()
			retry := c.captureGroup(context.Background(), leader, "", 2)
			cms, _ := kc.CoreV1().ConfigMaps(a.criuFailedNamespace()).List(context.Background(), metav1.ListOptions{})
			if retry != tc.wantRetry || len(cms.Items) != 0 {
				t.Errorf("retry = %v with %d records; want retry %v and no record", retry, len(cms.Items), tc.wantRetry)
			}
		})
	}
}

// The agent lists Grove scaling groups through the dynamic client, which
// rbac_test.go does not see: its rule is checked here.
func TestAgentRBACAllowsListingGroveScalingGroups(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/helm/nvsnap/templates/agent-rbac.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`apiGroups: \["grove.io"\]\s+resources: \["podcliquescalinggroups"\]\s+verbs: \[[^\]]*"list"`).Match(raw) {
		t.Error(`the agent ClusterRole does not allow list on grove.io podcliquescalinggroups`)
	}
}

// A read that keeps failing is retried without a wait after the last try,
// and a cancelled context ends the waits: admission waits for it.
func TestGroveScalingGroupReplicas_RetriesBounded(t *testing.T) {
	saved := groveReadBackoff
	groveReadBackoff = 100 * time.Millisecond
	t.Cleanup(func() { groveReadBackoff = saved })
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{groveScalingGroups: "PodCliqueScalingGroupList"})
	var tries atomic.Int32
	dyn.PrependReactor("list", "podcliquescalinggroups", func(k8stesting.Action) (bool, runtime.Object, error) {
		tries.Add(1)
		return true, nil, apierrors.NewServiceUnavailable("etcd timeout")
	})
	a := &Agent{dynClient: dyn}
	t0 := time.Now()
	if _, err := a.groveScalingGroupReplicas(context.Background(), "fn"); err == nil {
		t.Fatal("a failing read returned no error")
	}
	// 3 tries, waits of 100 and 200 ms between them, none after the last.
	if d := time.Since(t0); tries.Load() != 3 || d >= 500*time.Millisecond {
		t.Errorf("%d tries in %s; want 3 tries in about 300ms", tries.Load(), d)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	t0 = time.Now()
	if _, err := a.groveScalingGroupReplicas(ctx, "fn"); err == nil {
		t.Fatal("a failing read returned no error")
	}
	if d := time.Since(t0); d >= 50*time.Millisecond {
		t.Errorf("a cancelled read took %s", d)
	}
}

// withDeletePreconditions makes kc's pod deletes honour a UID and
// resourceVersion precondition, as the API server does.
func withDeletePreconditions(kc *fake.Clientset) {
	gvr := corev1.SchemeGroupVersion.WithResource("pods")
	kc.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		da := action.(k8stesting.DeleteActionImpl)
		obj, err := kc.Tracker().Get(gvr, da.GetNamespace(), da.GetName())
		if err != nil {
			return true, nil, err
		}
		cur := obj.(*corev1.Pod)
		if pc := da.DeleteOptions.Preconditions; pc != nil {
			if (pc.UID != nil && *pc.UID != cur.UID) || (pc.ResourceVersion != nil && *pc.ResourceVersion != cur.ResourceVersion) {
				return true, nil, apierrors.NewConflict(gvr.GroupResource(), cur.Name, fmt.Errorf("precondition failed"))
			}
		}
		return true, nil, kc.Tracker().Delete(gvr, cur.Namespace, cur.Name)
	})
}

// A restore ending deletes its companions, but one another live leader
// claimed after this restore read it is that leader's: the delete, made
// with the resourceVersion it read, conflicts and leaves it alone. An
// unclaimed companion is deleted.
func TestDeleteCompanionUnlessClaimed(t *testing.T) {
	ctx := context.Background()
	log := logrus.NewEntry(logrus.New())
	ending, other := groveRank("w-0-ldr", "0", "0"), groveRank("w-2-ldr", "2", "0")
	etcd, nats := claimablePlaceholder("etcd-0"), claimablePlaceholder("nats-0")
	etcd.UID, nats.UID = "etcd-uid", "nats-uid"
	kc := conditionalPatchClient(ending, other, etcd, nats)
	withDeletePreconditions(kc)
	read := func(name string) *corev1.Pod {
		p, err := kc.CoreV1().Pods("fn").Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	seenEtcd, seenNats := read("etcd-0"), read("nats-0") // what the ending restore listed
	if err := claimCompanions(ctx, kc, []*corev1.Pod{other, read("etcd-0")}, other); err != nil {
		t.Fatal(err) // the other leader claims etcd after the list
	}
	if err := deleteCompanionUnlessClaimed(ctx, kc, seenEtcd, ending, log); err != nil {
		t.Fatal(err)
	}
	if err := deleteCompanionUnlessClaimed(ctx, kc, seenNats, ending, log); err != nil {
		t.Fatal(err)
	}
	if _, err := kc.CoreV1().Pods("fn").Get(ctx, "etcd-0", metav1.GetOptions{}); err != nil {
		t.Errorf("the companion another live leader claimed was deleted: %v", err)
	}
	if _, err := kc.CoreV1().Pods("fn").Get(ctx, "nats-0", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("the unclaimed companion was not deleted: %v", err)
	}
}
