// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/modelvolume"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

// capturedInstance is an instance of two ranks just captured: its record
// verifying, its pods dumped and not yet deleted.
func capturedInstance(t *testing.T) (*criuAutoRestorer, *fake.Clientset, []*corev1.Pod) {
	t.Helper()
	ctx := context.Background()
	kc := fake.NewSimpleClientset(parallelSTS())
	a := &Agent{kubeClient: kc, log: logrus.New()}
	record := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: criuGroupConfigMapName("k1"), Namespace: "nvsnap-system"},
		Data: map[string]string{"state": criuGroupStateVerifying, "sourceNamespace": "fn", "size": "2", "captures": "1",
			"startedAt": "t0", "checkpoint.0": "c0", "checkpoint.1": "c1"}}
	if _, err := kc.CoreV1().ConfigMaps("nvsnap-system").Create(ctx, record, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	var members []*corev1.Pod
	for i, name := range []string{"engine-0", "engine-1"} {
		// Dumped: its engine stopped, so the pod is not Ready.
		p := stsRank(name, []string{"0", "1"}[i], "2", "sts-a", false)
		created, err := kc.CoreV1().Pods("fn").Create(ctx, &p, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		members = append(members, created)
	}
	return a.criuRestorer(), kc, members
}

// parallelSTS is the StatefulSet stsRank's pods name as their controller,
// creating its pods together.
func parallelSTS() *appsv1.StatefulSet {
	p := stsRank("x", "0", "1", "sts-a", false)
	return &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: metav1.GetControllerOf(&p).Name, Namespace: "fn", UID: "sts-a"},
		Spec: appsv1.StatefulSetSpec{PodManagementPolicy: appsv1.ParallelPodManagement}}
}

// replacement is the pod a controller creates for a deleted rank: the
// same name, a new UID.
func replacement(t *testing.T, kc *fake.Clientset, old *corev1.Pod) *corev1.Pod {
	t.Helper()
	ordinal := old.Annotations[modelvolume.CacheOrdinalAnnotation]
	p := stsRank(old.Name, ordinal, "2", "sts-a", false)
	p.Annotations = map[string]string{}
	for k, v := range old.Annotations {
		p.Annotations[k] = v
	}
	p.UID = types.UID(old.Name + "-new")
	created, err := kc.CoreV1().Pods("fn").Create(context.Background(), &p, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return created
}

// The bad order: one rank's delete is accepted and its controller
// recreates it, and the replacement is admitted, before the API server
// accepts its sibling's delete (the sibling has no deletionTimestamp yet).
// The replacement must still be admitted as its rank's restore
// placeholder: admission must not depend on the order of the deletes.
func TestDeleteInstance_ReplacementAdmittedBeforeSiblingDelete(t *testing.T) {
	c, kc, members := capturedInstance(t)
	admitted := make(chan struct{})
	var got bool
	var why string
	prev := deleteMember
	deleteMember = func(a *Agent, ctx context.Context, ns, name string, uid types.UID) error {
		if name == "engine-1" {
			// Delivered only after rank 0's replacement was admitted.
			select {
			case <-admitted:
			case <-time.After(5 * time.Second):
				t.Error("rank 0's replacement was never admitted")
			}
			return prev(a, ctx, ns, name, uid)
		}
		if err := prev(a, ctx, ns, name, uid); err != nil {
			return err
		}
		defer close(admitted)
		r := replacement(t, kc, members[0])
		got, why = admitReplacement(t, c.a, r)
		return nil
	}
	t.Cleanup(func() { deleteMember = prev })

	c.deleteInstance(context.Background(), "k1", "t0", members, c.a.log.WithField("t", 1))

	if !got {
		t.Errorf("rank 0's replacement, admitted before rank 1's delete was accepted, starts cold: %s", why)
	}
	cm, err := kc.CoreV1().ConfigMaps("nvsnap-system").Get(context.Background(), criuGroupConfigMapName("k1"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if cm.Data[groupLeavingKey] != "engine-0,engine-1" || cm.Data[groupLeavingAtKey] == "" {
		t.Errorf("record lists %q leaving at %q, want both captured pods", cm.Data[groupLeavingKey], cm.Data[groupLeavingAtKey])
	}
}

// The capture's own commit (captureGroup's last step) records the capture
// to verify and lists its pods as leaving on that record before the first
// delete reaches the API server.
func TestCommitCapture_ListsLeavingBeforeFirstDelete(t *testing.T) {
	c, kc, members := capturedInstance(t)
	ctx := context.Background()
	if err := c.updateGroupRecord(ctx, "k1", func(d map[string]string) bool {
		for k := range d {
			delete(d, k)
		}
		d["state"], d["startedAt"] = criuGroupStateCapturing, "t0"
		return true
	}); err != nil {
		t.Fatal(err)
	}
	data := map[string]string{"state": criuGroupStateVerifying, "sourceNamespace": "fn", "size": "2", "captures": "1",
		"startedAt": "t0", "checkpoint.0": "c0", "checkpoint.1": "c1"}
	var mu sync.Mutex
	var firstDelete map[string]string
	prev := deleteMember
	deleteMember = func(a *Agent, ctx context.Context, ns, name string, uid types.UID) error {
		mu.Lock()
		if firstDelete == nil {
			cm, err := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, criuGroupConfigMapName("k1"), metav1.GetOptions{})
			if err != nil {
				t.Error(err)
			} else {
				firstDelete = cm.Data
			}
		}
		mu.Unlock()
		return prev(a, ctx, ns, name, uid)
	}
	t.Cleanup(func() { deleteMember = prev })
	published := false

	c.commitCapture(ctx, "k1", "t0", data, members, false, func() { published = true }, c.a.log.WithField("t", 1))

	if !published {
		t.Error("the capture to verify was not published")
	}
	if firstDelete[groupLeavingKey] != "engine-0,engine-1" || firstDelete["state"] != criuGroupStateVerifying {
		t.Errorf("at the first delete the record was %v; want it verifying and listing both captured pods", firstDelete)
	}
	if g, ok := c.a.lookupCRIUGroup(ctx, "k1"); !ok || len(g.Leaving) != 2 {
		t.Errorf("the webhook's view lists %v leaving (ok %v)", g.Leaving, ok)
	}
	if left := remainingPods(t, kc); len(left) != 0 {
		t.Errorf("pods left %v, want the captured pods deleted", left)
	}
}

// A failed capture that stopped its instance deletes its pods without
// reading the group record first: the record is not being verified, so
// nothing is listed, and a stalled read would only hold the stopped pods.
// The failure is then recorded and nothing is published.
func TestCommitCapture_StoppedFailureDeletesWithoutRecordRead(t *testing.T) {
	c, kc, members := capturedInstance(t)
	ctx := context.Background()
	if err := c.updateGroupRecord(ctx, "k1", func(d map[string]string) bool {
		d["state"], d["startedAt"] = criuGroupStateCapturing, "t0"
		return true
	}); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	reads, readsBeforeDelete, deletes := 0, -1, 0
	kc.PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
		mu.Lock()
		reads++
		mu.Unlock()
		return false, nil, nil
	})
	prev := deleteMember
	deleteMember = func(a *Agent, ctx context.Context, ns, name string, uid types.UID) error {
		mu.Lock()
		if deletes == 0 {
			readsBeforeDelete = reads
		}
		deletes++
		mu.Unlock()
		return prev(a, ctx, ns, name, uid)
	}
	t.Cleanup(func() { deleteMember = prev })
	data := map[string]string{"state": criuGroupStateFailed, "reason": "dump failed", "startedAt": "t0"}

	c.commitCapture(ctx, "k1", "t0", data, members, true, func() { t.Error("a failed capture was published") }, c.a.log.WithField("t", 1))

	if readsBeforeDelete != 0 {
		t.Errorf("%d group record read(s) before the first delete, want none", readsBeforeDelete)
	}
	if left := remainingPods(t, kc); len(left) != 0 {
		t.Errorf("pods left %v, want the stopped instance's pods deleted", left)
	}
	cm, err := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, criuGroupConfigMapName("k1"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if cm.Data["state"] != criuGroupStateFailed || cm.Data[groupLeavingKey] != "" {
		t.Errorf("record %v, want the failure recorded and nothing listed", cm.Data)
	}
}

// A capture whose commit is rejected (another capture took the record
// over since it claimed it) neither publishes nor touches the newer
// capture's leaving list. Its own pods were stopped by its dump, so they
// are still deleted, unlisted: their controllers replace them, cold.
func TestCommitCapture_StaleCommitKeepsNewerListing(t *testing.T) {
	c, kc, members := capturedInstance(t)
	ctx := context.Background()
	newerAt := time.Now().UTC().Format(time.RFC3339)
	if err := c.updateGroupRecord(ctx, "k1", func(d map[string]string) bool {
		d["startedAt"], d[groupLeavingKey], d[groupLeavingAtKey] = "t1", "newer-0,newer-1", newerAt
		return true
	}); err != nil {
		t.Fatal(err)
	}
	data := map[string]string{"state": criuGroupStateVerifying, "sourceNamespace": "fn", "size": "2", "captures": "1",
		"startedAt": "t0", "checkpoint.0": "old0", "checkpoint.1": "old1"}
	published := false

	c.commitCapture(ctx, "k1", "t0", data, members, false, func() { published = true }, c.a.log.WithField("t", 1))

	if published {
		t.Error("a capture whose commit was rejected was published")
	}
	cm, err := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, criuGroupConfigMapName("k1"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if cm.Data[groupLeavingKey] != "newer-0,newer-1" || cm.Data[groupLeavingAtKey] != newerAt || cm.Data["checkpoint.0"] != "c0" {
		t.Errorf("record %v, want the newer capture's record and leaving list intact", cm.Data)
	}
	if left := remainingPods(t, kc); len(left) != 0 {
		t.Errorf("pods left %v, want the stale capture's stopped pods deleted", left)
	}
}

// admitReplacement is the webhook's decision for a replacement rank: the
// record it restores from, then the group admission against it.
func admitReplacement(t *testing.T, a *Agent, pod *corev1.Pod) (bool, string) {
	t.Helper()
	g, ok := a.lookupCRIUGroup(context.Background(), "k1")
	if !ok {
		return false, "no usable group record"
	}
	return a.criuGroupAdmits(context.Background(), pod, g)
}

// Only a record being verified lists its members as leaving: a failed
// capture's record (its replacements start cold) is left as it is.
func TestDeleteInstance_ListsLeavingOnlyOnVerifyingRecord(t *testing.T) {
	c, kc, members := capturedInstance(t)
	ctx := context.Background()
	if err := c.updateGroupRecord(ctx, "k1", func(d map[string]string) bool {
		d["state"] = criuGroupStateFailed
		return true
	}); err != nil {
		t.Fatal(err)
	}
	c.deleteInstance(ctx, "k1", "t0", members, c.a.log.WithField("t", 1))
	cm, err := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, criuGroupConfigMapName("k1"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if cm.Data[groupLeavingKey] != "" || cm.Data[groupLeavingAtKey] != "" {
		t.Errorf("failed record lists %q leaving", cm.Data[groupLeavingKey])
	}
	if left := remainingPods(t, kc); len(left) != 0 {
		t.Errorf("pods left %v, want the captured pods deleted all the same", left)
	}
}

// The list is written only on the record generation the deleting capture
// claimed: a record verifying another capture (claimed at another time)
// keeps its own list.
func TestDeleteInstance_ListsLeavingOnlyOnOwnCapture(t *testing.T) {
	c, kc, members := capturedInstance(t)
	ctx := context.Background()
	c.deleteInstance(ctx, "k1", "t-other", members, c.a.log.WithField("t", 1))
	cm, err := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, criuGroupConfigMapName("k1"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if cm.Data[groupLeavingKey] != "" {
		t.Errorf("another capture's record lists %q leaving", cm.Data[groupLeavingKey])
	}
}

// A pod the record lists as leaving is not running; any other pod of the
// instance is, and a listing older than leavingWindow (a delete that never
// happened) or more than a minute ahead of the admitting agent's clock no
// longer vouches for it. Nor does it for a listed pod that became Ready,
// or whose container started, after the listing: it was restarted after
// its dump and may run an engine. A pod still Ready from before the
// listing (its status not yet updated after the dump) is leaving. Fail
// closed.
func TestCRIUGroupAdmits_Leaving(t *testing.T) {
	now := time.Now()
	readySince := func(at time.Time) func(*corev1.Pod) {
		return func(p *corev1.Pod) {
			p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(at)}}
		}
	}
	startedAt := func(at time.Time) func(*corev1.Pod) {
		return func(p *corev1.Pod) {
			p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "engine",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(at)}}}}
		}
	}
	// In restart backoff: no current start; the run that started after the
	// listing and ended is the last termination.
	backingOff := func(at time.Time) func(*corev1.Pod) {
		return func(p *corev1.Pod) {
			p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "engine", RestartCount: 1,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					StartedAt: metav1.NewTime(at), FinishedAt: metav1.NewTime(at.Add(time.Second))}}}}
		}
	}
	for _, tc := range []struct {
		name     string
		siblings []string // pods besides the replacement of rank 0
		leaving  []string
		at       time.Time
		sibling  func(*corev1.Pod)
		want     bool
		reason   string
	}{
		{"captured sibling listed, its delete not yet accepted", []string{"engine-1"}, []string{"engine-0", "engine-1"}, now, nil, true, ""},
		{"listed one second inside the window", []string{"engine-1"}, []string{"engine-1"}, now.Add(-leavingWindow() + time.Second), nil, true, ""},
		{"listed 30s ahead of this clock (skew tolerated)", []string{"engine-1"}, []string{"engine-1"}, now.Add(30 * time.Second), nil, true, ""},
		{"listed, its container started before the listing", []string{"engine-1"}, []string{"engine-1"}, now.Add(-time.Minute), startedAt(now.Add(-time.Hour)), true, ""},
		{"sibling not captured: not listed", []string{"engine-1"}, []string{"engine-0"}, now, nil, false, "engine-1 of the instance already runs"},
		{"no listing", []string{"engine-1"}, nil, now, nil, false, "already runs"},
		{"listing stale: the delete never happened", []string{"engine-1"}, []string{"engine-1"}, now.Add(-leavingWindow() - time.Second), nil, false, "already runs"},
		{"listing from the future", []string{"engine-1"}, []string{"engine-1"}, now.Add(2 * time.Minute), nil, false, "already runs"},
		{"listed, still Ready from before the listing (status not yet updated)", []string{"engine-1"}, []string{"engine-1"}, now.Add(-time.Minute), readySince(now.Add(-time.Hour)), true, ""},
		{"listed but became Ready after the listing", []string{"engine-1"}, []string{"engine-1"}, now.Add(-time.Minute), readySince(now.Add(-10 * time.Second)), false, "already runs"},
		{"listed but its container started after the listing", []string{"engine-1"}, []string{"engine-1"}, now.Add(-time.Minute), startedAt(now.Add(-10 * time.Second)), false, "already runs"},
		{"listed, in restart backoff after a run started after the listing", []string{"engine-1"}, []string{"engine-1"}, now.Add(-time.Minute), backingOff(now.Add(-10 * time.Second)), false, "already runs"},
		{"listed, in backoff after a run that started before the listing", []string{"engine-1"}, []string{"engine-1"}, now.Add(-time.Minute), backingOff(now.Add(-time.Hour)), true, ""},
		{"one sibling listed, another not", []string{"engine-1", "engine-2"}, []string{"engine-1"}, now, nil, false, "engine-2 of the instance already runs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kc := fake.NewSimpleClientset(parallelSTS())
			for i, name := range tc.siblings {
				p := stsRank(name, []string{"1", "2"}[i], "3", "sts-a", false)
				if tc.sibling != nil {
					tc.sibling(&p)
				}
				if _, err := kc.CoreV1().Pods("fn").Create(context.Background(), &p, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			r := stsRank("engine-0", "0", "3", "sts-a", false)
			r.UID = "engine-0-new"
			a := &Agent{kubeClient: kc, log: logrus.New()}
			g := webhook.CRIUGroup{Key: "k1", Leaving: tc.leaving}
			if tc.leaving != nil {
				g.LeavingAt = tc.at
			}
			got, why := a.criuGroupAdmits(context.Background(), &r, g)
			if got != tc.want || !strings.Contains(why, tc.reason) {
				t.Errorf("admits = %v (%q), want %v (%q)", got, why, tc.want, tc.reason)
			}
		})
	}
}

// A listing ignored for the clock (stale or ahead of this agent) is logged
// once per record, listing and reason, not on every admission: the skew
// between agents shows, without flooding the log.
func TestCRIUGroupAdmits_IgnoredListingLoggedOnce(t *testing.T) {
	kc := fake.NewSimpleClientset(parallelSTS())
	p := stsRank("engine-1", "1", "2", "sts-a", false)
	if _, err := kc.CoreV1().Pods("fn").Create(context.Background(), &p, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	var logs strings.Builder
	lg := logrus.New()
	lg.SetOutput(&logs)
	a := &Agent{kubeClient: kc, log: lg}
	g := webhook.CRIUGroup{Key: "k1", Leaving: []string{"engine-1"}, LeavingAt: time.Now().Add(5 * time.Minute)}
	for range 3 {
		r := stsRank("engine-0", "0", "2", "sts-a", false)
		if ok, _ := a.criuGroupAdmits(context.Background(), &r, g); ok {
			t.Fatal("admitted beside a pod listed from the future")
		}
	}
	if n := strings.Count(logs.String(), "listing ignored"); n != 1 {
		t.Errorf("ignored listing logged %d times, want once:\n%s", n, logs.String())
	}
	if !strings.Contains(logs.String(), "ahead") {
		t.Errorf("log does not say the listing is ahead of this clock:\n%s", logs.String())
	}
}

// A companion's replacement is admitted beside a captured GPU rank the
// record lists as leaving, not beside any other GPU pod.
func TestCRIUCompanionAdmits_Leaving(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name    string
		leaving []string
		at      time.Time
		readyAt time.Duration // Ready since now+readyAt; 0: not Ready
		want    bool
	}{
		{"captured rank listed", []string{"engine-0"}, now, 0, true},
		{"rank not listed", []string{"engine-9"}, now, 0, false},
		{"listing stale", []string{"engine-0"}, now.Add(-leavingWindow() - time.Second), 0, false},
		{"listing 30s ahead (skew tolerated)", []string{"engine-0"}, now.Add(30 * time.Second), 0, true},
		{"captured rank still Ready from before the listing", []string{"engine-0"}, now.Add(-time.Minute), -time.Hour, true},
		{"captured rank became Ready after the listing", []string{"engine-0"}, now.Add(-time.Minute), -10 * time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kc := fake.NewSimpleClientset()
			rank := stsRank("engine-0", "0", "1", "sts-a", tc.readyAt != 0)
			if tc.readyAt != 0 {
				rank.Status.Conditions[0].LastTransitionTime = metav1.NewTime(now.Add(tc.readyAt))
			}
			if _, err := kc.CoreV1().Pods("fn").Create(context.Background(), &rank, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			a := &Agent{kubeClient: kc, log: logrus.New(), groveReplicas: replicasOf(map[string]int{}, nil)}
			etcd := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "etcd-0", Namespace: "fn", UID: "etcd-new"}}
			got, why := a.criuCompanionAdmits(context.Background(), etcd, webhook.CRIUGroup{Key: "k1", Leaving: tc.leaving, LeavingAt: tc.at})
			if got != tc.want {
				t.Errorf("admits = %v (%q), want %v", got, why, tc.want)
			}
		})
	}
}

// The listing is read back from the record by the webhook's lookup.
func TestParseCRIUGroup_Leaving(t *testing.T) {
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	data := map[string]string{"state": "complete", "size": "1", "checkpoint.0": "a",
		groupLeavingKey: "u0,u1", groupLeavingAtKey: at.Format(time.RFC3339)}
	g, ok := parseCRIUGroup("k1", data)
	if !ok || strings.Join(g.Leaving, ",") != "u0,u1" || !g.LeavingAt.Equal(at) {
		t.Errorf("parsed leaving %v at %v (ok %v)", g.Leaving, g.LeavingAt, ok)
	}
	data[groupLeavingAtKey] = "not a time"
	if g, _ := parseCRIUGroup("k1", data); len(g.Leaving) != 0 {
		t.Errorf("leaving %v parsed without a valid time", g.Leaving)
	}
}
