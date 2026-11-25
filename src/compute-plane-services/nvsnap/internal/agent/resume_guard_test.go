// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/metrics"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

func guardLog() *logrus.Entry {
	l := logrus.New()
	l.SetOutput(os.Stderr)
	l.SetLevel(logrus.WarnLevel)
	return logrus.NewEntry(l)
}

// A leave-running cycle whose resume raised Xid 137 on the workload's GPUs
// fails with that reason, and keeps the kernel's lines with its logs.
func TestGPUFaultWatch_FailsOnXid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kmsg")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	drv := t.TempDir()
	if err := os.MkdirAll(filepath.Join(drv, "0008:06:00.0"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(drv, "0008:06:00.0", "information"), []byte("GPU UUID: \t GPU-aaa\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	w := startGPUFaultWatchAt(path, guardLog())
	defer w.close()
	appendFile(t, path, kmsgRecord(7, "eth0: link up")+kmsgRecord(8, xid137))
	dir := t.TempDir()
	err := w.check(drv, "GPU-aaa\n", dir)
	if !errors.Is(err, errXidFault) {
		t.Fatalf("check = %v, want a GPU fault", err)
	}
	if got := metrics.Reason(err.Error()); got != "gpu_xid" {
		t.Errorf("reason %q", got)
	}
	b, rerr := os.ReadFile(filepath.Join(dir, kernelGPUFaultsLog))
	if rerr != nil || !strings.Contains(string(b), xid137) {
		t.Errorf("the kernel's lines are not kept with the logs: %q %v", b, rerr)
	}
	// Nothing new since: the next check passes.
	if err := w.check(drv, "GPU-aaa\n", t.TempDir()); err != nil {
		t.Errorf("a quiet log failed the check: %v", err)
	}
}

// Faults on another pod's GPUs do not fail this capture.
func TestGPUFaultWatch_OtherGPUs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kmsg")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	drv := t.TempDir()
	if err := os.MkdirAll(filepath.Join(drv, "0018:06:00.0"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(drv, "0018:06:00.0", "information"), []byte("GPU UUID: GPU-ccc\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	w := startGPUFaultWatchAt(path, guardLog())
	defer w.close()
	appendFile(t, path, kmsgRecord(8, xid137))
	if err := w.check(drv, "GPU-ccc\n", t.TempDir()); err != nil {
		t.Errorf("a fault on another GPU failed the capture: %v", err)
	}
}

// Without a kernel log to read, the watch is off: it says so once and
// fails nothing.
func TestGPUFaultWatch_NoKernelLog(t *testing.T) {
	w := startGPUFaultWatchAt(filepath.Join(t.TempDir(), "missing"), guardLog())
	defer w.close()
	if err := w.check(t.TempDir(), "GPU-aaa", t.TempDir()); err != nil {
		t.Errorf("no kernel log failed the capture: %v", err)
	}
}

// Every GPU process the capture suspended still runs after the resume;
// one that ended fails it as a stall.
func TestMembersAfterResume(t *testing.T) {
	base := t.TempDir()
	fakeGPUProc(t, base, 5000, 1, 5000, []string{testShim}, nil)
	fakeGPUProc(t, base, 5100, 811, 5000, []string{testShim, "/dev/nvidia0"}, nil)
	fakeGPUProc(t, base, 5200, 812, 5000, []string{testShim, "/dev/nvidia1"}, nil)
	if err := membersAfterResume(base, 5000, []int{811, 812}); err != nil {
		t.Fatalf("all running: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(base, "5200")); err != nil {
		t.Fatal(err)
	}
	err := membersAfterResume(base, 5000, []int{811, 812})
	if !errors.Is(err, errResumeStalled) || !strings.Contains(err.Error(), "812") {
		t.Fatalf("one ended: %v, want a stall naming 812", err)
	}
	if got := metrics.Reason(err.Error()); got != "resume_stalled" {
		t.Errorf("reason %q", got)
	}
	if sourceMustBeReplaced(err) {
		t.Error("a stall replaces the pods")
	}
}

func memberPod(name string, ready bool, restarts int32) *corev1.Pod {
	st := corev1.ConditionFalse
	if ready {
		st = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, UID: types.UID("uid-" + name)},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: st}},
			ContainerStatuses: []corev1.ContainerStatus{{Name: "engine", RestartCount: restarts,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}},
		},
	}
}

var fastWatch = resumeWatchOpts{ReadyWait: 400 * time.Millisecond, Steady: 200 * time.Millisecond, Poll: 20 * time.Millisecond}

func TestWatchMembersAfterResume_Healthy(t *testing.T) {
	a, b := memberPod("a", true, 0), memberPod("b", true, 2)
	cs := fake.NewClientset(a, b)
	if err := watchMembersAfterResume(context.Background(), cs, []memberBaseline{baselineOf(a), baselineOf(b)}, fastWatch); err != nil {
		t.Fatalf("healthy members: %v", err)
	}
}

// A pod not Ready again once the hold ended (its probes fail while held)
// is given ReadyWait; one that comes back in time passes.
func TestWatchMembersAfterResume_ReadyAgainInTime(t *testing.T) {
	a := memberPod("a", true, 0)
	cs := fake.NewClientset(memberPod("a", false, 0))
	go func() {
		time.Sleep(100 * time.Millisecond)
		_, _ = cs.CoreV1().Pods("ns").UpdateStatus(context.Background(), memberPod("a", true, 0), metav1.UpdateOptions{})
	}()
	if err := watchMembersAfterResume(context.Background(), cs, []memberBaseline{baselineOf(a)}, fastWatch); err != nil {
		t.Fatalf("ready again in time: %v", err)
	}
}

func TestWatchMembersAfterResume_Failures(t *testing.T) {
	base := memberPod("a", true, 0)
	replaced := memberPod("a", true, 0)
	replaced.UID = "uid-new"
	for name, tc := range map[string]struct {
		now  *corev1.Pod
		want string
	}{
		"never ready":       {memberPod("a", false, 0), "not Ready"},
		"container restart": {memberPod("a", true, 1), "restarted"},
		"replaced":          {replaced, "replaced"},
		"gone":              {nil, "gone"},
	} {
		t.Run(name, func(t *testing.T) {
			cs := fake.NewClientset()
			if tc.now != nil {
				cs = fake.NewClientset(tc.now)
			}
			err := watchMembersAfterResume(context.Background(), cs, []memberBaseline{baselineOf(base)}, fastWatch)
			if !errors.Is(err, errResumeStalled) || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "ns/a") {
				t.Fatalf("%v, want a stall naming ns/a and %q", err, tc.want)
			}
			if got := metrics.Reason(err.Error()); got != "resume_stalled" {
				t.Errorf("reason %q", got)
			}
			if sourceMustBeReplacedText(err.Error()) {
				t.Error("a stall deletes the pods")
			}
		})
	}
}

// A container that restarts during the steady window, after the pods were
// Ready, fails the cycle too.
func TestWatchMembersAfterResume_RestartAfterReady(t *testing.T) {
	a := memberPod("a", true, 0)
	cs := fake.NewClientset(memberPod("a", true, 0))
	o := resumeWatchOpts{ReadyWait: 400 * time.Millisecond, Steady: 400 * time.Millisecond, Poll: 20 * time.Millisecond}
	go func() {
		time.Sleep(150 * time.Millisecond)
		_, _ = cs.CoreV1().Pods("ns").UpdateStatus(context.Background(), memberPod("a", false, 1), metav1.UpdateOptions{})
	}()
	if err := watchMembersAfterResume(context.Background(), cs, []memberBaseline{baselineOf(a)}, o); !errors.Is(err, errResumeStalled) {
		t.Fatalf("restart in the steady window: %v", err)
	}
}

// withInit adds a one-shot init container that completed and a sidecar
// (restartable init container) restarted sidecarRestarts times.
func withInit(p *corev1.Pod, sidecarRestarts int32) *corev1.Pod {
	always := corev1.ContainerRestartPolicyAlways
	p.Spec.InitContainers = []corev1.Container{{Name: "setup"}, {Name: "proxy", RestartPolicy: &always}}
	p.Status.InitContainerStatuses = []corev1.ContainerStatus{
		{Name: "setup", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Completed"}}},
		{Name: "proxy", RestartCount: sidecarRestarts, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
	}
	return p
}

// A sidecar (restartable init container) that restarts during the watch
// fails it as a container would; a one-shot init container that completed
// before the capture does not.
func TestWatchMembersAfterResume_Sidecars(t *testing.T) {
	a := withInit(memberPod("a", true, 0), 0)
	cs := fake.NewClientset(withInit(memberPod("a", true, 0), 0))
	if err := watchMembersAfterResume(context.Background(), cs, []memberBaseline{baselineOf(a)}, fastWatch); err != nil {
		t.Fatalf("a completed init container failed the watch: %v", err)
	}
	cs = fake.NewClientset(withInit(memberPod("a", true, 0), 1))
	err := watchMembersAfterResume(context.Background(), cs, []memberBaseline{baselineOf(a)}, fastWatch)
	if !errors.Is(err, errResumeStalled) || !strings.Contains(err.Error(), "container proxy restarted") {
		t.Fatalf("a sidecar restarted after the resume: %v, want a stall naming it", err)
	}
}

// A pod read that never returns (an API server that accepts and does not
// answer) ends at the watch's deadline, and the watch fails as stalled.
func TestWatchMembersAfterResume_ReadEndsAtTheDeadline(t *testing.T) {
	stop := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-stop: // the test is over: let Close return
		}
	}))
	defer srv.Close()
	defer close(stop)
	cs, err := kubernetes.NewForConfig(&rest.Config{Host: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- watchMembersAfterResume(context.Background(), cs, []memberBaseline{baselineOf(memberPod("a", true, 0))}, fastWatch)
	}()
	select {
	case err := <-done:
		if !errors.Is(err, errResumeStalled) {
			t.Fatalf("a read that never returned: %v, want stalled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the watch ran past its deadline on a read that never returned")
	}
}

// groupLeaveRunningFixture runs a leave-running group checkpoint of two
// pods against a fake peer agent whose members succeed at once; onCapture
// runs as each member's checkpoint arrives.
func groupLeaveRunningFixture(t *testing.T, onCapture func(cs *fake.Clientset)) error {
	t.Helper()
	old := postResumeWatch
	postResumeWatch = resumeWatchOpts{ReadyWait: 300 * time.Millisecond, Steady: 100 * time.Millisecond, Poll: 20 * time.Millisecond}
	defer func() { postResumeWatch = old }()

	var cs *fake.Clientset
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/checkpoint" {
			onCapture(cs)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"checkpointId":"c"}`))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: "InternalIP", Address: u.Hostname()}}}}
	a, b := memberPod("a", true, 0), memberPod("b", true, 0)
	a.Spec.NodeName, b.Spec.NodeName = "n1", "n1"
	cs = fake.NewClientset(node, a, b)
	ag := &Agent{kubeClient: cs, config: Config{ListenAddr: ":" + u.Port()}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = ag.groupCheckpoint(ctx, GroupCheckpointRequest{LeaveRunning: true,
		Members: []CheckpointRequest{{Namespace: "ns", PodName: "a"}, {Namespace: "ns", PodName: "b"}}}, guardLog())
	return err
}

// A leave-running group checkpoint whose members all report success is a
// success only once its pods are seen serving after the resume.
func TestGroupCheckpoint_LeaveRunningWatchesThePods(t *testing.T) {
	if err := groupLeaveRunningFixture(t, func(*fake.Clientset) {}); err != nil {
		t.Fatalf("healthy instance: %v", err)
	}
	err := groupLeaveRunningFixture(t, func(cs *fake.Clientset) {
		p := memberPod("b", true, 1)
		p.Spec.NodeName = "n1"
		_, _ = cs.CoreV1().Pods("ns").UpdateStatus(context.Background(), p, metav1.UpdateOptions{})
	})
	if !errors.Is(err, errResumeStalled) || !strings.Contains(err.Error(), "ns/b") {
		t.Fatalf("a member restarted after the resume: %v, want a stall naming ns/b", err)
	}
}

// Rolling upgrade: this driver over members on the released agent. They
// serve the released fabric state (recorded: no holdsOpen, no resumed) and
// ignore gpushareSyncOpen, so they resume at once. The driver must not wait
// for them, send them a verdict, or fail the capture as stalled.
func TestGroupCheckpoint_ReleasedMembersAreNotWaitedFor(t *testing.T) {
	old := postResumeWatch
	postResumeWatch = resumeWatchOpts{ReadyWait: 300 * time.Millisecond, Steady: 100 * time.Millisecond, Poll: 20 * time.Millisecond}
	defer func() { postResumeWatch = old }()
	state := compatFixture(t, "nvsnap-v0.3.2", "member-fabric-state.json")
	var mu sync.Mutex
	var verdicts []string
	reads := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/gpushare/fabric/"), "/")
		switch {
		case r.URL.Path == "/v1/checkpoint":
			// The member's capture returns only once the driver has read
			// its state a few times: the driver sees it running.
			var req CheckpointRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
				mu.Lock()
				n := reads[req.PodName]
				mu.Unlock()
				if n >= 3 {
					break
				}
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"checkpointId":"c"}`))
		case strings.HasPrefix(r.URL.Path, "/v1/gpushare/fabric/") && r.Method == http.MethodGet && len(parts) == 3:
			mu.Lock()
			reads[parts[2]]++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(state)
		case strings.HasPrefix(r.URL.Path, "/v1/gpushare/fabric/") && len(parts) == 4 && parts[3] == "open":
			mu.Lock()
			verdicts = append(verdicts, parts[2]+" "+r.URL.Query().Get("verdict"))
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: "InternalIP", Address: u.Hostname()}}}}
	a, b := memberPod("a", true, 0), memberPod("b", true, 0)
	a.Spec.NodeName, b.Spec.NodeName = "n1", "n1"
	ag := &Agent{kubeClient: fake.NewClientset(node, a, b), config: Config{ListenAddr: ":" + u.Port()}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	_, err = ag.groupCheckpoint(ctx, GroupCheckpointRequest{LeaveRunning: true,
		Members: []CheckpointRequest{{Namespace: "ns", PodName: "a"}, {Namespace: "ns", PodName: "b"}}}, guardLog())
	if err != nil {
		t.Fatalf("released members: %v", err)
	}
	if took := time.Since(start); took > 20*time.Second {
		t.Errorf("the driver waited %s for released members", took)
	}
	mu.Lock()
	defer mu.Unlock()
	if reads["a"] < 3 || reads["b"] < 3 {
		t.Errorf("the driver read the members' state %v times; want it seen while they ran", reads)
	}
	if len(verdicts) != 0 {
		t.Errorf("released members got open verdicts %v", verdicts)
	}
}

// groupOpenFixture runs a leave-running group checkpoint of pods a and b
// against a fake peer agent that plays both members' tools on the fabric
// routes: each holds its launches and reports resumed as its checkpoint
// runs, and its checkpoint returns once the driver answered it (or after
// 10 s). failB: b's checkpoint fails after a moment without resuming.
// Returns the verdict each member got.
func groupOpenFixture(t *testing.T, failB bool) (map[string]string, error) {
	t.Helper()
	old := postResumeWatch
	postResumeWatch = resumeWatchOpts{ReadyWait: 300 * time.Millisecond, Steady: 100 * time.Millisecond, Poll: 20 * time.Millisecond}
	defer func() { postResumeWatch = old }()

	var mu sync.Mutex
	verdicts, resumed := map[string]string{}, map[string]bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/gpushare/fabric/"), "/")
		switch {
		case r.URL.Path == "/v1/checkpoint":
			var req CheckpointRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			// A member holds its launches only when the driver asks
			// (syncOpenFor): one not asked would never wait for a verdict.
			if !req.GPUShareSyncOpen {
				http.Error(w, "the driver did not ask for the held resume", http.StatusBadRequest)
				return
			}
			if failB && req.PodName == "b" {
				time.Sleep(5 * fabricPollInterval) // the driver sees it hold first
				http.Error(w, "gpushare resume: remap failed", http.StatusInternalServerError)
				return
			}
			mu.Lock()
			resumed[req.PodName] = true
			mu.Unlock()
			for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
				mu.Lock()
				v := verdicts[req.PodName]
				mu.Unlock()
				if v != "" {
					break
				}
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"checkpointId":"c"}`))
		case strings.HasPrefix(r.URL.Path, "/v1/gpushare/fabric/") && r.Method == http.MethodGet && len(parts) == 3:
			mu.Lock()
			st := fabricState{HoldsOpen: true}
			if resumed[parts[2]] {
				st.Resumed = map[string]string{"a": "11-0", "b": "22-0"}[parts[2]]
			}
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(st)
		case strings.HasPrefix(r.URL.Path, "/v1/gpushare/fabric/") && len(parts) == 4 && parts[3] == "open":
			mu.Lock()
			verdicts[parts[2]] = r.URL.Query().Get("token") + " " + r.URL.Query().Get("verdict")
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: "InternalIP", Address: u.Hostname()}}}}
	a, b := memberPod("a", true, 0), memberPod("b", true, 0)
	a.Spec.NodeName, b.Spec.NodeName = "n1", "n1"
	ag := &Agent{kubeClient: fake.NewClientset(node, a, b), config: Config{ListenAddr: ":" + u.Port()}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = ag.groupCheckpoint(ctx, GroupCheckpointRequest{LeaveRunning: true,
		Members: []CheckpointRequest{{Namespace: "ns", PodName: "a"}, {Namespace: "ns", PodName: "b"}}}, guardLog())
	mu.Lock()
	defer mu.Unlock()
	return maps.Clone(verdicts), err
}

// A leave-running group checkpoint opens its members' GPU launch gates
// once every member has resumed; a member that fails without resuming
// gets the others "abort" (they open anyway), and the capture fails as
// stalled, naming it.
func TestGroupCheckpoint_LeaveRunningOpensOnceEveryMemberResumed(t *testing.T) {
	got, err := groupOpenFixture(t, false)
	if err != nil {
		t.Fatalf("healthy instance: %v", err)
	}
	if want := map[string]string{"a": "11-0 open", "b": "22-0 open"}; !maps.Equal(got, want) {
		t.Errorf("verdicts = %v, want %v", got, want)
	}
	got, err = groupOpenFixture(t, true)
	if want := map[string]string{"a": "11-0 abort"}; !maps.Equal(got, want) {
		t.Errorf("verdicts = %v, want %v", got, want)
	}
	if !errors.Is(err, errResumeStalled) || !strings.Contains(err.Error(), "ended before reporting resumed: ns/b") {
		t.Fatalf("b failed without resuming: %v, want a stall naming ns/b", err)
	}
	if sourceMustBeReplacedText(err.Error()) {
		t.Errorf("an unsynced open deletes the pods: %v", err)
	}
}
