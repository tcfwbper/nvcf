// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8stesting "k8s.io/client-go/testing"

	"golang.org/x/sys/unix"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/metrics"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

// stubMounts stands in for the kernel: attaching replaces the companion
// directory with a link to the version (so sameDir sees the version there,
// as it would through a mount), detaching removes it.
func stubMounts(t *testing.T) (attached, detached *int) {
	t.Helper()
	savedA, savedU := attachReadOnly, unmountDir
	t.Cleanup(func() { attachReadOnly, unmountDir = savedA, savedU })
	var a, d int
	attachReadOnly = func(src, dst string) error {
		a++
		if err := os.Remove(dst); err != nil {
			return err
		}
		return os.Symlink(src, dst)
	}
	unmountDir = func(dir string) error {
		d++
		if err := os.Remove(dir); err != nil {
			return err
		}
		return os.Mkdir(dir, 0o755)
	}
	return &a, &d
}

// stagedVersion stages version id with its criu, as restore-bundle-init.sh does.
func stagedVersion(t *testing.T, a *Agent, id string) {
	t.Helper()
	stageVersion(t, a, id, time.Hour)
	dir := filepath.Join(a.bundleVersionsDir(), id)
	//nolint:gosec // id is a test constant
	if err := os.WriteFile(filepath.Join(dir, "criu"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	// Staged an hour ago: old enough for the reaper to consider.
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(dir, old, old); err != nil {
		t.Fatal(err)
	}
}

// A staged version is mounted on its companion directory once; a version
// not staged here is not; releasing unmounts and removes the directory.
func TestCompanionTools_EnsureAndRelease(t *testing.T) {
	attached, detached := stubMounts(t)
	a := bundleAgent(t)
	stagedVersion(t, a, "v9")
	for i := 0; i < 2; i++ {
		if err := a.ensureCompanionTools("v9"); err != nil {
			t.Fatal(err)
		}
	}
	if *attached != 1 || !sameDir(filepath.Join(a.bundleVersionsDir(), "v9"), a.companionToolsDir("v9")) {
		t.Errorf("attached %d time(s), mounted %v; want once, mounted", *attached, sameDir(filepath.Join(a.bundleVersionsDir(), "v9"), a.companionToolsDir("v9")))
	}
	if err := a.ensureCompanionTools("v8"); err == nil {
		t.Error("a version not staged here was mounted")
	}
	if err := a.ensureCompanionTools("../x"); err == nil {
		t.Error("an invalid version name was accepted")
	}
	if err := a.releaseCompanionTools("v9"); err != nil {
		t.Fatal(err)
	}
	if *detached != 1 {
		t.Errorf("detached %d time(s), want 1", *detached)
	}
	if _, err := os.Stat(a.companionToolsDir("v9")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the companion directory is left: %v", err)
	}
}

// The node sweep mounts every staged version; one being fetched (no criu
// yet) waits.
func TestSyncCompanionTools(t *testing.T) {
	attached, _ := stubMounts(t)
	a := bundleAgent(t)
	stagedVersion(t, a, "v8")
	stagedVersion(t, a, "v9")
	stageVersion(t, a, "v10", time.Hour) // fetched, criu not in yet
	a.syncCompanionTools()
	if *attached != 2 {
		t.Errorf("attached %d version(s), want the 2 staged", *attached)
	}
	if _, err := os.Stat(a.companionToolsDir("v10")); err == nil && sameDir(filepath.Join(a.bundleVersionsDir(), "v10"), a.companionToolsDir("v10")) {
		t.Error("a version not yet staged was mounted")
	}
}

// An unused version is unmounted from its companion directory before it is
// removed; one whose directory cannot be released is kept.
func TestReapBundleVersions_ReleasesCompanionTools(t *testing.T) {
	_, detached := stubMounts(t)
	a := bundleAgent(t)
	stagedVersion(t, a, "old")
	if err := a.ensureCompanionTools("old"); err != nil {
		t.Fatal(err)
	}
	a.reapBundleVersions(nil, time.Now())
	if *detached != 1 {
		t.Errorf("detached %d time(s) before removing the version, want 1", *detached)
	}
	if _, err := os.Stat(filepath.Join(a.bundleVersionsDir(), "old")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the unused version was kept: %v", err)
	}

	stagedVersion(t, a, "stuck")
	if err := a.ensureCompanionTools("stuck"); err != nil {
		t.Fatal(err)
	}
	unmountDir = func(string) error { return syscall.EBUSY }
	a.reapBundleVersions(nil, time.Now())
	if _, err := os.Stat(filepath.Join(a.bundleVersionsDir(), "stuck")); err != nil {
		t.Errorf("a version whose companion directory could not be released was removed: %v", err)
	}
}

// Pod events, a capture and the node sweep may mount the same version at
// once: each sees the directory unmounted, but only one mount is attached.
func TestEnsureCompanionTools_Concurrent(t *testing.T) {
	savedA := attachReadOnly
	t.Cleanup(func() { attachReadOnly = savedA })
	var attached atomic.Int32
	attachReadOnly = func(src, dst string) error {
		attached.Add(1)
		time.Sleep(20 * time.Millisecond) // the window a second caller checks in
		_ = os.Remove(dst)
		return os.Symlink(src, dst)
	}
	a := bundleAgent(t)
	stagedVersion(t, a, "v9")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = a.ensureCompanionTools("v9")
		}()
	}
	wg.Wait()
	if n := attached.Load(); n != 1 {
		t.Errorf("attached %d mounts on one companion directory, want 1", n)
	}
}

// Mounts stacked on a companion directory (by an agent before companionMu)
// are all detached before it is removed; one that does not go after
// maxCompanionUnmounts keeps the version.
func TestReleaseCompanionTools_Stacked(t *testing.T) {
	savedA, savedU := attachReadOnly, unmountDir
	t.Cleanup(func() { attachReadOnly, unmountDir = savedA, savedU })
	depth, detached := 0, 0
	attachReadOnly = func(src, dst string) error {
		depth++
		_ = os.Remove(dst)
		return os.Symlink(src, dst)
	}
	unmountDir = func(dir string) error {
		detached++
		depth--
		if depth > 0 {
			return nil // the mount below still shows the version
		}
		if err := os.Remove(dir); err != nil {
			return err
		}
		return os.Mkdir(dir, 0o755)
	}
	a := bundleAgent(t)
	stagedVersion(t, a, "v9")
	dst := a.companionToolsDir("v9")
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ { // stacked as an unserialized agent could
		if err := attachReadOnly(filepath.Join(a.bundleVersionsDir(), "v9"), dst); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.removeBundleVersion("v9"); err != nil {
		t.Fatal(err)
	}
	if detached != 3 {
		t.Errorf("detached %d of 3 stacked mounts", detached)
	}
	if _, err := os.Stat(dst); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the companion directory is left: %v", err)
	}

	stagedVersion(t, a, "stuck")
	if err := a.ensureCompanionTools("stuck"); err != nil {
		t.Fatal(err)
	}
	detached = 0
	unmountDir = func(string) error { detached++; return nil } // never goes
	a.reapBundleVersions(nil, time.Now())
	if detached != maxCompanionUnmounts {
		t.Errorf("tried %d unmounts, want the bound %d", detached, maxCompanionUnmounts)
	}
	if _, err := os.Stat(filepath.Join(a.bundleVersionsDir(), "stuck")); err != nil {
		t.Errorf("a version still mounted on its companion directory was removed: %v", err)
	}
}

// A companion (or a companion's restore placeholder) pending on this node
// gets its version mounted before it starts, not at the next sweep.
func TestEnsurePlaceholderBundles_MountsCompanionTools(t *testing.T) {
	attached, _ := stubMounts(t)
	a := bundleAgent(t)
	a.config.NodeName = "g1"
	stagedVersion(t, a, "v9")
	c := &criuAutoRestorer{a: a}
	orCreate := corev1.HostPathDirectoryOrCreate
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "fn"},
		Spec: corev1.PodSpec{NodeName: "g1", Volumes: []corev1.Volume{{Name: "nvsnap-criu-bundle", VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{Path: "/var/lib/containerd/nvsnap-bundle/" + webhook.BundleCompanionDir + "/v9", Type: &orCreate}}}}},
		Status: corev1.PodStatus{Phase: corev1.PodPending}}
	c.ensurePlaceholderBundles(context.Background(), pod)
	if *attached != 1 {
		t.Errorf("attached %d time(s) for a pending companion, want 1", *attached)
	}
}

// The kernel's own mount, where the test may mount (root): the version
// shows on the companion directory, read-only; detaching leaves the
// directory empty.
func TestAttachReadOnly_Kernel(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root to mount")
	}
	src, dst := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "criu"), []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := attachReadOnly(src, dst); err != nil {
		if errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EPERM) {
			t.Skipf("the mount API is not available here: %v", err)
		}
		t.Fatal(err)
	}
	defer func() { _ = unmountDir(dst) }()
	if !sameDir(src, dst) {
		t.Fatal("the version does not show on the companion directory")
	}
	if err := os.WriteFile(filepath.Join(dst, "w"), []byte("x"), 0o644); !errors.Is(err, syscall.EROFS) {
		t.Errorf("writing through the companion directory: %v, want EROFS", err)
	}
	if err := unmountDir(dst); err != nil {
		t.Fatal(err)
	}
	if sameDir(src, dst) {
		t.Error("the version still shows after detaching")
	}
}

// An instance with a companion on a node no nvsnap agent runs on is decided
// at once, as not captured: before that node is asked for anything, and
// before the capture waits on the agents; without a record; counted. A pod
// on such a node that does not connect to the instance does not matter;
// one that does (seen from the companion's side) does.
func TestCaptureGroup_CompanionWithoutAgent(t *testing.T) {
	saved := groupWarmupWait
	groupWarmupWait = 0
	t.Cleanup(func() { groupWarmupWait = saved })
	isController := true
	pod := func(name, node, ip string, containers ...string) *corev1.Pod {
		p := ready(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "fn",
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: name, UID: types.UID("rs-" + name), Controller: &isController}}},
			Spec: corev1.PodSpec{NodeName: node}, Status: corev1.PodStatus{PodIP: ip}})
		for _, c := range containers {
			p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: c, Image: c})
		}
		return p
	}
	for _, tc := range []struct {
		name        string
		rankPeers   []string // the rank's connections
		etcdPeers   []string // a companion's connections, both ways
		pods        []*corev1.Pod
		wantRefused bool
		waited      time.Duration // how long the capture has waited for an agent already
	}{
		{"a single-container companion on an agentless node", []string{"10.9.9.9"}, nil,
			[]*corev1.Pod{pod("etcd", "cpu-1", "10.9.9.9", "etcd")}, true, 0},
		{"a companion of several containers on an agentless node", []string{"10.9.9.9"}, nil,
			[]*corev1.Pod{pod("etcd", "cpu-1", "10.9.9.9", "etcd", "metrics")}, true, 0},
		{"a client of a companion on an agentless node", []string{"10.9.9.7"}, []string{"10.9.9.8"},
			[]*corev1.Pod{pod("etcd", "gpu-1", "10.9.9.7", "etcd"), pod("web", "cpu-1", "10.9.9.8", "web")}, true, 0},
		// The companion sees the client's node address, not its pod's: a
		// source-NATed connection (cpu-1's other address is 10.20.0.1).
		{"a client of a companion on an agentless node, source-NATed", []string{"10.9.9.7"}, []string{"10.20.0.1"},
			[]*corev1.Pod{pod("etcd", "gpu-1", "10.9.9.7", "etcd"), pod("web", "cpu-1", "10.9.9.8", "web")}, true, 0},
		{"an unrelated pod on an agentless node", []string{"10.9.9.7"}, []string{"10.0.0.1"},
			[]*corev1.Pod{pod("etcd", "gpu-1", "10.9.9.7", "etcd"), pod("web", "cpu-1", "10.9.9.8", "web")}, false, 0},
		// gpu-2 is a GPU node whose agent pod is being replaced: retried.
		{"a companion on a GPU node whose agent is not running yet", []string{"10.9.9.6"}, nil,
			[]*corev1.Pod{pod("etcd", "gpu-2", "10.9.9.6", "etcd")}, false, 0},
		// ... but not for ever.
		{"a companion on a GPU node whose agent has not come", []string{"10.9.9.6"}, nil,
			[]*corev1.Pod{pod("etcd", "gpu-2", "10.9.9.6", "etcd")}, true, agentArrivalWait + time.Minute},
		// gpu-3 has a taint the agent does not tolerate: none will come.
		{"a companion on a GPU node the agent cannot run on", []string{"10.9.9.5"}, nil,
			[]*corev1.Pod{pod("etcd", "gpu-3", "10.9.9.5", "etcd")}, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, cs := fallbackAgent(t)
			a.specCacheURI = func(*corev1.Pod) (string, bool) { return "cache://companion", true }
			var cpuAsked atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.Contains(r.URL.Path, "/fn/engine-0/"):
					_ = json.NewEncoder(w).Encode(tc.rankPeers)
				case strings.Contains(r.URL.Path, "/fn/etcd/"):
					_ = json.NewEncoder(w).Encode(tc.etcdPeers)
				default:
					for _, p := range tc.pods {
						if p.Spec.NodeName == "cpu-1" && strings.Contains(r.URL.Path, "/fn/"+p.Name+"/") {
							cpuAsked.Add(1)
						}
					}
					_ = json.NewEncoder(w).Encode([]string{})
				}
			}))
			defer srv.Close()
			_, port, err := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
			if err != nil {
				t.Fatal(err)
			}
			a.config.ListenAddr = ":" + port
			ctx := context.Background()
			// Both nodes resolve to the test agent, so an ask meant for
			// cpu-1's (absent) agent is seen rather than refused.
			for _, name := range []string{"gpu-1", "gpu-2", "gpu-3", "cpu-1"} {
				n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name},
					Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "127.0.0.1"}}}}
				if strings.HasPrefix(name, "gpu-") {
					n.Status.Allocatable = corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("4")}
				}
				if name == "cpu-1" {
					n.Status.Addresses = append(n.Status.Addresses, corev1.NodeAddress{Type: corev1.NodeExternalIP, Address: "10.20.0.1"})
				}
				if name == "gpu-3" {
					n.Spec.Taints = []corev1.Taint{{Key: "maintenance", Value: "true", Effect: corev1.TaintEffectNoSchedule}}
				}
				if _, cerr := cs.CoreV1().Nodes().Create(ctx, n, metav1.CreateOptions{}); cerr != nil {
					t.Fatal(cerr)
				}
			}
			// An agent runs on gpu-1 only.
			agent := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "nvsnap-agent-g", Namespace: a.criuFailedNamespace(), Labels: map[string]string{"app": "nvsnap-agent"}},
				Spec: corev1.PodSpec{NodeName: "gpu-1"}}
			if _, cerr := cs.CoreV1().Pods(agent.Namespace).Create(ctx, agent, metav1.CreateOptions{}); cerr != nil {
				t.Fatal(cerr)
			}
			leader := withLibrary(admittedPod(0, 1, time.Second))
			leader.Spec.NodeName = "gpu-1"
			leader.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "engine", UID: "sts", Controller: &isController}}
			for _, p := range append([]*corev1.Pod{leader}, tc.pods...) {
				if _, cerr := cs.CoreV1().Pods("fn").Create(ctx, p, metav1.CreateOptions{}); cerr != nil {
					t.Fatal(cerr)
				}
			}
			agentLists := 0
			cs.PrependReactor("list", "pods", func(act k8stesting.Action) (bool, runtime.Object, error) {
				if l, ok := act.(k8stesting.ListAction); ok && strings.Contains(l.GetListRestrictions().Labels.String(), "nvsnap-agent") {
					agentLists++
				}
				return false, nil, nil
			})
			refused := map[string]string{"outcome": metrics.CaptureRefused, "reason": "no_agent"}
			before := metricValue(t, metrics.CaptureOutcomes, refused)
			var logs strings.Builder
			lg := logrus.New()
			lg.SetOutput(&logs)
			a.log = lg

			c := &criuAutoRestorer{a: a}
			if tc.waited > 0 {
				c.agentWaitSince.Store(leader.UID, time.Now().Add(-tc.waited))
			}
			retry := c.captureGroup(ctx, leader, "", 1)
			if n := cpuAsked.Load(); n != 0 {
				t.Errorf("a pod on the agentless node was asked for its connections %d time(s)", n)
			}
			counted := metricValue(t, metrics.CaptureOutcomes, refused) - before
			if tc.wantRefused {
				if retry {
					t.Fatalf("the capture is left to be tried again; the instance's held pods would wait on it\n%s", logs.String())
				}
				if agentLists != 1 {
					t.Errorf("the node agents were listed %d time(s); want 1 (discovery), none waiting on them (agentsSettled)", agentLists)
				}
				if counted != 1 {
					t.Errorf("refusals counted with reason no_agent: %v, want 1\n%s", counted, logs.String())
				}
				cms, err := cs.CoreV1().ConfigMaps(a.criuFailedNamespace()).List(ctx, metav1.ListOptions{})
				if err != nil {
					t.Fatal(err)
				}
				for _, cm := range cms.Items {
					if strings.HasPrefix(cm.Name, "nvsnap-criu-group-") {
						t.Errorf("a group record was written (%s): the refusal must hold for this instance only", cm.Name)
					}
				}
			} else if counted != 0 || strings.Contains(logs.String(), "no nvsnap agent") {
				t.Errorf("the instance was refused\n%s", logs.String())
			}
			if strings.Contains(tc.name, "not running yet") && !retry {
				t.Errorf("a capture waiting for an agent being replaced was settled\n%s", logs.String())
			}
		})
	}
}

// A node agent that does not answer cannot stall the capture that asks it
// for a pod's connections.
func TestFetchPeers_Bounded(t *testing.T) {
	saved := peersTimeout
	peersTimeout = 100 * time.Millisecond
	t.Cleanup(func() { peersTimeout = saved })
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	defer srv.Close()
	defer close(release)
	start := time.Now()
	if _, err := fetchPeers(context.Background(), srv.URL, "fn", "etcd", "etcd", false); err == nil {
		t.Fatal("a peer agent that never answered gave no error")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("the ask took %s, want about peersTimeout", d)
	}
}

// A companion that started before its version reached this node (an
// agent rollout, a failed fetch) keeps getting it fetched while it runs:
// a failed fetch is retried on a later event, and the version is mounted
// for it once fetched. A running pod that mounts no companion directory
// starts no fetch.
func TestEnsurePlaceholderBundles_RunningCompanionRecovers(t *testing.T) {
	attached, _ := stubMounts(t)
	a := bundleAgent(t)
	a.config.NodeName = "g1"
	fails := 1
	fetches := make(chan string, 4)
	c := &criuAutoRestorer{a: a, fetchBundle: func(_ context.Context, id string) error {
		fetches <- id
		if fails > 0 {
			fails--
			return errors.New("no peer has it yet")
		}
		stagedVersion(t, a, id)
		return nil
	}}
	orCreate := corev1.HostPathDirectoryOrCreate
	running := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "fn"},
		Spec: corev1.PodSpec{NodeName: "g1", Volumes: []corev1.Volume{{Name: "nvsnap-criu-bundle", VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{Path: "/var/lib/containerd/nvsnap-bundle/" + webhook.BundleCompanionDir + "/v9", Type: &orCreate}}}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	wait := func() {
		t.Helper()
		select {
		case <-fetches:
		case <-time.After(5 * time.Second):
			t.Fatal("no fetch for the running companion's version")
		}
		// the fetch goroutine finishes (and forgets its attempt) after it
		for i := 0; i < 100; i++ {
			if _, busy := c.attempted.Load("bundle/v9"); !busy {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("the fetch attempt was not released")
	}
	c.ensurePlaceholderBundles(context.Background(), running)
	wait() // fails
	c.ensurePlaceholderBundles(context.Background(), running)
	wait() // succeeds
	if *attached != 1 {
		t.Errorf("attached %d time(s) after the version was fetched, want 1", *attached)
	}

	gpu := running.DeepCopy()
	gpu.Spec.Volumes[0].HostPath.Path = "/var/lib/containerd/nvsnap-bundle/" + webhook.BundleVersionsDir + "/v7"
	c.ensurePlaceholderBundles(context.Background(), gpu)
	select {
	case id := <-fetches:
		t.Errorf("a running pod that mounts no companion directory fetched %s", id)
	case <-time.After(200 * time.Millisecond):
	}
}

// The capture waits, for a bounded time, only while every blocking finding
// is a companion's missing node bundle; after that, or with any other
// block, it decides.
func TestCompanionBundlesPending(t *testing.T) {
	c := &criuAutoRestorer{a: bundleAgent(t)}
	leader := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "engine-0", UID: "l1"}}
	companions := []*corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "etcd"}}}
	bundle := PreflightFinding{Pod: "etcd", Severity: preflightBlock, Check: "bundle"}
	t0 := time.Now()
	if !c.companionBundlesPending(leader, companions, []PreflightFinding{bundle}, t0) {
		t.Error("a companion's missing bundle is not waited for")
	}
	if !c.companionBundlesPending(leader, companions, []PreflightFinding{bundle}, t0.Add(companionBundleWait-time.Second)) {
		t.Error("the wait ended early")
	}
	if c.companionBundlesPending(leader, companions, []PreflightFinding{bundle}, t0.Add(companionBundleWait)) {
		t.Error("the wait did not end at companionBundleWait")
	}
	other := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "engine-1", UID: "l2"}}
	for name, fs := range map[string][]PreflightFinding{
		"another block":        {{Pod: "etcd", Severity: preflightBlock, Check: "liveness"}},
		"a bundle and another": {bundle, {Pod: "engine-1", Severity: preflightBlock, Check: "rdma"}},
		"a rank's bundle":      {{Pod: "engine-1", Severity: preflightBlock, Check: "bundle"}},
		"nothing blocking":     nil,
	} {
		if c.companionBundlesPending(other, companions, fs, t0) {
			t.Errorf("%s: waited", name)
		}
	}
}

// A companion's restore placeholder names its version in its mount table:
// the companion directory it mounts at /criu-bundle, whatever filesystem
// holds the bundle root.
func TestCompanionToolsVersion(t *testing.T) {
	for name, tc := range map[string]struct {
		mountinfo, want string
	}{
		"bundle root on the root filesystem": {"30 25 259:1 /var/lib/containerd/nvsnap-bundle/companion/versions/v0.2.91-mv230 /criu-bundle ro,relatime - ext4 /dev/root rw\n", "v0.2.91-mv230"},
		"bundle root on its own filesystem":  {"30 25 259:2 /nvsnap-bundle/companion/versions/v9 /criu-bundle ro,relatime - xfs /dev/nvme1n1 rw\n", "v9"},
		"a GPU pod's staged version":         {"30 25 259:1 /var/lib/containerd/nvsnap-bundle/versions/v9 /criu-bundle ro - ext4 /dev/root rw\n", ""},
		"the companion directory elsewhere":  {"30 25 259:1 /x/companion/versions/v9 /data ro - ext4 /dev/root rw\n", ""},
		"an invalid version":                 {"30 25 259:1 /b/companion/versions/..x /criu-bundle ro - ext4 /dev/root rw\n", ""},
	} {
		if got := companionToolsVersion(readMountinfoFrom(t, tc.mountinfo)); got != tc.want {
			t.Errorf("%s: %q, want %q", name, got, tc.want)
		}
	}
}

// startInMountNS runs a stand-in workload, sleep under unshare with args,
// and returns its pid once it is in a mount namespace of its own. Where the
// kernel refuses unshare (a container without CAP_SYS_ADMIN, or user
// namespaces turned off), the test is skipped; any other early exit fails it.
func startInMountNS(t *testing.T, args ...string) int {
	t.Helper()
	var stderr strings.Builder
	//nolint:gosec // args are this file's constant unshare flags
	cmd := exec.CommandContext(context.Background(), "unshare", append(args, "sleep", "60")...)
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var waitErr error
	exited := make(chan struct{}) // closed once waitErr is set
	go func() { waitErr = cmd.Wait(); close(exited) }()
	t.Cleanup(func() { _ = cmd.Process.Kill(); <-exited })
	nsPath := fmt.Sprintf("/proc/%d/ns/mnt", cmd.Process.Pid)
	self, _ := os.Readlink("/proc/self/ns/mnt")
	deadline := time.After(10 * time.Second)
	for {
		if ns, _ := os.Readlink(nsPath); ns != "" && ns != self {
			return cmd.Process.Pid
		}
		select {
		case <-exited:
			if strings.Contains(stderr.String(), "Operation not permitted") {
				t.Skipf("the kernel refuses unshare %v here: %s", args, strings.TrimSpace(stderr.String()))
			}
			t.Fatalf("unshare %v exited before entering its mount namespace: %v: %s", args, waitErr, strings.TrimSpace(stderr.String()))
		case <-deadline:
			t.Fatalf("unshare %v did not enter a mount namespace of its own", args)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// A companion's mount namespace gets the version mounted read-only at its
// tools path; the agent's own namespace does not. Run as root, or in a
// user namespace: unshare -Urm go test -run TestInjectReadOnly_Kernel.
func TestInjectReadOnly_Kernel(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root to mount")
	}
	if _, err := exec.LookPath("unshare"); err != nil {
		t.Skip("needs unshare to make the workload's mount namespace")
	}
	src, dst := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "criu"), []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
	pod := startInMountNS(t, "-m", "--propagation", "private")
	nsPath := fmt.Sprintf("/proc/%d/ns/mnt", pod)
	if err := injectReadOnly(src, nsPath, dst); err != nil {
		if errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EPERM) {
			t.Skipf("the mount API is not available here: %v", err)
		}
		t.Fatal(err)
	}
	inPod := fmt.Sprintf("/proc/%d/root%s", pod, dst)
	if _, err := os.Stat(filepath.Join(inPod, "criu")); err != nil {
		t.Fatalf("the workload does not see the version: %v", err)
	}
	if err := os.WriteFile(filepath.Join(inPod, "x"), nil, 0o600); !errors.Is(err, syscall.EROFS) {
		t.Errorf("a write in the workload's mount = %v, want EROFS", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "criu")); err == nil {
		t.Error("the mount leaked into the agent's mount namespace")
	}

	// A mount namespace the workload owns through a user namespace of its
	// own is refused: there it could make the mount writable.
	own := startInMountNS(t, "-U", "-r", "-m", "--propagation", "private")
	ownNS := fmt.Sprintf("/proc/%d/ns/mnt", own)
	dst2 := t.TempDir()
	if err := injectReadOnly(src, ownNS, dst2); err == nil || !strings.Contains(err.Error(), "user namespace") {
		t.Errorf("injecting into a mount namespace the workload owns = %v, want refused", err)
	}
	if _, err := os.Stat(fmt.Sprintf("/proc/%d/root%s/criu", own, dst2)); err == nil {
		t.Error("the version was mounted into a mount namespace the workload owns")
	}
}

// The thread that attached the version returns to its own mount namespace,
// whichever thread it is: the runtime parks a locked main thread rather
// than end it, and one left in a workload's namespace would hold it for the
// agent's life and show it at /proc/self. Run as root, or in a container
// with CAP_SYS_ADMIN.
func TestAttachInNamespace_ReturnsToOwnNamespace(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root to mount")
	}
	src, dst := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "criu"), []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
	pod := startInMountNS(t, "-m", "--propagation", "private")
	type result struct {
		before, after string
		err           error
	}
	done := make(chan result, 1)
	go func() {
		goruntime.LockOSThread() // never unlocked, as in injectReadOnly
		var r result
		r.before, _ = os.Readlink("/proc/thread-self/ns/mnt")
		fd, err := unix.OpenTree(unix.AT_FDCWD, src, unix.OPEN_TREE_CLONE|unix.OPEN_TREE_CLOEXEC)
		if err != nil {
			r.err = err
			done <- r
			return
		}
		defer func() { _ = unix.Close(fd) }()
		ns, err := unix.Open(fmt.Sprintf("/proc/%d/ns/mnt", pod), unix.O_RDONLY|unix.O_CLOEXEC, 0)
		if err != nil {
			r.err = err
			done <- r
			return
		}
		defer func() { _ = unix.Close(ns) }()
		r.err = attachInNamespace(ns, fd, dst)
		r.after, _ = os.Readlink("/proc/thread-self/ns/mnt")
		done <- r
	}()
	r := <-done
	if r.err != nil {
		if errors.Is(r.err, syscall.ENOSYS) || errors.Is(r.err, syscall.EPERM) {
			t.Skipf("the mount API is not available here: %v", r.err)
		}
		t.Fatal(r.err)
	}
	if r.after != r.before {
		t.Errorf("the attaching thread is left in %s, want its own %s", r.after, r.before)
	}
	if _, err := os.Stat(fmt.Sprintf("/proc/%d/root%s/criu", pod, dst)); err != nil {
		t.Errorf("the workload does not see the version: %v", err)
	}
}

// A companion that does not see its version has it mounted into its own
// mount namespace; one that sees it, a GPU pod, and a privileged workload
// are left as they are. A version not staged here is not fetched here.
func TestEnsureCompanionToolsIn(t *testing.T) {
	const id = "criu-bundle" // the fake root is the versions directory, as in TestBundleToolsOf
	companion := "30 22 259:1 /var/lib/containerd/nvsnap-bundle/companion/versions/" + id + " /criu-bundle ro,relatime - ext4 /dev/x rw\n"
	gpuPod := "30 22 259:1 /var/lib/containerd/nvsnap-bundle/versions/" + id + " /criu-bundle ro,relatime - ext4 /dev/x rw\n"
	for name, tc := range map[string]struct {
		mountinfo, caps string
		seesIt, staged  bool
		wantInject      bool
		wantErr         bool
		ownUserNS       bool
	}{
		"a companion without its version, staged here":      {companion, capsWorker, false, true, true, false, false},
		"a companion whose version is not on the node":      {companion, capsWorker, false, false, false, true, false},
		"a companion that sees its version":                 {companion, capsWorker, true, true, false, false, false},
		"a GPU pod":                                         {gpuPod, capsWorker, false, true, false, false, false},
		"a privileged companion (bundleToolsOf refuses it)": {companion, capsAdmin, false, true, false, false, false},
		"a companion in a user namespace of its own":        {companion, capsWorker, false, true, false, false, true},
	} {
		t.Run(name, func(t *testing.T) {
			_, _ = stubMounts(t)
			a := bundleAgent(t)
			if tc.staged {
				stagedVersion(t, a, id)
			} else if tc.seesIt {
				t.Fatal("bad case")
			}
			savedI := injectReadOnly
			t.Cleanup(func() { injectReadOnly = savedI })
			var injected []string
			injectReadOnly = func(src, nsPath, dst string) error {
				injected = append(injected, src+" "+filepath.Base(filepath.Dir(filepath.Dir(nsPath)))+" "+dst)
				return nil
			}
			root := t.TempDir() // a /criu-bundle that is not a version
			if err := os.MkdirAll(filepath.Join(root, "criu-bundle"), 0o755); err != nil {
				t.Fatal(err)
			}
			if tc.seesIt {
				root = a.bundleVersionsDir()
			}
			proc := fakeWorkload(t, root, tc.mountinfo, tc.caps)
			if err := os.MkdirAll(filepath.Join(proc, "7", "ns"), 0o755); err != nil {
				t.Fatal(err)
			}
			userNS := filepath.Join(proc, "7", "ns", "user")
			if tc.ownUserNS {
				err := os.WriteFile(userNS, nil, 0o644) // not the agent's user namespace
				if err != nil {
					t.Fatal(err)
				}
			} else if err := os.Symlink("/proc/self/ns/user", userNS); err != nil {
				t.Fatal(err)
			}
			err := a.ensureCompanionToolsIn(proc, 7)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, want error %v", err, tc.wantErr)
			}
			want := []string(nil)
			if tc.wantInject {
				want = []string{filepath.Join(a.bundleVersionsDir(), id) + " 7 /criu-bundle"}
			}
			if fmt.Sprint(injected) != fmt.Sprint(want) {
				t.Errorf("injected %v, want %v", injected, want)
			}
		})
	}
}

// A node with a NoSchedule or NoExecute taint the agent does not tolerate
// gets no agent; the DaemonSet's own tolerations count.
func TestAgentTolerates(t *testing.T) {
	gpuTaint := corev1.Taint{Key: "nvidia.com/gpu", Value: "present", Effect: corev1.TaintEffectNoSchedule}
	tolerateGPU := []corev1.Toleration{{Key: "nvidia.com/gpu", Operator: corev1.TolerationOpExists}}
	for name, tc := range map[string]struct {
		tolerations []corev1.Toleration
		taints      []corev1.Taint
		want        bool
	}{
		"no taints":                       {nil, nil, true},
		"a tolerated taint":               {tolerateGPU, []corev1.Taint{gpuTaint}, true},
		"an untolerated NoSchedule taint": {nil, []corev1.Taint{gpuTaint}, false},
		"an untolerated NoExecute taint":  {tolerateGPU, []corev1.Taint{{Key: "drain", Effect: corev1.TaintEffectNoExecute}}, false},
		"PreferNoSchedule":                {nil, []corev1.Taint{{Key: "x", Effect: corev1.TaintEffectPreferNoSchedule}}, true},
		"a cordon (the DaemonSet's own)":  {nil, []corev1.Taint{{Key: "node.kubernetes.io/unschedulable", Effect: corev1.TaintEffectNoSchedule}}, true},
		"not ready (the DaemonSet's own)": {nil, []corev1.Taint{{Key: "node.kubernetes.io/not-ready", Effect: corev1.TaintEffectNoExecute}}, true},
	} {
		n := &corev1.Node{Spec: corev1.NodeSpec{Taints: tc.taints}}
		if got := agentTolerates(tc.tolerations, n); got != tc.want {
			t.Errorf("%s: %v, want %v", name, got, tc.want)
		}
	}
}
