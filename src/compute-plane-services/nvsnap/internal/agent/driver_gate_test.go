// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

func TestParseDriverMajor(t *testing.T) {
	for _, tc := range []struct {
		text string
		want int
	}{
		{"NVRM version: NVIDIA UNIX Open Kernel Module for aarch64  610.57.04  Release Build  (dvs-builder@U16-I1-N08-12-2)  Thu Sep 25 2026\nGCC version:  gcc version 13.3.0", 610},
		{"NVRM version: NVIDIA UNIX x86_64 Kernel Module  580.95.05  Tue Sep 23 18:30:17 UTC 2025\n", 580},
		{"NVRM version: NVIDIA UNIX Open Kernel Module for x86_64  580.126.16  Release Build", 580},
		{"NVRM version: NVIDIA UNIX Open Kernel Module for aarch64  1010.2  Release Build", 1010},
		{"GCC version:  gcc version 13.3.0 (Ubuntu 13.3.0-6ubuntu2~24.04)\n", 0},
		{"", 0},
		{"NVRM version: garbled", 0},
	} {
		if got := parseDriverMajor(tc.text); got != tc.want {
			t.Errorf("parseDriverMajor(%q) = %d, want %d", tc.text, got, tc.want)
		}
	}
}

func TestClusterDriverGate(t *testing.T) {
	for _, tc := range []struct {
		majors []int
		major  int
		gated  string // a word the reason has; "" for no gate
	}{
		{nil, 0, ""},             // no agent reports: as before
		{[]int{610}, 610, ""},    // one major, new enough
		{[]int{615}, 615, ""},    // newer
		{[]int{580}, 580, "580"}, // too old
		{[]int{580, 610}, 0, "majors 580, 610"},
		{[]int{610, 615}, 0, "majors 610, 615"}, // both new enough, but not one
	} {
		d := clusterDriver{Majors: tc.majors}
		if d.Major() != tc.major {
			t.Errorf("%v: Major %d, want %d", tc.majors, d.Major(), tc.major)
		}
		why := d.Gate(DefaultCRIUMinDriverMajor)
		if (why == "") != (tc.gated == "") || !strings.Contains(why, tc.gated) {
			t.Errorf("%v: Gate %q, want one naming %q", tc.majors, why, tc.gated)
		}
	}
	if why := (clusterDriver{Majors: []int{580}}).Gate(580); why != "" {
		t.Errorf("a minimum lowered to 580 still gates 580: %q", why)
	}
}

// driverAgentPod is an agent pod on node with driver major (0: none
// reported).
func driverAgentPod(t *testing.T, cs *fake.Clientset, ns, node string, major int) {
	t.Helper()
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "nvsnap-agent-" + node, Namespace: ns,
		Labels: map[string]string{"app": "nvsnap-agent"}}, Spec: corev1.PodSpec{NodeName: node}}
	if major > 0 {
		p.Annotations = map[string]string{DriverMajorAnnotation: strconv.Itoa(major)}
	}
	if _, err := cs.CoreV1().Pods(ns).Create(context.Background(), p, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func noServiceAccount(t *testing.T) {
	t.Helper()
	old := serviceAccountNS
	serviceAccountNS = filepath.Join(t.TempDir(), "none")
	t.Cleanup(func() { serviceAccountNS = old })
}

// The view is the distinct majors of the agents that report one; a pod
// being deleted and an agent that reports none do not count.
func TestClusterDriverReadsTheAgents(t *testing.T) {
	noServiceAccount(t)
	cs := fake.NewClientset()
	a := &Agent{kubeClient: cs, log: logrus.New()}
	ns := a.criuFailedNamespace()
	driverAgentPod(t, cs, ns, "gpu-1", 610)
	driverAgentPod(t, cs, ns, "gpu-2", 610)
	driverAgentPod(t, cs, ns, "cpu-1", 0)
	gone := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "nvsnap-agent-old", Namespace: ns, Labels: map[string]string{"app": "nvsnap-agent"},
		Annotations: map[string]string{DriverMajorAnnotation: "580"}, DeletionTimestamp: &metav1.Time{Time: time.Now()}, Finalizers: []string{"x"}}}
	if _, err := cs.CoreV1().Pods(ns).Create(context.Background(), gone, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if d := a.clusterDriver(ctx); d.Major() != 610 || len(d.Majors) != 1 {
		t.Fatalf("view %v, want [610]", d.Majors)
	}
	// Cached: a node joining with 580 is seen once the view expires.
	driverAgentPod(t, cs, ns, "gpu-3", 580)
	if d := a.clusterDriver(ctx); d.Major() != 610 {
		t.Fatalf("the view was read again within its TTL: %v", d.Majors)
	}
	a.drivers.at = time.Now().Add(-driverViewTTL - time.Second)
	if why := a.driverGate(ctx); !strings.Contains(why, "580, 610") {
		t.Fatalf("after the TTL the gate is %q, want the two majors", why)
	}
}

// An agent stamps its node's driver on its own pod only.
func TestReportDriver(t *testing.T) {
	noServiceAccount(t)
	dir := t.TempDir()
	old := nvidiaVersionFile
	nvidiaVersionFile = filepath.Join(dir, "version")
	t.Cleanup(func() { nvidiaVersionFile = old })
	// A node the GPU labels do not mark yet, with no driver file.
	cs := fake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "gpu-1"}})
	a := &Agent{kubeClient: cs, log: logrus.New(), config: Config{NodeName: "gpu-1"}}
	ns := a.criuFailedNamespace()
	driverAgentPod(t, cs, ns, "gpu-1", 0)
	driverAgentPod(t, cs, ns, "gpu-2", 0)
	ctx := context.Background()

	// No driver on the node (no file, no label): nothing to stamp.
	if done, err := a.reportDriver(ctx); !done || err != nil {
		t.Fatalf("no driver: done %v err %v", done, err)
	}
	if err := os.WriteFile(nvidiaVersionFile, []byte("NVRM version: NVIDIA UNIX Open Kernel Module for aarch64  610.57.04  Release Build\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if done, err := a.reportDriver(ctx); !done || err != nil {
		t.Fatalf("done %v err %v", done, err)
	}
	for node, want := range map[string]string{"gpu-1": "610", "gpu-2": ""} {
		p, err := cs.CoreV1().Pods(ns).Get(ctx, "nvsnap-agent-"+node, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got := p.Annotations[DriverMajorAnnotation]; got != want {
			t.Errorf("%s's agent pod reports %q, want %q", node, got, want)
		}
	}
	// Its pod not created yet (its node read fails here too): tried again.
	a.config.NodeName = "gpu-9"
	if done, err := a.reportDriver(ctx); done || err == nil {
		t.Fatalf("no pod of its own: done %v err %v, want a retry", done, err)
	}
}

// The driver major is in the key; without one the key is the old one.
func TestCRIUGroupKeyDriver(t *testing.T) {
	base := criuGroupKey("cache://cfg", "function-version/fv")
	if criuGroupKeyDriver("cache://cfg", "function-version/fv", 0) != base {
		t.Fatal("no driver changed the key")
	}
	k610 := criuGroupKeyDriver("cache://cfg", "function-version/fv", 610)
	if k610 == base || k610 == criuGroupKeyDriver("cache://cfg", "function-version/fv", 615) {
		t.Fatal("the driver major does not tell keys apart")
	}
	if k610 == criuGroupKeyDriver("cache://cfg", "function-version/other", 610) {
		t.Fatal("two tenant scopes share a key")
	}
}

// Gated, a configuration falls back with or without a record, and its
// complete capture is not restored, for ranks and companions alike.
func TestDriverGateAdmission(t *testing.T) {
	noServiceAccount(t)
	a, cs := fallbackAgent(t)
	ctx := context.Background()
	scope := fnScope(t, a)
	ns := a.criuFailedNamespace()

	// One major, new enough: a complete capture keyed with it restores,
	// and a configuration without a record does not fall back.
	driverAgentPod(t, cs, ns, "gpu-1", 610)
	key := criuGroupKeyDriver("cache://cfg", scope, 610)
	rec := record(criuGroupConfigMapName(key), map[string]string{"state": "complete", "size": "1", "checkpoint.0": "c0", recordDriverKey: "610"})
	if _, err := cs.CoreV1().ConfigMaps(ns).Create(ctx, &rec, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if g, ok := a.lookupCRIUGroupFor(ctx, "fn", "cache://cfg"); !ok || g.Key != key {
		t.Fatalf("driver 610: lookup %+v %v, want the capture keyed with 610", g, ok)
	}
	if fb, ok := a.criuFallbackFor(ctx, "fn", "cache://cfg", 0, ""); ok {
		t.Fatalf("driver 610: falls back %+v", fb)
	}

	// A 580 node joins: no restore, and the configuration falls back with
	// the driver reason though its record is complete; one without a
	// record falls back too.
	driverAgentPod(t, cs, ns, "gpu-2", 580)
	a.drivers.at = time.Time{}
	if _, ok := a.lookupCRIUGroupFor(ctx, "fn", "cache://cfg"); ok {
		t.Fatal("a capture restores while a GPU node runs 580")
	}
	for _, uri := range []string{"cache://cfg", "cache://never-captured"} {
		fb, ok := a.criuFallbackFor(ctx, "fn", uri, 0, "")
		if !ok || fb.Reason != webhook.FallbackDriver {
			t.Fatalf("%s with a 580 node: %+v %v, want the driver fallback", uri, fb, ok)
		}
	}

	// Every node on 580: gated as well.
	if err := cs.CoreV1().Pods(ns).Delete(ctx, "nvsnap-agent-gpu-1", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	a.drivers.at = time.Time{}
	if fb, ok := a.criuFallbackFor(ctx, "fn", "cache://cfg", 0, ""); !ok || fb.Reason != webhook.FallbackDriver {
		t.Fatalf("all on 580: %+v %v", fb, ok)
	}
	// ... unless the minimum is lowered to it.
	a.config.CRIUMinDriverMajor = 580
	if fb, ok := a.criuFallbackFor(ctx, "fn", "cache://cfg", 0, ""); ok {
		t.Fatalf("minimum 580, all on 580: falls back %+v", fb)
	}
	// The 610 capture is not restored under 580: its key names 610.
	if g, ok := a.lookupCRIUGroupFor(ctx, "fn", "cache://cfg"); ok {
		t.Fatalf("a 610 capture is restored under 580: %+v", g)
	}
}

// A pod admitted under one driver major restores after the cluster's has
// moved on: the trust check takes the record's driver. Another tenant's
// key is still refused.
func TestGroupKeyRefusalTakesTheRecordsDriver(t *testing.T) {
	noServiceAccount(t)
	a, cs := fallbackAgent(t)
	ctx := context.Background()
	scope := fnScope(t, a)
	ns := a.criuFailedNamespace()
	key := criuGroupKeyDriver("cache://cfg", scope, 610)
	rec := record(criuGroupConfigMapName(key), map[string]string{"state": "complete", "size": "1", "checkpoint.0": "c0", recordDriverKey: "610"})
	if _, err := cs.CoreV1().ConfigMaps(ns).Create(ctx, &rec, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	a.seedDrivers(615)
	c := &criuAutoRestorer{a: a}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "fn", Annotations: map[string]string{cacheURIAnnotation: "cache://cfg"}}}
	if why, err := c.groupKeyRefusal(ctx, pod, key); err != nil || why != "" {
		t.Fatalf("admitted under 610, the cluster now on 615: refused %q %v", why, err)
	}
	other := criuGroupKeyDriver("cache://cfg", "function-version/someone-else", 610)
	orec := record(criuGroupConfigMapName(other), map[string]string{"state": "complete", "size": "1", "checkpoint.0": "c0", recordDriverKey: "610"})
	if _, err := cs.CoreV1().ConfigMaps(ns).Create(ctx, &orec, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if why, err := c.groupKeyRefusal(ctx, pod, other); err != nil || why == "" {
		t.Fatalf("another tenant's key was accepted (%q %v)", why, err)
	}
}

// A capture claims its record with the driver its key was made with.
func TestClaimGroupRecordKeepsTheDriver(t *testing.T) {
	a, cs := fallbackAgent(t)
	c := &criuAutoRestorer{a: a}
	leader := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "l", Namespace: "fn"}}
	rec, err := c.claimGroupRecord(context.Background(), "k610", 610, leader, 2)
	if err != nil || rec == nil {
		t.Fatalf("claim: %v %v", rec, err)
	}
	got, err := cs.CoreV1().ConfigMaps(a.criuFailedNamespace()).Get(context.Background(), criuGroupConfigMapName("k610"), metav1.GetOptions{})
	if err != nil || recordDriver(got.Data) != 610 {
		t.Fatalf("record %v %v, want driver 610", got.Data, err)
	}
	if rec, err := c.claimGroupRecord(context.Background(), "k0", 0, leader, 2); err != nil || rec.Data[recordDriverKey] != "" {
		t.Fatalf("no driver: %v %v", rec, err)
	}
}

// While the drivers keep CRIU off, the cachedir records the driver
// fallback seeds from have no group record, and are kept; once CRIU
// serves again, they go as before.
func TestReaperKeepsDriverFallbackRecords(t *testing.T) {
	noServiceAccount(t)
	a, cs := fallbackAgent(t, record(cachedirRecordName("k580"), map[string]string{"rank.0": "complete"}))
	ctx := context.Background()
	a.seedDrivers(580)
	if _, err := a.retirableCachedirCaptures(ctx, nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.CoreV1().ConfigMaps(a.criuFailedNamespace()).Get(ctx, cachedirRecordName("k580"), metav1.GetOptions{}); err != nil {
		t.Fatalf("a driver fallback's cachedir record was deleted under the gate: %v", err)
	}
	a.seedDrivers(610)
	if _, err := a.retirableCachedirCaptures(ctx, nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.CoreV1().ConfigMaps(a.criuFailedNamespace()).Get(ctx, cachedirRecordName("k580"), metav1.GetOptions{}); err == nil {
		t.Fatal("with CRIU serving again, a cachedir record without a group record was kept")
	}
}

// A GPU node whose driver is not loaded yet keeps trying; a node without
// GPUs reports none and stops.
func TestReportDriverWaitsForTheDriver(t *testing.T) {
	noServiceAccount(t)
	old := nvidiaVersionFile
	nvidiaVersionFile = filepath.Join(t.TempDir(), "version")
	t.Cleanup(func() { nvidiaVersionFile = old })
	ctx := context.Background()
	gpu := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "gpu-1"}}
	gpu.Status.Capacity = corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("4")}
	cpu := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "cpu-1"}}
	// Marked by Node Feature Discovery only, before the device plugin.
	nfd := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "gpu-nfd", Labels: map[string]string{"feature.node.kubernetes.io/pci-10de.present": "true"}}}
	cs := fake.NewClientset(gpu, cpu, nfd)
	a := &Agent{kubeClient: cs, log: logrus.New(), config: Config{NodeName: "gpu-1"}}
	driverAgentPod(t, cs, a.criuFailedNamespace(), "gpu-1", 0)
	if done, err := a.reportDriver(ctx); done || err == nil {
		t.Fatalf("a GPU node without its driver yet: done %v err %v, want a retry", done, err)
	}
	a.config.NodeName = "gpu-nfd"
	if done, err := a.reportDriver(ctx); done || err == nil {
		t.Fatalf("a node NFD marks as GPU, no driver yet: done %v err %v, want a retry", done, err)
	}
	a.config.NodeName = "cpu-1"
	if done, err := a.reportDriver(ctx); !done || err != nil {
		t.Fatalf("a node without GPUs: done %v err %v", done, err)
	}
	// The driver loads: reported.
	a.config.NodeName = "gpu-1"
	if err := os.WriteFile(nvidiaVersionFile, []byte("NVRM version: NVIDIA UNIX Open Kernel Module for aarch64  580.95.05  Release Build\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if done, err := a.reportDriver(ctx); !done || err != nil {
		t.Fatalf("driver loaded: done %v err %v", done, err)
	}
	if a.ownDriver.Load() != 580 {
		t.Fatalf("own driver %d, want 580", a.ownDriver.Load())
	}
}

// The agent's own driver counts before its stamp is read, and when the
// agents cannot be listed; a failed list is not retried per admission.
func TestClusterDriverOwnDriverAndListFailure(t *testing.T) {
	noServiceAccount(t)
	cs := fake.NewClientset()
	a := &Agent{kubeClient: cs, log: logrus.New()}
	driverAgentPod(t, cs, a.criuFailedNamespace(), "gpu-2", 610)
	a.ownDriver.Store(580)
	ctx := context.Background()
	if why := a.driverGate(ctx); !strings.Contains(why, "580, 610") {
		t.Fatalf("own 580 beside a reported 610: gate %q", why)
	}
	lists := 0
	cs.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		lists++
		return true, nil, errors.New("injected: API down")
	})
	a.drivers = driverView{}
	for range 3 {
		if d := a.clusterDriver(ctx); d.Major() != 580 {
			t.Fatalf("list failing: view %v, want this node's 580", d.Majors)
		}
	}
	if lists != 1 {
		t.Fatalf("a failed list was tried %d times within the TTL, want 1", lists)
	}
}

// A node restores only under the capture's driver; unknown on either side
// lets it.
func TestDriverMismatch(t *testing.T) {
	a, cs := fallbackAgent(t)
	ctx := context.Background()
	for key, drv := range map[string]string{"k610": "610", "knone": ""} {
		d := map[string]string{"state": "complete", "size": "1", "checkpoint.0": "c0"}
		if drv != "" {
			d[recordDriverKey] = drv
		}
		r := record(criuGroupConfigMapName(key), d)
		if _, err := cs.CoreV1().ConfigMaps(a.criuFailedNamespace()).Create(ctx, &r, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	c := &criuAutoRestorer{a: a}
	a.ownDriver.Store(615)
	if why := c.driverMismatch(ctx, "k610"); !strings.Contains(why, "615") {
		t.Fatalf("615 node, 610 capture: %q", why)
	}
	for _, key := range []string{"knone", "missing", ""} {
		if why := c.driverMismatch(ctx, key); why != "" {
			t.Errorf("key %q: %q, want none", key, why)
		}
	}
	a.ownDriver.Store(610)
	if why := c.driverMismatch(ctx, "k610"); why != "" {
		t.Fatalf("same driver: %q", why)
	}
}

// Each rank is checked on its own node: the leader's driver matching the
// capture's does not let a rank on a node of another driver through. A
// node whose driver is unknown, or a record without one, lets it.
func TestGroupDriverMismatch(t *testing.T) {
	noServiceAccount(t)
	a, cs := fallbackAgent(t)
	ctx := context.Background()
	for key, drv := range map[string]string{"k610": "610", "knone": ""} {
		d := map[string]string{"state": "complete", "size": "2", "checkpoint.0": "c0", "checkpoint.1": "c1"}
		if drv != "" {
			d[recordDriverKey] = drv
		}
		r := record(criuGroupConfigMapName(key), d)
		if _, err := cs.CoreV1().ConfigMaps(a.criuFailedNamespace()).Create(ctx, &r, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	ns := a.agentNamespace()
	driverAgentPod(t, cs, ns, "n1", 610)
	driverAgentPod(t, cs, ns, "n2", 615)
	driverAgentPod(t, cs, ns, "n3", 0)
	a.ownDriver.Store(610) // n0, this node
	c := &criuAutoRestorer{a: a}
	on := func(nodes ...string) []*corev1.Pod {
		ps := make([]*corev1.Pod, 0, len(nodes))
		for i, n := range nodes {
			ps = append(ps, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "r" + strconv.Itoa(i)}, Spec: corev1.PodSpec{NodeName: n}})
		}
		return ps
	}
	if why := c.groupDriverMismatch(ctx, "k610", on("n0", "n2")); !strings.Contains(why, "r1") || !strings.Contains(why, "615") {
		t.Fatalf("a rank on a 615 node, 610 capture: %q", why)
	}
	for name, tc := range map[string]struct {
		key   string
		ranks []*corev1.Pod
	}{
		"same driver everywhere":    {"k610", on("n0", "n1")},
		"a node reporting none":     {"k610", on("n0", "n3")},
		"a node without an agent":   {"k610", on("n0", "n9")},
		"a record without a driver": {"knone", on("n0", "n2")},
		"no record":                 {"missing", on("n0", "n2")},
		"no key":                    {"", on("n0", "n2")},
	} {
		if why := c.groupDriverMismatch(ctx, tc.key, tc.ranks); why != "" {
			t.Errorf("%s: %q, want none", name, why)
		}
	}
	// This node's own driver is its own, whatever its agent pod says.
	a.ownDriver.Store(615)
	if why := c.groupDriverMismatch(ctx, "k610", on("n0", "n1")); !strings.Contains(why, "n0") {
		t.Fatalf("this node on 615, 610 capture: %q", why)
	}
}

// A group restore whose ranks include one on a node of another driver
// than the capture's does not run: the instance starts again, its
// checkpoints stay usable, and it counts as a stall.
func TestRestoreGroup_RankOnAnotherDriver(t *testing.T) {
	noServiceAccount(t)
	c, kc, leader := groupFixture(t)
	var logs strings.Builder
	c.a.log.SetOutput(&logs)
	ctx := context.Background()
	setRecord(t, kc, map[string]string{recordDriverKey: "610"})
	for node, drv := range map[string]int{"node-kimi-0": 610, "node-kimi-1": 615} {
		driverAgentPod(t, kc, c.a.agentNamespace(), node, drv)
	}
	for _, name := range []string{"kimi-0", "kimi-1"} {
		p, err := kc.CoreV1().Pods("fn").Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		p.Annotations[webhook.CRIURestoreContainerAnnotation] = "engine"
		p.Annotations[webhook.CRIUGroupOrdinalAnnotation] = name[len(name)-1:]
		p.Annotations[webhook.CRIUGroupSizeAnnotation] = "2"
		p.Spec.NodeName = "node-" + name
		p.Status.Phase = corev1.PodRunning
		p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "engine", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
		if p, err = kc.CoreV1().Pods("fn").Update(ctx, p, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		if name == "kimi-0" {
			leader = p
		}
	}
	rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	c.restoreGroup(rctx, leader, "k1", 2)
	cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, criuGroupConfigMapName("k1"), metav1.GetOptions{})
	if cm.Data["stalls"] != "1" || !strings.Contains(logs.String(), "driver 615") {
		t.Fatalf("record = %v; want the restore ended as not run over the rank's driver\n%s", cm.Data, logs.String())
	}
	if c.Blocked(ctx, "c0") || c.Blocked(ctx, "c1") {
		t.Error("a restore that never ran blocked its checkpoints")
	}
	if got := remainingPods(t, kc); len(got) != 1 || got[0] != "other-0" {
		t.Errorf("pods left = %v, want only the other instance", got)
	}
}
