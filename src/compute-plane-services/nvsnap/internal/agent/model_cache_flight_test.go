// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/modelcache"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

// Single flight across nodes (docs/design/model-cache-single-flight.md),
// in process: every node's controller shares one fake API server and
// serves its routes over httptest.

// flightPod is a cold pod admitted with single flight: the helper's plan
// flags, the pod-uid variable and the signal volume. Its init is running.
func flightPod(t *testing.T, name, node string, uid types.UID) *corev1.Pod {
	t.Helper()
	pod := cachePod(t, name, node, uid)
	init := &pod.Spec.InitContainers[0]
	var cmd []string
	for _, a := range init.Command {
		if a == "--" && len(cmd) > 0 && cmd[len(cmd)-1] != "--" && !hasArg(cmd, "--plan-wait") {
			cmd = append(cmd, "--plan-wait", "60", "--signal", webhook.ModelCacheSignalMount)
		}
		cmd = append(cmd, a)
	}
	init.Command = cmd
	init.Env = append(init.Env, corev1.EnvVar{Name: webhook.PodUIDEnv, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}})
	init.VolumeMounts = append(init.VolumeMounts, corev1.VolumeMount{Name: webhook.ModelCacheSignalVolume, MountPath: webhook.ModelCacheSignalMount})
	limit := resource.MustParse("1Mi")
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: webhook.ModelCacheSignalVolume,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: &limit}}})
	d, ok := webhook.ResolveModelCacheDownload(context.Background(), nil, pod, 0)
	if !ok || d.Key != pod.Labels[webhook.ModelCacheKeyLabel] {
		t.Fatalf("single-flight fixture resolves to key %q ok=%v; labelled %q", d.Key, ok, pod.Labels[webhook.ModelCacheKeyLabel])
	}
	pod.Status.Phase = corev1.PodPending
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "download-ngc-model",
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}}}
	return pod
}

func hasArg(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

func newFlightNode(t *testing.T, kube *fake.Clientset, node string, urls map[string]string) *mcNode {
	t.Helper()
	n := newMCNode(t, kube, node, urls)
	n.c.singleFlight = true
	n.c.podsWrite = n.c.podsRoot
	n.c.timing = streamTiming{settle: 100 * time.Millisecond, poll: 50 * time.Millisecond, stall: 10 * time.Second}
	n.c.leaseDuration = 2 * time.Second
	n.c.client.Timeout = 5 * time.Second
	n.c.addLiveRoutes(n.router)
	return n
}

func (n *mcNode) volume(pod *corev1.Pod, vol string) string {
	return filepath.Join(n.c.podsRoot, string(pod.UID), "volumes", "kubernetes.io~empty-dir", vol)
}

func (n *mcNode) plan(pod *corev1.Pod) string {
	return modelcache.ReadPlan(n.c.store.KeyDir(keyOf(pod)), string(pod.UID))
}

func (n *mcNode) marker(pod *corev1.Pod, name string) bool {
	_, err := os.Stat(filepath.Join(n.c.store.KeyDir(keyOf(pod)), name))
	return err == nil
}

// signal is the helper's word that its download is over.
func (n *mcNode) signal(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	ents, err := modelcache.Scan(n.volume(pod, "ngc-models"), 0)
	if err != nil {
		t.Fatal(err)
	}
	dir := n.volume(pod, webhook.ModelCacheSignalVolume)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, modelcache.SignalFile), []byte(modelcache.Fingerprint(ents)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func emptyVolumes(t *testing.T, nodes map[*mcNode]*corev1.Pod) {
	t.Helper()
	for n, p := range nodes {
		if err := os.MkdirAll(n.volume(p, "ngc-models"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
}

func tickAll(nodes ...*mcNode) {
	for _, n := range nodes {
		n.c.tick(context.Background())
	}
}

func TestSingleFlightOnePodDownloadsAndTheOthersStreamItsTree(t *testing.T) {
	kube := fake.NewSimpleClientset()
	urls := map[string]string{}
	a, b, c := newFlightNode(t, kube, "a", urls), newFlightNode(t, kube, "b", urls), newFlightNode(t, kube, "c", urls)
	pa, pb, pc := flightPod(t, "pa", "a", "ua"), flightPod(t, "pb", "b", "ub"), flightPod(t, "pc", "c", "uc")
	for _, p := range []*corev1.Pod{pa, pb, pc} {
		addPod(t, kube, p)
	}
	emptyVolumes(t, map[*mcNode]*corev1.Pod{a: pa, b: pb, c: pc})
	tickAll(a, b, c)

	// The first to try holds the Lease and downloads; the others wait.
	if got := []string{a.plan(pa), b.plan(pb), c.plan(pc)}; got[0] != modelcache.PlanDownload || got[1] != modelcache.PlanWait || got[2] != modelcache.PlanWait {
		t.Fatalf("plans %q; want download, wait, wait", got)
	}
	for _, x := range []struct {
		n *mcNode
		p *corev1.Pod
	}{{b, pb}, {c, pc}} {
		if !x.n.marker(x.p, modelcache.StreamingFileOf(string(x.p.UID))) {
			t.Fatal("a waiting pod has no streaming heartbeat of its own")
		}
	}

	// a's "download" lands files over time; b and c stream them.
	files := map[string]string{"model/config.json": "{}", "model/w-1.bin": "weights one", "model/w-2.bin": "weights two"}
	a.downloadedLive(t, pa, map[string]string{"model/config.json": "{}", "model/w-1.bin": "weights one"})
	for i := 0; i < 10; i++ {
		tickAll(a, b, c)
		time.Sleep(50 * time.Millisecond)
	}
	a.downloadedLive(t, pa, map[string]string{"model/w-2.bin": "weights two"})
	a.signal(t, pa)
	mcEventually(t, "a's final manifest", func() bool { tickAll(a); return a.marker(pa, modelcache.FinalFile(string(pa.UID))) })
	for _, x := range []struct {
		n *mcNode
		p *corev1.Pod
	}{{b, pb}, {c, pc}} {
		mcEventually(t, "done on "+x.n.c.node, func() bool {
			tickAll(x.n)
			return x.n.marker(x.p, modelcache.DoneFile(string(x.p.UID)))
		})
		assertTree(t, x.n.volume(x.p, "ngc-models"), files)
	}
}

// downloadedLive writes files into the pod's volume as a running download
// does: no termination message yet.
func (n *mcNode) downloadedLive(t *testing.T, pod *corev1.Pod, files map[string]string) {
	t.Helper()
	for p, b := range files {
		write(t, n.volume(pod, "ngc-models"), p, b)
	}
}

// A pod whose init is not running yet (an image pull, an earlier init)
// takes no Lease: a holder that cannot start would hold everyone.
func TestSingleFlightElectsOnlyARunningInit(t *testing.T) {
	kube := fake.NewSimpleClientset()
	a := newFlightNode(t, kube, "a", map[string]string{})
	p := flightPod(t, "pa", "a", "ua")
	p.Status.InitContainerStatuses[0].State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}
	addPod(t, kube, p)
	emptyVolumes(t, map[*mcNode]*corev1.Pod{a: p})
	a.c.tick(context.Background())
	if _, err := kube.CoordinationV1().Leases(mcNS).Get(context.Background(), leaseName(keyOf(p)), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("a Lease for a pod whose init is not running (err %v)", err)
	}
	if a.plan(p) != "" {
		t.Fatal("planned a pod whose init is not running")
	}
}

// A pod whose volume is not empty is downloading already (its helper gave
// up waiting for a plan): it gets no plan, and nothing streams into it.
func TestSingleFlightLeavesADownloadingVolumeAlone(t *testing.T) {
	kube := fake.NewSimpleClientset()
	a := newFlightNode(t, kube, "a", map[string]string{})
	p := flightPod(t, "pa", "a", "ua")
	addPod(t, kube, p)
	write(t, a.volume(p, "ngc-models"), "partial.bin", "half")
	a.c.tick(context.Background())
	if a.plan(p) != "" {
		t.Fatalf("plan %q for a volume a download writes", a.plan(p))
	}
}

// Expiry is judged by how long this agent has seen the renew time stand
// still, not by comparing clocks across nodes.
func TestSingleFlightTakesOverALeaseOnlyAfterSeeingItStandStill(t *testing.T) {
	kube := fake.NewSimpleClientset()
	b := newFlightNode(t, kube, "b", map[string]string{})
	key := "0123456789abcdef0123456789abcdef"
	dur := int32(2)
	old := metav1.NewMicroTime(time.Now().Add(-time.Hour)) // far behind on b's clock
	holder := "a/ua"
	if _, err := kube.CoordinationV1().Leases(mcNS).Create(context.Background(), &coordv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: leaseName(key), Namespace: mcNS},
		Spec:       coordv1.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &dur, RenewTime: &old}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if got, by, err := b.c.acquire(context.Background(), key, "b/ub"); err != nil || got || by != holder {
		t.Fatalf("first look: got=%v by=%q err=%v; want held by a", got, by, err)
	}
	// Its renew time is an hour old on b's clock, but b has only just seen it.
	if got, _, _ := b.c.acquire(context.Background(), key, "b/ub"); got {
		t.Fatal("took over a Lease it had seen for no time")
	}
	time.Sleep(2500 * time.Millisecond)
	if got, _, err := b.c.acquire(context.Background(), key, "b/ub"); err != nil || !got {
		t.Fatalf("after the renew time stood still past the duration: got=%v err=%v", got, err)
	}
}

func TestSingleFlightReleaseDoesNotDeleteASuccessorsLease(t *testing.T) {
	kube := fake.NewSimpleClientset()
	a := newFlightNode(t, kube, "a", map[string]string{})
	key := "0123456789abcdef0123456789abcdef"
	if got, _, err := a.c.acquire(context.Background(), key, "a/ua"); err != nil || !got {
		t.Fatalf("acquire: %v %v", got, err)
	}
	// Taken over while a was away.
	l, _ := kube.CoordinationV1().Leases(mcNS).Get(context.Background(), leaseName(key), metav1.GetOptions{})
	other := "b/ub"
	l.Spec.HolderIdentity = &other
	if _, err := kube.CoordinationV1().Leases(mcNS).Update(context.Background(), l, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	a.c.release(context.Background(), key, "a/ua")
	if l, err := kube.CoordinationV1().Leases(mcNS).Get(context.Background(), leaseName(key), metav1.GetOptions{}); err != nil || *l.Spec.HolderIdentity != other {
		t.Fatalf("the successor's Lease is gone or changed: %v", err)
	}
}

// The holder's pod goes: a waiting pod takes over. Its volume, part
// streamed, is emptied before its plan says download.
func TestSingleFlightAWaitingPodTakesOverWhenTheHolderGoes(t *testing.T) {
	kube := fake.NewSimpleClientset()
	urls := map[string]string{}
	a, b := newFlightNode(t, kube, "a", urls), newFlightNode(t, kube, "b", urls)
	pa, pb := flightPod(t, "pa", "a", "ua"), flightPod(t, "pb", "b", "ub")
	addPod(t, kube, pa)
	addPod(t, kube, pb)
	emptyVolumes(t, map[*mcNode]*corev1.Pod{a: pa, b: pb})
	tickAll(a, b)
	if a.plan(pa) != modelcache.PlanDownload || b.plan(pb) != modelcache.PlanWait {
		t.Fatalf("plans %q %q", a.plan(pa), b.plan(pb))
	}
	a.downloadedLive(t, pa, map[string]string{"w.bin": "part"})
	mcEventually(t, "b streams", func() bool {
		tickAll(a, b)
		_, err := os.Stat(filepath.Join(b.volume(pb, "ngc-models"), "w.bin"))
		return err == nil
	})
	if err := kube.CoreV1().Pods(pa.Namespace).Delete(context.Background(), pa.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	mcEventually(t, "b takes over", func() bool { tickAll(a, b); return b.plan(pb) == modelcache.PlanDownload })
	ents, _ := os.ReadDir(b.volume(pb, "ngc-models"))
	if len(ents) != 0 {
		t.Fatalf("b's volume holds %d entries when its plan says download", len(ents))
	}
}

// The holder's agent restarts mid-download: the new one finds the Lease
// naming a pod on its node and keeps it, rather than let it expire into
// a second download.
func TestSingleFlightARestartedAgentKeepsItsPodsLease(t *testing.T) {
	kube := fake.NewSimpleClientset()
	urls := map[string]string{}
	a := newFlightNode(t, kube, "a", urls)
	pa := flightPod(t, "pa", "a", "ua")
	addPod(t, kube, pa)
	emptyVolumes(t, map[*mcNode]*corev1.Pod{a: pa})
	a.c.tick(context.Background())
	if a.plan(pa) != modelcache.PlanDownload {
		t.Fatalf("plan %q", a.plan(pa))
	}
	a.downloadedLive(t, pa, map[string]string{"w.bin": "half"})

	// The old agent is gone: its renewals stop, so only the new one can
	// renew the Lease below. A renewal in flight lands within a tick.
	a.c.mu.Lock()
	old := a.c.flights[pa.UID]
	a.c.mu.Unlock()
	old.cancel()
	time.Sleep(2 * a.c.leaseDuration / 6)

	// A new controller for node a: same disk, no memory.
	a2 := newFlightNode(t, kube, "a", urls)
	a2.c.store, a2.c.podsRoot, a2.c.podsWrite = a.c.store, a.c.podsRoot, a.c.podsWrite
	before, _ := kube.CoordinationV1().Leases(mcNS).Get(context.Background(), leaseName(keyOf(pa)), metav1.GetOptions{})
	a2.c.tick(context.Background())
	mcEventually(t, "the restarted agent renews", func() bool {
		l, err := kube.CoordinationV1().Leases(mcNS).Get(context.Background(), leaseName(keyOf(pa)), metav1.GetOptions{})
		return err == nil && *l.Spec.HolderIdentity == "a/ua" && !l.Spec.RenewTime.Equal(before.Spec.RenewTime)
	})
	a2.c.mu.Lock()
	f := a2.c.flights[pa.UID]
	a2.c.mu.Unlock()
	if f == nil || f.role != roleHolder {
		t.Fatalf("flight %+v; want the holder adopted", f)
	}
}

func leaseWithFeeds(t *testing.T, kube *fake.Clientset, key, holder string, feeds map[string]string) {
	t.Helper()
	b, _ := json.Marshal(feeds)
	dur := int32(30)
	now := metav1.NewMicroTime(time.Now())
	if _, err := kube.CoordinationV1().Leases(mcNS).Create(context.Background(), &coordv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: leaseName(key), Namespace: mcNS, Annotations: map[string]string{feedsAnnotation: string(b)}},
		Spec:       coordv1.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &dur, RenewTime: &now}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}

// No source feeds more than maxFeeds; a source that failed this pod is
// not picked again; and no pod streams from its own downstream.
func TestSingleFlightPicksARelayWithRoomAndNoCycle(t *testing.T) {
	kube := fake.NewSimpleClientset()
	n := newFlightNode(t, kube, "n", map[string]string{})
	key := "0123456789abcdef0123456789abcdef"
	leaseWithFeeds(t, kube, key, "h/1", map[string]string{"a/1": "h/1", "b/1": "h/1", "c/1": "h/1", "e/1": "a/1"})
	ctx := context.Background()
	if got := n.c.pickSource(ctx, key, "d/1", "h/1", nil); got != "b/1" {
		t.Fatalf("picked %q; want b/1 (the holder feeds 3, a feeds 1, b and c none)", got)
	}
	if got := n.c.pickSource(ctx, key, "f/1", "h/1", map[string]bool{"c/1": true}); got == "c/1" {
		t.Fatal("picked a source that failed this pod")
	}
	// a's downstream (e) must not become a's source.
	if got := n.c.pickSource(ctx, key, "a/1", "h/1", nil); got == "e/1" {
		t.Fatal("a pod picked its own downstream: a cycle")
	}
}

// A transient failure to read the Lease is retried like a write conflict:
// a pod that streamed unrecorded would not count against its source's
// maxFeeds, and later pods could pile onto that source.
func TestSingleFlightPickSourceRetriesALeaseReadError(t *testing.T) {
	kube := fake.NewSimpleClientset()
	n := newFlightNode(t, kube, "n", map[string]string{})
	key := "0123456789abcdef0123456789abcdef"
	leaseWithFeeds(t, kube, key, "h/1", map[string]string{"a/1": "h/1"})
	failures := 2
	kube.PrependReactor("get", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
		if failures > 0 {
			failures--
			return true, nil, apierrors.NewServerTimeout(coordv1.Resource("leases"), "get", 1)
		}
		return false, nil, nil
	})
	if got := n.c.pickSource(context.Background(), key, "d/1", "h/1", nil); got != "h/1" && got != "a/1" {
		t.Fatalf("picked %q", got)
	}
	l, err := kube.CoordinationV1().Leases(mcNS).Get(context.Background(), leaseName(key), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	feeds := map[string]string{}
	_ = json.Unmarshal([]byte(l.Annotations[feedsAnnotation]), &feeds)
	if feeds["d/1"] == "" {
		t.Fatalf("feeds %v: the pod streams without its feed recorded after a transient read error", feeds)
	}
}

// The holder's renewals survive the conflicts that streaming pods'
// annotations cause; only another holder ends them.
func TestSingleFlightRenewRetriesAConflict(t *testing.T) {
	kube := fake.NewSimpleClientset()
	a := newFlightNode(t, kube, "a", map[string]string{})
	key := "0123456789abcdef0123456789abcdef"
	if got, _, err := a.c.acquire(context.Background(), key, "a/ua"); !got || err != nil {
		t.Fatal(got, err)
	}
	conflicts := 2
	kube.PrependReactor("update", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
		if conflicts > 0 {
			conflicts--
			return true, nil, apierrors.NewConflict(coordv1.Resource("leases"), leaseName(key), errors.New("changed"))
		}
		return false, nil, nil
	})
	if err := a.c.renew(context.Background(), key, "a/ua"); err != nil {
		t.Fatalf("renew after two conflicts: %v", err)
	}
	if err := a.c.renew(context.Background(), key, "z/uz"); !errors.Is(err, errNotHolder) {
		t.Fatalf("renew by a non-holder: %v", err)
	}
}

// The holder releases only once others can fetch the copy: published on
// disk is not enough, it must be listed.
func TestSingleFlightListedNeedsTheSourcesEntry(t *testing.T) {
	kube := fake.NewSimpleClientset()
	urls := map[string]string{}
	a := newFlightNode(t, kube, "a", urls)
	p := initDone(cachePod(t, "p1", "a", "u1"), 0)
	addPod(t, kube, p)
	a.downloaded(t, p, map[string]string{"w.bin": "w"})
	key := keyOf(p)
	src := filepath.Join(a.c.podsRoot, "u1", "volumes", "kubernetes.io~empty-dir", "ngc-models")
	if _, err := a.c.store.Harvest(key, src, modelcache.Limits{}); err != nil {
		t.Fatal(err)
	}
	if a.c.listed(context.Background(), key) {
		t.Fatal("listed with no sources entry")
	}
	gen, _, _ := a.c.store.Latest(key)
	if err := a.c.register(context.Background(), key, filepath.Base(gen)); err != nil {
		t.Fatal(err)
	}
	if !a.c.listed(context.Background(), key) {
		t.Fatal("not listed after registering")
	}
}

// A waiting pod whose helper gave up and downloads: the agent stops
// writing into its volume and stops its heartbeat.
func TestSingleFlightStopsStreamingWhenTheHelperDownloads(t *testing.T) {
	kube := fake.NewSimpleClientset()
	urls := map[string]string{}
	a, b := newFlightNode(t, kube, "a", urls), newFlightNode(t, kube, "b", urls)
	pa, pb := flightPod(t, "pa", "a", "ua"), flightPod(t, "pb", "b", "ub")
	addPod(t, kube, pa)
	addPod(t, kube, pb)
	emptyVolumes(t, map[*mcNode]*corev1.Pod{a: pa, b: pb})
	tickAll(a, b)
	if b.plan(pb) != modelcache.PlanWait {
		t.Fatalf("plan %q", b.plan(pb))
	}
	sig := b.volume(pb, webhook.ModelCacheSignalVolume)
	if err := os.MkdirAll(sig, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sig, modelcache.SignalFile), []byte(modelcache.SignalDownloading), 0o644); err != nil {
		t.Fatal(err)
	}
	a.downloadedLive(t, pa, map[string]string{"w.bin": "after the helper gave up"})
	mcEventually(t, "b stops", func() bool {
		tickAll(a, b)
		return !b.marker(pb, modelcache.StreamingFileOf(string(pb.UID)))
	})
	time.Sleep(500 * time.Millisecond)
	if _, err := os.Stat(filepath.Join(b.volume(pb, "ngc-models"), "w.bin")); !os.IsNotExist(err) {
		t.Fatal("the agent wrote into a volume the pod's own download writes")
	}
}

// A stream has a deadline below the helper's: it never writes into a
// volume the helper has given up on.
func TestSingleFlightFollowEndsAtItsDeadline(t *testing.T) {
	kube := fake.NewSimpleClientset()
	urls := map[string]string{}
	a, b := newFlightNode(t, kube, "a", urls), newFlightNode(t, kube, "b", urls)
	b.c.followMax = time.Second
	pa, pb := flightPod(t, "pa", "a", "ua"), flightPod(t, "pb", "b", "ub")
	addPod(t, kube, pa)
	addPod(t, kube, pb)
	emptyVolumes(t, map[*mcNode]*corev1.Pod{a: pa, b: pb})
	tickAll(a, b)
	stop := time.Now().Add(5 * time.Second)
	for i := 0; time.Now().Before(stop); i++ { // a keeps making progress
		a.downloadedLive(t, pa, map[string]string{fmt.Sprintf("w-%d.bin", i): "x"})
		tickAll(a, b)
		if !b.marker(pb, modelcache.StreamingFileOf(string(pb.UID))) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("the stream outlived its deadline")
}

// Pods that join one by one form a tree, not a chain: the shallowest
// source with room wins, so the holder feeds maxFeeds before any relay
// does. (A chain of three relays was seen on a cluster: each hop added
// its lag.)
func TestSingleFlightRelaysFormATreeNotAChain(t *testing.T) {
	kube := fake.NewSimpleClientset()
	n := newFlightNode(t, kube, "n", map[string]string{})
	key := "0123456789abcdef0123456789abcdef"
	leaseWithFeeds(t, kube, key, "h/1", map[string]string{})
	ctx := context.Background()
	got := map[string]string{}
	for _, id := range []string{"a/1", "b/1", "c/1", "d/1", "e/1"} {
		got[id] = n.c.pickSource(ctx, key, id, "h/1", nil)
	}
	for _, id := range []string{"a/1", "b/1", "c/1"} {
		if got[id] != "h/1" {
			t.Fatalf("%s streams from %s; want the holder while it has room (picks %v)", id, got[id], got)
		}
	}
	for _, id := range []string{"d/1", "e/1"} {
		if got[id] == "h/1" || got[id] == "d/1" {
			t.Fatalf("%s streams from %s; want a relay of depth 1 (picks %v)", id, got[id], got)
		}
	}
}

// A pod that streamed its model is a copy like any other: once its init
// exits, its node keeps the tree the agent wrote and checked, and lists
// itself as a source, so the next cold start there needs no network.
func TestSingleFlightStreamedNodesKeepACopy(t *testing.T) {
	kube := fake.NewSimpleClientset()
	urls := map[string]string{}
	a, b := newFlightNode(t, kube, "a", urls), newFlightNode(t, kube, "b", urls)
	pa, pb := flightPod(t, "pa", "a", "ua"), flightPod(t, "pb", "b", "ub")
	addPod(t, kube, pa)
	addPod(t, kube, pb)
	emptyVolumes(t, map[*mcNode]*corev1.Pod{a: pa, b: pb})
	tickAll(a, b)
	a.downloadedLive(t, pa, map[string]string{"model/w.bin": "weights"})
	a.signal(t, pa)
	mcEventually(t, "b done", func() bool { tickAll(a, b); return b.marker(pb, modelcache.DoneFile(string(pb.UID))) })

	// b's helper exits: seeded, as it reports.
	pb.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "download-ngc-model",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Message: modelcache.InitSeeded,
			FinishedAt: metav1.NewTime(time.Now().Add(time.Second))}}}}
	pb.Status.Phase = corev1.PodRunning
	if _, err := kube.CoreV1().Pods(pb.Namespace).UpdateStatus(context.Background(), pb, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	mcEventually(t, "b keeps a copy", func() bool { tickAll(b); return b.has(keyOf(pb)) })
	mcEventually(t, "b is listed", func() bool { return sourcesOf(t, kube, keyOf(pb))["b"] != "" })
	gen, m, _ := b.c.store.Latest(keyOf(pb))
	if err := modelcache.Seed(gen, m, t.TempDir()); err != nil {
		t.Fatalf("seeding from b's copy: %v", err)
	}
}

// A node can have two waiting pods of a key; only the first is its relay.
// A pod told to stream from the other gets no bytes from the relay's
// volume under the other's name: the request names its pod, and the node
// refuses it at once, so the pod picks another source.
func TestSingleFlightARemoteReadIsServedOnlyByThePodItNames(t *testing.T) {
	kube := fake.NewSimpleClientset()
	urls := map[string]string{}
	b, c := newFlightNode(t, kube, "b", urls), newFlightNode(t, kube, "c", urls)
	key := "0123456789abcdef0123456789abcdef"
	vol := t.TempDir()
	write(t, vol, "w.bin", "the relay's bytes")
	b.c.mu.Lock()
	b.c.liveSources[key] = newLiveSource(vol, "ub1", b.c.timing)
	b.c.mu.Unlock()
	ctx := context.Background()

	v, err := c.c.clientFor(key, "b/ub2").View(ctx)
	if err != nil || v.State != liveFailed {
		t.Fatalf("view of b/ub2 = %+v, %v; want failed at once, not b/ub1's volume", v, err)
	}
	if r, err := c.c.clientFor(key, "b/ub2").Open(ctx, "w.bin"); err == nil {
		r.Close()
		t.Fatal("served b/ub1's file to a reader of b/ub2")
	}
	if v, err := c.c.clientFor(key, "b/ub1").View(ctx); err != nil || v.UID != "ub1" || v.State != liveDownloading {
		t.Fatalf("view of b/ub1 = %+v, %v; want its source", v, err)
	}
}

// The holder published a copy and its pod left: a stream that loses it
// copies the published copy, checked against its manifest, instead of
// trying the departed holder until its deadline.
func TestSingleFlightAStreamCopiesThePublishedCopyWhenTheHolderLeaves(t *testing.T) {
	kube := fake.NewSimpleClientset()
	urls := map[string]string{}
	a, b := newFlightNode(t, kube, "a", urls), newFlightNode(t, kube, "b", urls)
	pa, pb := flightPod(t, "pa", "a", "ua"), flightPod(t, "pb", "b", "ub")
	addPod(t, kube, pa)
	addPod(t, kube, pb)
	emptyVolumes(t, map[*mcNode]*corev1.Pod{a: pa, b: pb})
	tickAll(a, b)
	if a.plan(pa) != modelcache.PlanDownload || b.plan(pb) != modelcache.PlanWait {
		t.Fatalf("plans %q %q", a.plan(pa), b.plan(pb))
	}
	files := map[string]string{"model/w-1.bin": "weights one", "model/w-2.bin": "weights two"}
	a.downloadedLive(t, pa, map[string]string{"model/w-1.bin": "weights one"})
	mcEventually(t, "b streams", func() bool {
		tickAll(a, b)
		_, err := os.Stat(filepath.Join(b.volume(pb, "ngc-models"), "model", "w-1.bin"))
		return err == nil
	})
	// a's download ends and a publishes its copy, then a's pod leaves
	// before b's stream has the final manifest.
	a.downloadedLive(t, pa, map[string]string{"model/w-2.bin": "weights two"})
	key := keyOf(pa)
	gen, err := a.c.store.Harvest(key, a.volume(pa, "ngc-models"), modelcache.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.c.register(context.Background(), key, filepath.Base(gen)); err != nil {
		t.Fatal(err)
	}
	if err := kube.CoreV1().Pods(pa.Namespace).Delete(context.Background(), pa.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	tickAll(a)
	mcEventually(t, "b done from the published copy", func() bool {
		tickAll(b)
		return b.marker(pb, modelcache.DoneFile(string(pb.UID)))
	})
	assertTree(t, b.volume(pb, "ngc-models"), files)
}

// promoteFixture is a waiting pod on node a that has just taken the
// Lease.
func promoteFixture(t *testing.T) (*mcNode, *flight, *fake.Clientset) {
	t.Helper()
	kube := fake.NewSimpleClientset()
	a := newFlightNode(t, kube, "a", map[string]string{})
	key := "0123456789abcdef0123456789abcdef"
	vol := t.TempDir()
	write(t, vol, "w.bin", "streamed so far")
	f := &flight{key: key, identity: "a/ua", uid: "ua", role: roleFollower, vol: vol, src: newLiveSource(vol, "ua", a.c.timing), cancel: func() {}}
	a.c.track(f)
	if got, _, err := a.c.acquire(context.Background(), key, f.identity); !got || err != nil {
		t.Fatalf("acquire: %v %v", got, err)
	}
	if err := os.MkdirAll(a.c.store.KeyDir(key), 0o755); err != nil {
		t.Fatal(err)
	}
	return a, f, kube
}

func leaseHolder(kube *fake.Clientset, key string) string {
	l, err := kube.CoordinationV1().Leases(mcNS).Get(context.Background(), leaseName(key), metav1.GetOptions{})
	if err != nil || l.Spec.HolderIdentity == nil {
		return ""
	}
	return *l.Spec.HolderIdentity
}

// A takeover whose volume cannot be emptied releases the Lease: nobody
// would renew or serve it, and the other pods would wait out its expiry.
func TestSingleFlightATakeoverThatCannotEmptyTheVolumeReleasesTheLease(t *testing.T) {
	a, f, kube := promoteFixture(t)
	// A stub, not a read-only directory: root (as in CI) removes those.
	prev := emptyFlightVolume
	emptyFlightVolume = func(string) error { return errors.New("remove w.bin: device or resource busy") }
	t.Cleanup(func() { emptyFlightVolume = prev })
	a.c.promote(context.Background(), f, a.c.log)
	if h := leaseHolder(kube, f.key); h == f.identity {
		t.Fatal("the Lease is still held by a pod that cannot download")
	}
	if a.plan(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: f.uid, Labels: map[string]string{webhook.ModelCacheKeyLabel: f.key}}}) == modelcache.PlanDownload {
		t.Fatal("told to download into a volume that was not emptied")
	}
}

// A waiting pod's helper that gave up and downloads on its own, signalled
// before the agent took the Lease for it: the takeover must not empty the
// volume under that download. It leaves the volume and releases the Lease.
// The helper, having signalled, waits while the pod's streaming heartbeat
// is fresh (it is, until follow returns), so this check closes the race:
// either the signal is there when promote reads it, or the helper starts
// only after promote is done.
func TestSingleFlightATakeoverLeavesAVolumeTheHelperDownloadsInto(t *testing.T) {
	a, f, kube := promoteFixture(t)
	sig := a.c.signalDir(f.vol)
	if err := os.MkdirAll(sig, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sig, modelcache.SignalFile), []byte(modelcache.SignalDownloading+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	a.c.promote(context.Background(), f, a.c.log)
	if got, err := os.ReadFile(filepath.Join(f.vol, "w.bin")); err != nil || string(got) != "streamed so far" {
		t.Fatalf("w.bin = %q, %v; the takeover emptied a volume the helper downloads into", got, err)
	}
	if h := leaseHolder(kube, f.key); h == f.identity {
		t.Fatal("the Lease is held for a pod that downloads on its own")
	}
	if a.plan(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: f.uid, Labels: map[string]string{webhook.ModelCacheKeyLabel: f.key}}}) == modelcache.PlanDownload {
		t.Fatal("a pod that downloads on its own was planned as the holder")
	}
}

// A pod that left the node between taking the Lease and its promotion is
// not made the holder: the Lease is released and no flight is revived.
func TestSingleFlightATakeoverOfAPodThatLeftReleasesTheLease(t *testing.T) {
	a, f, kube := promoteFixture(t)
	a.c.endFlights(context.Background(), map[types.UID]bool{}) // the pod is gone
	if leaseHolder(kube, f.key) != f.identity {
		t.Fatal("fixture: endFlights released a follower's Lease")
	}
	a.c.promote(context.Background(), f, a.c.log)
	if h := leaseHolder(kube, f.key); h == f.identity {
		t.Fatal("the Lease is held for a pod that left the node")
	}
	a.c.mu.Lock()
	revived := a.c.flights[f.uid]
	a.c.mu.Unlock()
	if revived != nil {
		t.Fatalf("a flight %+v was registered for a pod that left", revived)
	}
}

// After an agent restart its flights are forgotten, so a plan written
// before the restart for a pod that has since left would stay and pin its
// key against eviction. Each tick removes the plans of pods no longer on
// the node, and keeps those of pods still there.
func TestSingleFlightATickRemovesThePlansOfPodsThatLeft(t *testing.T) {
	kube := fake.NewSimpleClientset()
	a := newFlightNode(t, kube, "a", map[string]string{})
	p := flightPod(t, "pa", "a", "ua")
	addPod(t, kube, p)
	emptyVolumes(t, map[*mcNode]*corev1.Pod{a: p})
	other := "0123456789abcdef0123456789abcdef"
	const gone = "0f1e2d3c-4b5a-4968-8776-655443322110"
	for _, k := range []string{keyOf(p), other} {
		dir := a.c.store.KeyDir(k)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := modelcache.WritePlan(dir, gone, modelcache.PlanWait); err != nil {
			t.Fatal(err)
		}
		if err := writeMarker(dir, modelcache.DoneFile(gone), ""); err != nil {
			t.Fatal(err)
		}
	}
	a.c.tick(context.Background())
	for _, k := range []string{keyOf(p), other} {
		dir := a.c.store.KeyDir(k)
		if plans := modelcache.Plans(dir); len(plans) > 0 && !(k == keyOf(p) && len(plans) == 1 && plans[0] == string(p.UID)) {
			t.Fatalf("key %s keeps plans %v; want only those of pods on the node", k, plans)
		}
		if _, err := os.Stat(filepath.Join(dir, modelcache.DoneFile(gone))); !os.IsNotExist(err) {
			t.Fatalf("key %s keeps the done marker of a pod that left", k)
		}
	}
	if a.plan(p) != modelcache.PlanDownload {
		t.Fatalf("the plan of the pod on the node is %q; want it kept", a.plan(p))
	}
}
