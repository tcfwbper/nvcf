// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gorilla/mux"
	dto "github.com/prometheus/client_model/go"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/metrics"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/modelcache"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

const mcNS = "nvsnap-system"

// cachePod is a cold NGC pod as admission leaves it: its download init
// wrapped by the helper, labelled with its key. Its init's state is set
// by the caller.
func cachePod(t *testing.T, name, node string, uid types.UID) *corev1.Pod {
	t.Helper()
	gpu := corev1.ResourceRequirements{Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("4")}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "sr-fn", Name: name, UID: uid, Labels: map[string]string{}},
		Spec: corev1.PodSpec{
			NodeName: node,
			InitContainers: []corev1.Container{{
				Name: "download-ngc-model", Image: "nvcr.io/org/ultra:vllm", Command: []string{"/bin/bash", "-c"},
				Args:         []string{"ngc registry model download-version --dest \"${NGC_MODEL_MOUNT}\" \"${NGC_MODEL_NAME}\""},
				Env:          []corev1.EnvVar{{Name: "NGC_MODEL_NAME", Value: "org/team/model:1"}, {Name: "NGC_MODEL_MOUNT", Value: "/config/models"}},
				VolumeMounts: []corev1.VolumeMount{{Name: "ngc-models", MountPath: "/config/models"}},
			}},
			Containers: []corev1.Container{{
				Name: "main", Image: "nvcr.io/org/ultra:vllm", Command: []string{"/bin/bash", "/start.sh"},
				Env:          []corev1.EnvVar{{Name: "MODEL_PATH", Value: "/config/models/model"}},
				Resources:    gpu,
				VolumeMounts: []corev1.VolumeMount{{Name: "ngc-models", MountPath: "/config/models"}},
			}},
			Volumes: []corev1.Volume{{Name: "ngc-models", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
	d, ok := webhook.ResolveModelCacheDownload(context.Background(), nil, pod, 0)
	if !ok {
		t.Fatal("fixture is not a cache download")
	}
	init := &pod.Spec.InitContainers[0]
	init.Command = append([]string{webhook.HelperBin, "model-cache", "--cache", webhook.ModelCacheMount, "--to", "/config/models", "--"}, d.Download...)
	init.Args = nil
	pod.Labels[webhook.ModelCacheKeyLabel] = d.Key
	return pod
}

func initDone(pod *corev1.Pod, code int32) *corev1.Pod {
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "download-ngc-model",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: code,
			// Later than anything the test writes, unless it says otherwise.
			FinishedAt: metav1.NewTime(time.Now().Add(time.Hour))}}}}
	return pod
}

func keyOf(pod *corev1.Pod) string { return pod.Labels[webhook.ModelCacheKeyLabel] }

type mcNode struct {
	c      *nodeModelCache
	kube   *fake.Clientset
	srv    *httptest.Server
	router *mux.Router
}

// newMCNode is one node's cache controller, with its peer routes served.
// Every node shares kube; peers resolve through urls.
func newMCNode(t *testing.T, kube *fake.Clientset, node string, urls map[string]string) *mcNode {
	t.Helper()
	cfg := NodeModelCacheConfig{Enabled: true, Root: t.TempDir(), MaxPercent: 50, FetchTimeout: 10 * time.Second}
	peer := func(_ context.Context, n string) (string, error) { return urls[n], nil }
	c := newNodeModelCache(cfg, kube, node, mcNS, nil, 0, peer, logrus.New())
	c.podsRoot = t.TempDir()
	c.client = &http.Client{} // its own: a test may set its timeout
	c.statfs = func(string) (int64, int64, error) { return 100 << 30, 90 << 30, nil }
	r := mux.NewRouter()
	r.Use(pathVarGuard)
	r.HandleFunc("/v1/model-cache/{key}", c.manifestHandler).Methods("GET")
	r.HandleFunc("/v1/model-cache/{key}/{gen}/file", c.fileHandler).Methods("GET")
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	urls[node] = srv.URL
	return &mcNode{c: c, kube: kube, srv: srv, router: r}
}

// downloaded writes what the pod's init downloaded into its emptyDir.
func (n *mcNode) downloaded(t *testing.T, pod *corev1.Pod, files map[string]string) {
	t.Helper()
	dir := filepath.Join(n.c.podsRoot, string(pod.UID), "volumes", "kubernetes.io~empty-dir", "ngc-models")
	for p, b := range files {
		must := func(err error) {
			if err != nil {
				t.Fatal(err)
			}
		}
		must(os.MkdirAll(filepath.Join(dir, filepath.Dir(p)), 0o755))
		must(os.WriteFile(filepath.Join(dir, p), []byte(b), 0o644))
	}
	// The init reports the tree it left, as the helper does.
	if len(pod.Status.InitContainerStatuses) > 0 && pod.Status.InitContainerStatuses[0].State.Terminated != nil {
		ents, err := modelcache.Scan(dir, 0)
		if err != nil {
			t.Fatal(err)
		}
		pod.Status.InitContainerStatuses[0].State.Terminated.Message = modelcache.InitDownloaded + " " + modelcache.TreeField + modelcache.Fingerprint(ents)
		if _, err := n.kube.CoreV1().Pods(pod.Namespace).UpdateStatus(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
}

func mcEventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting: %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (n *mcNode) has(key string) bool { _, _, ok := n.c.store.Latest(key); return ok }

func sourcesOf(t *testing.T, kube *fake.Clientset, key string) map[string]string {
	t.Helper()
	cm, err := kube.CoreV1().ConfigMaps(mcNS).Get(context.Background(), sourcesName(key), metav1.GetOptions{})
	if err != nil {
		return nil
	}
	return cm.Data
}

func addPod(t *testing.T, kube *fake.Clientset, pod *corev1.Pod) {
	t.Helper()
	if _, err := kube.CoreV1().Pods(pod.Namespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestModelCacheHarvestsAFinishedDownloadAndRegistersIt(t *testing.T) {
	kube := fake.NewSimpleClientset()
	a := newMCNode(t, kube, "a", map[string]string{})
	pod := initDone(cachePod(t, "p1", "a", "u1"), 0)
	addPod(t, kube, pod)
	a.downloaded(t, pod, map[string]string{"model/config.json": "{}", "model/w.bin": "weights"})

	a.c.tick(context.Background())
	mcEventually(t, "harvest", func() bool { return a.has(keyOf(pod)) })
	mcEventually(t, "registration", func() bool { return sourcesOf(t, kube, keyOf(pod))["a"] != "" })
}

func TestModelCacheDoesNotHarvestAFailedDownload(t *testing.T) {
	kube := fake.NewSimpleClientset()
	a := newMCNode(t, kube, "a", map[string]string{})
	pod := initDone(cachePod(t, "p1", "a", "u1"), 1)
	addPod(t, kube, pod)
	a.downloaded(t, pod, map[string]string{"model/w.bin": "half"})
	a.c.tick(context.Background())
	time.Sleep(200 * time.Millisecond)
	if a.has(keyOf(pod)) {
		t.Fatal("a failed download was harvested")
	}
}

// A pod that labels itself with another key (another tenant's model)
// must not get its files cached under that key.
func TestModelCacheIgnoresAForgedKeyLabel(t *testing.T) {
	kube := fake.NewSimpleClientset()
	a := newMCNode(t, kube, "a", map[string]string{})
	pod := initDone(cachePod(t, "p1", "a", "u1"), 0)
	victim := "ffffffffffffffffffffffffffffffff"
	pod.Labels[webhook.ModelCacheKeyLabel] = victim
	addPod(t, kube, pod)
	a.downloaded(t, pod, map[string]string{"model/w.bin": "poison"})
	a.c.tick(context.Background())
	time.Sleep(200 * time.Millisecond)
	if a.has(victim) {
		t.Fatal("files were cached under a key the pod chose")
	}
	for _, k := range a.c.store.Keys() {
		t.Fatalf("something was cached: %s", k)
	}
}

func TestModelCacheFetchesFromAPeerForAWaitingPod(t *testing.T) {
	kube := fake.NewSimpleClientset()
	urls := map[string]string{}
	a, b := newMCNode(t, kube, "a", urls), newMCNode(t, kube, "b", urls)

	src := initDone(cachePod(t, "p1", "a", "u1"), 0)
	addPod(t, kube, src)
	a.downloaded(t, src, map[string]string{"model/config.json": "{}", "model/w.bin": "weights"})
	a.c.tick(context.Background())
	mcEventually(t, "harvest on a", func() bool { return sourcesOf(t, kube, keyOf(src))["a"] != "" })

	cold := cachePod(t, "p2", "b", "u2") // init not run yet
	addPod(t, kube, cold)
	b.c.tick(context.Background())
	mcEventually(t, "fetch on b", func() bool { return b.has(keyOf(cold)) })
	mcEventually(t, "b registered", func() bool { return sourcesOf(t, kube, keyOf(cold))["b"] != "" })
	// The heartbeat goes in fill's deferred cleanup, after the
	// registration: wait for the fill to end, then for the heartbeat.
	mcEventually(t, "the fill ended", func() bool {
		b.c.mu.Lock()
		defer b.c.mu.Unlock()
		return b.c.busy[keyOf(cold)] == 0
	})
	mcEventually(t, "the fetch heartbeat went with the fetch", func() bool {
		_, err := os.Stat(filepath.Join(b.c.store.KeyDir(keyOf(cold)), modelcache.IncomingFile))
		return os.IsNotExist(err)
	})
	gen, m, _ := b.c.store.Latest(keyOf(cold))
	dst := t.TempDir()
	if err := modelcache.Seed(gen, m, dst); err != nil {
		t.Fatalf("seeding from the fetched copy: %v", err)
	}
}

func TestModelCacheRefusesAPeerThatServesOtherBytes(t *testing.T) {
	kube := fake.NewSimpleClientset()
	urls := map[string]string{}
	a, b := newMCNode(t, kube, "a", urls), newMCNode(t, kube, "b", urls)
	src := initDone(cachePod(t, "p1", "a", "u1"), 0)
	addPod(t, kube, src)
	a.downloaded(t, src, map[string]string{"model/w.bin": "weights"})
	a.c.tick(context.Background())
	mcEventually(t, "harvest on a", func() bool { return a.has(keyOf(src)) })
	// b finds a only once a is registered; without a source its fill
	// would end before trying a, and record no failure.
	mcEventually(t, "a registered", func() bool { return sourcesOf(t, kube, keyOf(src))["a"] != "" })
	// Corrupt a's copy after publication, same size.
	gen, _, _ := a.c.store.Latest(keyOf(src))
	if err := os.WriteFile(filepath.Join(gen, modelcache.TreeDir, "model/w.bin"), []byte("WEIGHTS"), 0o644); err != nil {
		t.Fatal(err)
	}
	cold := cachePod(t, "p2", "b", "u2")
	addPod(t, kube, cold)
	b.c.tick(context.Background())
	mcEventually(t, "the failed fetch is recorded", func() bool {
		b.c.mu.Lock()
		defer b.c.mu.Unlock()
		_, failed := b.c.failed[keyOf(cold)]
		return failed && b.c.busy[keyOf(cold)] == 0
	})
	if b.has(keyOf(cold)) {
		t.Fatal("b published a copy that does not match its manifest")
	}
}

func TestModelCacheEvictsOnlyUnusedKeys(t *testing.T) {
	kube := fake.NewSimpleClientset()
	a := newMCNode(t, kube, "a", map[string]string{})
	used := initDone(cachePod(t, "p1", "a", "u1"), 0)
	addPod(t, kube, used)
	a.downloaded(t, used, map[string]string{"model/w.bin": "0123456789"})
	a.c.tick(context.Background())
	mcEventually(t, "harvest", func() bool { return a.has(keyOf(used)) })

	// An unused key, cached earlier.
	other := filepath.Join(t.TempDir(), "m")
	if err := os.MkdirAll(other, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(other, "w.bin"), []byte("0123456789"), 0o644); err != nil {
		t.Fatal(err)
	}
	stale := "0123456789abcdef0123456789abcdef"
	gen, err := a.c.store.Harvest(stale, other, modelcache.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.c.register(context.Background(), stale, filepath.Base(gen)); err != nil {
		t.Fatal(err)
	}

	// A budget that holds one key.
	a.c.statfs = func(string) (int64, int64, error) { return 30, 30, nil }
	a.c.maxPercent = 50
	a.c.tick(context.Background())
	if a.has(stale) {
		t.Fatal("the unused key was not evicted")
	}
	if sourcesOf(t, kube, stale)["a"] != "" {
		t.Fatal("the evicted key is still advertised")
	}
	if !a.has(keyOf(used)) {
		t.Fatal("the key a pod on the node uses was evicted")
	}
}

func TestModelCacheReserveRefusesPastTheBudget(t *testing.T) {
	a := newMCNode(t, fake.NewSimpleClientset(), "a", map[string]string{})
	a.c.statfs = func(string) (int64, int64, error) { return 1000, 1000, nil }
	a.c.maxPercent = 50
	r1, err := a.c.reserve(context.Background(), 300)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.c.reserve(context.Background(), 300); err == nil {
		t.Fatal("two reservations past the budget both succeeded")
	}
	r1()
	r1() // idempotent
	if _, err := a.c.reserve(context.Background(), 300); err != nil {
		t.Fatalf("after release: %v", err)
	}
	// The free-space floor holds whatever the budget allows.
	a.c.statfs = func(string) (int64, int64, error) { return 1000, 150, nil }
	if _, err := a.c.reserve(context.Background(), 100); err == nil {
		t.Fatal("a reservation went under the free-space floor")
	}
	// A negative request would shrink what is promised and let others past
	// the budget.
	a.c.statfs = func(string) (int64, int64, error) { return 1000, 1000, nil }
	if _, err := a.c.reserve(context.Background(), -500); err == nil {
		t.Fatal("a negative reservation was accepted")
	}
}

func TestModelCacheFileRouteStaysInTheTree(t *testing.T) {
	kube := fake.NewSimpleClientset()
	a := newMCNode(t, kube, "a", map[string]string{})
	pod := initDone(cachePod(t, "p1", "a", "u1"), 0)
	addPod(t, kube, pod)
	a.downloaded(t, pod, map[string]string{"model/w.bin": "weights"})
	a.c.tick(context.Background())
	mcEventually(t, "harvest", func() bool { return a.has(keyOf(pod)) })
	gen, _, _ := a.c.store.Latest(keyOf(pod))
	base := a.srv.URL + "/v1/model-cache/" + keyOf(pod) + "/" + filepath.Base(gen) + "/file?path="
	for _, p := range []string{"../MANIFEST", "..%2F..%2F..%2Fetc%2Fpasswd", "%2Fetc%2Fpasswd", "model"} {
		resp, err := http.Get(base + p)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Fatalf("path %q served", p)
		}
	}
	resp, err := http.Get(base + "model%2Fw.bin")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("a cached file: %s", resp.Status)
	}
}

func TestModelCacheNeedsRequiredAuth(t *testing.T) {
	for _, mode := range []AuthMode{AuthDisabled, AuthPermissive} {
		a := &Agent{config: Config{AuthMode: mode, NodeModelCache: NodeModelCacheConfig{Enabled: true, Root: t.TempDir()}}}
		if a.nodeModelCacheOn() || a.webhookNodeModelCache() != nil {
			t.Fatalf("auth %s: the cache is on", mode)
		}
	}
	a := &Agent{config: Config{AuthMode: AuthRequired, NodeModelCache: NodeModelCacheConfig{Enabled: true, Root: t.TempDir()}}}
	if !a.nodeModelCacheOn() || a.webhookNodeModelCache() == nil {
		t.Fatal("auth required: the cache is off")
	}
}

func TestModelCacheStartupRemovesAbandonedAttemptsAndStaleSources(t *testing.T) {
	kube := fake.NewSimpleClientset()
	a := newMCNode(t, kube, "a", map[string]string{})
	key := "0123456789abcdef0123456789abcdef"
	att := filepath.Join(a.c.store.KeyDir(key), modelcache.AttemptPrefix+"dead")
	if err := os.MkdirAll(att, 0o755); err != nil {
		t.Fatal(err)
	}
	// Listed as a source of a key it no longer has.
	if err := a.c.register(context.Background(), key, "gen-x"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { a.c.run(ctx); close(done) }()
	mcEventually(t, "startup", func() bool {
		_, err := os.Stat(att)
		return os.IsNotExist(err) && sourcesOf(t, kube, key)["a"] == ""
	})
	cancel()
	<-done
}

func TestModelCacheCountsEachInitOnce(t *testing.T) {
	a := newMCNode(t, fake.NewSimpleClientset(), "a", map[string]string{})
	get := func(r string) float64 {
		var m dto.Metric
		if err := metrics.ModelCacheInits.WithLabelValues(r).Write(&m); err != nil {
			t.Fatal(err)
		}
		return m.GetCounter().GetValue()
	}
	s0, d0, f0 := get("seeded"), get("downloaded"), get("failed")
	a.c.countInit("u1", &corev1.ContainerStateTerminated{ExitCode: 0, Message: modelcache.InitSeeded})
	a.c.countInit("u1", &corev1.ContainerStateTerminated{ExitCode: 0, Message: modelcache.InitSeeded})
	a.c.countInit("u2", &corev1.ContainerStateTerminated{ExitCode: 0, Message: modelcache.InitDownloaded})
	a.c.countInit("u3", &corev1.ContainerStateTerminated{ExitCode: 1})
	if get("seeded")-s0 != 1 || get("downloaded")-d0 != 1 || get("failed")-f0 != 1 {
		t.Fatalf("seeded +%v downloaded +%v failed +%v; want 1 each", get("seeded")-s0, get("downloaded")-d0, get("failed")-f0)
	}
}

// The engine starts after the init and may write into the volume before
// the harvest reads it: such a tree is not the download's.
func TestModelCacheDoesNotHarvestATreeTheEngineWrote(t *testing.T) {
	kube := fake.NewSimpleClientset()
	a := newMCNode(t, kube, "a", map[string]string{})
	pod := initDone(cachePod(t, "p1", "a", "u1"), 0)
	pod.Status.InitContainerStatuses[0].State.Terminated.FinishedAt = metav1.NewTime(time.Now().Add(-time.Hour))
	addPod(t, kube, pod)
	a.downloaded(t, pod, map[string]string{"model/w.bin": "weights"}) // written "after" the init ended
	a.c.tick(context.Background())
	time.Sleep(300 * time.Millisecond)
	if a.has(keyOf(pod)) {
		t.Fatal("a tree written after the download finished was cached")
	}
}

func TestModelCacheHarvestsOnlyTheTreeTheDownloadReported(t *testing.T) {
	kube := fake.NewSimpleClientset()
	a := newMCNode(t, kube, "a", map[string]string{})
	// No reported tree (an init that never reported one): nothing kept.
	silent := initDone(cachePod(t, "p1", "a", "u1"), 0)
	addPod(t, kube, silent)
	dir := filepath.Join(a.c.podsRoot, "u1", "volumes", "kubernetes.io~empty-dir", "ngc-models", "model")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "w.bin"), []byte("weights"), 0o644); err != nil {
		t.Fatal(err)
	}
	a.c.tick(context.Background())
	time.Sleep(300 * time.Millisecond)
	if a.has(keyOf(silent)) {
		t.Fatal("a tree no download reported was cached")
	}

	// Reported, then the engine removed a file before the harvest read it.
	b := newMCNode(t, kube, "b", map[string]string{})
	pod := initDone(cachePod(t, "p2", "b", "u2"), 0)
	addPod(t, kube, pod)
	b.downloaded(t, pod, map[string]string{"model/w.bin": "weights", "model/w2.bin": "more"})
	if err := os.Remove(filepath.Join(b.c.podsRoot, "u2", "volumes", "kubernetes.io~empty-dir", "ngc-models", "model/w2.bin")); err != nil {
		t.Fatal(err)
	}
	b.c.tick(context.Background())
	time.Sleep(300 * time.Millisecond)
	if b.has(keyOf(pod)) {
		t.Fatal("a tree missing a downloaded file was cached")
	}
}

func TestModelCacheDeletesASourceListOnceEmpty(t *testing.T) {
	kube := fake.NewSimpleClientset()
	a := newMCNode(t, kube, "a", map[string]string{})
	key := "0123456789abcdef0123456789abcdef"
	if err := a.c.register(context.Background(), key, "gen-1"); err != nil {
		t.Fatal(err)
	}
	if err := a.c.deregister(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if _, err := kube.CoreV1().ConfigMaps(mcNS).Get(context.Background(), sourcesName(key), metav1.GetOptions{}); err == nil {
		t.Fatal("an empty source list was kept")
	}
}
