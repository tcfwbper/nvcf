// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/checkpointstore"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/metrics"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

func bundleAgent(t *testing.T) *Agent {
	t.Helper()
	root := t.TempDir()
	return &Agent{log: logrus.New(), config: Config{CheckpointDir: t.TempDir(), Webhook: WebhookConfig{HostBundleRoot: root}}}
}

func stageVersion(t *testing.T, a *Agent, id string, age time.Duration) {
	t.Helper()
	dir := filepath.Join(a.bundleVersionsDir(), id)
	if err := os.MkdirAll(filepath.Join(dir, "lib"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "libnvsnap_gpushare.so"), []byte("lib-"+id), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../libnvsnap_gpushare.so", filepath.Join(dir, "lib", "link")); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-age)
	if err := os.Chtimes(dir, old, old); err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestCurrentBundle(t *testing.T) {
	a := bundleAgent(t)
	if a.currentBundle() != "" {
		t.Fatal("no staging yet: no current version")
	}
	stageVersion(t, a, "8ab0d751c1f6277b", 0)
	for content, want := range map[string]string{"8ab0d751c1f6277b\n": "8ab0d751c1f6277b", "../etc": "", "v9-unstaged": ""} {
		if err := os.WriteFile(filepath.Join(a.bundleRoot(), "current-bundle"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		if got := a.currentBundle(); got != want {
			t.Errorf("current-bundle %q: got %q, want %q (an unstaged or invalid id is none)", content, got, want)
		}
	}
}

// A version served by one agent arrives whole, modes and links kept.
func TestBundleTarRoundTrip(t *testing.T) {
	src := bundleAgent(t)
	stageVersion(t, src, "aaaa", 0)
	r := mux.NewRouter()
	r.HandleFunc("/v1/bundles/{id}", src.bundleHandler)
	srv := httptest.NewServer(r)
	defer srv.Close()
	resp := get(t, srv.URL+"/v1/bundles/aaaa")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("serve: %v", resp.Status)
	}
	defer func() { _ = resp.Body.Close() }()
	dst := t.TempDir()
	if err := readBundleTar(resp.Body, dst); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(dst, "libnvsnap_gpushare.so")); err != nil || string(b) != "lib-aaaa" {
		t.Fatalf("library: %q %v", b, err)
	}
	if st, _ := os.Stat(filepath.Join(dst, "libnvsnap_gpushare.so")); st.Mode().Perm()&0o100 == 0 {
		t.Error("exec bit lost")
	}
	if l, err := os.Readlink(filepath.Join(dst, "lib", "link")); err != nil || l != "../libnvsnap_gpushare.so" {
		t.Errorf("symlink: %q %v", l, err)
	}
	for _, bad := range []string{"/v1/bundles/zzzz", "/v1/bundles/0123"} {
		resp := get(t, srv.URL+bad)
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Errorf("%s served", bad)
		}
	}
}

// A fetched version digests the same as the served one under the agent's
// umask: modes it would strip (group write on 0775) are kept, so the
// fetch passes the recorded digest.
func TestBundleTarKeepsModesPastUmask(t *testing.T) {
	old := syscall.Umask(0o022)
	defer syscall.Umask(old)
	src := bundleAgent(t)
	stageVersion(t, src, "bbbb", 0)
	dir := filepath.Join(src.bundleVersionsDir(), "bbbb")
	if err := os.Mkdir(filepath.Join(dir, "plugins"), 0o775); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"cuda-checkpoint", "plugins/cuda_plugin.so"} {
		if err := os.WriteFile(filepath.Join(dir, p), []byte(p), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range []string{"plugins", "cuda-checkpoint", "plugins/cuda_plugin.so"} {
		if err := os.Chmod(filepath.Join(dir, p), 0o775); err != nil {
			t.Fatal(err)
		}
	}
	want, err := bundleDigest(dir)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := writeBundleTar(&buf, dir); err != nil {
		t.Fatal(err)
	}
	dst := t.TempDir()
	if err := readBundleTar(&buf, dst); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"plugins", "cuda-checkpoint", "plugins/cuda_plugin.so"} {
		if st, err := os.Stat(filepath.Join(dst, p)); err != nil || st.Mode().Perm() != 0o775 {
			t.Errorf("%s: mode %v %v, want 0775", p, st.Mode().Perm(), err)
		}
	}
	if got, err := bundleDigest(dst); err != nil || got != want {
		t.Errorf("fetched digest %s %v, want the served one %s", got, err, want)
	}
}

// A symlink the stream creates cannot carry a later entry outside the
// bundle.
func TestReadBundleTarConfinesSymlinks(t *testing.T) {
	outside := t.TempDir()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: "lib", Typeflag: tar.TypeSymlink, Linkname: outside}); err != nil {
		t.Fatal(err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: "lib/planted.so", Mode: 0o644, Size: 1, Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := readBundleTar(&buf, t.TempDir()); err == nil {
		t.Error("a file was written through a symlink out of the bundle")
	}
	if _, err := os.Stat(filepath.Join(outside, "planted.so")); err == nil {
		t.Fatal("a file landed outside the bundle")
	}
}

// A link the stream makes inside the bundle is not followed by a later
// entry: "x -> ." and then a directory "x" would otherwise change the mode
// of the extraction root itself. A link no later entry goes through is
// kept.
func TestReadBundleTarRefusesEntriesThroughLinks(t *testing.T) {
	for name, entries := range map[string][]tar.Header{
		"directory through a link": {{Name: "x", Typeflag: tar.TypeSymlink, Linkname: "."}, {Name: "x", Mode: 0o777, Typeflag: tar.TypeDir}},
		"file under a link":        {{Name: "d", Mode: 0o755, Typeflag: tar.TypeDir}, {Name: "x", Typeflag: tar.TypeSymlink, Linkname: "d"}, {Name: "x/f", Mode: 0o644, Typeflag: tar.TypeReg}},
	} {
		var buf bytes.Buffer
		tw := tar.NewWriter(&buf)
		for i := range entries {
			if err := tw.WriteHeader(&entries[i]); err != nil {
				t.Fatal(err)
			}
		}
		if err := tw.Close(); err != nil {
			t.Fatal(err)
		}
		dst := t.TempDir()
		if err := os.Chmod(dst, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := readBundleTar(&buf, dst); err == nil || !strings.Contains(err.Error(), "symbolic link") {
			t.Errorf("%s: %v, want it refused", name, err)
		}
		if st, _ := os.Stat(dst); st.Mode().Perm() != 0o700 {
			t.Errorf("%s: the extraction root became %v", name, st.Mode().Perm())
		}
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, h := range []tar.Header{{Name: "lib", Mode: 0o755, Typeflag: tar.TypeDir}, {Name: "lib/link", Typeflag: tar.TypeSymlink, Linkname: "../x.so"}} {
		if err := tw.WriteHeader(&h); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	dst := t.TempDir()
	if err := readBundleTar(&buf, dst); err != nil {
		t.Fatalf("a plain link was refused: %v", err)
	}
	if l, err := os.Readlink(filepath.Join(dst, "lib", "link")); err != nil || l != "../x.so" {
		t.Errorf("link %q %v", l, err)
	}
}

func TestReadBundleTarRejectsEscapes(t *testing.T) {
	for _, name := range []string{"../../escape.so", "/etc/escape.so", ".."} {
		var buf bytes.Buffer
		tw := tar.NewWriter(&buf)
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: 1, Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := tw.Close(); err != nil {
			t.Fatal(err)
		}
		dst := t.TempDir()
		err := readBundleTar(&buf, dst)
		if err == nil || !strings.Contains(err.Error(), "leaves the bundle") {
			t.Errorf("%q: %v, want it refused for leaving the bundle", name, err)
		}
	}
}

// Unused versions go; the current one, those pods mount, those
// checkpoints recorded and young ones stay.
func TestReapBundleVersions(t *testing.T) {
	a := bundleAgent(t)
	for _, id := range []string{"c0", "d0", "e0", "f0"} {
		stageVersion(t, a, id, 2*time.Hour)
	}
	stageVersion(t, a, "a0", time.Minute)
	stageVersion(t, a, ".a1.fetch-x", 2*time.Hour)
	if err := os.WriteFile(filepath.Join(a.bundleRoot(), "current-bundle"), []byte("c0"), 0o644); err != nil {
		t.Fatal(err)
	}
	ck := filepath.Join(a.config.CheckpointDir, "ck1")
	if err := os.MkdirAll(ck, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ck, "metadata.json"), []byte(`{"bundle":"e0"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	pod := corev1.Pod{Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "nvsnap-gpushare-lib",
		VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: a.bundleRoot() + "/versions/d0"}}}}}}
	a.reapBundleVersions([]corev1.Pod{pod}, time.Now())
	for id, want := range map[string]bool{"c0": true, "d0": true, "e0": true, "a0": true, "f0": false, ".a1.fetch-x": false} {
		_, err := os.Stat(filepath.Join(a.bundleVersionsDir(), id))
		if got := err == nil; got != want {
			t.Errorf("%s kept = %v, want %v", id, got, want)
		}
	}
}

func TestValidBundleID(t *testing.T) {
	for id, want := range map[string]bool{"8ab0d751c1f6277b": true, "v0.2.76-mv152": true, "": false, "../x": false, ".hidden": false, "a/b": false, "a b": false, "..": false} {
		if validBundleID(id) != want {
			t.Errorf("%q: want %v", id, want)
		}
	}
	_ = webhook.BundleMeta
}

// TestPublishBuildInfo: every agent exports nvsnap_build_info, with the
// bundle it serves or "unknown" before one is installed, so a missing
// series means an agent is not scraped (docs/operate/METRICS.md).
func TestPublishBuildInfo(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.BuildInfo)
	buildInfo := func() map[string]float64 {
		t.Helper()
		mfs, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]float64{}
		for _, mf := range mfs {
			for _, m := range mf.GetMetric() {
				got[m.GetLabel()[0].GetValue()] = m.GetGauge().GetValue()
			}
		}
		return got
	}

	a := bundleAgent(t)
	a.publishBuildInfo()
	if got := buildInfo(); len(got) != 1 || got["unknown"] != 1 {
		t.Fatalf("nvsnap_build_info before a bundle = %v, want {unknown: 1}", got)
	}
	stageVersion(t, a, "c0", 0)
	if err := os.WriteFile(filepath.Join(a.bundleRoot(), "current-bundle"), []byte("c0"), 0o644); err != nil {
		t.Fatal(err)
	}
	a.publishBuildInfo()
	if got := buildInfo(); len(got) != 1 || got["c0"] != 1 {
		t.Fatalf("nvsnap_build_info with bundle c0 = %v, want {c0: 1}", got)
	}
}

// lostBundleFixture is a node n1 whose restore placeholder for checkpoint
// t0 (tenant "tenant", lone) mounts bundle version "lost", which n1 lacks,
// beside the current version it has. Its same-architecture peers n2 and n3
// run agents that answer through the real handler from (empty) bundle
// versions, unless peers gives a node its own handler; n4 is of another
// architecture.
func lostBundleFixture(t *testing.T, peers map[string]http.Handler) (*criuAutoRestorer, *fake.Clientset, *corev1.Pod) {
	t.Helper()
	c, kc := trustFixture(t)
	a := c.a
	a.config.NodeName = "n1"
	a.config.Webhook.HostBundleRoot = t.TempDir()
	stageVersion(t, a, "current", time.Minute)

	// One server for every peer, told apart by the loopback address each
	// node lists: a peer agent is reached at its node's address.
	peer := bundleAgent(t)
	r := mux.NewRouter()
	r.HandleFunc("/v1/bundles/{id}", peer.bundleHandler).Methods("GET")
	ips := map[string]string{"n1": "127.0.0.1", "n2": "127.0.0.2", "n3": "127.0.0.3", "n4": "127.0.0.4"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		host, _, _ := net.SplitHostPort(req.Host)
		for node, ip := range ips {
			if ip == host && peers[node] != nil {
				peers[node].ServeHTTP(w, req)
				return
			}
		}
		r.ServeHTTP(w, req)
	}))
	t.Cleanup(srv.Close)
	_, port, err := net.SplitHostPort(srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	// The same server at each peer's address, on the same port: every
	// agent listens on one.
	for _, node := range []string{"n2", "n3", "n4"} {
		ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", net.JoinHostPort(ips[node], port))
		if err != nil {
			t.Skipf("cannot listen on %s: %v", ips[node], err)
		}
		go func() { _ = srv.Config.Serve(ln) }()
		t.Cleanup(func() { _ = ln.Close() })
	}
	a.config.ListenAddr = ":" + port

	ctx := context.Background()
	for _, n := range []struct{ name, arch string }{{"n1", "amd64"}, {"n2", "amd64"}, {"n3", "amd64"}, {"n4", "arm64"}} {
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: n.name, Labels: map[string]string{corev1.LabelArchStable: n.arch}},
			Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: ips[n.name]}}}}
		if _, err := kc.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		agent := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "agent-" + n.name, Namespace: "nvsnap-system",
			Labels: map[string]string{"app": "nvsnap-agent", "controller-revision-hash": "r1"}},
			Spec:   corev1.PodSpec{NodeName: n.name},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
		if _, err := kc.CoreV1().Pods("nvsnap-system").Create(ctx, agent, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	isController := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "fn-0", Namespace: "tenant", UID: "fn-0-uid",
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "fn", UID: "rs", Controller: &isController}},
			Annotations:     map[string]string{webhook.CRIURestoreAnnotation: "t0", webhook.CRIURestoreContainerAnnotation: "engine"}},
		Spec: corev1.PodSpec{NodeName: "n1",
			Containers: []corev1.Container{{Name: "engine"}},
			Volumes: []corev1.Volume{bundleVolume(a, "nvsnap-gpushare-lib", "lost"), bundleVolume(a, "nvsnap-criu-bundle", "lost"),
				bundleVolume(a, "nvsnap-helper", "current")}},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
	if _, err := kc.CoreV1().Pods("tenant").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	recordBundle(t, c, "tenant", "t0", "lost")
	return c, kc, pod
}

// recordBundle rewrites lone capture id's record (namespace ns's scope) to
// name bundle version id as the one it was captured with.
func recordBundle(t *testing.T, c *criuAutoRestorer, ns, id, bundle string) {
	t.Helper()
	sum := sha256.Sum256([]byte(id))
	hash := hex.EncodeToString(sum[:])
	meta := map[string]string{"engine": "criu", "checkpoint_id": id, checkpointstore.CRIUScopeMetaKey: criuScopeOf(ns, nil), webhook.BundleMeta: bundle}
	if err := c.a.captureBackend.(checkpointstore.ManifestRecorder).RecordManifest(context.Background(), hash,
		checkpointstore.Manifest{Hash: hash, CaptureMethod: "criu", SourcePodMeta: meta}); err != nil {
		t.Fatal(err)
	}
}

// bundleVolume is a pod's volume of bundle version id, as the webhook
// places it.
func bundleVolume(a *Agent, name, id string) corev1.Volume {
	dir := corev1.HostPathDirectory
	return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
		Path: a.bundleRoot() + "/" + webhook.BundleVersionsDir + "/" + id, Type: &dir}}}
}

// podGone reports whether the fixture's placeholder tenant/fn-0 is deleted within 5 seconds.
func podGone(t *testing.T, kc *fake.Clientset) bool {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := kc.CoreV1().Pods("tenant").Get(context.Background(), "fn-0", metav1.GetOptions{}); apierrors.IsNotFound(err) {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// settle runs ensurePlaceholderBundles on pod and waits for its fetch of
// version id, and what follows it, to finish.
func settle(t *testing.T, c *criuAutoRestorer, pod *corev1.Pod, id string) {
	t.Helper()
	c.ensurePlaceholderBundles(context.Background(), pod)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, busy := c.attempted.Load("bundle/" + id); !busy {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the bundle fetch did not finish")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// notOnAnyPeer stubs a fetch that finds version id on no peer.
func notOnAnyPeer(_ context.Context, id string) error {
	return fmt.Errorf("%w: version %s: 2 agents of this node's architecture asked, none has it", errBundleNotOnAnyPeer, id)
}

// A restore placeholder whose bundle version no peer has (every
// same-architecture peer answers that it does not) never starts: the
// hostPath it mounts cannot exist. Its checkpoint is blocked and the pod
// deleted, so its replacement starts cold, rather than waiting forever.
// Drives the production path: the real fetch, the real peer handler.
func TestEnsurePlaceholderBundles_LostVersionStartsCold(t *testing.T) {
	defer func(d time.Duration) { bundleMissConfirm = d }(bundleMissConfirm)
	bundleMissConfirm = 0
	c, kc, pod := lostBundleFixture(t, nil)
	ctx := context.Background()
	c.ensurePlaceholderBundles(ctx, pod)
	if !podGone(t, kc) {
		t.Fatal("the placeholder whose bundle version no peer has was not deleted")
	}
	if !c.Blocked(ctx, "t0") {
		t.Error("its checkpoint was not blocked: the replacement would be admitted to it again")
	}
	cm, err := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, criuRestoreFailedConfigMap, metav1.GetOptions{})
	if err != nil || !strings.Contains(cm.Data["t0"], metrics.BundleUnavailableText) {
		t.Errorf("the block does not say why: %v %v", cm, err)
	}
}

// A miss is final only when every same-architecture peer said it does not
// have the version, with no agent being replaced. Any other answer may
// pass: the placeholder waits and the fetch is tried again.
func TestFetchBundle_DefinitiveMiss(t *testing.T) {
	plain404 := http.NotFoundHandler()
	unavailable := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "restarting", http.StatusServiceUnavailable)
	})
	for _, tc := range []struct {
		name  string
		peers map[string]http.Handler
		setup func(t *testing.T, kc *fake.Clientset)
		want  bool
	}{
		{name: "every peer answers it does not have it", want: true},
		{name: "a peer answers 503", peers: map[string]http.Handler{"n3": unavailable}},
		{name: "a peer answers a 404 that is not about the version", peers: map[string]http.Handler{"n3": plain404}},
		{name: "a peer's address cannot be told", setup: func(t *testing.T, kc *fake.Clientset) {
			editNode(t, kc, "n3", func(n *corev1.Node) { n.Status.Addresses = nil })
		}},
		{name: "a peer does not answer", setup: func(t *testing.T, kc *fake.Clientset) {
			editNode(t, kc, "n3", func(n *corev1.Node) { n.Status.Addresses[0].Address = "192.0.2.1" })
		}},
		{name: "a node's architecture cannot be told", setup: func(t *testing.T, kc *fake.Clientset) {
			editNode(t, kc, "n4", func(n *corev1.Node) { delete(n.Labels, corev1.LabelArchStable) })
		}},
		{name: "this node's architecture cannot be told", setup: func(t *testing.T, kc *fake.Clientset) {
			editNode(t, kc, "n1", func(n *corev1.Node) { delete(n.Labels, corev1.LabelArchStable) })
		}},
		{name: "an agent is terminating", setup: func(t *testing.T, kc *fake.Clientset) {
			editAgent(t, kc, "agent-n2", func(p *corev1.Pod) {
				now := metav1.Now()
				p.DeletionTimestamp, p.Finalizers = &now, []string{"test"}
			})
		}},
		{name: "a rollout is in progress", setup: func(t *testing.T, kc *fake.Clientset) {
			editAgent(t, kc, "agent-n1", func(p *corev1.Pod) { p.Labels["controller-revision-hash"] = "r2" })
		}},
		{name: "a peer's node not ready past the bound", want: true, setup: func(t *testing.T, kc *fake.Clientset) {
			editNode(t, kc, "n3", func(n *corev1.Node) {
				n.Status.Addresses[0].Address = "192.0.2.1"
				n.Status.Conditions = notReadySince(peerDownBound + time.Minute)
			})
		}},
		{name: "a peer's node not ready within the bound", setup: func(t *testing.T, kc *fake.Clientset) {
			editNode(t, kc, "n3", func(n *corev1.Node) {
				n.Status.Addresses[0].Address = "192.0.2.1"
				n.Status.Conditions = notReadySince(peerDownBound - time.Minute)
			})
		}},
		{name: "a peer's agent not ready past the bound", want: true, peers: map[string]http.Handler{"n3": unavailable}, setup: func(t *testing.T, kc *fake.Clientset) {
			editAgent(t, kc, "agent-n3", func(p *corev1.Pod) { p.Status.Conditions = podNotReadySince(peerDownBound + time.Minute) })
		}},
		{name: "a peer's agent not ready within the bound", peers: map[string]http.Handler{"n3": unavailable}, setup: func(t *testing.T, kc *fake.Clientset) {
			editAgent(t, kc, "agent-n3", func(p *corev1.Pod) { p.Status.Conditions = podNotReadySince(peerDownBound - time.Minute) })
		}},
		{name: "an agent terminating on a node down past the bound", want: true, setup: func(t *testing.T, kc *fake.Clientset) {
			editNode(t, kc, "n3", func(n *corev1.Node) { n.Status.Conditions = notReadySince(peerDownBound + time.Minute) })
			editAgent(t, kc, "agent-n3", func(p *corev1.Pod) {
				now := metav1.Now()
				p.DeletionTimestamp, p.Finalizers = &now, []string{"test"}
			})
		}},
		{name: "another revision on a node down past the bound", want: true, setup: func(t *testing.T, kc *fake.Clientset) {
			editNode(t, kc, "n3", func(n *corev1.Node) { n.Status.Conditions = notReadySince(peerDownBound + time.Minute) })
			editAgent(t, kc, "agent-n3", func(p *corev1.Pod) { p.Labels["controller-revision-hash"] = "r2" })
		}},
		{name: "an agent not placed yet", setup: func(t *testing.T, kc *fake.Clientset) {
			if _, err := kc.CoreV1().Pods("nvsnap-system").Create(context.Background(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "agent-new", Namespace: "nvsnap-system", Labels: map[string]string{"app": "nvsnap-agent", "controller-revision-hash": "r1"}}},
				metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "this node's agent not listed", setup: func(t *testing.T, kc *fake.Clientset) {
			if err := kc.CoreV1().Pods("nvsnap-system").Delete(context.Background(), "agent-n1", metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "no peer and this node's agent not listed", setup: func(t *testing.T, kc *fake.Clientset) {
			for _, name := range []string{"agent-n1", "agent-n2", "agent-n3"} {
				if err := kc.CoreV1().Pods("nvsnap-system").Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
			}
		}},
		{name: "a node the agents run on has none listed", setup: func(t *testing.T, kc *fake.Clientset) {
			addGPUNode(t, kc, "n5", corev1.ConditionTrue, 0)
		}},
		{name: "a node the agents run on, down past the bound, has none listed", want: true, setup: func(t *testing.T, kc *fake.Clientset) {
			addGPUNode(t, kc, "n5", corev1.ConditionUnknown, peerDownBound+time.Minute)
		}},
		// No other agent of this architecture: none can serve the version.
		{name: "no peer of this architecture", want: true, setup: func(t *testing.T, kc *fake.Clientset) {
			for _, name := range []string{"agent-n2", "agent-n3"} {
				if err := kc.CoreV1().Pods("nvsnap-system").Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
			}
		}},
		// Another architecture's agents may be mid-rollout: they are not
		// asked, and do not hold the answer back.
		{name: "another architecture's agent is terminating", want: true, setup: func(t *testing.T, kc *fake.Clientset) {
			editAgent(t, kc, "agent-n4", func(p *corev1.Pod) {
				now := metav1.Now()
				p.DeletionTimestamp, p.Finalizers = &now, []string{"test"}
			})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, kc, _ := lostBundleFixture(t, tc.peers)
			if tc.setup != nil {
				tc.setup(t, kc)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err := c.a.fetchBundle(ctx, "lost")
			if err == nil {
				t.Fatal("fetched a version no peer has")
			}
			if got := errors.Is(err, errBundleNotOnAnyPeer); got != tc.want {
				t.Errorf("definitive = %v, want %v: %v", got, tc.want, err)
			}
		})
	}
}

func editNode(t *testing.T, kc *fake.Clientset, name string, edit func(*corev1.Node)) {
	t.Helper()
	n, err := kc.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	edit(n)
	if _, err := kc.CoreV1().Nodes().Update(context.Background(), n, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func editAgent(t *testing.T, kc *fake.Clientset, name string, edit func(*corev1.Pod)) {
	t.Helper()
	p, err := kc.CoreV1().Pods("nvsnap-system").Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	edit(p)
	if _, err := kc.CoreV1().Pods("nvsnap-system").Update(context.Background(), p, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

// A peer that may have the version (here one answering 503: an agent
// restarting) keeps the placeholder waiting: nothing is blocked and the
// pod is left alone. Through the real fetch.
func TestEnsurePlaceholderBundles_PeerErrorWaits(t *testing.T) {
	defer func(d time.Duration) { bundleMissConfirm = d }(bundleMissConfirm)
	bundleMissConfirm = 0
	c, kc, pod := lostBundleFixture(t, map[string]http.Handler{"n2": http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "restarting", http.StatusServiceUnavailable)
	})})
	settle(t, c, pod, "lost")
	assertUntouched(t, c, kc, "tenant", "fn-0", "t0")
}

func assertUntouched(t *testing.T, c *criuAutoRestorer, kc *fake.Clientset, ns, name, ckpt string) {
	t.Helper()
	ctx := context.Background()
	if c.Blocked(ctx, ckpt) {
		t.Errorf("checkpoint %s was blocked", ckpt)
	}
	if _, err := kc.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{}); err != nil {
		t.Errorf("pod %s/%s was acted on: %v", ns, name, err)
	}
}

// Only a restore placeholder that cannot start without the version is
// given up. A companion starts without it; the version every pod mounts
// as its helper is staged by an agent rollout; a pod past its restore, or
// running, does not wait on it; a pod naming another tenant's checkpoint
// gets nothing, as admitted would give it. A miss not yet confirmed waits.
func TestEnsurePlaceholderBundles_LostVersionLeavesOthersAlone(t *testing.T) {
	for _, tc := range []struct {
		name    string
		confirm time.Duration
		edit    func(a *Agent, p *corev1.Pod)
		ckpt    string
		record  string
	}{
		{name: "companion placeholder", edit: func(_ *Agent, p *corev1.Pod) { p.Annotations[webhook.CRIUGroupCompanionAnnotation] = "true" }},
		{name: "mounts a companion directory", edit: func(a *Agent, p *corev1.Pod) {
			orCreate := corev1.HostPathDirectoryOrCreate
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{Name: "nvsnap-companion", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: a.bundleRoot() + "/" + webhook.BundleCompanionDir + "/lost", Type: &orCreate}}})
		}},
		{name: "the helper's version", edit: func(a *Agent, p *corev1.Pod) {
			p.Spec.Volumes[2] = bundleVolume(a, "nvsnap-helper", "lost")
		}},
		{name: "already restored", edit: func(_ *Agent, p *corev1.Pod) { p.Annotations[webhook.CRIURestoredAnnotation] = "2026-11-20T00:00:00Z" }},
		{name: "started cold", edit: func(_ *Agent, p *corev1.Pod) { p.Annotations[webhook.ColdStartedAnnotation] = "2026-11-20T00:00:00Z" }},
		{name: "not a placeholder", edit: func(_ *Agent, p *corev1.Pod) { delete(p.Annotations, webhook.CRIURestoreAnnotation) }},
		{name: "another tenant's checkpoint", ckpt: "v0", edit: func(_ *Agent, p *corev1.Pod) { p.Annotations[webhook.CRIURestoreAnnotation] = "v0" }},
		{name: "miss not confirmed yet", confirm: time.Hour},
		{name: "its capture recorded another version", edit: func(_ *Agent, p *corev1.Pod) {}, record: "other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func(d time.Duration) { bundleMissConfirm = d }(bundleMissConfirm)
			bundleMissConfirm = tc.confirm
			c, kc, pod := lostBundleFixture(t, nil)
			c.fetchBundle = notOnAnyPeer
			if tc.record != "" {
				recordBundle(t, c, "tenant", "t0", tc.record)
			}
			if tc.edit != nil {
				tc.edit(c.a, pod)
				if _, err := kc.CoreV1().Pods("tenant").Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			ckpt := tc.ckpt
			if ckpt == "" {
				ckpt = "t0"
			}
			settle(t, c, pod, "lost")
			assertUntouched(t, c, kc, "tenant", "fn-0", ckpt)
		})
	}
}

// A rank of an instance restore whose version no peer has ends the whole
// group as not run: its checkpoint is blocked, which keeps the webhook from
// admitting any rank of the instance to that capture again, and every
// placeholder of the instance is deleted. Another instance is left alone.
func TestEnsurePlaceholderBundles_LostVersionEndsTheGroup(t *testing.T) {
	defer func(d time.Duration) { bundleMissConfirm = d }(bundleMissConfirm)
	bundleMissConfirm = 0
	c, kc, _ := groupFixture(t)
	ctx := context.Background()
	c.a.config.NodeName = "n1"
	c.a.config.Webhook.HostBundleRoot = t.TempDir()
	c.fetchBundle = notOnAnyPeer
	setRecord(t, kc, map[string]string{"bundle.0": "kept", "bundle.1": "lost"})
	var rank *corev1.Pod
	for i, name := range []string{"kimi-0", "kimi-1"} {
		p, err := kc.CoreV1().Pods("fn").Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		p.Annotations[webhook.CRIUGroupOrdinalAnnotation], p.Annotations[webhook.CRIUGroupSizeAnnotation] = strconv.Itoa(i), "2"
		p.Annotations[webhook.CRIURestoreContainerAnnotation] = "engine"
		if name == "kimi-1" {
			p.Spec.NodeName = "n1"
			p.Spec.Volumes = []corev1.Volume{bundleVolume(c.a, "nvsnap-gpushare-lib", "lost")}
			p.Status.Phase = corev1.PodPending
		}
		if p, err = kc.CoreV1().Pods("fn").Update(ctx, p, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		rank = p
	}
	settle(t, c, rank, "lost")
	if !c.Blocked(ctx, "c1") {
		t.Error("the rank's checkpoint was not blocked: the instance would be admitted to the same capture again")
	}
	if got := remainingPods(t, kc); len(got) != 1 || got[0] != "other-0" {
		t.Errorf("pods left = %v, want only the other instance", got)
	}
	cm, err := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, criuGroupConfigMapName("k1"), metav1.GetOptions{})
	if err != nil || cm.Data["stalls"] != "1" {
		t.Errorf("the group record does not count the restore that did not run: %v %v", cm, err)
	}
}

// fetchScript stubs fetches that answer, per version, the next error in
// line (nil: the last one again).
type fetchScript struct {
	mu   sync.Mutex
	errs map[string]error
}

func (f *fetchScript) set(id string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs[id] = err
}

func (f *fetchScript) fetch(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.errs[id]
}

var errMayPass = errors.New("n2: 503 Service Unavailable")

// The miss must hold, unbroken, across a second fetch at least
// bundleMissConfirm after the first: one answer of a peer that may have it
// in between starts the wait again, and each version a pod waits on keeps
// its own timer.
func TestEnsurePlaceholderBundles_MissConfirmation(t *testing.T) {
	defer func(d time.Duration) { bundleMissConfirm = d }(bundleMissConfirm)
	bundleMissConfirm = 300 * time.Millisecond
	definitive := fmt.Errorf("%w: test", errBundleNotOnAnyPeer)
	past := func() { time.Sleep(bundleMissConfirm + 50*time.Millisecond) }

	t.Run("fires on a later sample past the window", func(t *testing.T) {
		c, kc, pod := lostBundleFixture(t, nil)
		f := &fetchScript{errs: map[string]error{"lost": definitive}}
		c.fetchBundle = f.fetch
		settle(t, c, pod, "lost")
		assertUntouched(t, c, kc, "tenant", "fn-0", "t0")
		past()
		settle(t, c, pod, "lost")
		if !podGone(t, kc) || !c.Blocked(context.Background(), "t0") {
			t.Fatal("a miss confirmed past the window did not start the pod cold")
		}
	})
	t.Run("an answer that may pass resets it", func(t *testing.T) {
		c, kc, pod := lostBundleFixture(t, nil)
		f := &fetchScript{errs: map[string]error{"lost": definitive}}
		c.fetchBundle = f.fetch
		settle(t, c, pod, "lost")
		past()
		f.set("lost", errMayPass)
		settle(t, c, pod, "lost")
		f.set("lost", definitive)
		settle(t, c, pod, "lost")
		assertUntouched(t, c, kc, "tenant", "fn-0", "t0")
		past()
		settle(t, c, pod, "lost")
		if !podGone(t, kc) {
			t.Fatal("a miss confirmed after the reset did not start the pod cold")
		}
	})
	t.Run("versions keep their own timers", func(t *testing.T) {
		c, kc, pod := lostBundleFixture(t, nil)
		pod.Spec.Volumes = append(pod.Spec.Volumes, bundleVolume(c.a, "extra", "other"))
		if _, err := kc.CoreV1().Pods("tenant").Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		f := &fetchScript{errs: map[string]error{"lost": definitive, "other": errMayPass}}
		c.fetchBundle = f.fetch
		settle(t, c, pod, "lost")
		settle(t, c, pod, "other")
		assertUntouched(t, c, kc, "tenant", "fn-0", "t0")
		past()
		settle(t, c, pod, "other") // answers that may pass for another version
		settle(t, c, pod, "lost")
		if !podGone(t, kc) {
			t.Fatal("another version's answer reset this version's wait")
		}
	})
}

// A placeholder whose fetch keeps failing without a final answer is given
// up after bundleUndecidedMax, as when the agent cannot decide. For an
// instance whose stuck pod is rank 0 (its group's own wait never starts:
// rank 0 drives it once running), the group is ended.
func TestEnsurePlaceholderBundles_UndecidedGivesUp(t *testing.T) {
	defer func(d time.Duration) { bundleUndecidedMax = d }(bundleUndecidedMax)
	bundleUndecidedMax = 300 * time.Millisecond
	c, kc, pod := lostBundleFixture(t, nil)
	c.fetchBundle = (&fetchScript{errs: map[string]error{"lost": errMayPass}}).fetch
	settle(t, c, pod, "lost")
	assertUntouched(t, c, kc, "tenant", "fn-0", "t0")
	time.Sleep(bundleUndecidedMax + 50*time.Millisecond)
	settle(t, c, pod, "lost")
	if !podGone(t, kc) || !c.Blocked(context.Background(), "t0") {
		t.Fatal("a lone placeholder undecided past the bound was not started cold")
	}
	cm, err := kc.CoreV1().ConfigMaps("nvsnap-system").Get(context.Background(), criuRestoreFailedConfigMap, metav1.GetOptions{})
	if err != nil || !strings.Contains(cm.Data["t0"], "without a final answer") {
		t.Errorf("the block does not say why: %v %v", cm, err)
	}

	gc, gkc, _ := groupFixture(t)
	gc.a.config.NodeName = "n1"
	gc.a.config.Webhook.HostBundleRoot = t.TempDir()
	gc.fetchBundle = (&fetchScript{errs: map[string]error{"lost": errMayPass}}).fetch
	setRecord(t, gkc, map[string]string{"bundle.0": "lost"})
	rank := groupRank(t, gc, gkc, "kimi-0", 0)
	settle(t, gc, rank, "lost")
	assertUntouched(t, gc, gkc, "fn", "kimi-0", "c0")
	time.Sleep(bundleUndecidedMax + 50*time.Millisecond)
	settle(t, gc, rank, "lost")
	if !gc.Blocked(context.Background(), "c0") {
		t.Error("the stuck rank 0's checkpoint was not blocked")
	}
	if got := remainingPods(t, gkc); len(got) != 1 || got[0] != "other-0" {
		t.Errorf("pods left = %v, want only the other instance", got)
	}
}

// groupRank makes groupFixture's pod name rank ordinal of a 2-rank group
// restore pending on node n1 on bundle version "lost", and returns it.
func groupRank(t *testing.T, c *criuAutoRestorer, kc *fake.Clientset, name string, ordinal int) *corev1.Pod {
	t.Helper()
	ctx := context.Background()
	p, err := kc.CoreV1().Pods("fn").Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	p.Annotations[webhook.CRIUGroupOrdinalAnnotation], p.Annotations[webhook.CRIUGroupSizeAnnotation] = strconv.Itoa(ordinal), "2"
	p.Annotations[webhook.CRIURestoreContainerAnnotation] = "engine"
	p.Spec.NodeName = "n1"
	p.Spec.Volumes = []corev1.Volume{bundleVolume(c.a, "nvsnap-gpushare-lib", "lost")}
	p.Status.Phase = corev1.PodPending
	if p, err = kc.CoreV1().Pods("fn").Update(ctx, p, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	return p
}

// Two ranks of one instance both lack the version: one agent ends the
// instance (one stall recorded), the other only blocks its checkpoint.
func TestEnsurePlaceholderBundles_GroupEndedOnce(t *testing.T) {
	defer func(d time.Duration) { bundleMissConfirm = d }(bundleMissConfirm)
	bundleMissConfirm = 0
	c, kc, _ := groupFixture(t)
	c.a.config.NodeName = "n1"
	c.a.config.Webhook.HostBundleRoot = t.TempDir()
	c.fetchBundle = notOnAnyPeer
	setRecord(t, kc, map[string]string{"bundle.0": "lost", "bundle.1": "lost"})
	r0, r1 := groupRank(t, c, kc, "kimi-0", 0), groupRank(t, c, kc, "kimi-1", 1)
	settle(t, c, r1, "lost")
	settle(t, c, r0, "lost") // the event of a rank already deleted, seen late
	ctx := context.Background()
	if !c.Blocked(ctx, "c0") || !c.Blocked(ctx, "c1") {
		t.Error("each rank's checkpoint must be blocked")
	}
	cm, err := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, criuGroupConfigMapName("k1"), metav1.GetOptions{})
	if err != nil || cm.Data["stalls"] != "1" {
		t.Errorf("stalls = %q, want one for the instance: %v", cm.Data["stalls"], err)
	}
	if got := remainingPods(t, kc); len(got) != 1 || got[0] != "other-0" {
		t.Errorf("pods left = %v, want only the other instance", got)
	}
	// The instance's claim holds until it expires, and is then taken over.
	log := logrus.NewEntry(logrus.New())
	if c.claimGroupEnd(ctx, "k1", instanceIdentity(r1), time.Now(), log) {
		t.Error("the instance's live claim was taken again")
	}
	if !c.claimGroupEnd(ctx, "k1", instanceIdentity(r1), time.Now().Add(groupEndClaimTTL+time.Minute), log) {
		t.Error("an expired claim was not taken over")
	}
}

// A pod refused for its tenant scope is not fetched for again.
func TestEnsurePlaceholderBundles_RefusedNotAskedAgain(t *testing.T) {
	defer func(d time.Duration) { bundleMissConfirm = d }(bundleMissConfirm)
	bundleMissConfirm = 0
	c, kc, pod := lostBundleFixture(t, nil)
	fetches := 0
	c.fetchBundle = func(ctx context.Context, id string) error { fetches++; return notOnAnyPeer(ctx, id) }
	pod.Annotations[webhook.CRIURestoreAnnotation] = "v0" // the other tenant's
	settle(t, c, pod, "lost")
	settle(t, c, pod, "lost")
	if fetches != 1 {
		t.Errorf("fetched %d times for a refused pod, want 1", fetches)
	}
	assertUntouched(t, c, kc, "tenant", "fn-0", "v0")
	c.forgetPod(pod)
	settle(t, c, pod, "lost")
	if fetches != 2 {
		t.Errorf("a pod's refusal outlived it: fetched %d times, want 2", fetches)
	}
}

// Only a version that is not there is reported absent; a version that
// cannot be read is an error a peer does not take as final.
func TestBundleHandler_AbsentOnlyWhenMissing(t *testing.T) {
	a := bundleAgent(t)
	r := mux.NewRouter()
	r.HandleFunc("/v1/bundles/{id}", a.bundleHandler).Methods("GET")
	srv := httptest.NewServer(r)
	defer srv.Close()
	resp := get(t, srv.URL+"/v1/bundles/nope")
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("missing version: %d, want 404", resp.StatusCode)
	}
	if err := os.WriteFile(a.bundleVersionsDir(), []byte("not a directory"), 0o644); err != nil {
		t.Fatal(err)
	}
	resp = get(t, srv.URL+"/v1/bundles/nope")
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("unreadable versions: %d, want 500", resp.StatusCode)
	}
}

// notReadySince is a node's conditions, not Ready since d ago.
func notReadySince(d time.Duration) []corev1.NodeCondition {
	return []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionUnknown, LastTransitionTime: metav1.NewTime(time.Now().Add(-d))}}
}

// podNotReadySince is a pod's conditions, not Ready since d ago.
func podNotReadySince(d time.Duration) []corev1.PodCondition {
	return []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse, LastTransitionTime: metav1.NewTime(time.Now().Add(-d))}}
}

// servingPeer is a peer agent that has bundle version id staged, with its
// digest recorded for this cluster's amd64 nodes, serving it through the
// real handler.
func servingPeer(t *testing.T, kc *fake.Clientset, id string) http.Handler {
	t.Helper()
	peer := bundleAgent(t)
	stageVersion(t, peer, id, time.Minute)
	digest, err := bundleDigest(filepath.Join(peer.bundleVersionsDir(), id))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kc.CoreV1().ConfigMaps("nvsnap-system").Create(context.Background(), &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: bundleDigestsConfigMap, Namespace: "nvsnap-system"},
		Data:       map[string]string{bundleDigestKey(id, "amd64"): digest}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	r := mux.NewRouter()
	r.HandleFunc("/v1/bundles/{id}", peer.bundleHandler).Methods("GET")
	return r
}

// A peer whose node has not been Ready for long can still serve (a dead
// kubelet, a partition from the control plane): it is asked, and when it
// serves the version the fetch succeeds.
func TestFetchBundle_DownPeerThatServesIsAsked(t *testing.T) {
	peers := map[string]http.Handler{}
	c, kc, _ := lostBundleFixture(t, peers)
	peers["n3"] = servingPeer(t, kc, "lost")
	editNode(t, kc, "n3", func(n *corev1.Node) { n.Status.Conditions = notReadySince(peerDownBound + time.Minute) })
	if err := c.a.fetchBundle(context.Background(), "lost"); err != nil {
		t.Fatalf("a down peer that has the version was not fetched from: %v", err)
	}
	if _, err := os.Stat(filepath.Join(c.a.bundleVersionsDir(), "lost")); err != nil {
		t.Errorf("the version is not installed: %v", err)
	}
}

// A peer that serves the version, but whose copy cannot be installed here
// (a fault of this node, or of the transfer), says the version exists: past
// the bound the pod is deleted so its replacement may land elsewhere, and
// the checkpoint is not blocked.
func TestEnsurePlaceholderBundles_ServedButNotInstalledIsNotBlocked(t *testing.T) {
	defer func(d time.Duration) { bundleUndecidedMax = d }(bundleUndecidedMax)
	bundleUndecidedMax = 300 * time.Millisecond
	c, kc, pod := lostBundleFixture(t, map[string]http.Handler{"n2": http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-tar")
		_, _ = w.Write([]byte("not a tar stream"))
	})})
	settle(t, c, pod, "lost")
	assertUntouched(t, c, kc, "tenant", "fn-0", "t0")
	time.Sleep(bundleUndecidedMax + 50*time.Millisecond)
	settle(t, c, pod, "lost")
	if !podGone(t, kc) {
		t.Fatal("a placeholder whose version could not be installed was not deleted past the bound")
	}
	if c.Blocked(context.Background(), "t0") {
		t.Error("the checkpoint was blocked although a peer has its version")
	}
}

// The end of a group is claimed for its instance, not for whichever rank
// a lookup finds as leader: with rank 0 already being deleted, a late rank
// 0 event does not end the instance a second time.
func TestEnsurePlaceholderBundles_GroupClaimSurvivesLeaderDeletion(t *testing.T) {
	defer func(d time.Duration) { bundleMissConfirm = d }(bundleMissConfirm)
	bundleMissConfirm = 0
	c, kc, _ := groupFixture(t)
	c.a.config.NodeName = "n1"
	c.a.config.Webhook.HostBundleRoot = t.TempDir()
	c.fetchBundle = notOnAnyPeer
	setRecord(t, kc, map[string]string{"bundle.0": "lost", "bundle.1": "lost"})
	r0, r1 := groupRank(t, c, kc, "kimi-0", 0), groupRank(t, c, kc, "kimi-1", 1)
	ctx := context.Background()
	terminating := r0.DeepCopy()
	now := metav1.Now()
	terminating.DeletionTimestamp, terminating.Finalizers = &now, []string{"test"}
	if _, err := kc.CoreV1().Pods("fn").Update(ctx, terminating, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	settle(t, c, r1, "lost")
	settle(t, c, r0, "lost") // seen before it was marked for deletion
	cm, err := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, criuGroupConfigMapName("k1"), metav1.GetOptions{})
	if err != nil || cm.Data["stalls"] != "1" {
		t.Errorf("stalls = %q, want one for the instance: %v", cm.Data["stalls"], err)
	}
}

// A capture record gone (or no longer naming the version) since the pod
// was admitted says nothing about the capture: once the pod has waited
// past the bound it is deleted, and no checkpoint is blocked.
func TestEnsurePlaceholderBundles_RecordGoneDeletesOnly(t *testing.T) {
	defer func(d, u time.Duration) { bundleMissConfirm, bundleUndecidedMax = d, u }(bundleMissConfirm, bundleUndecidedMax)
	bundleMissConfirm, bundleUndecidedMax = 0, 300*time.Millisecond
	c, kc, pod := lostBundleFixture(t, nil)
	recordBundle(t, c, "tenant", "t0", "")
	c.fetchBundle = notOnAnyPeer
	settle(t, c, pod, "lost")
	assertUntouched(t, c, kc, "tenant", "fn-0", "t0")
	time.Sleep(bundleUndecidedMax + 50*time.Millisecond)
	settle(t, c, pod, "lost")
	if !podGone(t, kc) {
		t.Fatal("a placeholder whose record is gone was left waiting past the bound")
	}
	if c.Blocked(context.Background(), "t0") {
		t.Error("a checkpoint was blocked although its record does not name the version")
	}
}

// A pod refused for its tenant scope is not fetched for again, but is not
// left waiting for good either: past the bound it alone is deleted.
func TestEnsurePlaceholderBundles_RefusedDeletedPastTheBound(t *testing.T) {
	defer func(d, u time.Duration) { bundleMissConfirm, bundleUndecidedMax = d, u }(bundleMissConfirm, bundleUndecidedMax)
	bundleMissConfirm, bundleUndecidedMax = 0, 300*time.Millisecond
	c, kc, pod := lostBundleFixture(t, nil)
	c.fetchBundle = notOnAnyPeer
	pod.Annotations[webhook.CRIURestoreAnnotation] = "v0" // the other tenant's
	if _, err := kc.CoreV1().Pods("tenant").Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	settle(t, c, pod, "lost")
	assertUntouched(t, c, kc, "tenant", "fn-0", "v0")
	time.Sleep(bundleUndecidedMax + 50*time.Millisecond)
	c.ensurePlaceholderBundles(context.Background(), pod)
	if !podGone(t, kc) {
		t.Fatal("a refused placeholder was left waiting past the bound")
	}
	if c.Blocked(context.Background(), "v0") {
		t.Error("another tenant's checkpoint was blocked")
	}
}

// A peer that served the version says it exists even when the miss later
// becomes final: the pod is deleted, the checkpoint not blocked.
func TestEnsurePlaceholderBundles_ServedThenMissingIsNotBlocked(t *testing.T) {
	defer func(d time.Duration) { bundleMissConfirm = d }(bundleMissConfirm)
	bundleMissConfirm = 0
	c, kc, pod := lostBundleFixture(t, nil)
	f := &fetchScript{errs: map[string]error{"lost": fmt.Errorf("n2: %w: digest mismatch", errBundleInstall)}}
	c.fetchBundle = f.fetch
	settle(t, c, pod, "lost")
	assertUntouched(t, c, kc, "tenant", "fn-0", "t0")
	f.set("lost", fmt.Errorf("%w: test", errBundleNotOnAnyPeer))
	settle(t, c, pod, "lost")
	if !podGone(t, kc) {
		t.Fatal("the placeholder was not deleted")
	}
	if c.Blocked(context.Background(), "t0") {
		t.Error("the checkpoint was blocked although a peer had served its version")
	}
}

// addGPUNode adds an amd64 node with GPUs (one the agents run on by
// default) whose Ready condition is ready since ago, and no agent.
func addGPUNode(t *testing.T, kc *fake.Clientset, name string, ready corev1.ConditionStatus, ago time.Duration) {
	t.Helper()
	n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{corev1.LabelArchStable: "amd64"}},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("8")},
			Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady, Status: ready, LastTransitionTime: metav1.NewTime(time.Now().Add(-ago))}},
		}}
	if _, err := kc.CoreV1().Nodes().Create(context.Background(), n, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}

// A fetch is bounded by the end of the placeholder's wait, so peers that
// accept and stall cannot stretch the wait past bundleUndecidedMax.
func TestEnsurePlaceholderBundles_FetchBoundedByTheWait(t *testing.T) {
	defer func(d time.Duration) { bundleUndecidedMax = d }(bundleUndecidedMax)
	bundleUndecidedMax = time.Hour
	c, _, pod := lostBundleFixture(t, nil)
	first := time.Now().Add(-bundleUndecidedMax + 5*time.Minute)
	c.bundleFailingSince.Store(bundleMissKey(pod.UID, "lost"), first)
	ctx, cancel := c.bundleFetchContext(context.Background(), pod, "lost")
	defer cancel()
	if d, ok := ctx.Deadline(); !ok || !d.Equal(first.Add(bundleUndecidedMax)) {
		t.Errorf("deadline = %v (%v), want the end of the wait %v", d, ok, first.Add(bundleUndecidedMax))
	}
	c.bundleFailingSince.Store(bundleMissKey(pod.UID, "lost"), time.Now().Add(-2*bundleUndecidedMax))
	ctx2, cancel2 := c.bundleFetchContext(context.Background(), pod, "lost")
	defer cancel2()
	if d, ok := ctx2.Deadline(); !ok || time.Until(d) < bundleSweepMin-time.Second {
		t.Errorf("a fetch past the wait gets %v, want at least %v", time.Until(d), bundleSweepMin)
	}
}
