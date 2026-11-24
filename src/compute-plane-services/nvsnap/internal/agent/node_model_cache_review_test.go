// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace/noop"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/modelcache"
)

// The cache starts before the agent has its own client. Its tenant scope
// must still read the tenant label, not fall back for good to one tenant
// per namespace.
func TestModelCacheTenantScopeReadsTheLabelBeforeTheAgentHasAClient(t *testing.T) {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-ns", Labels: map[string]string{"org": "acme"}}}
	kube := fake.NewSimpleClientset(ns)
	prev := buildKubeClient
	buildKubeClient = func() (kubernetes.Interface, error) { return kube, nil }
	defer func() { buildKubeClient = prev }()
	a := &Agent{log: logrus.New(), config: Config{AuthMode: AuthRequired, NodeName: "a", TenantLabel: "org",
		NodeModelCache: NodeModelCacheConfig{Enabled: true, Root: t.TempDir()}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.startNodeModelCache(ctx, mux.NewRouter())
	if got := a.tenantScope("team-ns"); got != "tenant:org=acme" {
		t.Fatalf("tenant scope = %q, want tenant:org=acme", got)
	}
}

// A key being evicted is not served: its files go while the transfer
// would read them.
func TestModelCacheDoesNotServeAKeyBeingEvicted(t *testing.T) {
	kube := fake.NewSimpleClientset()
	a := newMCNode(t, kube, "a", map[string]string{})
	src := filepath.Join(t.TempDir(), "m")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "w.bin"), []byte("0123456789"), 0o644); err != nil {
		t.Fatal(err)
	}
	key := "0123456789abcdef0123456789abcdef"
	gen, err := a.c.store.Harvest(key, src, modelcache.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.c.register(context.Background(), key, filepath.Base(gen)); err != nil {
		t.Fatal(err)
	}
	// Hold the eviction in its deregistration, after it claimed the key
	// and before its files go.
	entered, release := make(chan struct{}), make(chan struct{})
	first := true
	block := func(k8stesting.Action) (bool, runtime.Object, error) {
		if first {
			first = false
			close(entered)
			<-release
		}
		return false, nil, nil
	}
	kube.PrependReactor("update", "configmaps", block)
	kube.PrependReactor("delete", "configmaps", block)
	a.c.statfs = func(string) (int64, int64, error) { return 30, 30, nil }
	done := make(chan struct{})
	go func() { defer close(done); a.c.evict(context.Background(), 1) }()
	<-entered

	base := a.srv.URL + "/v1/model-cache/" + key
	for _, u := range []string{base, base + "/" + filepath.Base(gen) + "/file?path=w.bin"} {
		resp, err := http.Get(u)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			close(release)
			t.Fatalf("%s served while the key is being evicted", u)
		}
	}
	close(release)
	<-done
	if a.has(key) {
		t.Fatal("the key was not evicted")
	}
	// Once evicted and harvested again, the key is served again.
	if _, err := a.c.store.Harvest(key, src, modelcache.Limits{}); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get(base)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("after the eviction the key's manifest is %s", resp.Status)
	}
}

// A peer fetch is traced: where the sources were found, which peer was
// asked, and how it ended.
func TestModelCachePeerFetchIsTraced(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	defer otel.SetTracerProvider(noop.NewTracerProvider())

	kube := fake.NewSimpleClientset()
	urls := map[string]string{}
	a, b := newMCNode(t, kube, "a", urls), newMCNode(t, kube, "b", urls)
	src := filepath.Join(t.TempDir(), "m")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "w.bin"), []byte("weights"), 0o644); err != nil {
		t.Fatal(err)
	}
	key := "0123456789abcdef0123456789abcdef"
	gen, err := a.c.store.Harvest(key, src, modelcache.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.c.register(context.Background(), key, filepath.Base(gen)); err != nil {
		t.Fatal(err)
	}
	b.c.fill(context.Background(), key)
	if !b.has(key) {
		t.Fatal("the fill did not fetch the key")
	}
	if _, _, err := b.c.fillFrom(context.Background(), key, "gone"); err == nil {
		t.Fatal("a fetch from a peer with no address succeeded")
	}

	var sources, ok, failed int
	for _, s := range rec.Ended() {
		attrs := map[string]string{}
		for _, kv := range s.Attributes() {
			attrs[string(kv.Key)] = kv.Value.Emit()
		}
		switch s.Name() {
		case "model_cache.sources":
			if attrs["nvsnap.model_cache.key"] == key {
				sources++
			}
		case "model_cache.peer_fetch":
			if attrs["nvsnap.model_cache.key"] != key {
				t.Fatalf("peer fetch span without the key: %v", attrs)
			}
			switch attrs["nvsnap.peer"] {
			case "a":
				if s.Status().Code == codes.Error {
					t.Fatalf("the fetch from a is marked failed: %v", s.Status())
				}
				ok++
			case "gone":
				if s.Status().Code != codes.Error || len(s.Events()) == 0 {
					t.Fatal("the failed fetch's span records no error")
				}
				failed++
			}
		}
	}
	if sources == 0 || ok != 1 || failed != 1 {
		t.Fatalf("spans: %d sources, %d fetches from a, %d failed fetches; want at least 1, 1, 1", sources, ok, failed)
	}
}
