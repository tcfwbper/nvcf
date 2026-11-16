// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service_test

import (
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	_ "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend/objectstore"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/service"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

// objectStore is a minimal S3-compatible server: it keeps every object a
// path-style PutObject stores.
type objectStore struct {
	mu           sync.Mutex
	objects      map[string][]byte
	traceparents []string
}

func (s *objectStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "only PutObject is served", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.objects[r.URL.Path] = body
	s.traceparents = append(s.traceparents, r.Header.Get("traceparent"))
	s.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (s *objectStore) records(t *testing.T) []string {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var lines []string
	for key, body := range s.objects {
		reader, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			t.Fatalf("object %s is not gzip: %v", key, err)
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("object %s: %v", key, err)
		}
		lines = append(lines, strings.Split(strings.TrimSpace(string(data)), "\n")...)
	}
	return lines
}

func TestObjectStoreBackendExportsEverySegmentEndToEnd(t *testing.T) {
	store := &objectStore{objects: map[string][]byte{}}
	server := httptest.NewTLSServer(store)
	t.Cleanup(server.Close)
	// The backend uses the default transport; let it trust the test
	// server's certificate for this test only.
	previous := http.DefaultTransport
	http.DefaultTransport = server.Client().Transport
	t.Cleanup(func() { http.DefaultTransport = previous })

	spool, closed := spoolWith(t,
		[]string{suppressedLine, uploadertest.RequestEnd("a", 1)},
		[]string{uploadertest.RequestEnd("b", 2)},
	)
	env := spoolEnv(t, spool.Dir, map[string]string{
		config.EnvBackend:              string(config.BackendObjectStore),
		config.EnvObjectStoreBucket:    "traces",
		config.EnvObjectStoreRegion:    "us-east-1",
		config.EnvObjectStoreEndpoint:  server.URL,
		config.EnvObjectStorePathStyle: "true",
		config.EnvSuppressNCAIDs:       "acme",
		config.EnvSuppressNCAHeaders:   ncaHeaders,
	})
	secrets := env[config.EnvSecretsFile]
	if err := os.WriteFile(secrets, []byte(`{"access_key_id":"test","secret_access_key":"test"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := loadEnv(t, env)
	client, err := backend.New(cfg)
	if err != nil {
		t.Fatalf("backend.New() error = %v", err)
	}
	metrics := newMetrics(t)
	provider := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	uploader := newUploader(t, spool, client, service.Options{Metrics: metrics, Tracer: provider.Tracer("test")}, env)

	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatalf("Pass() error = %v", err)
	}
	// Every store call goes through the core's transport: it carries the
	// trace context and is counted.
	if got, _ := metrics.Value(telemetry.BackendCalls, telemetry.Labels{"operation": "upload", "status": "2xx"}); got < 1 {
		t.Errorf("backend_calls{upload,2xx} = %v, want the store calls counted", got)
	}
	store.mu.Lock()
	for _, traceparent := range store.traceparents {
		if traceparent == "" {
			t.Error("a store call carried no trace context")
		}
	}
	store.mu.Unlock()

	got := store.records(t)
	if len(got) != 3 {
		t.Fatalf("stored records = %q, want 3", got)
	}
	joined := strings.Join(got, "\n")
	if !strings.Contains(joined, shellStub) || strings.Contains(joined, "secret") {
		t.Fatalf("stored records = %s, want the suppression stub and no suppressed body", joined)
	}
	for _, path := range closed {
		if !gone(path) {
			t.Errorf("source %s kept after the store accepted it", filepath.Base(path))
		}
	}
}
