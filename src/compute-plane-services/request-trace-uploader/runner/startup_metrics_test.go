// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry/otlp"
)

// failureCollector keeps the latest cumulative startup_failures value of
// each backend and reason an OTLP/HTTP export carried.
type failureCollector struct {
	mu     sync.Mutex
	values map[string]float64
}

func (c *failureCollector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var export colmetricpb.ExportMetricsServiceRequest
	if err := proto.Unmarshal(body, &export); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	for _, rm := range export.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				if m.GetName() != otlp.MetricPrefix+telemetry.StartupFailures {
					continue
				}
				for _, point := range m.GetSum().GetDataPoints() {
					var reason, backend string
					for _, kv := range point.GetAttributes() {
						switch kv.GetKey() {
						case "reason":
							reason = kv.GetValue().GetStringValue()
						case telemetry.BackendLabel:
							backend = kv.GetValue().GetStringValue()
						}
					}
					c.values[backend+"/"+reason] = point.GetAsDouble()
				}
			}
		}
	}
	c.mu.Unlock()
	out, _ := proto.Marshal(&colmetricpb.ExportMetricsServiceResponse{})
	_, _ = w.Write(out)
}

func (c *failureCollector) value(key string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.values[key]
}

func (c *failureCollector) keys() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for key, value := range c.values {
		if value > 0 {
			out = append(out, key)
		}
	}
	return strings.Join(out, ",")
}

// A stuck uploader says so: every failed startup attempt adds one to
// startup_failures under its reason, on one counter for the life of the
// process, so an alert on its increase sees the failures pile up. A refused
// setting is counted too, because metrics start before the uploader
// settings load.
func TestRunCountsEveryFailedStartupByReason(t *testing.T) {
	tests := map[string]struct {
		change  func(t *testing.T, env map[string]string)
		wantKey string
	}{
		"invalid setting": {
			change:  func(_ *testing.T, env map[string]string) { env[config.EnvScanInterval] = "soon" },
			wantKey: "debug/config",
		},
		"backend this build does not link": {
			change:  func(_ *testing.T, env map[string]string) { env[config.EnvBackend] = "absent" },
			wantKey: "unknown/config",
		},
		"invalid log export setting": {
			change: func(_ *testing.T, env map[string]string) {
				env[otlp.EnvLogsEndpoint] = "http://127.0.0.1:1/v1/logs"
			},
			wantKey: "debug/config",
		},
		"state dir that cannot be created": {
			change: func(t *testing.T, env map[string]string) {
				file := filepath.Join(t.TempDir(), "a-file")
				if err := os.WriteFile(file, nil, 0o600); err != nil {
					t.Fatal(err)
				}
				env[config.EnvStateDir] = filepath.Join(file, "state")
			},
			wantKey: "debug/spool",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			fastStartRetry(t)
			logs := captureLog(t)
			received := &failureCollector{values: map[string]float64{}}
			server := httptest.NewServer(received)
			defer server.Close()
			env := validEnv(t)
			env[otlp.EnvMetricsEndpoint] = server.URL + "/v1/metrics"
			env[otlp.EnvMetricsProtocol] = "http/protobuf"
			tt.change(t, env)
			ctx, cancel := context.WithCancel(context.Background())
			exited := make(chan int, 1)
			go func() {
				exited <- run(ctx, lookupOf(env), environOf(env), "test-build", collect([]Option{WithDefaultBackend(config.BackendDebug)}))
			}()
			const retried = `"msg":"request trace uploader did not start; retrying"`
			deadline := time.Now().Add(5 * time.Second)
			for strings.Count(logs.String(), retried) < 3 {
				if time.Now().After(deadline) {
					cancel()
					<-exited
					t.Fatalf("fewer than 3 retries in 5 s\n%s", logs.String())
				}
				time.Sleep(10 * time.Millisecond)
			}
			cancel()
			<-exited
			// The last attempt may fail after shutdown began, without a
			// retry logged, so the count is the retries or one more.
			attempts := float64(strings.Count(logs.String(), retried))
			got := received.value(tt.wantKey)
			if got != attempts && got != attempts+1 {
				t.Fatalf("startup_failures %s = %v after %v failed attempts, want one per attempt on one counter; all: [%s]", tt.wantKey, got, attempts, received.keys())
			}
			if keys := received.keys(); keys != tt.wantKey {
				t.Fatalf("startup_failures counted [%s], want only %s", keys, tt.wantKey)
			}
		})
	}
}

// An unreadable journal keeps the process live and not ready by design; it
// is not a startup attempt that failed, so nothing is counted.
func TestRunCountsNoStartupFailureForAnUnreadableJournal(t *testing.T) {
	logs := captureLog(t)
	received := &failureCollector{values: map[string]float64{}}
	server := httptest.NewServer(received)
	defer server.Close()
	env := validEnv(t)
	env[otlp.EnvMetricsEndpoint] = server.URL + "/v1/metrics"
	env[otlp.EnvMetricsProtocol] = "http/protobuf"
	segments := filepath.Join(env[config.EnvSourceDir], "request-trace-uploader-state", "segments")
	if err := os.MkdirAll(segments, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(segments, "no-segment-name.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan int, 1)
	go func() {
		exited <- run(ctx, lookupOf(env), environOf(env), "test-build", collect([]Option{WithDefaultBackend(config.BackendDebug)}))
	}()
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(logs.String(), "an operator must look at the journal") {
		if time.Now().After(deadline) {
			cancel()
			<-exited
			t.Fatalf("journal not rejected in 5 s\n%s", logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-exited
	if keys := received.keys(); keys != "" {
		t.Fatalf("startup_failures counted [%s] for an unreadable journal", keys)
	}
}

func TestRunCountsNoStartupFailureWhenItStarts(t *testing.T) {
	logs := captureLog(t)
	received := &failureCollector{values: map[string]float64{}}
	server := httptest.NewServer(received)
	defer server.Close()
	env := validEnv(t)
	env[otlp.EnvMetricsEndpoint] = server.URL + "/v1/metrics"
	env[otlp.EnvMetricsProtocol] = "http/protobuf"
	if code := serve(t, env, "test-build", WithDefaultBackend(config.BackendDebug)); code != 0 {
		t.Fatalf("exit = %d, want 0\n%s", code, logs.String())
	}
	if keys := received.keys(); keys != "" {
		t.Fatalf("startup_failures counted [%s] for an uploader that started", keys)
	}
}

// A missing secrets file is not a startup failure: the uploader starts and
// the backend waits for the file, so nothing is counted.
func TestRunStartsAndCountsNothingWithoutTheSecretsFile(t *testing.T) {
	logs := captureLog(t)
	received := &failureCollector{values: map[string]float64{}}
	server := httptest.NewServer(received)
	defer server.Close()
	env := validEnv(t)
	env[otlp.EnvMetricsEndpoint] = server.URL + "/v1/metrics"
	env[otlp.EnvMetricsProtocol] = "http/protobuf"
	if err := os.Remove(env[config.EnvSecretsFile]); err != nil {
		t.Fatal(err)
	}
	if code := serve(t, env, "test-build", WithDefaultBackend(config.BackendDebug)); code != 0 {
		t.Fatalf("exit = %d, want 0\n%s", code, logs.String())
	}
	if keys := received.keys(); keys != "" {
		t.Fatalf("startup_failures counted [%s] without the secrets file", keys)
	}
}
