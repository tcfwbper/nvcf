// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	collogpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry/otlp"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

// Every line is one JSON object that carries the workload's identity, so a
// line can be traced to its function and pod without the collector.
func TestLogLinesAreJSONWithContextFields(t *testing.T) {
	var out bytes.Buffer
	logger := newLogger(&out, lookupOf(map[string]string{
		"NVCF_FUNCTION_ID":         "fn-1",
		"NVCF_FUNCTION_VERSION_ID": "fv-1",
		"OTEL_RESOURCE_ATTRIBUTES": "k8s.pod.name=pod-a,k8s.cluster.name=cluster-1",
	}))
	logger.Info("prepared request trace segment", "segment", 3)

	var line map[string]any
	if err := json.Unmarshal(out.Bytes(), &line); err != nil {
		t.Fatalf("log line %q is not JSON: %v", out.String(), err)
	}
	want := map[string]any{
		"msg": "prepared request trace segment", "level": "INFO", "segment": float64(3),
		"function_id": "fn-1", "function_version_id": "fv-1", "pod": "pod-a", "cluster": "cluster-1",
	}
	for key, value := range want {
		if line[key] != value {
			t.Errorf("%s = %v, want %v (line %s)", key, line[key], value, out.String())
		}
	}
}

func TestLogLinesLeaveOutIdentityThatIsNotSet(t *testing.T) {
	var out bytes.Buffer
	newLogger(&out, lookupOf(nil)).Info("starting")
	for _, key := range []string{"function_id", "function_version_id", "pod", "cluster"} {
		if strings.Contains(out.String(), key) {
			t.Errorf("line %s has %s, want it left out when unset", out.String(), key)
		}
	}
}

// signalCollector receives OTLP traces and logs over HTTP.
type signalCollector struct {
	mu     sync.Mutex
	spans  []string
	bodies []string
}

func (c *signalCollector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	c.mu.Lock()
	defer c.mu.Unlock()
	switch r.URL.Path {
	case "/v1/traces":
		var req coltracepb.ExportTraceServiceRequest
		if proto.Unmarshal(body, &req) == nil {
			for _, rs := range req.GetResourceSpans() {
				for _, ss := range rs.GetScopeSpans() {
					for _, span := range ss.GetSpans() {
						c.spans = append(c.spans, span.GetName())
					}
				}
			}
		}
		out, _ := proto.Marshal(&coltracepb.ExportTraceServiceResponse{})
		_, _ = w.Write(out)
	case "/v1/logs":
		var req collogpb.ExportLogsServiceRequest
		if proto.Unmarshal(body, &req) == nil {
			for _, rl := range req.GetResourceLogs() {
				for _, sl := range rl.GetScopeLogs() {
					for _, record := range sl.GetLogRecords() {
						c.bodies = append(c.bodies, record.GetBody().GetStringValue())
					}
				}
			}
		}
		out, _ := proto.Marshal(&collogpb.ExportLogsServiceResponse{})
		_, _ = w.Write(out)
	}
}

func (c *signalCollector) got() (spans, bodies []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.spans...), append([]string(nil), c.bodies...)
}

// spoolWithSegment puts one closed segment in env's source directory, so a
// pass prepares and submits it.
func spoolWithSegment(t *testing.T, env map[string]string) {
	t.Helper()
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	spool.Dir = env[config.EnvSourceDir]
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("a", 1)).Bytes())
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("b", 2)).Bytes())
}

func TestRunExportsLogsAndTracesWhenTheirEndpointsAreSet(t *testing.T) {
	logs := captureLog(t)
	received := &signalCollector{}
	server := httptest.NewServer(received)
	defer server.Close()
	env := validEnv(t)
	spoolWithSegment(t, env)
	env[otlp.EnvTracesEndpoint] = server.URL + "/v1/traces"
	env[otlp.EnvTracesProtocol] = "http/protobuf"
	env[otlp.EnvLogsEndpoint] = server.URL + "/v1/logs"
	env[otlp.EnvLogsProtocol] = "http/protobuf"

	if code := serve(t, env, "test-build", WithDefaultBackend(config.BackendDebug)); code != 0 {
		t.Fatalf("exit = %d, want 0\n%s", code, logs.String())
	}
	spans, bodies := received.got()
	if !contains(spans, "request-trace-uploader.prepare") || !contains(spans, "request-trace-uploader.submit") {
		t.Errorf("spans = %v, want the prepare and submit spans", spans)
	}
	if !contains(bodies, "request trace uploader ready") {
		t.Errorf("log records = %v, want the ready line", bodies)
	}
	if !strings.Contains(logs.String(), "request trace uploader ready") {
		t.Errorf("local log = %s, want the line there too", logs.String())
	}
}

// A collector that is down costs only the telemetry: the uploader starts,
// becomes ready, and stops within the shutdown budget.
func TestRunIsNotHeldUpByADeadCollector(t *testing.T) {
	logs := captureLog(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := "http://" + listener.Addr().String()
	_ = listener.Close()
	env := validEnv(t)
	spoolWithSegment(t, env)
	for name, value := range map[string]string{
		otlp.EnvTracesEndpoint: dead + "/v1/traces", otlp.EnvTracesProtocol: "http/protobuf",
		otlp.EnvLogsEndpoint: dead + "/v1/logs", otlp.EnvLogsProtocol: "http/protobuf",
		otlp.EnvMetricsEndpoint: dead + "/v1/metrics", otlp.EnvMetricsProtocol: "http/protobuf",
	} {
		env[name] = value
	}
	started := time.Now()
	if code := serve(t, env, "test-build", WithDefaultBackend(config.BackendDebug)); code != 0 {
		t.Fatalf("exit = %d, want 0\n%s", code, logs.String())
	}
	if elapsed := time.Since(started); elapsed > 12*time.Second {
		t.Fatalf("start, run and stop took %s with a dead collector, want the 5 s flush budget kept", elapsed)
	}
}

// A secret in the secrets file never reaches a log line.
func TestRunLogsNoSecret(t *testing.T) {
	logs := captureLog(t)
	env := validEnv(t)
	if err := os.WriteFile(env[config.EnvSecretsFile], []byte(`{"token":"secret-marker-123"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	spoolWithSegment(t, env)
	if code := serve(t, env, "test-build", WithDefaultBackend(config.BackendDebug)); code != 0 {
		t.Fatalf("exit = %d, want 0\n%s", code, logs.String())
	}
	if strings.Contains(logs.String(), "secret-marker-123") {
		t.Fatalf("a log line carries the secret:\n%s", logs.String())
	}
	if strings.Contains(logs.String(), filepath.Dir(env[config.EnvSecretsFile])) {
		t.Fatalf("a log line carries a local path:\n%s", logs.String())
	}
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}
