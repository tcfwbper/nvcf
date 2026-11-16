// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package otlp

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	collogpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestSignalConfigFromEnv(t *testing.T) {
	signals := []struct {
		name               string
		read               func(func(string) (string, bool)) (SignalConfig, bool, error)
		endpoint, protocol string
	}{
		{name: "traces", read: TracesConfigFromEnv, endpoint: EnvTracesEndpoint, protocol: EnvTracesProtocol},
		{name: "logs", read: LogsConfigFromEnv, endpoint: EnvLogsEndpoint, protocol: EnvLogsProtocol},
	}
	for _, s := range signals {
		t.Run(s.name, func(t *testing.T) {
			lookup := func(env map[string]string) func(string) (string, bool) {
				return func(name string) (string, bool) { v, ok := env[name]; return v, ok }
			}
			if _, on, err := s.read(lookup(nil)); on || err != nil {
				t.Fatalf("no endpoint: on = %v err = %v, want off with no error", on, err)
			}
			if _, on, err := s.read(lookup(map[string]string{s.endpoint: " "})); on || err != nil {
				t.Fatalf("blank endpoint: on = %v err = %v, want off with no error", on, err)
			}
			_, _, err := s.read(lookup(map[string]string{s.endpoint: "http://collector:4318/v1/" + s.name}))
			if err == nil || !strings.Contains(err.Error(), s.protocol) {
				t.Fatalf("endpoint without protocol: err = %v, want it to name %s", err, s.protocol)
			}
			_, _, err = s.read(lookup(map[string]string{s.endpoint: "collector:4318", s.protocol: "grpc"}))
			if err == nil || !strings.Contains(err.Error(), s.endpoint) {
				t.Fatalf("endpoint without a scheme: err = %v, want it to name %s", err, s.endpoint)
			}
			cfg, on, err := s.read(lookup(map[string]string{s.endpoint: " http://collector:4318/v1/" + s.name + " ", s.protocol: "http"}))
			if err != nil || !on || cfg.Protocol != ProtocolHTTP || cfg.Endpoint != "http://collector:4318/v1/"+s.name {
				t.Fatalf("valid: cfg = %+v on = %v err = %v", cfg, on, err)
			}
		})
	}
}

// signalReceiver is a collector for traces and logs over both protocols.
type signalReceiver struct {
	coltracepb.UnimplementedTraceServiceServer
	collogpb.UnimplementedLogsServiceServer
	mu     sync.Mutex
	traces []*coltracepb.ExportTraceServiceRequest
	logs   []*collogpb.ExportLogsServiceRequest
}

func (r *signalReceiver) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.traces = append(r.traces, req)
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

// logsServer adapts the receiver to the logs service, whose Export method has
// the same name as the traces one.
type logsServer struct {
	collogpb.UnimplementedLogsServiceServer
	r *signalReceiver
}

func (s logsServer) Export(_ context.Context, req *collogpb.ExportLogsServiceRequest) (*collogpb.ExportLogsServiceResponse, error) {
	s.r.mu.Lock()
	defer s.r.mu.Unlock()
	s.r.logs = append(s.r.logs, req)
	return &collogpb.ExportLogsServiceResponse{}, nil
}

func (r *signalReceiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var out []byte
	switch req.URL.Path {
	case "/v1/traces":
		var export coltracepb.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &export); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		r.mu.Lock()
		r.traces = append(r.traces, &export)
		r.mu.Unlock()
		out, _ = proto.Marshal(&coltracepb.ExportTraceServiceResponse{})
	case "/v1/logs":
		var export collogpb.ExportLogsServiceRequest
		if err := proto.Unmarshal(body, &export); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		r.mu.Lock()
		r.logs = append(r.logs, &export)
		r.mu.Unlock()
		out, _ = proto.Marshal(&collogpb.ExportLogsServiceResponse{})
	default:
		http.NotFound(w, req)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	_, _ = w.Write(out)
}

// startSignalReceiver returns the receiver and the base URL to append a
// signal path to (http) or to use as is (grpc).
func startSignalReceiver(t *testing.T, protocol Protocol) (*signalReceiver, string) {
	t.Helper()
	r := &signalReceiver{}
	if protocol == ProtocolHTTP {
		server := httptest.NewServer(r)
		t.Cleanup(server.Close)
		return r, server.URL
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(server, r)
	collogpb.RegisterLogsServiceServer(server, logsServer{r: r})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return r, "http://" + listener.Addr().String()
}

func signalEndpoint(base string, protocol Protocol, path string) string {
	if protocol == ProtocolHTTP {
		return base + path
	}
	return base
}

func resourceValue(attrs []*commonpb.KeyValue, key string) string {
	for _, kv := range attrs {
		if kv.GetKey() == key {
			return kv.GetValue().GetStringValue()
		}
	}
	return ""
}

func TestTracingDeliversSpansOverEachProtocol(t *testing.T) {
	for _, protocol := range []Protocol{ProtocolHTTP, ProtocolGRPC} {
		t.Run(string(protocol), func(t *testing.T) {
			t.Setenv("OTEL_SERVICE_NAME", "")
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "k8s.pod.name=pod-t")
			r, base := startSignalReceiver(t, protocol)
			tracing, err := NewTracing(context.Background(), SignalConfig{Endpoint: signalEndpoint(base, protocol, "/v1/traces"), Protocol: protocol})
			if err != nil {
				t.Fatalf("NewTracing() error = %v", err)
			}
			_, span := tracing.Tracer().Start(context.Background(), "request-trace-uploader.test")
			span.End()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := tracing.Shutdown(ctx); err != nil {
				t.Fatalf("Shutdown() error = %v", err)
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if len(r.traces) == 0 {
				t.Fatal("the collector got no spans")
			}
			rs := r.traces[0].GetResourceSpans()[0]
			if got := rs.GetScopeSpans()[0].GetSpans()[0].GetName(); got != "request-trace-uploader.test" {
				t.Errorf("span name = %q", got)
			}
			if resourceValue(rs.GetResource().GetAttributes(), "service.name") != ServiceName || resourceValue(rs.GetResource().GetAttributes(), "k8s.pod.name") != "pod-t" {
				t.Errorf("resource = %v, want the shared resource", rs.GetResource().GetAttributes())
			}
		})
	}
}

func TestLogsDeliverRecordsOverEachProtocol(t *testing.T) {
	for _, protocol := range []Protocol{ProtocolHTTP, ProtocolGRPC} {
		t.Run(string(protocol), func(t *testing.T) {
			t.Setenv("OTEL_SERVICE_NAME", "uploader-logs")
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
			r, base := startSignalReceiver(t, protocol)
			logs, err := NewLogs(context.Background(), SignalConfig{Endpoint: signalEndpoint(base, protocol, "/v1/logs"), Protocol: protocol})
			if err != nil {
				t.Fatalf("NewLogs() error = %v", err)
			}
			slog.New(logs.Handler()).Warn("submit failed", "batch", "b-1")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := logs.Shutdown(ctx); err != nil {
				t.Fatalf("Shutdown() error = %v", err)
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if len(r.logs) == 0 {
				t.Fatal("the collector got no log records")
			}
			rl := r.logs[0].GetResourceLogs()[0]
			record := rl.GetScopeLogs()[0].GetLogRecords()[0]
			if record.GetBody().GetStringValue() != "submit failed" || resourceValue(record.GetAttributes(), "batch") != "b-1" {
				t.Errorf("record = %v, want the message and its attributes", record)
			}
			if got := resourceValue(rl.GetResource().GetAttributes(), "service.name"); got != "uploader-logs" {
				t.Errorf("service.name = %q, want the env override", got)
			}
		})
	}
}

// Queue and batch settings for the SDK processors are fixed here, so a
// hostile or mistaken OTEL_BSP_* or OTEL_BLRP_* value cannot stop startup.
func TestSignalsIgnoreProcessorEnvironment(t *testing.T) {
	for name, value := range map[string]string{
		"OTEL_BSP_MAX_QUEUE_SIZE": "-1", "OTEL_BSP_MAX_EXPORT_BATCH_SIZE": "-1",
		"OTEL_BLRP_MAX_QUEUE_SIZE": "-1", "OTEL_BLRP_MAX_EXPORT_BATCH_SIZE": "-1",
	} {
		t.Setenv(name, value)
	}
	r, base := startSignalReceiver(t, ProtocolHTTP)
	tracing, err := NewTracing(context.Background(), SignalConfig{Endpoint: base + "/v1/traces", Protocol: ProtocolHTTP})
	if err != nil {
		t.Fatal(err)
	}
	logs, err := NewLogs(context.Background(), SignalConfig{Endpoint: base + "/v1/logs", Protocol: ProtocolHTTP})
	if err != nil {
		t.Fatal(err)
	}
	_, span := tracing.Tracer().Start(context.Background(), "request-trace-uploader.test")
	span.End()
	slog.New(logs.Handler()).Info("ready")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tracing.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := logs.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.traces) == 0 || len(r.logs) == 0 {
		t.Fatalf("traces = %d logs = %d, want both delivered", len(r.traces), len(r.logs))
	}
}

// The function identity NVCF sets in the pod reaches the shared resource
// when the chart's resource attributes leave it out, as it reaches the
// local log lines.
func TestResourceCarriesTheNVCFFunctionIdentity(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "k8s.pod.name=pod-n")
	t.Setenv("OTEL_SERVICE_NAME", "")
	t.Setenv("NVCF_FUNCTION_ID", "fn-9")
	t.Setenv("NVCF_FUNCTION_VERSION_ID", "fv-9")
	r, base := startSignalReceiver(t, ProtocolHTTP)
	tracing, err := NewTracing(context.Background(), SignalConfig{Endpoint: base + "/v1/traces", Protocol: ProtocolHTTP})
	if err != nil {
		t.Fatal(err)
	}
	_, span := tracing.Tracer().Start(context.Background(), "request-trace-uploader.test")
	span.End()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tracing.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	attrs := r.traces[0].GetResourceSpans()[0].GetResource().GetAttributes()
	for key, want := range map[string]string{"nvcf.function.id": "fn-9", "nvcf.function.version.id": "fv-9", "k8s.pod.name": "pod-n"} {
		if got := resourceValue(attrs, key); got != want {
			t.Errorf("resource %s = %q, want %q", key, got, want)
		}
	}
}
