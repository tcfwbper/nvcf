// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend/backendtest"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/service"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

// boundedSpanAttributes are the only span attributes the pipeline sets. None
// can hold an id, a path or a value from a record.
var boundedSpanAttributes = []string{"backend", "status", "class", "scope", "reason", "operation"}

func tracedUploader(t *testing.T, client backend.Client, metrics telemetry.Metrics) (*service.Uploader, *tracetest.SpanRecorder) {
	t.Helper()
	spool, _ := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	opts := service.Options{Tracer: provider.Tracer("test")}
	if metrics != nil {
		opts.Metrics = metrics
	}
	return newUploader(t, spool, client, opts, nil), recorder
}

func spansNamed(recorder *tracetest.SpanRecorder, name string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, span := range recorder.Ended() {
		if span.Name() == name {
			out = append(out, span)
		}
	}
	return out
}

func TestPipelineSpansCarryOnlyBoundedAttributes(t *testing.T) {
	uploader, recorder := tracedUploader(t, backendtest.NewFake(), nil)
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := uploader.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"request-trace-uploader.prepare", "request-trace-uploader.submit", "request-trace-uploader.poll"} {
		if len(spansNamed(recorder, name)) == 0 {
			t.Errorf("no %s span", name)
		}
	}
	for _, span := range recorder.Ended() {
		for _, kv := range span.Attributes() {
			if !slices.Contains(boundedSpanAttributes, string(kv.Key)) {
				t.Errorf("span %s has attribute %s=%v, want only %v", span.Name(), kv.Key, kv.Value.Emit(), boundedSpanAttributes)
			}
		}
		if span.Status().Code == codes.Error {
			t.Errorf("span %s has error status on a clean run", span.Name())
		}
	}
}

func TestAFailedSubmitSpanHasErrorStatus(t *testing.T) {
	fake := backendtest.NewFake()
	fake.Script(backendtest.SubmitStep{Outcome: backendtest.Refuse, Err: &backend.Fault{Class: backend.ClassRetryable, Scope: backend.ScopeBackend, Reason: backend.ReasonUnavailable}})
	uploader, recorder := tracedUploader(t, fake, nil)
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	spans := spansNamed(recorder, "request-trace-uploader.submit")
	if len(spans) == 0 {
		t.Fatal("no submit span")
	}
	span := spans[0]
	if span.Status().Code != codes.Error || span.Status().Description != string(backend.ReasonUnavailable) {
		t.Fatalf("status = %+v, want error with the bounded reason only", span.Status())
	}
	attrs := map[string]string{}
	for _, kv := range span.Attributes() {
		attrs[string(kv.Key)] = kv.Value.Emit()
	}
	if attrs["class"] != "retryable" || attrs["scope"] != "backend" || attrs["reason"] != "unavailable" || attrs["status"] != "failure" {
		t.Fatalf("attributes = %v, want class, scope, reason and status failure", attrs)
	}
	if len(span.Events()) != 0 {
		t.Fatalf("events = %v, want none: an error event would carry its text", span.Events())
	}
}

// httpBackend is a backend that calls its destination over HTTP through the
// transport the uploader hands it.
type httpBackend struct {
	*backendtest.Fake
	url    string
	client *http.Client
}

func (b *httpBackend) UseTransport(wrap telemetry.TransportWrapper) {
	b.client = &http.Client{Transport: wrap("submit", http.DefaultTransport)}
}

func (b *httpBackend) Submit(ctx context.Context, request backend.SubmitRequest) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.url, nil)
	if err != nil {
		return "", err
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return "", err
	}
	_ = resp.Body.Close()
	return b.Fake.Submit(ctx, request)
}

func TestABackendHTTPCallIsAChildSpanAndCounted(t *testing.T) {
	var traceparent string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceparent = r.Header.Get("traceparent")
	}))
	defer server.Close()
	metrics := newMetrics(t)
	client := &httpBackend{Fake: backendtest.NewFake(), url: server.URL}
	uploader, recorder := tracedUploader(t, client, metrics)
	if client.client == nil {
		t.Fatal("the backend was not given a transport")
	}
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}

	calls := spansNamed(recorder, "request-trace-uploader.backend_call")
	submits := spansNamed(recorder, "request-trace-uploader.submit")
	if len(calls) != 1 || len(submits) != 1 {
		t.Fatalf("backend_call spans = %d submit spans = %d, want one each", len(calls), len(submits))
	}
	if calls[0].Parent().SpanID() != submits[0].SpanContext().SpanID() {
		t.Fatal("the backend call is not a child of the submit span")
	}
	if traceparent == "" {
		t.Fatal("the call did not carry the trace context")
	}
	if got, _ := metrics.Value(telemetry.BackendCalls, telemetry.Labels{"operation": "submit", "status": "2xx"}); got != 1 {
		t.Fatalf("backend_calls{submit,2xx} = %v, want 1", got)
	}
	if count, _ := samples(metrics, telemetry.BackendCallDuration, telemetry.Labels{"operation": "submit", "status": "2xx"}); count != 1 {
		t.Fatalf("backend_call.duration samples = %d, want 1", count)
	}
}

// A transport the wrapper returns can be called directly with a request that
// has no header map; it still adds the trace context, leaves the caller's
// request unchanged, and calls the next transport.
func TestTheBackendTransportHandlesARequestWithNoHeaderMap(t *testing.T) {
	var got http.Header
	next := roundTripper(func(req *http.Request) (*http.Response, error) {
		got = req.Header
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Request: req}, nil
	})
	var wrap telemetry.TransportWrapper
	client := &transportCatcher{Fake: backendtest.NewFake(), got: &wrap}
	_, _ = tracedUploader(t, client, nil)
	provider := sdktrace.NewTracerProvider()
	ctx, span := provider.Tracer("test").Start(context.Background(), "parent")
	defer span.End()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://store.invalid/", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header = nil
	resp, err := wrap("upload", next).RoundTrip(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("RoundTrip() = %v, %v", resp, err)
	}
	if req.Header != nil {
		t.Fatal("the caller's request was changed")
	}
	if got.Get("traceparent") == "" {
		t.Fatal("the next transport got no trace context")
	}
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// transportCatcher keeps the wrapper the uploader hands it.
type transportCatcher struct {
	*backendtest.Fake
	got *telemetry.TransportWrapper
}

func (c *transportCatcher) UseTransport(wrap telemetry.TransportWrapper) { *c.got = wrap }
