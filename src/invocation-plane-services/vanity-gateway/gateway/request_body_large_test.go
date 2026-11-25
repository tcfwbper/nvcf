/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gateway

import (
	config "ai-api-gateway-service/gateway_config"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

const (
	overLimitTestHost  = "models.test"
	overLimitTestLimit = 1 << 20
	overLimitTestAgent = "OpenAI/Python 3.49.0"

	overLimitOfflineModel = "acme/offline-model"
)

// overLimitGateway serves the assembled model API router on a real listener
// with a 1 MiB body limit. It counts the client body bytes the gateway reads
// and the requests that reach the upstream.
type overLimitGateway struct {
	addr          string
	bodyBytesRead atomic.Int64
	upstreamCalls atomic.Int32
}

func newOverLimitGateway(t *testing.T) *overLimitGateway {
	t.Helper()
	g := &overLimitGateway{}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.upstreamCalls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(backend.Close)

	mappings := &config.GatewayConfig{}
	mappings.OpenAI.Host = overLimitTestHost
	mappings.OpenAI.ChatCompletions = map[string]config.ModelFunctionDetails{
		"direct":  {ModelName: prefixTestModel, FunctionID: "direct-func"},
		"offline": {ModelName: overLimitOfflineModel, FunctionID: "offline-func", OfflineMessage: "offline"},
	}
	mappings.OpenAI.ImageEdits = map[string]config.ModelFunctionDetails{
		"edits": {ModelName: prefixTestModel, FunctionID: "edits-func"},
	}
	mappings.AnthropicMessages = map[string]config.ModelFunctionDetails{
		// The upstream User-Agent differs from the client's.
		"messages": {ModelName: prefixTestModel, FunctionID: "messages-func", CustomHeaders: config.CustomHeaders{"User-Agent": "backend-adapter/2.0"}},
	}
	mux, err := buildChiMux(mappings, Config{NvcfApiEndpoint: backend.URL, MaxRequestSizeBytes: int64Ptr(overLimitTestLimit)})
	require.NoError(t, err)

	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = &countingReadCloser{ReadCloser: r.Body, read: &g.bodyBytesRead}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(gateway.Close)
	g.addr = gateway.Listener.Addr().String()
	return g
}

type countingReadCloser struct {
	io.ReadCloser
	read *atomic.Int64
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	c.read.Add(int64(n))
	return n, err
}

// overLimitCase is one request over the body limit.
type overLimitCase struct {
	name        string
	path        string
	contentType string
	// declaredLength, when set, is sent as Content-Length with no body, so
	// the gateway must answer from the header alone.
	declaredLength int64
	expect         bool
	// body is sent with chunked encoding, so its length is unknown.
	body string
}

func overLimitMultipartBody(t *testing.T, fileSize int) (string, string) {
	t.Helper()
	var buf bytes.Buffer
	form := multipart.NewWriter(&buf)
	require.NoError(t, form.WriteField("model", prefixTestModel))
	part, err := form.CreateFormFile("image", "input.png")
	require.NoError(t, err)
	_, err = io.WriteString(part, filler(fileSize))
	require.NoError(t, err)
	require.NoError(t, form.Close())
	return form.FormDataContentType(), buf.String()
}

func overLimitCases(t *testing.T) []overLimitCase {
	multipartType, multipartBody := overLimitMultipartBody(t, overLimitTestLimit)
	early := earlyBody(`{"model":"`+prefixTestModel+`","stream":false`, overLimitTestLimit)
	late := `{"input":"` + filler(overLimitTestLimit) + `","model":"` + prefixTestModel + `","stream":false}`
	offline := earlyBody(`{"model":"`+overLimitOfflineModel+`","stream":false`, overLimitTestLimit)
	modelFirst := `{"model":"` + prefixTestModel + `","input":"` + filler(overLimitTestLimit) + `","stream":false}`
	unrouted := earlyBody(`{"model":"client/any-name","stream":false`, overLimitTestLimit)
	return []overLimitCase{
		{name: "chat known length with expect", path: "/v1/chat/completions", contentType: "application/json", declaredLength: 40 << 20, expect: true},
		{name: "messages known length", path: "/v1/messages", contentType: "application/json", declaredLength: 100 << 20},
		{name: "image edits multipart known length", path: "/v1/images/edits", contentType: multipartType, declaredLength: 200 << 20, expect: true},
		{name: "chat unknown length late fields", path: "/v1/chat/completions", contentType: "application/json", body: late},
		{name: "chat unknown length model before late stream", path: "/v1/chat/completions", contentType: "application/json", body: modelFirst},
		{name: "chat unknown length offline model", path: "/v1/chat/completions", contentType: "application/json", body: offline},
		{name: "chat unknown length unrouted model", path: "/v1/chat/completions", contentType: "application/json", body: unrouted},
		{name: "image edits multipart unknown length", path: "/v1/images/edits", contentType: multipartType, body: multipartBody},
		{name: "chat unknown length early fields", path: "/v1/chat/completions", contentType: "application/json", body: early},
		{name: "messages unknown length early fields", path: "/v1/messages", contentType: "application/json", body: early},
	}
}

// send writes the request on a raw connection, so the test sees every
// interim response and controls exactly which body bytes are sent.
func (g *overLimitGateway) send(t *testing.T, tc overLimitCase) *http.Response {
	t.Helper()
	conn, err := net.Dial("tcp", g.addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))

	var head strings.Builder
	fmt.Fprintf(&head, "POST %s HTTP/1.1\r\nHost: %s\r\nContent-Type: %s\r\nUser-Agent: %s\r\nX-Request-Id: req-over-limit\r\n", tc.path, overLimitTestHost, tc.contentType, overLimitTestAgent)
	if tc.declaredLength > 0 {
		fmt.Fprintf(&head, "Content-Length: %d\r\n", tc.declaredLength)
	} else {
		head.WriteString("Transfer-Encoding: chunked\r\n")
	}
	if tc.expect {
		head.WriteString("Expect: 100-continue\r\n")
	}
	head.WriteString("Connection: close\r\n\r\n")
	_, err = io.WriteString(conn, head.String())
	require.NoError(t, err)

	if tc.body != "" {
		// The gateway may answer and close before the whole body is written,
		// so write errors are expected and ignored.
		go func() {
			w := bufio.NewWriter(conn)
			for chunk := range slices.Chunk([]byte(tc.body), 32<<10) {
				fmt.Fprintf(w, "%x\r\n%s\r\n", len(chunk), chunk)
			}
			_, _ = io.WriteString(w, "0\r\n\r\n")
			_ = w.Flush()
		}()
	}

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// dumpClientResponse renders what a client sees, without the headers that
// change per run.
func dumpClientResponse(t *testing.T, resp *http.Response) string {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var out strings.Builder
	fmt.Fprintf(&out, "%s\n", resp.Status)
	names := make([]string, 0, len(resp.Header))
	for name := range resp.Header {
		if name == "Date" {
			continue
		}
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		fmt.Fprintf(&out, "%s: %s\n", name, strings.Join(resp.Header[name], ", "))
	}
	fmt.Fprintf(&out, "\n%s", body)
	return out.String()
}

// TestOverLimitRejectionResponseUnchanged pins the exact response each over
// limit request gets, as written before rejection telemetry was added.
// Telemetry must not change a status, header or body byte.
func TestOverLimitRejectionResponseUnchanged(t *testing.T) {
	openAIResponse := "400 Bad Request\nContent-Length: 222\nContent-Type: text/plain; charset=utf-8\nNvcf-Error-Source: gateway_rejected\nVary: Origin\nX-Request-Id: req-over-limit\n\n" +
		`{"error":{"message":"Please make sure your payload is below 1048576 bytes in size. If larger assets are required please refer to our Assets API.","type":"invalid_request_error","param":null,"code":"invalid_image_format"}}` + "\n"
	messagesResponse := "413 Request Entity Too Large\nContent-Length: 114\nContent-Type: application/json\nNvcf-Error-Source: gateway_rejected\nRequest-Id: req-over-limit\nVary: Origin\nX-Request-Id: req-over-limit\n\n" +
		`{"error":{"message":"request body is too large (limit 1048576 bytes)","type":"request_too_large"},"type":"error"}` + "\n"
	want := map[string]string{
		"chat known length with expect":                openAIResponse,
		"messages known length":                        messagesResponse,
		"image edits multipart known length":           openAIResponse,
		"chat unknown length late fields":              openAIResponse,
		"chat unknown length offline model":            openAIResponse,
		"chat unknown length model before late stream": openAIResponse,
		"chat unknown length unrouted model":           openAIResponse,
		"image edits multipart unknown length":         openAIResponse,
		"chat unknown length early fields":             openAIResponse,
		"messages unknown length early fields":         messagesResponse,
	}
	for _, tc := range overLimitCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			g := newOverLimitGateway(t)
			got := dumpClientResponse(t, g.send(t, tc))
			require.Equal(t, want[tc.name], got)
		})
	}
}

// installRequestBodyMetrics points the request body counters at a manual
// reader for the test and restores the previous provider afterwards.
func installRequestBodyMetrics(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previousProvider := otel.GetMeterProvider()
	previousReads, previousLarge := requestBodyReadsCounter, requestBodyLargeCounter
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(previousProvider)
		requestBodyReadsCounter, requestBodyLargeCounter = previousReads, previousLarge
		require.NoError(t, provider.Shutdown(context.Background()))
	})
	require.NoError(t, setupRequestBodyMetrics())
	return reader
}

type largeBodyKey struct{ outcome, size string }

func collectRequestBodyLarge(t *testing.T, reader *sdkmetric.ManualReader) map[largeBodyKey]int64 {
	t.Helper()
	var resourceMetrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &resourceMetrics))
	counts := map[largeBodyKey]int64{}
	for _, scopeMetrics := range resourceMetrics.ScopeMetrics {
		for _, metric := range scopeMetrics.Metrics {
			if metric.Name != requestBodyLargeMetricName {
				continue
			}
			sum, ok := metric.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			require.True(t, sum.IsMonotonic)
			for _, point := range sum.DataPoints {
				outcome, _ := point.Attributes.Value(requestBodyLargeOutcomeLabel)
				size, _ := point.Attributes.Value(requestBodyLargeSizeLabel)
				counts[largeBodyKey{outcome.AsString(), size.AsString()}] = point.Value
			}
		}
	}
	return counts
}

// requireOnlyLargeBodyCount checks that every outcome and size pair is
// emitted and that only want has a count, of 1.
func requireOnlyLargeBodyCount(t *testing.T, counts map[largeBodyKey]int64, want largeBodyKey) {
	t.Helper()
	require.Len(t, counts, len(requestBodyLargeOutcomes)*len(requestBodySizeBuckets), "every outcome and size pair is emitted")
	for key, value := range counts {
		if key == want {
			assert.Equal(t, int64(1), value, "%v", key)
			continue
		}
		assert.Zero(t, value, "%v", key)
	}
}

func observeLogs(t *testing.T) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(zap.DebugLevel)
	undo := zap.ReplaceGlobals(zap.New(core))
	t.Cleanup(undo)
	return logs
}

// TestOverLimitRejectionTelemetry sends requests over the body limit to the
// assembled router on a real listener. Each is counted once by outcome and
// advertised size and logged once with its context. A known length is
// rejected from the header alone: no 100 Continue and no body byte read.
func TestOverLimitRejectionTelemetry(t *testing.T) {
	want := map[string]struct {
		outcome    string
		size       string
		reason     string
		model      string
		functionID string
	}{
		"chat known length with expect":      {outcome: "rejected_unread", size: "le_64MiB"},
		"messages known length":              {outcome: "rejected_unread", size: "le_128MiB"},
		"image edits multipart known length": {outcome: "rejected_unread", size: "gt_128MiB"},
		"chat unknown length late fields":    {outcome: "rejected_buffered", size: "unknown", reason: requestBodyReasonLateFields},
		// The prefix named a routed model, so the log names it and its function.
		"chat unknown length model before late stream": {outcome: "rejected_buffered", size: "unknown", reason: requestBodyReasonLateFields, model: prefixTestModel, functionID: "direct-func"},
		"chat unknown length offline model":            {outcome: "rejected_buffered", size: "unknown", reason: requestBodyReasonFunctionStatus, model: overLimitOfflineModel, functionID: "offline-func"},
		// A model name the mapping does not route is client text and is not logged.
		"chat unknown length unrouted model":   {outcome: "rejected_buffered", size: "unknown", reason: requestBodyReasonModelNotFound},
		"image edits multipart unknown length": {outcome: "rejected_buffered", size: "unknown", reason: requestBodyReasonMultipart},
		"chat unknown length early fields":     {outcome: "rejected_streamed", size: "unknown", model: prefixTestModel, functionID: "direct-func"},
		"messages unknown length early fields": {outcome: "rejected_streamed", size: "unknown", model: prefixTestModel, functionID: "messages-func"},
	}
	for _, tc := range overLimitCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			want := want[tc.name]
			reader := installRequestBodyMetrics(t)
			logs := observeLogs(t)
			g := newOverLimitGateway(t)

			resp := g.send(t, tc)
			require.NotEqual(t, http.StatusContinue, resp.StatusCode, "the gateway must not ask for the body")
			_, _ = io.Copy(io.Discard, resp.Body)

			if tc.declaredLength > 0 {
				assert.Zero(t, g.bodyBytesRead.Load(), "no body byte may be read")
				assert.Zero(t, g.upstreamCalls.Load())
			}
			requireOnlyLargeBodyCount(t, collectRequestBodyLarge(t, reader), largeBodyKey{want.outcome, want.size})

			entries := logs.FilterField(zap.String("request_id", "req-over-limit")).All()
			require.Len(t, entries, 1, "one log line per rejection")
			entry := entries[0]
			assert.Equal(t, zap.InfoLevel, entry.Level)
			assert.Equal(t, requestBodyRejectedLogMessage, entry.Message)
			fields := entry.ContextMap()
			wantFields := map[string]any{
				"request_id":  "req-over-limit",
				"outcome":     want.outcome,
				"size":        want.size,
				"limit_bytes": int64(overLimitTestLimit),
				"user_agent":  overLimitTestAgent,
			}
			if tc.declaredLength > 0 {
				wantFields["content_length"] = tc.declaredLength
			}
			if want.reason != "" {
				wantFields["body_read_reason"] = want.reason
			}
			if want.model != "" {
				wantFields["model"] = want.model
				wantFields["function_id"] = want.functionID
			}
			assert.Equal(t, wantFields, fields, "only these fields; no body and no other headers")
		})
	}
}

// TestRequestBodyLargeCounter checks that every outcome and size pair starts
// at zero and that a primary rejection counts once while a shadow replay is
// not counted.
func TestRequestBodyLargeCounter(t *testing.T) {
	reader := installRequestBodyMetrics(t)
	counts := collectRequestBodyLarge(t, reader)
	require.Len(t, counts, 20)
	for _, outcome := range []string{"rejected_unread", "rejected_buffered", "rejected_streamed", "streamed_complete"} {
		for _, size := range []string{"le_32MiB", "le_64MiB", "le_128MiB", "gt_128MiB", "unknown"} {
			value, ok := counts[largeBodyKey{outcome, size}]
			assert.True(t, ok, "%s/%s is emitted at startup", outcome, size)
			assert.Zero(t, value)
		}
	}

	logs := observeLogs(t)
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1, 2, 3},
		SpanID:  trace.SpanID{4, 5, 6},
	})
	primary := httptest.NewRequestWithContext(trace.ContextWithSpanContext(context.Background(), spanContext), http.MethodPost, "/v1/chat/completions", nil)
	primary.ContentLength = 40 << 20
	recordRequestBodyRejected(primary, requestBodyRejection{outcome: requestBodyRejectedUnread, limit: 25 << 20})
	shadow := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	shadow.Header.Set(shadowHeader, shadowHeaderValue)
	shadow.ContentLength = 40 << 20
	recordRequestBodyRejected(shadow, requestBodyRejection{outcome: requestBodyRejectedUnread, limit: 25 << 20})

	requireOnlyLargeBodyCount(t, collectRequestBodyLarge(t, reader), largeBodyKey{"rejected_unread", "le_64MiB"})
	entries := logs.FilterMessage(requestBodyRejectedLogMessage).All()
	require.Len(t, entries, 1, "shadow replays are not logged")
	assert.Equal(t, spanContext.TraceID().String(), entries[0].ContextMap()["trace_id"])
}

func TestRequestBodySizeBucket(t *testing.T) {
	const mib = 1 << 20
	tests := []struct {
		contentLength int64
		want          string
	}{
		{-1, "unknown"},
		{0, "le_32MiB"},
		{32 * mib, "le_32MiB"},
		{32*mib + 1, "le_64MiB"},
		{64 * mib, "le_64MiB"},
		{64*mib + 1, "le_128MiB"},
		{128 * mib, "le_128MiB"},
		{128*mib + 1, "gt_128MiB"},
		{1 << 62, "gt_128MiB"},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.want, requestBodySizeBucket(tc.contentLength), "%d", tc.contentLength)
	}
}

// TestClientProductVersion keeps the leading product and version of a
// User-Agent, bounded and limited to a safe character set.
func TestClientProductVersion(t *testing.T) {
	tests := []struct {
		name, userAgent, want string
	}{
		{"empty", "", ""},
		{"product and version", "curl/8.7.1", "curl/8.7.1"},
		{"product then version token", "OpenAI/Python 3.49.0", "OpenAI/Python 3.49.0"},
		{"comment dropped", "codex_cli_rs/0.167.0 (Mac OS 15.5.0; arm64) iTerm.app/3.5.0", "codex_cli_rs/0.167.0"},
		{"browser keeps first product", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/131.0", "Mozilla/5.0"},
		{"non-version second token dropped", "myapp/1.0 user=alice@example.com", "myapp/1.0"},
		{"invalid product dropped", "evil\"app\\/1.0\x00\n", ""},
		{"address as product dropped", "alice@example.com", ""},
		{"comment without a space dropped", "client/1.0(alice@example.com)", "client/1.0"},
		{"comment only", "(alice@example.com)", ""},
		{"comment after version token dropped", "client/1.0 12345(alice@example.com)", "client/1.0 12345"},
		{"leading spaces", "   node-fetch/1.0", "node-fetch/1.0"},
		{"long product cut", strings.Repeat("a", 500) + "/1.0", strings.Repeat("a", maxClientProductLength)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := clientProductVersion(tc.userAgent)
			assert.Equal(t, tc.want, got)
			assert.LessOrEqual(t, len(got), maxClientProductLength)
		})
	}
}

// TestBodyLimitInsidePrefixIsBuffered sets a limit smaller than the prefix,
// so an unknown-length body passes it before the gateway picks a path. The
// body was being read, so the rejection is buffered, not unread.
func TestBodyLimitInsidePrefixIsBuffered(t *testing.T) {
	reader := installRequestBodyMetrics(t)
	logs := observeLogs(t)
	_, handler := newPrefixTestDirector(t, 1<<10, http.DefaultTransport)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", io.MultiReader(strings.NewReader(earlyBody(`{"model":"`+prefixTestModel+`","stream":false`, 4<<10))))
	req.ContentLength = -1
	req.Header.Set(contentTypeHeader, "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	requireOnlyLargeBodyCount(t, collectRequestBodyLarge(t, reader), largeBodyKey{requestBodyRejectedBuffered, "unknown"})
	entries := logs.FilterMessage(requestBodyRejectedLogMessage).All()
	require.Len(t, entries, 1)
	assert.Equal(t, requestBodyRejectedBuffered, entries[0].ContextMap()["outcome"])
}
