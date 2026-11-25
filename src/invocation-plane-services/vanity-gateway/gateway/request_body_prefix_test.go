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
	"bytes"
	"context"
	"crypto/sha256"
	stdjson "encoding/json"
	"errors"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/bytebufferpool"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const prefixTestModel = "acme/early-model"

// filler returns n bytes that are valid inside a JSON string.
func filler(n int) string {
	return strings.Repeat("a", n)
}

// earlyBody puts the routing fields first and pads the body past the prefix.
func earlyBody(head string, size int) string {
	return head + `,"input":"` + filler(size) + `"}`
}

func directMappings() *config.GatewayConfig {
	cfg := &config.GatewayConfig{}
	cfg.OpenAI.ChatCompletions = map[string]config.ModelFunctionDetails{
		"direct":  {ModelName: prefixTestModel, FunctionID: "direct-func"},
		"shadow":  {ModelName: "acme/shadowed", FunctionID: "shadowed-func", ShadowModelName: prefixTestModel},
		"llm":     {ModelName: "acme/llm", FunctionID: "llm-func", FunctionType: config.FunctionTypeLLM},
		"offline": {ModelName: "acme/offline", FunctionID: "offline-func", OfflineMessage: "offline"},
		"expired": {ModelName: "acme/expired", FunctionID: "expired-func", EOL: time.Now().Add(-time.Hour)},
	}
	cfg.OpenAI.Decisions = map[string]config.ModelFunctionDetails{
		"decision": {ModelName: prefixTestModel, FunctionID: "decision-func"},
	}
	cfg.AnthropicMessages = map[string]config.ModelFunctionDetails{
		"messages": {ModelName: prefixTestModel, FunctionID: "messages-func"},
	}
	return cfg
}

// newPrefixTestDirector builds a director whose NVCF and LLM Gateway
// upstreams use transport, and routes chat, messages and decisions behind the
// same body limit middleware as the model API host.
func newPrefixTestDirector(t testing.TB, limit int64, transport http.RoundTripper) (*ModelDirector, http.Handler) {
	t.Helper()
	return newPrefixTestDirectorWithStreamedLimit(t, limit, limit, transport)
}

// newPrefixTestDirectorWithStreamedLimit is newPrefixTestDirector with a
// separate limit for streamed requests.
func newPrefixTestDirectorWithStreamedLimit(t testing.TB, limit, streamedLimit int64, transport http.RoundTripper) (*ModelDirector, http.Handler) {
	t.Helper()
	vanity, err := NewVanityDirector("http://nvcf.test", transport)
	require.NoError(t, err)
	llm, err := NewLLMGatewayDirector("http://llm.test", transport)
	require.NoError(t, err)
	director, err := NewModelDirector(directMappings(), regexp.MustCompile("^$"), vanity, llm, NewTrafficShadower(64, time.Minute))
	require.NoError(t, err)
	director.requestBodyLimit = limit
	director.streamedRequestBodyLimit = streamedLimit

	router := chi.NewRouter()
	sized := limitModelRequestBody(limit)
	router.Method(http.MethodPost, "/v1/chat/completions", sized(http.HandlerFunc(director.ServeChatCompletions)))
	router.Method(http.MethodPost, "/v1/messages", sized(http.HandlerFunc(director.ServeMessages)))
	router.Method(http.MethodPost, decisionsPath, sized(http.HandlerFunc(director.ServeDecisions)))
	return director, router
}

func readBody(t testing.TB, director *ModelDirector, path, contentType, body string) (openAIRequestBody, requestBodyMode, *http.Request, error) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set(contentTypeHeader, contentType)
	}
	if path == decisionsPath {
		req = withNativeProtocol(req, nativeProtocolDecisions)
	}
	if path == "/v1/messages" {
		req = withNativeProtocol(req, nativeProtocolMessages)
	}
	mapping := director.chatCompletions.modelNameToNVCFUrl
	if path == decisionsPath {
		mapping = director.decisions.modelNameToNVCFUrl
	}
	if path == "/v1/messages" {
		mapping = director.messages.modelNameToNVCFUrl
	}
	parsed, mode, err := director.readModelRequestBody(req, mapping)
	return parsed, mode, req, err
}

// TestReadModelRequestBodyChoosesPath names the read mode for each kind of
// request. Only an eligible direct route with early fields streams.
func TestReadModelRequestBodyChoosesPath(t *testing.T) {
	director, _ := newPrefixTestDirector(t, 25<<20, http.DefaultTransport)
	large := requestPrefixLimit
	multipartBody, multipartType := largeMultipartModelBody(t, prefixTestModel, large)

	tests := []struct {
		name        string
		path        string
		contentType string
		body        string
		mode        requestBodyMode
		model       string
		stream      bool
	}{
		{"early fields stream", "/v1/chat/completions", "application/json", earlyBody(`{"model":"`+prefixTestModel+`","stream":true`, large), streamedBodyRead(), prefixTestModel, true},
		{"stream false first", "/v1/chat/completions", "application/json", earlyBody(`{"stream":false,"model":"`+prefixTestModel+`"`, large), streamedBodyRead(), prefixTestModel, false},
		{"whitespace and case", "/v1/chat/completions", "", earlyBody(" {\n \"MODEL\" : \""+prefixTestModel+"\" ,\t\"Stream\":true", large), streamedBodyRead(), prefixTestModel, true},
		{"small skipped members", "/v1/chat/completions", "", earlyBody(`{"temperature":0.5,"tools":[{"a":"}]\""}],"model":"`+prefixTestModel+`","stream":true`, large), streamedBodyRead(), prefixTestModel, true},
		{"first model wins", "/v1/chat/completions", "", earlyBody(`{"model":"`+prefixTestModel+`","Model":"acme/llm","model":{"x":1},"stream":true`, large), streamedBodyRead(), prefixTestModel, true},
		{"decision route needs only model", decisionsPath, "application/json", earlyBody(`{"model":"`+prefixTestModel+`"`, large), streamedBodyRead(), prefixTestModel, false},
		{"decision route ignores stream type", decisionsPath, "application/json", earlyBody(`{"stream":"yes","model":"`+prefixTestModel+`"`, large), streamedBodyRead(), prefixTestModel, false},
		{"messages route", "/v1/messages", "application/json", earlyBody(`{"model":"`+prefixTestModel+`","stream":true`, large), streamedBodyRead(), prefixTestModel, true},
		{"member nested within the decoder limit", "/v1/chat/completions", "", earlyBody(`{"x":`+nestedArray(9999)+`,"model":"`+prefixTestModel+`","stream":true`, large), streamedBodyRead(), prefixTestModel, true},

		{"complete small body", "/v1/chat/completions", "", `{"model":"` + prefixTestModel + `","stream":true}`, bufferedBodyRead(requestBodyReasonComplete), prefixTestModel, true},
		{"known length of exactly the prefix", "/v1/chat/completions", "", exactPrefixBody(), bufferedBodyRead(requestBodyReasonComplete), prefixTestModel, true},
		{"absent stream", "/v1/chat/completions", "", earlyBody(`{"model":"`+prefixTestModel+`"`, large), bufferedBodyRead(requestBodyReasonLateFields), prefixTestModel, false},
		{"late model", "/v1/chat/completions", "", `{"input":"` + filler(large) + `","model":"` + prefixTestModel + `","stream":true}`, bufferedBodyRead(requestBodyReasonLateFields), prefixTestModel, true},
		{"escaped key", "/v1/chat/completions", "", earlyBody(`{"mod\u0065l":"`+prefixTestModel+`","stream":true`, large), bufferedBodyRead(requestBodyReasonNonstandardFields), prefixTestModel, true},
		{"unicode folded key", "/v1/chat/completions", "", earlyBody(`{"model":"`+prefixTestModel+`","`+"\u017f"+`tream":true,"stream":true`, large), bufferedBodyRead(requestBodyReasonNonstandardFields), prefixTestModel, true},
		{"escaped model value", "/v1/chat/completions", "", earlyBody(`{"model":"acme\/early-model","stream":true`, large), bufferedBodyRead(requestBodyReasonNonstandardFields), prefixTestModel, true},
		{"null stream", "/v1/chat/completions", "", earlyBody(`{"stream":null,"stream":true,"model":"`+prefixTestModel+`"`, large), bufferedBodyRead(requestBodyReasonNonstandardFields), prefixTestModel, false},
		{"array body", "/v1/chat/completions", "", `[` + filler(large) + `]`, bufferedBodyRead(requestBodyReasonInvalidPrefix), "", false},
		{"multipart", "/v1/chat/completions", multipartType, multipartBody, bufferedBodyRead(requestBodyReasonMultipart), prefixTestModel, false},
		{"model not found", "/v1/chat/completions", "", earlyBody(`{"model":"acme/missing","stream":true`, large), bufferedBodyRead(requestBodyReasonModelNotFound), "acme/missing", true},
		{"offline", "/v1/chat/completions", "", earlyBody(`{"model":"acme/offline","stream":true`, large), bufferedBodyRead(requestBodyReasonFunctionStatus), "acme/offline", true},
		{"expired", "/v1/chat/completions", "", earlyBody(`{"model":"acme/expired","stream":true`, large), bufferedBodyRead(requestBodyReasonFunctionStatus), "acme/expired", true},
		{"llm gateway rewrite", "/v1/chat/completions", "", earlyBody(`{"model":"acme/llm","stream":true`, large), bufferedBodyRead(requestBodyReasonLLMGateway), "acme/llm", true},
		{"shadowed", "/v1/chat/completions", "", earlyBody(`{"model":"acme/shadowed","stream":true`, large), bufferedBodyRead(requestBodyReasonShadow), "acme/shadowed", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parsed, mode, req, err := readBody(t, director, tc.path, tc.contentType, tc.body)
			if tc.mode.reason == requestBodyReasonInvalidPrefix {
				require.ErrorIs(t, err, errInvalidJSONBody)
				assert.Equal(t, tc.mode, mode)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.mode, mode)
			assert.Equal(t, tc.model, parsed.Model)
			assert.Equal(t, tc.stream, parsed.Stream)

			if mode.mode == requestBodyModeStreamed {
				assert.Nil(t, parsed.raw, "the streamed path must not expose a complete body to later stages")
				assert.IsType(t, &prefixReplayBody{}, req.Body)
			}
			forwarded, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			require.NoError(t, req.Body.Close())
			assert.Equal(t, tc.body, string(forwarded), "the forwarded bytes must equal the client bytes")
		})
	}
}

// exactPrefixBody is a valid early-field body of exactly requestPrefixLimit
// bytes.
func exactPrefixBody() string {
	head := `{"model":"` + prefixTestModel + `","stream":true,"input":"`
	return head + filler(requestPrefixLimit-len(head)-2) + `"}`
}

// nestedArray returns an empty array nested depth levels deep. The JSON
// decoders accept 10000 levels in a document, and the top-level object is one
// of them, so a member may nest 9999 levels.
func nestedArray(depth int) string {
	return strings.Repeat("[", depth) + strings.Repeat("]", depth)
}

func largeMultipartModelBody(t *testing.T, model string, fileSize int) (string, string) {
	t.Helper()
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	require.NoError(t, writer.WriteField("model", model))
	part, err := writer.CreateFormFile("image", "image.png")
	require.NoError(t, err)
	_, err = part.Write([]byte(filler(fileSize)))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return buf.String(), writer.FormDataContentType()
}

// TestReadModelRequestBodyKeepsCompleteBodyErrors checks that a body that
// fails the complete-body decoder fails the same way whichever path scanned
// it first.
func TestReadModelRequestBodyKeepsCompleteBodyErrors(t *testing.T) {
	director, _ := newPrefixTestDirector(t, 25<<20, http.DefaultTransport)
	large := requestPrefixLimit
	tests := []struct {
		name string
		body string
		err  error
	}{
		{"model wrong type", earlyBody(`{"model":1,"stream":true`, large), errInvalidJSONBody},
		{"model null first", earlyBody(`{"model":null,"model":"`+prefixTestModel+`","stream":true`, large), errModelFieldMissing},
		{"model empty", earlyBody(`{"model":"","stream":true`, large), errModelFieldMissing},
		{"stream wrong type", earlyBody(`{"model":"`+prefixTestModel+`","stream":"yes"`, large), errInvalidJSONBody},
		{"syntax error before fields", `{"x":[1,2,},"model":"` + prefixTestModel + `","stream":true,"input":"` + filler(large) + `"}`, errInvalidJSONBody},
		{"truncated literal", earlyBody(`{"model":"`+prefixTestModel+`","stream":tru`, large), errInvalidJSONBody},
		{"null body", " null" + strings.Repeat(" ", large), errInvalidJSONBody},
		{"model missing", `{"input":"` + filler(large) + `"}`, errModelFieldMissing},
		{"member nested past the decoder limit", earlyBody(`{"x":`+nestedArray(10000)+`,"model":"`+prefixTestModel+`","stream":true`, large), errInvalidJSONBody},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, mode, _, err := readBody(t, director, "/v1/chat/completions", "application/json", tc.body)
			require.ErrorIs(t, err, tc.err)
			assert.Equal(t, requestBodyModeBuffered, mode.mode)
		})
	}
}

// TestReadModelRequestBodyDoesNotValidateAfterRouting documents the streamed
// path contract: bytes after the routing fields are forwarded unchanged and
// left to the backend, including JSON the complete-body decoder would reject.
func TestReadModelRequestBodyDoesNotValidateAfterRouting(t *testing.T) {
	director, _ := newPrefixTestDirector(t, 25<<20, http.DefaultTransport)
	body := `{"model":"` + prefixTestModel + `","stream":false,"input":"` + filler(requestPrefixLimit) + `"} trailing`
	parsed, mode, req, err := readBody(t, director, "/v1/chat/completions", "application/json", body)
	require.NoError(t, err)
	assert.Equal(t, streamedBodyRead(), mode)
	assert.Equal(t, prefixTestModel, parsed.Model)
	forwarded, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	assert.Equal(t, body, string(forwarded))

	_, err = extractOpenAIJSONBody(httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)))
	require.ErrorIs(t, err, errInvalidJSONBody, "the buffered path rejects the same body before dispatch")
}

// TestReadModelRequestBodyRejectsDeclaredOverLimitWithoutReading checks that
// a Content-Length over the route limit fails with the limit error before any
// body byte is read.
func TestReadModelRequestBodyRejectsDeclaredOverLimitWithoutReading(t *testing.T) {
	for _, contentType := range []string{"application/json", "multipart/form-data; boundary=bdd"} {
		t.Run(contentType, func(t *testing.T) {
			director, _ := newPrefixTestDirector(t, 1<<20, http.DefaultTransport)
			client := newGatedBody(earlyBody(`{"model":"`+prefixTestModel+`","stream":true`, 1<<20), "")
			close(client.release) // a read reaches EOF instead of blocking the test
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", client)
			req.Header.Set(contentTypeHeader, contentType)
			req.ContentLength = 1<<20 + 1
			_, mode, err := director.readModelRequestBody(req, director.chatCompletions.modelNameToNVCFUrl)
			var maxBytesErr *http.MaxBytesError
			require.ErrorAs(t, err, &maxBytesErr)
			assert.Equal(t, int64(1<<20), maxBytesErr.Limit)
			assert.Equal(t, requestBodyMode{}, mode)
			assert.Zero(t, client.read.Load(), "no body byte may be read")
			assert.True(t, client.closed.Load(), "the body must be closed")
		})
	}
}

// TestStreamedRequestClientUploadFailureIsClientCanceled fails the client
// upload after a streamed request was dispatched through a real transport.
// The gateway must attribute it to the client, not report an upstream proxy
// error.
func TestStreamedRequestClientUploadFailureIsClientCanceled(t *testing.T) {
	upstreamErr := make(chan error, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.Copy(io.Discard, r.Body)
		upstreamErr <- err
	}))
	t.Cleanup(backend.Close)
	vanity, err := NewVanityDirector(backend.URL, &http.Transport{})
	require.NoError(t, err)
	director, err := NewModelDirector(directMappings(), regexp.MustCompile("^$"), vanity, nil, nil)
	require.NoError(t, err)

	for _, path := range []string{"/v1/chat/completions", "/v1/messages"} {
		t.Run(path, func(t *testing.T) {
			head := `{"model":"` + prefixTestModel + `","stream":false,"input":"` + filler(2*requestPrefixLimit)
			req := httptest.NewRequest(http.MethodPost, path, io.MultiReader(strings.NewReader(head), iotestErrReader{io.ErrUnexpectedEOF}))
			req.ContentLength = -1
			rec := httptest.NewRecorder()
			if path == "/v1/messages" {
				director.ServeMessages(rec, req)
			} else {
				director.ServeChatCompletions(rec, req)
			}
			assert.Equal(t, statusClientClosedRequest, rec.Code)
			assert.Equal(t, "client_canceled", rec.Header().Get("NVCF-Error-Source"))
			select {
			case err := <-upstreamErr:
				assert.Error(t, err, "the upstream must see an incomplete body")
			case <-time.After(5 * time.Second):
				t.Fatal("upstream handler did not finish")
			}
		})
	}
}

// FuzzScanRequestPrefix checks that whenever the scanner resolves routing
// fields from a valid document, the complete-body decoder reads the same
// values from it.
func FuzzScanRequestPrefix(f *testing.F) {
	seeds := []string{
		`{"model":"m","stream":true}`,
		`{"Model":"m","model":"n","STREAM":false}`,
		`{"stream":true,"stream":false,"model":"m"}`,
		`{"a":{"b":["}",{"c":"\""}]},"model":"m","stream":true}`,
		`{"model":"m","model":1,"stream":false}`,
		`{"mod\u0065l":"m","stream":true}`,
		`{"model":"m\u0062","stream":true}`,
		`{"model":null,"stream":true}`,
		`{"stream":1,"model":"m"}`,
		`{"x":1e999,"model":"m","stream":true}`,
		` { "model" : "m" , "stream" : true } `,
		`{"model":"m","stream":true,"stream":"bad"}`,
	}
	for _, seed := range seeds {
		f.Add([]byte(seed), true)
		f.Add([]byte(seed), false)
	}
	f.Fuzz(func(t *testing.T, doc []byte, needStream bool) {
		scan := scanRequestPrefix(doc, needStream)
		if scan.fallbackReason != "" || !stdjson.Valid(doc) {
			return
		}
		bodyBytes := bytebufferpool.Get()
		_, _ = bodyBytes.Write(doc)
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		buffered, err := bufferRemainingRequestBody(req, bodyBytes)
		require.NoError(t, err)
		defer buffered.Close()
		decode := decodeOpenAIJSONBody
		if !needStream {
			decode = decodeDecisionRequestBody
		}
		parsed, err := decode(buffered)
		require.NoError(t, err, "scanner resolved a document the decoder rejects: %q", doc)
		require.Equal(t, parsed.Model, scan.model, "document %q", doc)
		if needStream {
			require.Equal(t, parsed.Stream, scan.stream, "document %q", doc)
		}
	})
}

// gatedBody serves head, then blocks until release is closed, then serves
// tail. read counts bytes handed to the gateway.
type gatedBody struct {
	head, tail []byte
	release    chan struct{}
	read       atomic.Int64
	closed     atomic.Bool
}

func newGatedBody(head, tail string) *gatedBody {
	return &gatedBody{head: []byte(head), tail: []byte(tail), release: make(chan struct{})}
}

func (b *gatedBody) Read(p []byte) (int, error) {
	if len(b.head) > 0 {
		n := copy(p, b.head)
		b.head = b.head[n:]
		b.read.Add(int64(n))
		return n, nil
	}
	<-b.release
	if len(b.tail) == 0 {
		return 0, io.EOF
	}
	n := copy(p, b.tail)
	b.tail = b.tail[n:]
	b.read.Add(int64(n))
	return n, nil
}

func (b *gatedBody) Close() error {
	b.closed.Store(true)
	return nil
}

// TestStreamedRequestReachesUpstreamBeforeClientEOF holds the client body
// after its first bytes. The upstream must receive the request, after the
// gateway read exactly one prefix, and read the held bytes before the client
// body ends.
func TestStreamedRequestReachesUpstreamBeforeClientEOF(t *testing.T) {
	head := `{"model":"` + prefixTestModel + `","stream":true,"input":"` + filler(3*requestPrefixLimit)
	tail := filler(1024) + `"}`
	client := newGatedBody(head, tail)

	readAtDispatch := make(chan int64, 1)
	upstreamSawHead := make(chan struct{})
	upstreamBody := make(chan []byte, 1)
	upstreamAccept := make(chan string, 1)
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		readAtDispatch <- client.read.Load()
		upstreamAccept <- req.Header.Get("Accept")
		got := make([]byte, len(head))
		_, err := io.ReadFull(req.Body, got)
		if err != nil {
			return nil, err
		}
		close(upstreamSawHead)
		rest, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		upstreamBody <- slices.Concat(got, rest)
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: http.NoBody, Request: req}, nil
	})
	_, handler := newPrefixTestDirector(t, 25<<20, transport)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", client)
	req.ContentLength = -1
	req.Header.Set(contentTypeHeader, "application/json")
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(rec, req)
	}()

	select {
	case read := <-readAtDispatch:
		assert.Equal(t, int64(requestPrefixLimit), read, "the gateway must read exactly one prefix before dispatch")
	case <-time.After(5 * time.Second):
		t.Fatal("the request was not dispatched before the client body ended")
	}
	select {
	case <-upstreamSawHead:
	case <-time.After(5 * time.Second):
		t.Fatal("the upstream did not receive the streamed bytes before the client body ended")
	}
	close(client.release)
	<-done

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "text/event-stream", <-upstreamAccept)
	assert.Equal(t, head+tail, string(<-upstreamBody), "the upstream must receive the client bytes unchanged")
}

// TestBufferedRequestWaitsForClientEOF is the fallback counterpart: a late
// field keeps dispatch until the whole body has been read.
func TestBufferedRequestWaitsForClientEOF(t *testing.T) {
	head := `{"input":"` + filler(2*requestPrefixLimit)
	tail := `","model":"` + prefixTestModel + `","stream":true}`
	client := newGatedBody(head, tail)

	dispatched := make(chan struct{})
	upstreamBody := make(chan []byte, 1)
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		close(dispatched)
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		upstreamBody <- body
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: http.NoBody, Request: req}, nil
	})
	_, handler := newPrefixTestDirector(t, 25<<20, transport)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", client)
	req.ContentLength = -1
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(rec, req)
	}()

	select {
	case <-dispatched:
		t.Fatal("a late-field request must not be dispatched before the client body ends")
	case <-time.After(200 * time.Millisecond):
	}
	close(client.release)
	<-done
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, head+tail, string(<-upstreamBody))
}

// realNetworkGateway serves handler on a listener and returns its URL, so the
// tests below exercise the HTTP server body reader and http.Transport.
func realNetworkGateway(t *testing.T, limit int64, upstream http.Handler) string {
	t.Helper()
	return realNetworkGatewayWithStreamedLimit(t, limit, limit, upstream)
}

// realNetworkGatewayWithStreamedLimit is realNetworkGateway with a separate
// limit for streamed requests.
func realNetworkGatewayWithStreamedLimit(t *testing.T, limit, streamedLimit int64, upstream http.Handler) string {
	t.Helper()
	backend := httptest.NewServer(upstream)
	t.Cleanup(backend.Close)
	vanity, err := NewVanityDirector(backend.URL, &http.Transport{})
	require.NoError(t, err)
	director, err := NewModelDirector(directMappings(), regexp.MustCompile("^$"), vanity, nil, nil)
	require.NoError(t, err)
	director.requestBodyLimit = limit
	director.streamedRequestBodyLimit = streamedLimit
	router := chi.NewRouter()
	sized := limitModelRequestBody(limit)
	router.Method(http.MethodPost, "/v1/chat/completions", sized(http.HandlerFunc(director.ServeChatCompletions)))
	router.Method(http.MethodPost, "/v1/messages", sized(http.HandlerFunc(director.ServeMessages)))
	gateway := httptest.NewServer(router)
	t.Cleanup(gateway.Close)
	return gateway.URL
}

func TestStreamedRequestOverRealTransport(t *testing.T) {
	head := `{"model":"` + prefixTestModel + `","stream":false,"input":"` + filler(4*requestPrefixLimit)
	tail := filler(10) + `"}`
	sawHead := make(chan struct{})
	result := make(chan [32]byte, 1)
	url := realNetworkGateway(t, 25<<20, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := make([]byte, 2*requestPrefixLimit)
		_, err := io.ReadFull(r.Body, got)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		close(sawHead)
		rest, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		result <- sha256.Sum256(slices.Concat(got, rest))
		w.WriteHeader(http.StatusOK)
	}))

	pr, pw := io.Pipe()
	req, err := http.NewRequest(http.MethodPost, url+"/v1/chat/completions", pr)
	require.NoError(t, err)
	response := sendAsync(http.DefaultClient, req)
	writeDone := make(chan error, 1)
	go func() {
		_, err := io.WriteString(pw, head)
		writeDone <- err
	}()

	select {
	case <-sawHead:
	case <-time.After(5 * time.Second):
		t.Fatal("the upstream did not receive streamed bytes before the client finished")
	}
	require.NoError(t, <-writeDone)
	_, err = io.WriteString(pw, tail)
	require.NoError(t, err)
	require.NoError(t, pw.Close())

	got := <-response
	require.NoError(t, got.err)
	require.Equal(t, http.StatusOK, got.status)
	assert.Equal(t, sha256.Sum256([]byte(head+tail)), <-result)
}

// TestRequestTooLargeAfterDispatchFollowsTheClientProtocol checks the error a
// client gets when its upload passes the limit after dispatch. It must match
// the client's route, not the outgoing path: a chat route whose outgoing path
// is /v1/messages still answers in the OpenAI shape.
func TestRequestTooLargeAfterDispatchFollowsTheClientProtocol(t *testing.T) {
	tests := []struct {
		name       string
		protocol   nativeProtocol
		wantStatus int
		wantBody   string
	}{
		{"chat route with a messages outgoing path", nativeProtocolNone, http.StatusBadRequest, "Please make sure your payload is below 1048576 bytes"},
		{"messages route", nativeProtocolMessages, http.StatusRequestEntityTooLarge, "1048576"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, messagesPath, nil)
			if tc.protocol != nativeProtocolNone {
				request = withNativeProtocol(request, tc.protocol)
			}
			recorder := httptest.NewRecorder()
			writeRequestTooLargeAfterDispatch(recorder, request, &http.MaxBytesError{Limit: 1 << 20})
			assert.Equal(t, tc.wantStatus, recorder.Code)
			assert.Contains(t, recorder.Body.String(), tc.wantBody)
		})
	}
}

// TestStreamedRequestLimitBoundaries covers the route limit on the streamed
// path. A declared length over the limit is rejected before dispatch. An
// unknown length that passes the limit after dispatch fails the upstream body
// and returns the same error as a limit reached before dispatch.
func TestStreamedRequestLimitBoundaries(t *testing.T) {
	const buffered = 4 * requestPrefixLimit
	for _, limits := range []struct {
		name     string
		streamed int64
	}{
		{"streamed limit unset", buffered},
		{"streamed limit above the buffered limit", 4 * buffered},
	} {
		t.Run(limits.name, func(t *testing.T) {
			testStreamedRequestLimitBoundaries(t, buffered, limits.streamed)
		})
	}
}

// testStreamedRequestLimitBoundaries checks that the streamed limit, not the
// buffered limit, bounds a streamed request and is the limit its error names.
func testStreamedRequestLimitBoundaries(t *testing.T, buffered, streamed int64) {
	limit := int(streamed)
	head := `{"model":"` + prefixTestModel + `","stream":false,"input":"`
	bodyOfSize := func(size int) string {
		return head + filler(size-len(head)-2) + `"}`
	}
	tests := []struct {
		name          string
		path          string
		size          int
		knownLength   bool
		status        int
		upstreamCalls int32
		upstreamOK    bool
	}{
		{"known length at limit", "/v1/chat/completions", limit, true, http.StatusOK, 1, true},
		{"known length over limit", "/v1/chat/completions", limit + 1, true, http.StatusBadRequest, 0, false},
		{"unknown length at limit", "/v1/chat/completions", limit, false, http.StatusOK, 1, true},
		{"unknown length over limit", "/v1/chat/completions", limit + 1, false, http.StatusBadRequest, 1, false},
		{"messages unknown length over limit", "/v1/messages", limit + 1, false, http.StatusRequestEntityTooLarge, 1, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			upstreamErr := make(chan error, 1)
			url := realNetworkGatewayWithStreamedLimit(t, buffered, streamed, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				body, err := io.ReadAll(r.Body)
				if err == nil && len(body) != tc.size {
					err = errors.New("upstream body has the wrong length")
				}
				upstreamErr <- err
				w.WriteHeader(http.StatusOK)
			}))

			body := bodyOfSize(tc.size)
			var reader io.Reader = strings.NewReader(body)
			if !tc.knownLength {
				reader = io.MultiReader(reader)
			}
			req, err := http.NewRequest(http.MethodPost, url+tc.path, reader)
			require.NoError(t, err)
			req.Header.Set(contentTypeHeader, "application/json")
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			payload, _ := io.ReadAll(resp.Body)

			assert.Equal(t, tc.status, resp.StatusCode, string(payload))
			assert.Equal(t, tc.upstreamCalls, calls.Load())
			if tc.status != http.StatusOK {
				assert.Equal(t, "gateway_rejected", resp.Header.Get("NVCF-Error-Source"))
			}
			if tc.status == http.StatusRequestEntityTooLarge {
				assert.Contains(t, string(payload), "too large")
			}
			if tc.status == http.StatusBadRequest {
				var openAIErr OpenAIError
				require.NoError(t, stdjson.Unmarshal(payload, &openAIErr))
				assert.Equal(t, "invalid_image_format", openAIErr.Error.Code)
			}
			if tc.status != http.StatusOK {
				assert.Contains(t, string(payload), " "+strconv.FormatInt(streamed, 10)+" bytes", "the error names the streamed limit")
			}
			if tc.upstreamCalls > 0 {
				select {
				case err := <-upstreamErr:
					if tc.upstreamOK {
						assert.NoError(t, err)
					} else {
						assert.Error(t, err, "the upstream must see an incomplete body, not a complete truncated one")
					}
				case <-time.After(5 * time.Second):
					t.Fatal("upstream handler did not finish")
				}
			}
		})
	}
}

// TestStreamedRequestClientDisconnect aborts the client upload after dispatch.
// The upstream must see an incomplete body and the gateway handler must
// return.
func TestStreamedRequestClientDisconnect(t *testing.T) {
	for _, mode := range []string{"body error", "context cancel"} {
		t.Run(mode, func(t *testing.T) {
			upstreamErr := make(chan error, 1)
			sawHead := make(chan struct{})
			url := realNetworkGateway(t, 25<<20, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got := make([]byte, requestPrefixLimit)
				_, err := io.ReadFull(r.Body, got)
				if err == nil {
					close(sawHead)
					_, err = io.Copy(io.Discard, r.Body)
				}
				upstreamErr <- err
			}))

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			pr, pw := io.Pipe()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/v1/chat/completions", pr)
			require.NoError(t, err)
			clientDone := make(chan error, 1)
			go func() {
				resp, err := http.DefaultClient.Do(req)
				if resp != nil {
					_ = resp.Body.Close()
				}
				clientDone <- err
			}()
			go func() {
				_, _ = io.WriteString(pw, `{"model":"`+prefixTestModel+`","stream":true,"input":"`+filler(2*requestPrefixLimit))
			}()

			select {
			case <-sawHead:
			case <-time.After(5 * time.Second):
				t.Fatal("upstream did not receive the streamed prefix")
			}
			if mode == "body error" {
				pw.CloseWithError(errors.New("client upload failed"))
			} else {
				cancel()
			}

			select {
			case err := <-upstreamErr:
				assert.Error(t, err, "the upstream must not see a complete body")
			case <-time.After(5 * time.Second):
				t.Fatal("upstream body did not fail after the client disconnected")
			}
			// The client transport waits for its own body reader to return.
			pw.CloseWithError(context.Canceled)
			select {
			case <-clientDone:
			case <-time.After(5 * time.Second):
				t.Fatal("client request did not finish")
			}
		})
	}
}

// TestFallbackPathsStillReceiveCompleteBodies checks the routes that read the
// body after routing: shadow replay and LLM Gateway rewriting keep working
// for bodies larger than the prefix.
func TestFallbackPathsStillReceiveCompleteBodies(t *testing.T) {
	type received struct {
		host string
		body []byte
	}
	requests := make(chan received, 4)
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		requests <- received{host: req.URL.Host, body: body}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: http.NoBody, Request: req}, nil
	})
	_, handler := newPrefixTestDirector(t, 25<<20, transport)

	t.Run("llm gateway rewrite", func(t *testing.T) {
		body := earlyBody(`{"model":"acme/llm","stream":false`, requestPrefixLimit)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
		require.Equal(t, http.StatusOK, rec.Code)
		got := <-requests
		assert.Equal(t, "llm.test", got.host)
		var payload map[string]any
		require.NoError(t, stdjson.Unmarshal(got.body, &payload))
		assert.Equal(t, "llm-func/acme/llm", payload["model"])
		assert.Len(t, payload["input"], requestPrefixLimit)
	})

	t.Run("shadow replay", func(t *testing.T) {
		body := earlyBody(`{"model":"acme/shadowed","stream":false`, requestPrefixLimit)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
		require.Equal(t, http.StatusOK, rec.Code)
		bodies := map[string]bool{}
		for range 2 {
			select {
			case got := <-requests:
				var payload map[string]any
				require.NoError(t, stdjson.Unmarshal(got.body, &payload))
				assert.Len(t, payload["input"], requestPrefixLimit)
				bodies[payload["model"].(string)] = true
			case <-time.After(5 * time.Second):
				t.Fatal("primary or shadow request missing")
			}
		}
		assert.Equal(t, map[string]bool{"acme/shadowed": true, prefixTestModel: true}, bodies)
	})
}

// TestShadowReplayLongerThanRouteLimitReachesTarget sends a primary request at
// the route limit whose shadow copy grows past it when its model is rewritten
// to a longer name. Shadow replays never pass through the route limit, so the
// copy must still reach its target.
func TestShadowReplayLongerThanRouteLimitReachesTarget(t *testing.T) {
	const limit = 4 * requestPrefixLimit
	type received struct {
		model string
		size  int
	}
	requests := make(chan received, 2)
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		var payload struct {
			Model string `json:"model"`
		}
		if err := stdjson.Unmarshal(body, &payload); err != nil {
			return nil, err
		}
		requests <- received{model: payload.Model, size: len(body)}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: http.NoBody, Request: req}, nil
	})
	_, handler := newPrefixTestDirector(t, limit, transport)

	head := `{"model":"acme/shadowed","stream":false,"input":"`
	body := head + strings.Repeat("<", limit-len(head)-2) + `"}`
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
	require.Equal(t, http.StatusOK, rec.Code)

	sizes := map[string]int{}
	for range 2 {
		select {
		case got := <-requests:
			sizes[got.model] = got.size
		case <-time.After(5 * time.Second):
			t.Fatalf("primary or shadow request missing, received %v", sizes)
		}
	}
	assert.Equal(t, len(body), sizes["acme/shadowed"])
	assert.Greater(t, sizes[prefixTestModel], limit, "the shadow copy must be longer than the route limit")
}

// TestReadRequestPrefixWithLargestDeclaredLength reads the prefix of a body
// that declares the largest Content-Length a streamed limit can allow into an
// empty, zero-capacity buffer. Sizing the buffer from that length must not
// overflow.
func TestReadRequestPrefixWithLargestDeclaredLength(t *testing.T) {
	prefix := &bytebufferpool.ByteBuffer{}
	complete, err := readRequestPrefix(prefix, strings.NewReader(filler(requestPrefixLimit+1)), math.MaxInt64)
	require.NoError(t, err)
	assert.False(t, complete)
	assert.Len(t, prefix.B, requestPrefixLimit)
}

func TestPrefixReplayBodyCloseDuringRead(t *testing.T) {
	for range 100 {
		prefix := bytebufferpool.Get()
		_, _ = prefix.WriteString(filler(1024))
		rest := io.NopCloser(strings.NewReader(filler(1024)))
		body := newPrefixReplayBody(prefix, rest)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			buf := make([]byte, 7)
			for {
				if _, err := body.Read(buf); err != nil {
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			_ = body.Close()
		}()
		wg.Wait()
		_, err := body.Read(make([]byte, 1))
		require.ErrorIs(t, err, http.ErrBodyReadAfterClose)
	}
}

func TestPrefixReplayBodyReleasesPrefixAfterReplay(t *testing.T) {
	prefix := bytebufferpool.Get()
	_, _ = prefix.WriteString("head")
	body := newPrefixReplayBody(prefix, io.NopCloser(strings.NewReader("tail")))
	buf := make([]byte, 4)
	n, err := body.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, "head", string(buf[:n]))
	assert.Nil(t, body.prefix, "the prefix must return to the pool once replayed")
	rest, err := io.ReadAll(body)
	require.NoError(t, err)
	assert.Equal(t, "tail", string(rest))
}

// TestReleasedPrefixBufferIsPoisoned checks that every path returning the
// scanned prefix to the pool poisons it in tests, so a read of the prefix
// after release fails like a read of any other released body buffer.
func TestReleasedPrefixBufferIsPoisoned(t *testing.T) {
	newPrefix := func() (*bytebufferpool.ByteBuffer, []byte) {
		prefix := bytebufferpool.Get()
		_, _ = prefix.WriteString(`{"model":"` + prefixTestModel + `"`)
		return prefix, prefix.B
	}
	tests := []struct {
		name    string
		release func(t *testing.T) []byte
	}{
		{"replayed", func(t *testing.T) []byte {
			prefix, released := newPrefix()
			_, err := io.ReadAll(newPrefixReplayBody(prefix, io.NopCloser(strings.NewReader("}"))))
			require.NoError(t, err)
			return released
		}},
		{"closed before replay", func(t *testing.T) []byte {
			prefix, released := newPrefix()
			require.NoError(t, newPrefixReplayBody(prefix, io.NopCloser(strings.NewReader("}"))).Close())
			return released
		}},
		{"prefix read fails", func(t *testing.T) []byte {
			director, _ := newPrefixTestDirector(t, 25<<20, http.DefaultTransport)
			upload := &truncatedUpload{content: `{"model":"` + prefixTestModel + `"`}
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", upload)
			_, _, err := director.readModelRequestBody(req, director.chatCompletions.modelNameToNVCFUrl)
			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			return upload.written
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			released := tt.release(t)
			require.NotEmpty(t, released)
			assert.Equal(t, strings.Repeat("X", len(released)), string(released))
		})
	}
}

// truncatedUpload writes content into the caller's buffer, keeps that buffer
// so a test can inspect it after release, and then fails like an upload that
// ended early.
type truncatedUpload struct {
	content string
	written []byte
}

func (u *truncatedUpload) Read(p []byte) (int, error) {
	n := copy(p, u.content)
	u.written = p[:n]
	return n, io.ErrUnexpectedEOF
}

func TestRequestBodyReadsMetricInitializesAndCountsPrimaryRequests(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previousProvider, previousCounter := otel.GetMeterProvider(), requestBodyReadsCounter
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(previousProvider)
		requestBodyReadsCounter = previousCounter
		require.NoError(t, provider.Shutdown(context.Background()))
	})
	require.NoError(t, setupRequestBodyMetrics())

	primary := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	recordRequestBodyMode(primary, streamedBodyRead())
	recordRequestBodyMode(primary, bufferedBodyRead(requestBodyReasonLateFields))
	shadow := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	shadow.Header.Set(shadowHeader, shadowHeaderValue)
	recordRequestBodyMode(shadow, streamedBodyRead())

	var resourceMetrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &resourceMetrics))
	counts := map[requestBodyMode]int64{}
	for _, scopeMetrics := range resourceMetrics.ScopeMetrics {
		for _, metric := range scopeMetrics.Metrics {
			if metric.Name != requestBodyMetricName {
				continue
			}
			sum, ok := metric.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, point := range sum.DataPoints {
				mode, _ := point.Attributes.Value(requestBodyModeLabel)
				reason, _ := point.Attributes.Value(requestBodyReasonLabel)
				counts[requestBodyMode{mode: mode.AsString(), reason: reason.AsString()}] = point.Value
			}
		}
	}
	require.Len(t, counts, len(requestBodyModes), "every mode and reason is emitted at zero on startup")
	for _, mode := range requestBodyModes {
		want := int64(0)
		if mode == streamedBodyRead() || mode == bufferedBodyRead(requestBodyReasonLateFields) {
			want = 1
		}
		assert.Equal(t, want, counts[mode], "%s/%s", mode.mode, mode.reason)
	}
}

// TestReadModelRequestBodyKeepsTruncatedUploadAsReadError checks that a
// client upload that ends early inside the prefix is a read failure, as on
// the buffered path, not a complete body.
func TestReadModelRequestBodyKeepsTruncatedUploadAsReadError(t *testing.T) {
	director, _ := newPrefixTestDirector(t, 25<<20, http.DefaultTransport)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", io.MultiReader(
		strings.NewReader(`{"model":"`+prefixTestModel+`","stream":true}`),
		iotestErrReader{io.ErrUnexpectedEOF},
	))
	_, _, err := director.readModelRequestBody(req, director.chatCompletions.modelNameToNVCFUrl)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	rec := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", io.MultiReader(
		strings.NewReader(`{"model":"`+prefixTestModel+`","stream":true}`),
		iotestErrReader{io.ErrUnexpectedEOF},
	))
	_, handled := director.resolveModelMappedRequest(rec, req, director.chatCompletions.modelNameToNVCFUrl)
	require.True(t, handled)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

type iotestErrReader struct{ err error }

func (r iotestErrReader) Read([]byte) (int, error) { return 0, r.err }

type clientResult struct {
	status int
	body   string
	err    error
}

// sendAsync sends req with client from another goroutine and delivers its
// status and body after the response has been read and closed.
func sendAsync(client *http.Client, req *http.Request) <-chan clientResult {
	result := make(chan clientResult, 1)
	go func() {
		resp, err := client.Do(req)
		if err != nil {
			result <- clientResult{err: err}
			return
		}
		defer resp.Body.Close()
		payload, err := io.ReadAll(resp.Body)
		result <- clientResult{status: resp.StatusCode, body: string(payload), err: err}
	}()
	return result
}

// TestReadModelRequestBodySmallBodiesStaySmall bounds the bytes allocated for
// a small request: the prefix buffer must not grow to the 64 KiB limit, and
// pooled buffers must be reused.
func TestReadModelRequestBodySmallBodiesStaySmall(t *testing.T) {
	director, _ := newPrefixTestDirector(t, 25<<20, http.DefaultTransport)
	body := `{"model":"` + prefixTestModel + `","stream":false,"messages":[{"role":"user","content":"` + filler(1024) + `"}]}`
	run := func(knownLength bool) {
		var reader io.Reader = strings.NewReader(body)
		if !knownLength {
			// A server request body has no WriteTo, so io.Copy uses the
			// buffer's ReadFrom; hide strings.Reader's WriteTo the same way.
			reader = struct{ io.Reader }{reader}
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", reader)
		if !knownLength {
			req.ContentLength = -1
		}
		_, mode, err := director.readModelRequestBody(req, director.chatCompletions.modelNameToNVCFUrl)
		require.NoError(t, err)
		require.Equal(t, bufferedBodyRead(requestBodyReasonComplete), mode)
		_ = req.Body.Close()
	}
	for _, knownLength := range []bool{true, false} {
		for range 100 {
			run(knownLength)
		}
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		const runs = 200
		for range runs {
			run(knownLength)
		}
		runtime.ReadMemStats(&after)
		perRequest := (after.TotalAlloc - before.TotalAlloc) / runs
		assert.Less(t, perRequest, uint64(16<<10), "known length %v: %d bytes per request", knownLength, perRequest)
	}
}

// TestStreamedRequestEarlyUpstreamResponse has the upstream answer without
// reading a streamed body while the client is still uploading. The client
// must receive the upstream response and the gateway handler must finish.
func TestStreamedRequestEarlyUpstreamResponse(t *testing.T) {
	url := realNetworkGateway(t, 25<<20, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":"bdd-early-reject"}`)
	}))

	pr, pw := io.Pipe()
	req, err := http.NewRequest(http.MethodPost, url+"/v1/chat/completions", pr)
	require.NoError(t, err)
	go func() {
		_, err := io.WriteString(pw, `{"model":"`+prefixTestModel+`","stream":false,"input":"`+filler(2*requestPrefixLimit))
		if err != nil {
			return
		}
		// Keep uploading slowly, as a large client body would.
		for range 50 {
			if _, err := io.WriteString(pw, filler(1024)); err != nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		_, _ = io.WriteString(pw, `"}`)
		_ = pw.Close()
	}()
	defer pw.Close()

	select {
	case got := <-sendAsync(http.DefaultClient, req):
		require.NoError(t, got.err, "client request failed")
		assert.Equal(t, http.StatusUnauthorized, got.status)
		assert.Contains(t, got.body, "bdd-early-reject")
	case <-time.After(10 * time.Second):
		t.Fatal("the client did not receive the early upstream response")
	}
}
