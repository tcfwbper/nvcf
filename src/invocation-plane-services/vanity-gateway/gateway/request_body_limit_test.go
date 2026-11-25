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
	"ai-api-gateway-service/middleware"
	"bufio"
	"crypto/sha256"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const (
	splitTestBuffered = 1 << 20
	splitTestStreamed = 4 << 20
	// splitTestBetween is a body size above the buffered limit and within
	// the streamed limit.
	splitTestBetween = 2 << 20
)

// upstreamBodyRead is what the fake upstream read from one request body.
type upstreamBodyRead struct {
	length int64
	sum    [32]byte
	err    error
}

// splitLimitGateway serves the assembled model API router on a real listener
// with separate buffered and streamed limits. It counts the client body bytes
// the router reads and the requests that reach the upstream.
type splitLimitGateway struct {
	url           string
	addr          string
	bodyBytesRead atomic.Int64
	upstreamCalls atomic.Int32
	upstreamReads chan upstreamBodyRead
	// handled receives a value each time the router returns from a request,
	// after which it reads no more of that request's body.
	handled chan struct{}
}

func splitLimitMappings() *config.GatewayConfig {
	mappings := directMappings()
	mappings.OpenAI.Host = overLimitTestHost
	mappings.OpenAI.ImageEdits = map[string]config.ModelFunctionDetails{
		"edits": {ModelName: prefixTestModel, FunctionID: "edits-func"},
	}
	return mappings
}

// newSplitLimitGateway builds the router with buildChiMux. A nil upstream
// reads the whole body and answers 200.
func newSplitLimitGateway(t *testing.T, buffered int64, streamed *int64, upstream http.Handler) *splitLimitGateway {
	t.Helper()
	g := &splitLimitGateway{upstreamReads: make(chan upstreamBodyRead, 16), handled: make(chan struct{}, 16)}
	if upstream == nil {
		upstream = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hash := sha256.New()
			n, err := io.Copy(hash, r.Body)
			read := upstreamBodyRead{length: n, err: err}
			copy(read.sum[:], hash.Sum(nil))
			g.upstreamReads <- read
			w.WriteHeader(http.StatusOK)
		})
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.upstreamCalls.Add(1)
		upstream.ServeHTTP(w, r)
	}))
	t.Cleanup(backend.Close)
	mux, err := buildChiMux(splitLimitMappings(), Config{
		NvcfApiEndpoint:             backend.URL,
		LLMGatewayEndpoint:          backend.URL,
		MaxRequestSizeBytes:         int64Ptr(buffered),
		MaxStreamedRequestSizeBytes: streamed,
	})
	require.NoError(t, err)
	// The h2c handler, as in production, also serves HTTP/2 with prior
	// knowledge; HTTP/1 requests pass through it unchanged.
	gateway := httptest.NewServer(h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = &countingReadCloser{ReadCloser: r.Body, read: &g.bodyBytesRead}
		mux.ServeHTTP(w, r)
		select {
		case g.handled <- struct{}{}:
		default:
		}
	}), &http2.Server{}))
	t.Cleanup(gateway.Close)
	g.url = gateway.URL
	g.addr = gateway.Listener.Addr().String()
	return g
}

// post sends body to path on a raw connection and returns the response. An
// unknown length is sent chunked. The body is written apart from the read,
// and write errors are ignored: the gateway may answer and close before the
// whole body is sent, and an HTTP client can then report the failed write
// instead of the response it was sent.
func (g *splitLimitGateway) post(t *testing.T, path, contentType, body string, knownLength bool) (int, string) {
	t.Helper()
	conn, err := net.Dial("tcp", g.addr)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(30*time.Second)))
	head := fmt.Sprintf("POST %s HTTP/1.1\r\nHost: %s\r\nContent-Type: %s\r\n%s: req-split-limit\r\nConnection: close\r\n", path, overLimitTestHost, contentType, middleware.RequestIDHeader)
	go func() {
		if knownLength {
			_, _ = io.WriteString(conn, head+fmt.Sprintf("Content-Length: %d\r\n\r\n", len(body))+body)
			return
		}
		if _, err := io.WriteString(conn, head+"Transfer-Encoding: chunked\r\n\r\n"); err == nil {
			writeChunked(conn, body, "")
		}
	}()
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(payload)
}

// requireBodyLimitError checks the error a route gives for a body over limit.
func requireBodyLimitError(t *testing.T, path string, status int, payload string, limit int64) {
	t.Helper()
	switch path {
	case messagesPath, decisionsPath:
		assert.Equal(t, http.StatusRequestEntityTooLarge, status, payload)
		assert.Contains(t, payload, "request body is too large (limit "+strconv.FormatInt(limit, 10)+" bytes)")
	default:
		assert.Equal(t, http.StatusBadRequest, status, payload)
		var openAIErr OpenAIError
		require.NoError(t, stdjson.Unmarshal([]byte(payload), &openAIErr), payload)
		assert.Equal(t, "invalid_image_format", openAIErr.Error.Code)
		assert.Contains(t, openAIErr.Error.Message, "below "+strconv.FormatInt(limit, 10)+" bytes")
	}
}

// earlyFieldsBody returns a JSON body of exactly size bytes with model, and
// stream unless decision is set, before the padding.
func earlyFieldsBody(model string, decision bool, size int) string {
	head := `{"model":"` + model + `","stream":false,"input":"`
	if decision {
		head = `{"model":"` + model + `","input":"`
	}
	return head + filler(size-len(head)-2) + `"}`
}

// TestStreamedBodyAboveBufferedLimitIsForwarded sends bodies above the
// buffered limit and within the streamed limit with the routing fields first.
// Each reaches the upstream byte for byte, and each is counted once as
// streamed_complete.
func TestStreamedBodyAboveBufferedLimitIsForwarded(t *testing.T) {
	for _, tc := range []struct {
		name        string
		path        string
		decision    bool
		knownLength bool
	}{
		{"chat known length", "/v1/chat/completions", false, true},
		{"chat unknown length", "/v1/chat/completions", false, false},
		{"messages known length", messagesPath, false, true},
		{"decision route with model only", decisionsPath, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := installRequestBodyMetrics(t)
			g := newSplitLimitGateway(t, splitTestBuffered, int64Ptr(splitTestStreamed), nil)
			body := earlyFieldsBody(prefixTestModel, tc.decision, splitTestBetween)

			status, payload := g.post(t, tc.path, "application/json", body, tc.knownLength)

			require.Equal(t, http.StatusOK, status, payload)
			require.Equal(t, int32(1), g.upstreamCalls.Load())
			read := <-g.upstreamReads
			require.NoError(t, read.err)
			assert.Equal(t, int64(len(body)), read.length)
			assert.Equal(t, sha256.Sum256([]byte(body)), read.sum, "the upstream must receive the body byte for byte")
			size := "le_32MiB"
			if !tc.knownLength {
				size = "unknown"
			}
			requireOnlyLargeBodyCount(t, collectRequestBodyLarge(t, reader), largeBodyKey{requestBodyStreamedComplete, size})
		})
	}
}

// TestStreamedBodyWithinBufferedLimitIsNotCountedAsLarge checks that
// streamed_complete counts only bodies above the buffered limit.
func TestStreamedBodyWithinBufferedLimitIsNotCountedAsLarge(t *testing.T) {
	reader := installRequestBodyMetrics(t)
	g := newSplitLimitGateway(t, splitTestBuffered, int64Ptr(splitTestStreamed), nil)
	for _, size := range []int{2 * requestPrefixLimit, splitTestBuffered} {
		body := earlyFieldsBody(prefixTestModel, false, size)
		status, payload := g.post(t, "/v1/chat/completions", "application/json", body, false)
		require.Equal(t, http.StatusOK, status, payload)
		require.Equal(t, int64(size), (<-g.upstreamReads).length)
	}
	for key, value := range collectRequestBodyLarge(t, reader) {
		assert.Zero(t, value, "%v", key)
	}
}

// TestStreamedPromotionBoundaries reads a body through the limit middleware,
// reads a prefix, promotes and reads the rest. The prefix counts once toward
// the streamed limit, the body may end exactly at the limit, a read that
// returns data with an error keeps its data, and one byte over the limit
// fails with the streamed limit.
func TestStreamedPromotionBoundaries(t *testing.T) {
	const buffered = requestPrefixLimit
	const streamed = requestPrefixLimit + 1000
	errUpload := errors.New("upload failed")
	for _, tc := range []struct {
		name    string
		size    int
		prefix  int
		dataErr bool
		failErr error
		wantErr bool
	}{
		{name: "full prefix, exactly the streamed limit", size: streamed, prefix: requestPrefixLimit},
		{name: "full prefix, one byte over", size: streamed + 1, prefix: requestPrefixLimit, wantErr: true},
		{name: "short prefix, exactly the streamed limit", size: streamed, prefix: 10},
		{name: "short prefix, one byte over", size: streamed + 1, prefix: 10, wantErr: true},
		{name: "data with EOF, exactly the streamed limit", size: streamed, prefix: 10, dataErr: true},
		{name: "data with EOF, one byte over", size: streamed + 1, prefix: 10, dataErr: true, wantErr: true},
		{name: "upload error keeps its cause", size: streamed - 10, prefix: 10, failErr: errUpload},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var body io.Reader = strings.NewReader(filler(tc.size))
			if tc.dataErr {
				body = iotest.DataErrReader(body)
			}
			if tc.failErr != nil {
				body = io.MultiReader(body, iotest.ErrReader(tc.failErr))
			}
			var (
				read    int64
				readErr error
			)
			handler := limitModelRequestBody(buffered)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				prefix := make([]byte, tc.prefix)
				_, err := io.ReadFull(r.Body, prefix)
				require.NoError(t, err)
				rest := promoteRequestBody(r, streamed)
				n, err := io.Copy(io.Discard, rest)
				read, readErr = int64(tc.prefix)+n, err
			}))
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", io.NopCloser(body))
			req.ContentLength = -1
			handler.ServeHTTP(httptest.NewRecorder(), req)

			switch {
			case tc.wantErr:
				var maxBytesErr *http.MaxBytesError
				require.ErrorAs(t, readErr, &maxBytesErr)
				assert.Equal(t, int64(streamed), maxBytesErr.Limit, "the error names the streamed limit")
				assert.Equal(t, int64(streamed), read, "every byte up to the limit is delivered")
			case tc.failErr != nil:
				require.ErrorIs(t, readErr, tc.failErr)
				assert.Equal(t, int64(tc.size), read)
			default:
				require.NoError(t, readErr)
				assert.Equal(t, int64(tc.size), read)
			}
		})
	}
}

// TestPromotionFailsClosed checks that only the body the limit middleware
// installed, on a primary request, is promoted. Anything else keeps the body
// it has, and with it the buffered limit.
func TestPromotionFailsClosed(t *testing.T) {
	const buffered = requestPrefixLimit
	t.Run("no limit middleware", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader("{}"))
		assert.True(t, req.Body == promoteRequestBody(req, 1<<30))
	})
	t.Run("body replaced after the middleware", func(t *testing.T) {
		limitModelRequestBody(buffered)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = io.NopCloser(strings.NewReader(filler(2 * buffered)))
			assert.True(t, r.Body == promoteRequestBody(r, 1<<30))
		})).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}")))
	})
	t.Run("shadow replay", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
		req.Header.Set(shadowHeader, shadowHeaderValue)
		limitModelRequestBody(buffered)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.True(t, r.Body == promoteRequestBody(r, 1<<30))
		})).ServeHTTP(httptest.NewRecorder(), req)
	})
	t.Run("limit not above the buffered limit", func(t *testing.T) {
		limitModelRequestBody(buffered)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.True(t, r.Body == promoteRequestBody(r, buffered))
		})).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}")))
	})
}

// fallbackCase is a request above the buffered limit that the gateway must
// read in full, so the buffered limit applies.
type fallbackCase struct {
	name        string
	path        string
	contentType string
	reason      string
	body        string
}

func fallbackCases(t *testing.T) []fallbackCase {
	pad := filler(splitTestBetween)
	multipartType, multipartBody := overLimitMultipartBody(t, splitTestBetween)
	return []fallbackCase{
		{"late fields", "/v1/chat/completions", "application/json", requestBodyReasonLateFields, `{"input":"` + pad + `","model":"` + prefixTestModel + `","stream":false}`},
		{"fields absent", "/v1/chat/completions", "application/json", requestBodyReasonFieldsAbsent, `{"input":"x"}` + strings.Repeat(" ", splitTestBetween)},
		{"nonstandard fields", "/v1/chat/completions", "application/json", requestBodyReasonNonstandardFields, `{"model":null,"stream":false,"input":"` + pad + `"}`},
		{"invalid prefix", "/v1/chat/completions", "application/json", requestBodyReasonInvalidPrefix, `["` + pad + `"]`},
		{"model not found", "/v1/chat/completions", "application/json", requestBodyReasonModelNotFound, earlyFieldsBody("acme/missing", false, splitTestBetween)},
		{"offline model", "/v1/chat/completions", "application/json", requestBodyReasonFunctionStatus, earlyFieldsBody("acme/offline", false, splitTestBetween)},
		{"LLM Gateway model", "/v1/chat/completions", "application/json", requestBodyReasonLLMGateway, earlyFieldsBody("acme/llm", false, splitTestBetween)},
		{"shadowed model", "/v1/chat/completions", "application/json", requestBodyReasonShadow, earlyFieldsBody("acme/shadowed", false, splitTestBetween)},
		{"messages late fields", messagesPath, "application/json", requestBodyReasonLateFields, `{"input":"` + pad + `","model":"` + prefixTestModel + `","stream":false}`},
		{"decision model not found", decisionsPath, "application/json", requestBodyReasonModelNotFound, earlyFieldsBody("acme/missing", true, splitTestBetween)},
		{"multipart", "/v1/images/edits", multipartType, requestBodyReasonMultipart, multipartBody},
	}
}

// TestBodyLimitHintNamesRoutingFields checks the hint added to the buffered
// limit error when a larger streamed limit is set and the routing fields were
// not in the prefix. The status and error shape stay as before. Other
// fallback reasons, and equal limits, get no hint.
func TestBodyLimitHintNamesRoutingFields(t *testing.T) {
	pad := filler(splitTestBetween)
	lateFields := `{"input":"` + pad + `","model":"` + prefixTestModel + `","stream":false}`
	streamHint := "Bodies up to 4194304 bytes can be accepted when the model and stream fields are in the first 65536 bytes of the JSON body."
	modelHint := "Bodies up to 4194304 bytes can be accepted when the model field is in the first 65536 bytes of the JSON body."
	split := int64Ptr(splitTestStreamed)
	tests := []struct {
		name     string
		path     string
		body     string
		streamed *int64
		hint     string
	}{
		{"late fields", "/v1/chat/completions", lateFields, split, streamHint},
		{"messages late fields", messagesPath, lateFields, split, streamHint},
		{"decision late fields", decisionsPath, `{"input":"` + pad + `","model":"` + prefixTestModel + `"}`, split, modelHint},
		{"fields absent", "/v1/chat/completions", `{"input":"x"}` + strings.Repeat(" ", splitTestBetween), split, streamHint},
		{"nonstandard fields", "/v1/chat/completions", `{"model":null,"stream":false,"input":"` + pad + `"}`, split, ""},
		{"model not found", "/v1/chat/completions", earlyFieldsBody("acme/missing", false, splitTestBetween), split, ""},
		{"equal limits", "/v1/chat/completions", lateFields, nil, ""},
	}
	for _, tc := range tests {
		for _, knownLength := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s known length %v", tc.name, knownLength), func(t *testing.T) {
				g := newSplitLimitGateway(t, splitTestBuffered, tc.streamed, nil)
				status, payload := g.post(t, tc.path, "application/json", tc.body, knownLength)
				requireBodyLimitError(t, tc.path, status, payload, splitTestBuffered)
				if tc.hint == "" {
					assert.NotContains(t, payload, "Bodies up to")
					return
				}
				assert.Contains(t, payload, tc.hint)
			})
		}
	}
}

// TestBufferedFallbackKeepsBufferedLimit sends bodies above the buffered
// limit and within the streamed limit that the gateway must buffer. With a
// known length the request is rejected with the buffered limit after the
// routing prefix at most (multipart before any read), and nothing reaches the
// upstream. With an unknown length the read stops at the buffered limit.
func TestBufferedFallbackKeepsBufferedLimit(t *testing.T) {
	for _, knownLength := range []bool{true, false} {
		for _, tc := range fallbackCases(t) {
			name := tc.name + "/unknown length"
			if knownLength {
				name = tc.name + "/known length"
			}
			t.Run(name, func(t *testing.T) {
				reader := installRequestBodyMetrics(t)
				logs := observeLogs(t)
				g := newSplitLimitGateway(t, splitTestBuffered, int64Ptr(splitTestStreamed), nil)

				status, payload := g.post(t, tc.path, tc.contentType, tc.body, knownLength)

				requireBodyLimitError(t, tc.path, status, payload, splitTestBuffered)
				assert.Zero(t, g.upstreamCalls.Load(), "nothing may reach the upstream")
				want := largeBodyKey{requestBodyRejectedBuffered, "unknown"}
				switch {
				case knownLength && tc.reason == requestBodyReasonMultipart:
					want = largeBodyKey{requestBodyRejectedUnread, "le_32MiB"}
					assert.Zero(t, g.bodyBytesRead.Load(), "multipart is rejected before any read")
				case knownLength:
					want = largeBodyKey{requestBodyRejectedBuffered, "le_32MiB"}
					assert.LessOrEqual(t, g.bodyBytesRead.Load(), int64(requestPrefixLimit), "only the routing prefix may be read")
				default:
					assert.Less(t, g.bodyBytesRead.Load(), int64(splitTestBuffered+requestPrefixLimit), "the read must stop at the buffered limit")
				}
				requireOnlyLargeBodyCount(t, collectRequestBodyLarge(t, reader), want)
				entries := logs.FilterField(zap.String("request_id", "req-split-limit")).All()
				require.Len(t, entries, 1)
				fields := entries[0].ContextMap()
				assert.Equal(t, int64(splitTestBuffered), fields["limit_bytes"])
				if want.outcome == requestBodyRejectedBuffered {
					assert.Equal(t, tc.reason, fields["body_read_reason"])
				}
			})
		}
	}
}

// TestEarlyUpstreamResponseStopsStreamedUpload has the upstream answer 401
// before it reads a body above the buffered limit. The client gets that
// status even when the rest of its upload then passes the streamed limit, and
// the gateway stops reading the upload. On HTTP/1 net/http makes the body
// unavailable once the answer is written; on HTTP/2, which is full duplex,
// closing the streamed body is what stops the reads.
//
// Gates, not timers, order the steps: the upload passes the buffered limit,
// waits until the upstream is answering, then keeps sending, below the
// streamed limit, until the client has the answer. It must keep sending: a
// client that stops first gets the answer only when its upload ends, because
// closing the streamed body waits for a read in progress.
func TestEarlyUpstreamResponseStopsStreamedUpload(t *testing.T) {
	for _, protocol := range []string{"http1", "h2c"} {
		t.Run(protocol, func(t *testing.T) {
			upstreamAnswering := make(chan struct{})
			g := newSplitLimitGateway(t, splitTestBuffered, int64Ptr(splitTestStreamed), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(upstreamAnswering)
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = io.WriteString(w, `{"error":"early-reject"}`)
			}))
			pr, pw := io.Pipe()
			t.Cleanup(func() { _ = pw.Close() })
			req, err := http.NewRequest(http.MethodPost, g.url+"/v1/chat/completions", pr)
			require.NoError(t, err)
			req.Host = overLimitTestHost
			req.Header.Set(contentTypeHeader, "application/json")
			result := sendAsync(idleTestClient(protocol), req)

			// Writes block until the gateway reads them, and may block for
			// good once it stops, so they run apart from the test. pw.Close
			// at cleanup ends a blocked write.
			body := earlyFieldsBody(prefixTestModel, false, splitTestStreamed+splitTestBuffered)
			answered := make(chan struct{})
			go func() {
				sent := 0
				write := func(n int) bool {
					_, err := io.WriteString(pw, body[sent:sent+n])
					sent += n
					return err == nil
				}
				if !write(splitTestBuffered + requestPrefixLimit) {
					return
				}
				<-upstreamAnswering
				const chunk = 16 << 10
				for sent+chunk < splitTestStreamed {
					select {
					case <-answered:
						_ = write(len(body) - sent)
						_ = pw.Close()
						return
					default:
					}
					if !write(chunk) {
						return
					}
				}
				// The streamed limit is reached with no answer yet; stop
				// below it, and the await below fails.
			}()
			got := await(t, result, "early upstream response")
			close(answered)
			require.NoError(t, got.err)
			assert.Equal(t, http.StatusUnauthorized, got.status, "the upstream's answer wins over a later overflow")
			assert.Contains(t, got.body, "early-reject")

			// Once the router returns it reads no more of the body, so the
			// count is final. The client offers the rest of its upload, past
			// the streamed limit, from the moment it has the answer.
			await(t, g.handled, "gateway handler return")
			assert.Less(t, g.bodyBytesRead.Load(), int64(splitTestStreamed), "the gateway must stop reading after the upstream answered")
		})
	}
}
