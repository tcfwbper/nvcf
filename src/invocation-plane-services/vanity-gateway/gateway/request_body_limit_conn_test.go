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
	"ai-api-gateway-service/router"
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
)

// assembledSplitLimitServer serves the production stack, the h2c handler
// from createHttp2Server around buildChiMux, on a real listener, so the
// connection handling of net/http and of every middleware wrapper applies.
func assembledSplitLimitServer(t *testing.T, buffered, streamed int64, upstream http.Handler) string {
	t.Helper()
	backend := httptest.NewServer(upstream)
	t.Cleanup(backend.Close)
	mux, err := buildChiMux(splitLimitMappings(), Config{
		NvcfApiEndpoint:             backend.URL,
		LLMGatewayEndpoint:          backend.URL,
		MaxRequestSizeBytes:         int64Ptr(buffered),
		MaxStreamedRequestSizeBytes: int64Ptr(streamed),
	})
	require.NoError(t, err)
	server := httptest.NewServer(createHttp2Server("", router.NewSwappableRouter(mux)).Handler)
	t.Cleanup(server.Close)
	return server.Listener.Addr().String()
}

func drainingUpstream() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	})
}

// writeChunked writes body with chunked framing, then the terminator, then
// trailer, ignoring write errors because the server may close first.
func writeChunked(conn net.Conn, body, trailer string) {
	w := bufio.NewWriter(conn)
	for chunk := range slices.Chunk([]byte(body), 32<<10) {
		fmt.Fprintf(w, "%x\r\n%s\r\n", len(chunk), chunk)
	}
	_, _ = io.WriteString(w, "0\r\n\r\n"+trailer)
	_ = w.Flush()
}

// http1OverflowOutcome sends body chunked on a raw connection with a second
// request pipelined after it, and returns the first response's status and
// body and what followed: the second response's status, or 0 when the
// server closed the connection.
func http1OverflowOutcome(t *testing.T, addr, body string) (int, string, int) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))
	_, err = io.WriteString(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: "+overLimitTestHost+"\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n")
	require.NoError(t, err)
	go writeChunked(conn, body, "GET /health HTTP/1.1\r\nHost: "+overLimitTestHost+"\r\n\r\n")
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	require.NoError(t, err)
	payload, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	next, err := http.ReadResponse(br, nil)
	if err != nil {
		require.True(t, resp.Close, "a connection closed after the response must say so")
		return resp.StatusCode, string(payload), 0
	}
	_ = next.Body.Close()
	return resp.StatusCode, string(payload), next.StatusCode
}

func h2cClient() *http.Client {
	return &http.Client{Transport: &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, network, addr)
		},
	}}
}

// TestLimitOverflowConnectionHandling checks connection handling when a body
// passes its limit, on the assembled stack and a real listener.
func TestLimitOverflowConnectionHandling(t *testing.T) {
	const buffered, streamed = splitTestBuffered, splitTestStreamed

	// The middleware wrappers hide net/http's close-on-overflow hook, so the
	// server drains a small remainder and keeps the connection, or closes it
	// when the remainder is large. Either way the next request on the
	// connection must be read at its real start, and a streamed overflow
	// must be handled exactly as a buffered one.
	t.Run("HTTP/1 keep-alive after overflow", func(t *testing.T) {
		addr := assembledSplitLimitServer(t, buffered, streamed, drainingUpstream())
		for _, extra := range []int{10, 2 << 20} {
			t.Run(strconv.Itoa(extra)+" bytes over", func(t *testing.T) {
				late := `{"input":"` + filler(buffered+extra) + `","model":"` + prefixTestModel + `","stream":false}`
				status, payload, bufferedNext := http1OverflowOutcome(t, addr, late)
				requireBodyLimitError(t, "/v1/chat/completions", status, payload, buffered)

				status, payload, streamedNext := http1OverflowOutcome(t, addr, earlyFieldsBody(prefixTestModel, false, streamed+extra))
				requireBodyLimitError(t, "/v1/chat/completions", status, payload, streamed)

				require.Contains(t, []int{0, http.StatusOK}, streamedNext, "the pipelined request must be read at its real start or the connection closed")
				require.Equal(t, bufferedNext, streamedNext, "a streamed overflow must leave the connection as a buffered one does")
			})
		}
	})

	t.Run("h2c overflow leaves the sibling stream intact", func(t *testing.T) {
		upstreamSums := make(chan [32]byte, 2)
		addr := assembledSplitLimitServer(t, buffered, streamed, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hash := sha256.New()
			if _, err := io.Copy(hash, r.Body); err != nil {
				return
			}
			var sum [32]byte
			copy(sum[:], hash.Sum(nil))
			upstreamSums <- sum
			w.WriteHeader(http.StatusOK)
		}))
		client := h2cClient()
		siblingBody := earlyFieldsBody(prefixTestModel, false, splitTestBetween)
		siblingReader, siblingWriter := io.Pipe()
		t.Cleanup(func() { _ = siblingWriter.Close() })
		sibling, err := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/chat/completions", siblingReader)
		require.NoError(t, err)
		sibling.Host = overLimitTestHost
		sibling.Header.Set(contentTypeHeader, "application/json")
		siblingResult := sendAsync(client, sibling)
		// Hold the sibling upload open across the overflow on the same
		// connection.
		_, err = io.WriteString(siblingWriter, siblingBody[:splitTestBetween/2])
		require.NoError(t, err)

		over, err := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/chat/completions", io.MultiReader(strings.NewReader(earlyFieldsBody(prefixTestModel, false, streamed+1))))
		require.NoError(t, err)
		over.Host = overLimitTestHost
		over.Header.Set(contentTypeHeader, "application/json")
		overResult := await(t, sendAsync(client, over), "over-limit response")
		require.NoError(t, overResult.err)
		requireBodyLimitError(t, "/v1/chat/completions", overResult.status, overResult.body, streamed)

		_, err = io.WriteString(siblingWriter, siblingBody[splitTestBetween/2:])
		require.NoError(t, err)
		require.NoError(t, siblingWriter.Close())
		got := await(t, siblingResult, "sibling response")
		require.NoError(t, got.err)
		require.Equal(t, http.StatusOK, got.status, got.body)
		require.Equal(t, sha256.Sum256([]byte(siblingBody)), await(t, upstreamSums, "sibling upstream body"))
	})

	// HTTP/2 answers while the upload stalls. HTTP/1 answers once the client
	// ends the upload: as on the buffered path, the server reads a small
	// remainder before it replies, because the middleware wrappers hide
	// net/http's close-on-overflow hook.
	t.Run("stalled upload at overflow is answered", func(t *testing.T) {
		for _, protocol := range []string{"http1", "h2c"} {
			t.Run(protocol, func(t *testing.T) {
				addr := assembledSplitLimitServer(t, buffered, streamed, drainingUpstream())
				pr, pw := io.Pipe()
				t.Cleanup(func() { _ = pw.Close() })
				req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/chat/completions", pr)
				require.NoError(t, err)
				req.Host = overLimitTestHost
				req.Header.Set(contentTypeHeader, "application/json")
				result := sendAsync(idleTestClient(protocol), req)
				body := earlyFieldsBody(prefixTestModel, false, streamed+requestPrefixLimit)
				// Send one byte past the limit and stall without ending the
				// upload.
				go func() {
					_, _ = io.WriteString(pw, body[:streamed+1])
					if protocol == "http1" {
						time.Sleep(100 * time.Millisecond)
						_ = pw.Close()
					}
				}()
				got := await(t, result, "over-limit response while the upload stalls")
				require.NoError(t, got.err)
				requireBodyLimitError(t, "/v1/chat/completions", got.status, got.body, streamed)
			})
		}
	})

	t.Run("Expect 100-continue", func(t *testing.T) {
		addr := assembledSplitLimitServer(t, buffered, streamed, drainingUpstream())
		for _, tc := range []struct {
			name           string
			length         int
			expectContinue bool
		}{
			{"between the limits is asked for and forwarded", splitTestBetween, true},
			{"over the streamed limit is rejected unasked", streamed + 1, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				conn, err := net.Dial("tcp", addr)
				require.NoError(t, err)
				defer conn.Close()
				require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))
				_, err = fmt.Fprintf(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n", overLimitTestHost, tc.length)
				require.NoError(t, err)
				br := bufio.NewReader(conn)
				resp, err := http.ReadResponse(br, nil)
				require.NoError(t, err)
				if !tc.expectContinue {
					require.NotEqual(t, http.StatusContinue, resp.StatusCode, "the gateway must not ask for a body it rejects")
					payload, err := io.ReadAll(resp.Body)
					require.NoError(t, err)
					requireBodyLimitError(t, "/v1/chat/completions", resp.StatusCode, string(payload), streamed)
					return
				}
				require.Equal(t, http.StatusContinue, resp.StatusCode)
				_, err = io.WriteString(conn, earlyFieldsBody(prefixTestModel, false, tc.length))
				require.NoError(t, err)
				// The proxy also relays the upstream's own 100 Continue.
				for resp.StatusCode == http.StatusContinue {
					resp, err = http.ReadResponse(br, nil)
					require.NoError(t, err)
				}
				payload, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode, string(payload))
			})
		}
	})
}

// TestRejectionDoesNotWaitForPausedUpload sends a request that is rejected
// for its declared length on a real HTTP/1 keep-alive connection, with an
// unread remainder small enough that net/http would drain it before
// replying. The client sends only what the gateway reads before it decides,
// then waits: the error must arrive while the client is paused.
func TestRejectionDoesNotWaitForPausedUpload(t *testing.T) {
	const buffered = requestPrefixLimit
	lateFields := `{"input":"` + filler(3*requestPrefixLimit) + `","model":"` + prefixTestModel + `","stream":false}`
	tests := []struct {
		name     string
		streamed int64
		body     string
		sent     int
		limit    int64
	}{
		// Falls back after the prefix with a length between the limits.
		{"fallback after the prefix", 1 << 20, lateFields, requestPrefixLimit, buffered},
		// Declares more than the streamed limit, so nothing is read.
		{"over the streamed limit", 2 * requestPrefixLimit, lateFields, 0, 2 * requestPrefixLimit},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Less(t, len(tc.body)-tc.sent, 256<<10, "the remainder must fit the window net/http drains")
			var upstreamCalls atomic.Int32
			addr := assembledSplitLimitServer(t, buffered, tc.streamed, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upstreamCalls.Add(1)
				w.WriteHeader(http.StatusOK)
			}))
			conn, err := net.Dial("tcp", addr)
			require.NoError(t, err)
			defer conn.Close()
			head := fmt.Sprintf("POST /v1/chat/completions HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n", overLimitTestHost, len(tc.body))
			_, err = io.WriteString(conn, head+tc.body[:tc.sent])
			require.NoError(t, err)

			// The client sends nothing more. The deadline only bounds a
			// gateway that waits for the rest of the upload.
			require.NoError(t, conn.SetReadDeadline(time.Now().Add(10*time.Second)))
			br := bufio.NewReader(conn)
			resp, err := http.ReadResponse(br, nil)
			require.NoError(t, err, "the gateway must answer while the client is paused")
			payload, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			requireBodyLimitError(t, "/v1/chat/completions", resp.StatusCode, string(payload), tc.limit)
			assert.True(t, resp.Close, "the connection must close after the reply, since the body was not read")
			assert.Zero(t, upstreamCalls.Load())

			// The client still sends nothing and keeps its side open. The
			// gateway must close the connection rather than wait for the rest
			// of the upload.
			_, err = br.ReadByte()
			require.ErrorIs(t, err, io.EOF, "the gateway must close the connection without the rest of the upload")
		})
	}
}
