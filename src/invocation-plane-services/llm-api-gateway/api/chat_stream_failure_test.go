/*
SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

package api

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	echo "github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/internal/ptr"
	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/models"
	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/provider"
	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/ratelimit"
	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/requestctx"
)

const chatStreamPartialChunk = `data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"company-name/model-name","choices":[{"index":0,"delta":{"role":"assistant","content":"partial"},"finish_reason":null}]}` + "\n\n"

// An upstream chat stream that stops before [DONE] (Pylon output timeout,
// worker hang) or reports an error mid-stream must reach the client as a
// failure, never as a finished answer.
func TestChatCompletionsFailsWhenUpstreamStreamEndsEarly(t *testing.T) {
	t.Parallel()

	const partial = chatStreamPartialChunk
	// n=2: choice 1 finished, choice 0 is still decoding when the stream stops.
	const siblingFinished = partial +
		`data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"company-name/model-name","choices":[{"index":1,"delta":{"role":"assistant","content":"HEADS"},"finish_reason":"stop"}]}` + "\n\n"
	// vLLM and LiteLLM report a failure after the 200 as an error event, then send [DONE].
	errorEvent := func(member string) string {
		return `data: {"error":` + member + "}\n\n" + "data: [DONE]\n\n"
	}
	upstreamError := func(member string) string {
		return partial + errorEvent(member)
	}
	const vllmError = `{"message":"engine failed","type":"InternalServerError","param":null,"code":500}`
	// Dynamo 1.6.0+ and vLLM put the status of a request they reject after the 200 into code.
	rejected := func(code string) string {
		return `{"message":"bad input","type":"invalid_request_error","param":"image","code":` + code + `}`
	}
	// OpenRouter puts the error next to choices, with finish_reason "error".
	const errorWithChoices = partial +
		`data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"company-name/model-name","error":{"code":502,"message":"engine failed"},"choices":[{"index":0,"delta":{"content":""},"finish_reason":"error"}]}` + "\n\n" +
		"data: [DONE]\n\n"

	type testCase struct {
		name         string
		stream       bool
		upstreamBody string
		// dropMidway aborts the upstream connection, which is what the gateway
		// sees once the tunnel propagates the Pylon reset.
		dropMidway bool
		// wantStatus is the client status of a unary request, or the status that
		// logs and metrics record for a stream; 0 means 502.
		wantStatus int
		// wantMessage and wantType are the error fields; empty means
		// msgUpstreamStreamIncomplete and server_error.
		wantMessage string
		wantType    string
		// wantParam is the error param as raw JSON; empty means null.
		wantParam string
		// wantCode is the error code as raw JSON; empty means null.
		wantCode string
	}
	tests := []testCase{
		{name: "stream clean end after output", stream: true, upstreamBody: partial},
		{name: "stream clean end before output", stream: true},
		{name: "stream clean end n=2 one choice finished", stream: true, upstreamBody: siblingFinished},
		{name: "stream connection drops after output", stream: true, upstreamBody: partial, dropMidway: true},
		{
			name: "stream upstream error event after output", stream: true, upstreamBody: upstreamError(vllmError),
			wantMessage: "engine failed", wantType: "InternalServerError", wantCode: `"500"`,
		},
		{
			name: "stream upstream error string after output", stream: true,
			upstreamBody: upstreamError(`"engine failed"`), wantMessage: "engine failed",
		},
		{
			name: "stream upstream error event without message", stream: true,
			upstreamBody: upstreamError(`{"code":500}`), wantCode: `"500"`,
		},
		{
			name: "stream upstream error with choices", stream: true, upstreamBody: errorWithChoices,
			wantMessage: "engine failed", wantCode: `"502"`,
		},
		{
			name: "stream upstream 4xx error event", stream: true, upstreamBody: errorEvent(rejected("400")),
			wantStatus: http.StatusBadRequest, wantMessage: "bad input",
			wantType: "invalid_request_error", wantParam: `"image"`, wantCode: `"400"`,
		},
		{
			name:   "stream upstream 4xx error event with string code",
			stream: true, upstreamBody: errorEvent(rejected(`"400"`)),
			wantStatus: http.StatusBadRequest, wantMessage: "bad input",
			wantType: "invalid_request_error", wantParam: `"image"`, wantCode: `"400"`,
		},
		{
			name:   "stream upstream 4xx error event without type",
			stream: true, upstreamBody: errorEvent(`{"message":"bad input","code":400}`),
			wantStatus: http.StatusBadRequest, wantMessage: "bad input", wantType: "invalid_request_error",
			wantCode: `"400"`,
		},
		{name: "unary clean end after output", upstreamBody: partial},
		{name: "unary clean end n=2 one choice finished", upstreamBody: siblingFinished},
		{name: "unary connection drops after output", upstreamBody: partial, dropMidway: true},
		{name: "unary upstream error event after output", upstreamBody: upstreamError(vllmError)},
		{name: "unary upstream error with choices", upstreamBody: errorWithChoices},
		// The client went away, so 499 is not the client's error to receive.
		{name: "unary upstream 499 error event", upstreamBody: errorEvent(rejected("499"))},
		{
			name:         "unary upstream error event with text code",
			upstreamBody: errorEvent(rejected(`"context_length_exceeded"`)),
		},
		{name: "unary upstream error event without code", upstreamBody: errorEvent(`{"message":"bad input"}`)},
		{name: "unary upstream error string", upstreamBody: errorEvent(`"engine failed"`)},
		{
			name: "unary upstream 4xx error event after output", upstreamBody: upstreamError(rejected("400")),
			wantStatus: http.StatusBadRequest, wantMessage: "bad input",
			wantType: "invalid_request_error", wantParam: `"image"`, wantCode: "400",
		},
		{
			name: "unary upstream 4xx error event with string code", upstreamBody: errorEvent(rejected(`"400"`)),
			wantStatus: http.StatusBadRequest, wantMessage: "bad input",
			wantType: "invalid_request_error", wantParam: `"image"`, wantCode: `"400"`,
		},
	}
	// A 4xx error event reaches a unary client with that status and the backend's error object.
	for _, status := range []int{400, 401, 403, 404, 413, 415, 422, 429} {
		code := strconv.Itoa(status)
		tests = append(tests, testCase{
			name: "unary upstream " + code + " error event", upstreamBody: errorEvent(rejected(code)),
			wantStatus: status, wantMessage: "bad input",
			wantType: "invalid_request_error", wantParam: `"image"`, wantCode: code,
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set(echo.HeaderContentType, "text/event-stream")
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, tt.upstreamBody)
				w.(http.Flusher).Flush()
				if tt.dropMidway {
					panic(http.ErrAbortHandler)
				}
			}))
			t.Cleanup(upstream.Close)
			e := newPassthroughAPI(t, upstream.URL)
			handlerErr := captureHandlerError(e)

			streamJSON := "false"
			if tt.stream {
				streamJSON = "true"
			}
			clientBody := `{"model":"fn-alpha/company-name/model-name","messages":[{"role":"user","content":"hi"}],"stream":` + streamJSON + `}`
			rec := servePassthroughRequest(t, e, "/v1/chat/completions", clientBody)
			body := rec.Body.String()

			wantStatus := cmp.Or(tt.wantStatus, http.StatusBadGateway)
			var errorBody struct {
				Error struct {
					Message string          `json:"message"`
					Type    string          `json:"type"`
					Param   json.RawMessage `json:"param"`
					Code    json.RawMessage `json:"code"`
				} `json:"error"`
			}
			if tt.stream {
				// The client keeps its 200; logs, metrics and traces record wantStatus.
				require.Equal(t, http.StatusOK, rec.Code)
				var httpErr *echo.HTTPError
				require.ErrorAs(t, *handlerErr, &httpErr)
				require.Equal(t, wantStatus, httpErr.Code)

				require.NotContains(t, body, "data: [DONE]", "a truncated stream must not end like a finished one")
				events := strings.Split(strings.TrimSuffix(body, "\n\n"), "\n\n")
				last, isData := strings.CutPrefix(events[len(events)-1], "data: ")
				require.True(t, isData, body)
				require.NoError(t, json.Unmarshal([]byte(last), &errorBody),
					"last event must be an error event: %q", body)
			} else {
				require.Equal(t, wantStatus, rec.Code, body)
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errorBody), body)
			}
			require.Equal(t, cmp.Or(tt.wantMessage, msgUpstreamStreamIncomplete), errorBody.Error.Message)
			require.Equal(t, cmp.Or(tt.wantType, "server_error"), errorBody.Error.Type)
			require.JSONEq(t, cmp.Or(tt.wantParam, "null"), cmp.Or(string(errorBody.Error.Param), "null"))
			require.JSONEq(t, cmp.Or(tt.wantCode, "null"), cmp.Or(string(errorBody.Error.Code), "null"))
		})
	}
}

func TestChatCompletionsStreamSendsNoErrorEventWhenClientGoesAway(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// The upstream holds the stream open until the gateway cancels it, which
	// happens when the client goes away after the first relayed chunk.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set(echo.HeaderContentType, "text/event-stream")
		_, _ = io.WriteString(w, chatStreamPartialChunk)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	t.Cleanup(upstream.Close)
	e := newPassthroughAPI(t, upstream.URL)
	handlerErr := captureHandlerError(e)

	clientBody := `{"model":"fn-alpha/company-name/model-name","messages":[{"role":"user","content":"hi"}],"stream":true}`
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", strings.NewReader(clientBody))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := &cancelOnWriteRecorder{ResponseRecorder: httptest.NewRecorder(), cancel: cancel}
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.Contains(t, body, `"content":"partial"`)
	require.NotContains(t, body, `"error"`)
	require.NotContains(t, body, "data: [DONE]")
	var httpErr *echo.HTTPError
	require.False(t, errors.As(*handlerErr, &httpErr), "client disconnect recorded as %v", *handlerErr)
}

// feedStreamProvider streams the events the test sends, so the test sees when
// nobody reads them any more.
type feedStreamProvider struct {
	*stubStreamProvider
	events chan provider.StreamEvent
}

func (p *feedStreamProvider) Stream(
	context.Context,
	*requestctx.RequestContext,
	*provider.NormalizedRequest,
) (<-chan provider.StreamEvent, error) {
	return p.events, nil
}

// A handler that stops early, here because the client write failed, must not
// strand the goroutines that relay the upstream stream.
func TestChatCompletionsStreamDrainsUpstreamAfterClientWriteFails(t *testing.T) {
	t.Parallel()

	events := make(chan provider.StreamEvent)
	handlers := NewHandlers(nil, &feedStreamProvider{stubStreamProvider: &stubStreamProvider{}, events: events}, nil, nil)
	gc, _ := newRateLimitGatewayContext()
	gc.Response().Writer = &failingWriteRecorder{ResponseRecorder: httptest.NewRecorder()}
	request := &provider.NormalizedRequest{ChatRequest: &models.ChatCompletionRequest{
		Model:    "fn-chat/company-name/model-name",
		Messages: &[]models.ChatMessage{{Role: models.ChatCompletionRoleUser, Content: models.SingleTextContent("hi")}},
	}}
	chunk := provider.StreamEvent{Chunk: &models.ChatCompletionChunk{
		Choices: []models.ChatCompletionChunkChoice{{Delta: models.ChatCompletionChunkDelta{Content: ptr.To("hi")}}},
	}}

	handlerErr := make(chan error, 1)
	go func() { handlerErr <- handlers.AsOpenAIChatHandlers().streamChatCompletion(gc, request) }()
	events <- chunk
	require.ErrorIs(t, <-handlerErr, errClientGone)

	for range 3 {
		select {
		case events <- chunk:
		case <-time.After(time.Second):
			t.Fatal("the upstream stream is not read after the handler returned")
		}
	}
	close(events)
}

// The drain keeps reading after the handler releases the output reservation, so a late
// usage chunk must not finalize the tokens again and charge the input twice.
func TestChatCompletionsStreamDoesNotFinalizeTokensAfterRelease(t *testing.T) {
	t.Parallel()

	limiter := &recordingRateLimiter{}
	plan := ratelimit.NewAdmissionPlan(
		limiter,
		ratelimit.ResourceLimit{
			SubjectKey:      rateLimitSubjectKey("nca-456", "", "fn-chat"),
			SubjectRepr:     "org `nca-456` function `fn-chat`",
			Level:           ratelimit.LevelOrg,
			TokensPerMinute: 100,
		},
		"req-release",
	)
	_, err := plan.Commit(context.Background(), ratelimit.ResourceRequest{InputTokens: 4, OutputTokens: 6})
	require.NoError(t, err)

	events := make(chan provider.StreamEvent)
	handlers := NewHandlers(nil, &feedStreamProvider{stubStreamProvider: &stubStreamProvider{}, events: events}, limiter, nil)
	gc, _ := newRateLimitGatewayContext()
	gc.Response().Writer = &failingWriteRecorder{ResponseRecorder: httptest.NewRecorder()}
	request := &provider.NormalizedRequest{
		ChatRequest:     &models.ChatCompletionRequest{Model: "fn-chat/company-name/model-name"},
		InputTokens:     4,
		MaxOutputTokens: 6,
		AdmissionPlan:   plan,
	}
	chunk := provider.StreamEvent{Chunk: &models.ChatCompletionChunk{
		Choices: []models.ChatCompletionChunkChoice{{Delta: models.ChatCompletionChunkDelta{Content: ptr.To("hi")}}},
	}}
	usageChunk := provider.StreamEvent{Chunk: &models.ChatCompletionChunk{
		Usage: &models.ChatCompletionUsage{PromptTokens: 4, CompletionTokens: 1, TotalTokens: 5},
	}}

	handlerErr := make(chan error, 1)
	go func() { handlerErr <- handlers.AsOpenAIChatHandlers().streamChatCompletion(gc, request) }()
	events <- chunk
	require.ErrorIs(t, <-handlerErr, errClientGone)

	// The relay handles an event before it reads the next one, so the second send
	// returns only after the usage chunk is handled.
	events <- usageChunk
	events <- chunk
	close(events)

	calls := limiter.Calls()
	require.Len(t, calls, 2, "want only the commit and the release, got %+v", calls)
	require.Equal(t, int64(-6), calls[1].tokensRequested)
}
