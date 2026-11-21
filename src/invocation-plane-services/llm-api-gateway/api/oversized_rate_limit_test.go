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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	echo "github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/config"
	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/internal/ptr"
	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/models"
	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/nvcf"
	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/ratelimit"
	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/requestctx"
)

func TestNormalizeChatRequestRejectsRequestLargerThanTokenLimit(t *testing.T) {
	t.Parallel()

	// Every bucket is full, so only the request's size can refuse it.
	limiter := &recordingRateLimiter{
		currentValueFn: func(call rateLimitCall) int64 {
			return call.limit.Limit
		},
	}
	handlers := NewHandlers(rateLimitConfig(), nil, limiter)
	gc, rec := newRateLimitGatewayContext()
	gc.store.Set(contextKeyRequestContext, &requestctx.RequestContext{
		RequestID:  "req-oversized",
		OrgID:      "nca-456",
		RoutingKey: "fn-chat",
		Model:      "company-name/model-name",
		ModelSpecs: map[string]nvcf.ModelSpec{
			"company-name/model-name": {TokenRateLimit: "100-M"},
		},
	})

	request := &models.ChatCompletionRequest{
		Model: "fn-chat/company-name/model-name",
		Messages: &[]models.ChatMessage{
			{Role: models.ChatCompletionRoleUser, Content: models.SingleTextContent("hello world")},
		},
		MaxCompletionTokens: ptr.To(uint32(1000)),
	}

	_, err := handlers.normalizeChatRequest(gc, request, nil)
	var httpErr *echo.HTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusRequestEntityTooLarge, httpErr.Code, err.Error())
	message, _ := httpErr.Message.(string)
	require.True(t, strings.HasPrefix(message, "request needs "), message)
	require.Contains(t, message, "more than the rate limit of 100 tokens per minute")
	require.Empty(t, rec.Header().Get("Retry-After"))
	require.Equal(t, "false", rec.Header().Get("X-Should-Retry"))
	for _, call := range limiter.Calls() {
		require.False(t, call.mustConsume, "unexpected must-consume call: %+v", call)
	}
}

func TestOversizedRequestNamesTheSmallestExceededLimit(t *testing.T) {
	t.Parallel()

	for range 50 {
		rlCtx := &RateLimitContext{Results: rateLimitResults{{
			ratelimit.TokensPerMinute: {CurrentValue: 5000, Requested: 6000, RateLimit: ratelimit.RateLimit{Limit: 5000, Period: time.Minute}},
			ratelimit.TokensPerSecond: {CurrentValue: 1000, Requested: 6000, RateLimit: ratelimit.RateLimit{Limit: 1000, Period: time.Second}},
		}}}
		rlCtx.calculateStats()
		err := oversizedRequestError(rlCtx.oversizedDimension, rlCtx.oversizedResult)
		require.EqualError(t, err, "code=413, message=request needs 6000 tokens, more than the rate limit of 1000 tokens per second")
	}
}

func TestNormalizeChatRequestAtExactlyTheTokenLimitStaysRetryable(t *testing.T) {
	t.Parallel()

	request := func() *models.ChatCompletionRequest {
		return &models.ChatCompletionRequest{
			Model: "fn-chat/company-name/model-name",
			Messages: &[]models.ChatMessage{
				{Role: models.ChatCompletionRoleUser, Content: models.SingleTextContent("hello world")},
			},
			MaxCompletionTokens: ptr.To(uint32(1000)),
		}
	}
	admit := func(tokenLimit string, limiter *recordingRateLimiter) (error, *httptest.ResponseRecorder) {
		handlers := NewHandlers(rateLimitConfig(), nil, limiter)
		gc, rec := newRateLimitGatewayContext()
		gc.store.Set(contextKeyRequestContext, &requestctx.RequestContext{
			RequestID:  "req-boundary",
			OrgID:      "nca-456",
			RoutingKey: "fn-chat",
			Model:      "company-name/model-name",
			ModelSpecs: map[string]nvcf.ModelSpec{
				"company-name/model-name": {TokenRateLimit: tokenLimit},
			},
		})
		_, err := handlers.normalizeChatRequest(gc, request(), nil)
		return err, rec
	}

	probe := &recordingRateLimiter{}
	err, _ := admit("1000000-M", probe)
	require.NoError(t, err)
	var requested int64
	for _, call := range probe.Calls() {
		if call.testOnly && strings.HasSuffix(call.key, ":"+string(ratelimit.TokensPerMinute)) {
			requested = call.tokensRequested
		}
	}
	require.Positive(t, requested)

	limiter := &recordingRateLimiter{
		currentValueFn: func(call rateLimitCall) int64 {
			return call.limit.Limit - 1
		},
	}
	err, rec := admit(fmt.Sprintf("%d-M", requested), limiter)
	var httpErr *echo.HTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusTooManyRequests, httpErr.Code, err.Error())
	require.NotEmpty(t, rec.Header().Get("Retry-After"))
}

func TestMessagesRequestLargerThanTokenLimitIsTooLarge(t *testing.T) {
	t.Parallel()

	hits := make(chan struct{}, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits <- struct{}{} }))
	defer backend.Close()
	limiter := &recordingRateLimiter{currentValueFn: func(call rateLimitCall) int64 { return call.limit.Limit }}
	e, _ := messagesTestAPI(t, backend.URL, config.Default(), limiter, nil)

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, messagesHTTPRequest(`{"model":"fn-alpha/company-name/model-name","max_tokens":20000,"messages":[{"role":"user","content":"hello"}]}`))

	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"type":"request_too_large"`)
	require.Contains(t, rec.Body.String(), "more than the rate limit of 10000 tokens per minute")
	require.Empty(t, rec.Header().Get("Retry-After"))
	require.Empty(t, hits)
}
