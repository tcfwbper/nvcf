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
	"ai-api-gateway-service/middleware"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
)

// Requests rejected for body size are counted and logged where the error
// response is written, from the request headers alone. The body of a
// rejected request is never read for telemetry.
const (
	requestBodyLargeMetricName    = "nvcf_ai_api_gateway_request_body_large"
	requestBodyLargeOutcomeLabel  = "outcome"
	requestBodyLargeSizeLabel     = "size"
	requestBodyRejectedLogMessage = "request body over the size limit rejected"

	// The declared Content-Length was over the limit, so no body byte was read.
	requestBodyRejectedUnread = "rejected_unread"
	// The body passed the limit while the gateway read it in full.
	requestBodyRejectedBuffered = "rejected_buffered"
	// The body passed the limit after the streamed request was dispatched.
	requestBodyRejectedStreamed = "rejected_streamed"
	// A streamed request body ended above the buffered limit. It is counted
	// once per request and is not a rejection.
	requestBodyStreamedComplete = "streamed_complete"

	// maxClientProductLength bounds the User-Agent product logged per rejection.
	maxClientProductLength = 64
)

var requestBodyLargeCounter otelmetric.Int64Counter = metricnoop.Int64Counter{}

var requestBodyLargeOutcomes = []string{requestBodyRejectedUnread, requestBodyRejectedBuffered, requestBodyRejectedStreamed, requestBodyStreamedComplete}

// requestBodySizeBuckets are exclusive ranges of the declared Content-Length.
var requestBodySizeBuckets = []string{"le_32MiB", "le_64MiB", "le_128MiB", "gt_128MiB", "unknown"}

func setupRequestBodyLargeMetrics(meter otelmetric.Meter) error {
	counter, err := meter.Int64Counter(
		requestBodyLargeMetricName,
		otelmetric.WithDescription("Number of primary model API requests rejected for body size, by where the limit was reached, or streamed past the buffered limit, by the declared Content-Length range."),
	)
	if err != nil {
		return fmt.Errorf("create request body large counter: %w", err)
	}
	requestBodyLargeCounter = counter
	for _, outcome := range requestBodyLargeOutcomes {
		for _, size := range requestBodySizeBuckets {
			addRequestBodyLarge(context.Background(), outcome, size, 0)
		}
	}
	return nil
}

func addRequestBodyLarge(ctx context.Context, outcome, size string, count int64) {
	requestBodyLargeCounter.Add(ctx, count, otelmetric.WithAttributes(
		attribute.String(requestBodyLargeOutcomeLabel, outcome),
		attribute.String(requestBodyLargeSizeLabel, size),
	))
}

// requestBodyRejection describes a request rejected for body size. reason,
// model and functionID are empty when not known at the rejection point.
// requestID and userAgent are the client's, read before dispatch, because
// dispatch can rewrite the request headers.
type requestBodyRejection struct {
	outcome    string
	reason     string
	model      string
	functionID string
	limit      int64
	requestID  string
	userAgent  string
}

func newRequestBodyRejection(request *http.Request) requestBodyRejection {
	return requestBodyRejection{
		requestID: request.Header.Get(middleware.RequestIDHeader),
		userAgent: request.UserAgent(),
	}
}

// recordRequestBodyRejected counts and logs a primary request rejected for
// body size. Call it once, where the error response is written. Shadow
// replays are skipped.
func recordRequestBodyRejected(request *http.Request, rejection requestBodyRejection) {
	if isShadowRequest(request) {
		return
	}
	size := requestBodySizeBucket(request.ContentLength)
	addRequestBodyLarge(request.Context(), rejection.outcome, size, 1)

	fields := append(middleware.TraceFields(request.Context()),
		zap.String("request_id", rejection.requestID),
		zap.String("outcome", rejection.outcome),
		zap.String("size", size),
		zap.Int64("limit_bytes", rejection.limit),
		zap.String("user_agent", clientProductVersion(rejection.userAgent)),
	)
	if request.ContentLength >= 0 {
		fields = append(fields, zap.Int64("content_length", request.ContentLength))
	}
	if rejection.reason != "" {
		fields = append(fields, zap.String("body_read_reason", rejection.reason))
	}
	if rejection.model != "" {
		fields = append(fields, zap.String("model", rejection.model))
	}
	if rejection.functionID != "" {
		fields = append(fields, zap.String("function_id", rejection.functionID))
	}
	zap.L().Info(requestBodyRejectedLogMessage, fields...)
}

// recordBodyLimitBeforeDispatch records a model request body read that failed
// on the route limit before dispatch. An empty mode means the declared
// Content-Length was rejected before any byte was read. A model the prefix
// named is logged only when the mapping routes it, so client text that names
// no route is not logged.
func recordBodyLimitBeforeDispatch(request *http.Request, body openAIRequestBody, mode requestBodyMode, modelToNVCFUrl map[string]FunctionInfo, err error) {
	var maxBytesErr *http.MaxBytesError
	if !errors.As(err, &maxBytesErr) {
		return
	}
	rejection := newRequestBodyRejection(request)
	rejection.outcome = requestBodyRejectedBuffered
	if mode.mode == "" {
		rejection.outcome = requestBodyRejectedUnread
	}
	rejection.reason = mode.reason
	rejection.limit = maxBytesErr.Limit
	if info, ok := modelToNVCFUrl[body.Model]; ok && body.Model != "" {
		rejection.model, rejection.functionID = body.Model, info.functionId
	}
	recordRequestBodyRejected(request, rejection)
}

// requestBodySizeBucket names the range of a declared Content-Length, or
// unknown when the length was not declared.
func requestBodySizeBucket(contentLength int64) string {
	const mib = 1 << 20
	switch {
	case contentLength < 0:
		return "unknown"
	case contentLength <= 32*mib:
		return "le_32MiB"
	case contentLength <= 64*mib:
		return "le_64MiB"
	case contentLength <= 128*mib:
		return "le_128MiB"
	default:
		return "gt_128MiB"
	}
}

// clientProductVersion keeps the leading product of a User-Agent, such as
// curl/8.7.1, plus a following version token, as in OpenAI/Python 3.49.0.
// Everything from the first comment on is dropped, a token with a character
// outside a safe set is dropped whole, and the result is at most
// maxClientProductLength bytes.
func clientProductVersion(userAgent string) string {
	if comment := strings.IndexByte(userAgent, '('); comment >= 0 {
		userAgent = userAgent[:comment]
	}
	tokens := strings.Fields(userAgent)
	if len(tokens) == 0 || !isUserAgentToken(tokens[0]) {
		return ""
	}
	product := tokens[0]
	if len(tokens) > 1 && tokens[1][0] >= '0' && tokens[1][0] <= '9' && isUserAgentToken(tokens[1]) {
		product += " " + tokens[1]
	}
	if len(product) > maxClientProductLength {
		product = product[:maxClientProductLength]
	}
	return product
}

func isUserAgentToken(token string) bool {
	for i := 0; i < len(token); i++ {
		switch c := token[i]; {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.' || c == '_' || c == '-' || c == '/' || c == '+':
		default:
			return false
		}
	}
	return true
}

// streamedRejectionKey carries what is known about a streamed request before
// dispatch to the proxy error handler, which records a body that passes the
// limit after dispatch.
type streamedRejectionKey struct{}

func withStreamedRequestRoute(request *http.Request, model, functionID string) *http.Request {
	rejection := newRequestBodyRejection(request)
	rejection.model, rejection.functionID = model, functionID
	return request.WithContext(context.WithValue(request.Context(), streamedRejectionKey{}, rejection))
}

// streamedRequestRejection returns the rejection captured before dispatch, or
// one read from request when none was captured.
func streamedRequestRejection(request *http.Request) requestBodyRejection {
	if rejection, ok := request.Context().Value(streamedRejectionKey{}).(requestBodyRejection); ok {
		return rejection
	}
	return newRequestBodyRejection(request)
}
