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
	"bytes"
	"context"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/valyala/bytebufferpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
)

// Model API requests are routed on model and, outside decision routes,
// stream. The gateway reads at most requestPrefixLimit bytes before it routes.
// A body that ends within that prefix is decoded and validated in full, as
// before. A longer body whose routing fields are complete within the prefix,
// as current Codex CLI and OpenAI Python SDK 3.x requests serialize them, is
// forwarded as the prefix followed by the unread body when its route forwards
// the body unchanged. Every other body keeps the buffered path, which reads the
// whole body and validates it before dispatch.
//
// requestPrefixLimit bounds the bytes read and retained per request before the
// gateway chooses between the streamed and buffered paths.
const requestPrefixLimit = 64 << 10

// streamedBodyIdleTimeout ends a streamed request whose client sends no body
// bytes for this long after dispatch, so a stalled upload does not hold the
// upstream request open. Tests shorten it.
var streamedBodyIdleTimeout = 60 * time.Second

// requestPrefixMinRead is the first read size when Content-Length is unknown.
// The prefix buffer grows from there, so small bodies do not take 64 KiB.
const requestPrefixMinRead = 4 << 10

const (
	requestBodyModeStreamed = "streamed"
	requestBodyModeBuffered = "buffered"

	// Streamed: model and stream resolved within the prefix on a direct route.
	requestBodyReasonEarlyFields = "early_fields"
	// Buffered: the whole body arrived within the prefix limit.
	requestBodyReasonComplete = "complete"
	// Buffered: the prefix limit was reached before the routing fields.
	requestBodyReasonLateFields = "late_fields"
	// Buffered: the object ended before the routing fields, or more bytes
	// follow it.
	requestBodyReasonFieldsAbsent = "fields_absent"
	// Buffered: a routing field uses a form the scanner leaves to the full
	// decoder, such as an escaped or non-ASCII key, a null or non-string model,
	// an escaped model value, or a non-boolean stream.
	requestBodyReasonNonstandardFields = "nonstandard_fields"
	// Buffered: the prefix is not a JSON object, has a syntax error, or nests
	// deeper than the decoders accept.
	requestBodyReasonInvalidPrefix = "invalid_prefix"
	// Buffered: multipart image requests are always read in full.
	requestBodyReasonMultipart = "multipart"
	// Buffered: the model is not routed by this endpoint.
	requestBodyReasonModelNotFound = "model_not_found"
	// Buffered: the route is offline or past end of life.
	requestBodyReasonFunctionStatus = "function_status"
	// Buffered: the LLM Gateway route rewrites model in the body.
	requestBodyReasonLLMGateway = "llm_gateway"
	// Buffered: shadow sampling and replay read the body.
	requestBodyReasonShadow = "shadow"
)

// requestBodyMode records how a model request body was read.
type requestBodyMode struct {
	mode   string
	reason string
}

func streamedBodyRead() requestBodyMode {
	return requestBodyMode{mode: requestBodyModeStreamed, reason: requestBodyReasonEarlyFields}
}

func bufferedBodyRead(reason string) requestBodyMode {
	return requestBodyMode{mode: requestBodyModeBuffered, reason: reason}
}

// readModelRequestBody reads the routing fields of a model request. On the
// streamed path request.Body replays the scanned prefix followed by the unread
// body and the returned body has no raw bytes. On the buffered path
// request.Body holds the complete body, as before. On a read error the mode
// names the path that was reading the body, or is empty when the body was
// rejected unread, and the body has the model the prefix named, if any.
func (d *ModelDirector) readModelRequestBody(request *http.Request, modelToNVCFUrl map[string]FunctionInfo) (openAIRequestBody, requestBodyMode, error) {
	decode := decodeOpenAIJSONBody
	needStream := true
	multipartBoundary, isMultipart := "", false
	if isDecisionRoute(request) {
		decode = decodeDecisionRequestBody
		needStream = false
	} else if contentType := parseOpenAIContentType(request.Header.Get(contentTypeHeader)); contentType.mediaType == contentTypeMultipartFormData {
		multipartBoundary, isMultipart = contentType.boundary, true
	}
	if limit, over := d.declaredLengthOverLimit(request, isMultipart); over {
		// The body cannot fit any limit that can apply to it, so fail as
		// reading it would, without reading or forwarding any of it.
		abandonRequestBody(request)
		return openAIRequestBody{}, requestBodyMode{}, fmt.Errorf("failed to read body: %w", &http.MaxBytesError{Limit: limit})
	}
	if isMultipart {
		body, err := extractOpenAIMultipartBody(request, multipartBoundary)
		return body, bufferedBodyRead(requestBodyReasonMultipart), err
	}
	if request.Body == nil {
		_, err := readOpenAIRequestBody(request, "json body")
		return openAIRequestBody{}, requestBodyMode{}, err
	}

	prefix := bytebufferpool.Get()
	complete, err := readRequestPrefix(prefix, request.Body, request.ContentLength)
	if err != nil {
		_ = request.Body.Close()
		releaseRequestBodyBuffer(prefix)
		// No path was chosen yet; only a buffered read can fail here.
		return openAIRequestBody{}, requestBodyMode{mode: requestBodyModeBuffered}, fmt.Errorf("failed to read body: %w", err)
	}
	if complete {
		return finishBufferedRequestBody(request, prefix, decode, requestBodyReasonComplete)
	}

	scan := scanRequestPrefix(prefix.B, needStream)
	reason := scan.fallbackReason
	if reason == "" {
		reason = d.streamedRouteIneligibility(request, modelToNVCFUrl, scan.model)
	}
	if reason != "" {
		if d.declaredOverBufferedLimit(request) {
			// Only a streamed request may pass the buffered limit, and the
			// declared length already does, so reject before reading more.
			abandonRequestBody(request)
			releaseRequestBodyBuffer(prefix)
			return openAIRequestBody{Model: scan.model}, bufferedBodyRead(reason), d.withFieldOrderHint(fmt.Errorf("failed to read body: %w", &http.MaxBytesError{Limit: d.requestBodyLimit}), reason, needStream)
		}
		body, mode, err := finishBufferedRequestBody(request, prefix, decode, reason)
		if err != nil {
			body.Model = scan.model
		}
		return body, mode, d.withFieldOrderHint(err, reason, needStream)
	}
	request.Body = newPrefixReplayBody(prefix, promoteRequestBody(request, d.streamedBodyLimit()))
	return openAIRequestBody{Model: scan.model, Stream: scan.stream}, streamedBodyRead(), nil
}

// bodyLimitHintError adds a hint for the client to a body limit error.
type bodyLimitHintError struct {
	error
	hint string
}

func (e *bodyLimitHintError) Unwrap() error { return e.error }

// bodyLimitHint returns the hint added to err, or "".
func bodyLimitHint(err error) string {
	var hintErr *bodyLimitHintError
	if errors.As(err, &hintErr) {
		return hintErr.hint
	}
	return ""
}

// withFieldOrderHint adds a hint to a buffered limit error when a larger
// streamed limit is set and the request took the buffered path only because
// its routing fields were not in the prefix. The hint names those fields.
func (d *ModelDirector) withFieldOrderHint(err error, reason string, needStream bool) error {
	var maxBytesErr *http.MaxBytesError
	if (reason != requestBodyReasonLateFields && reason != requestBodyReasonFieldsAbsent) ||
		d.streamedBodyLimit() <= d.requestBodyLimit ||
		!errors.As(err, &maxBytesErr) || maxBytesErr.Limit != d.requestBodyLimit {
		return err
	}
	fields := "model and stream fields are"
	if !needStream {
		fields = "model field is"
	}
	return &bodyLimitHintError{error: err, hint: fmt.Sprintf("Bodies up to %d bytes can be accepted when the %s in the first %d bytes of the JSON body.", d.streamedBodyLimit(), fields, requestPrefixLimit)}
}

// streamedBodyLimit is the body limit for a request on the streamed path.
func (d *ModelDirector) streamedBodyLimit() int64 {
	return max(d.requestBodyLimit, d.streamedRequestBodyLimit)
}

// declaredOverBufferedLimit reports a primary request whose declared
// Content-Length is over the buffered limit. Shadow replays are exempt: they
// never pass through the route limit, and rewriting their model can make them
// longer than the primary request.
func (d *ModelDirector) declaredOverBufferedLimit(request *http.Request) bool {
	return request.Body != nil && d.requestBodyLimit > 0 && request.ContentLength > d.requestBodyLimit && !isShadowRequest(request)
}

// declaredLengthOverLimit returns the limit a declared Content-Length is
// over, when no path can accept the body: the buffered limit for multipart
// bodies, which are always buffered, and the streamed limit otherwise.
func (d *ModelDirector) declaredLengthOverLimit(request *http.Request, isMultipart bool) (int64, bool) {
	if !d.declaredOverBufferedLimit(request) {
		return 0, false
	}
	if isMultipart {
		return d.requestBodyLimit, true
	}
	limit := d.streamedBodyLimit()
	return limit, request.ContentLength > limit
}

// readRequestPrefix fills prefix with up to requestPrefixLimit bytes of body.
// complete reports that the body ended within the prefix. Only io.EOF ends the
// body; any other error, including io.ErrUnexpectedEOF from a truncated
// upload, is returned. A known Content-Length sizes the buffer and marks the
// body complete once that many bytes are read, including a body of exactly
// requestPrefixLimit bytes.
func readRequestPrefix(prefix *bytebufferpool.ByteBuffer, body io.Reader, contentLength int64) (bool, error) {
	prefix.B = prefix.B[:0]
	for len(prefix.B) < requestPrefixLimit {
		if len(prefix.B) == cap(prefix.B) {
			grow := max(cap(prefix.B), requestPrefixMinRead)
			if contentLength >= 0 && len(prefix.B) == 0 {
				// Clamp before adding one, so the largest declared length
				// does not overflow.
				grow = int(min(contentLength, requestPrefixLimit-1) + 1)
			}
			prefix.B = slices.Grow(prefix.B, min(grow, requestPrefixLimit-len(prefix.B)))
		}
		end := min(cap(prefix.B), requestPrefixLimit)
		n, err := body.Read(prefix.B[len(prefix.B):end])
		prefix.B = prefix.B[:len(prefix.B)+n]
		if errors.Is(err, io.EOF) || (err == nil && int64(len(prefix.B)) == contentLength) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
	}
	return false, nil
}

// finishBufferedRequestBody reads the rest of the body after prefix and
// applies the complete-body decoder, so buffered requests keep the existing
// validation, field semantics and error precedence.
func finishBufferedRequestBody(request *http.Request, prefix *bytebufferpool.ByteBuffer, decode func(*pooledRequestBody) (openAIRequestBody, error), reason string) (openAIRequestBody, requestBodyMode, error) {
	bodyBytes, err := bufferRemainingRequestBody(request, prefix)
	if err != nil {
		return openAIRequestBody{}, bufferedBodyRead(reason), err
	}
	body, err := decode(bodyBytes)
	return body, bufferedBodyRead(reason), err
}

// streamedRouteIneligibility returns why a resolved model must still be read
// in full, or "" when its route forwards the request body unchanged. Every
// stage that reads the body after routing is excluded here, so the streamed
// path never buffers the body again later.
func (d *ModelDirector) streamedRouteIneligibility(request *http.Request, modelToNVCFUrl map[string]FunctionInfo, model string) string {
	info, ok := modelToNVCFUrl[model]
	switch {
	case !ok:
		return requestBodyReasonModelNotFound
	case info.offlineMessage != "" || isModelExpired(info.eol):
		return requestBodyReasonFunctionStatus
	case info.targetsLLMGateway():
		return requestBodyReasonLLMGateway
	case len(info.shadows) > 0 && d.shadower != nil && !isShadowRequest(request):
		return requestBodyReasonShadow
	}
	return ""
}

// requestPrefixScan holds the routing fields found in a body prefix, or the
// reason the body must be decoded in full.
type requestPrefixScan struct {
	needStream bool

	model      string
	haveModel  bool
	stream     bool
	haveStream bool

	fallbackReason string
}

// scanRequestPrefix finds the routing fields in the leading members of a JSON
// object without decoding skipped values. It mirrors the complete-body
// decoder for the forms it accepts: keys match without regard to ASCII case,
// and the first model and the first stream member win. Each member up to the
// routing fields must be complete and valid within buf. Any other form sets
// fallbackReason so the complete-body decoder decides.
func scanRequestPrefix(buf []byte, needStream bool) requestPrefixScan {
	s := requestPrefixScan{needStream: needStream}
	s.fallbackReason = s.scan(buf)
	return s
}

func (s *requestPrefixScan) resolved() bool {
	return s.haveModel && (s.haveStream || !s.needStream)
}

func (s *requestPrefixScan) scan(buf []byte) string {
	i := skipJSONSpace(buf, 0)
	if i >= len(buf) || buf[i] != '{' {
		return requestBodyReasonInvalidPrefix
	}
	i++
	for {
		i = skipJSONSpace(buf, i)
		if i >= len(buf) {
			return requestBodyReasonLateFields
		}
		if buf[i] == '}' {
			return requestBodyReasonFieldsAbsent
		}
		if buf[i] != '"' {
			return requestBodyReasonInvalidPrefix
		}
		keyEnd, keyEscaped, complete := prefixStringEnd(buf, i)
		if !complete {
			return requestBodyReasonLateFields
		}
		key := buf[i:keyEnd]
		i = skipJSONSpace(buf, keyEnd)
		if i >= len(buf) {
			return requestBodyReasonLateFields
		}
		if buf[i] != ':' {
			return requestBodyReasonInvalidPrefix
		}
		valueStart := skipJSONSpace(buf, i+1)
		valueEnd, valueDepth, complete := prefixValueEnd(buf, valueStart)
		if !complete {
			return requestBodyReasonLateFields
		}
		// The delimiter must be in the prefix so a literal such as true is
		// known to end there.
		i = skipJSONSpace(buf, valueEnd)
		if i >= len(buf) {
			return requestBodyReasonLateFields
		}
		if buf[i] != ',' && buf[i] != '}' {
			return requestBodyReasonInvalidPrefix
		}
		value := buf[valueStart:valueEnd]
		if !stdjson.Valid(key) || !stdjson.Valid(value) || valueDepth >= jsonMaxDepth {
			return requestBodyReasonInvalidPrefix
		}
		if reason := s.member(key, keyEscaped, value); reason != "" {
			return reason
		}
		if s.resolved() {
			return ""
		}
		if buf[i] == '}' {
			return requestBodyReasonFieldsAbsent
		}
		i++
	}
}

// member records a routing field from one complete member. Later members
// that match an already recorded field are skipped, as the complete-body
// decoder skips them under first-wins priority.
func (s *requestPrefixScan) member(key []byte, keyEscaped bool, value []byte) string {
	field, reason := s.routingField(key, keyEscaped)
	if reason != "" {
		return reason
	}
	switch {
	case field == "model" && !s.haveModel:
		model, ok := plainJSONString(value)
		if !ok {
			return requestBodyReasonNonstandardFields
		}
		s.model, s.haveModel = model, true
	case field == "stream" && !s.haveStream:
		switch string(value) {
		case "true":
			s.stream = true
		case "false":
			s.stream = false
		default:
			return requestBodyReasonNonstandardFields
		}
		s.haveStream = true
	}
	return ""
}

// routingField names the routing field a key selects, or returns a fallback
// reason when the key may select one in a form the scanner does not mirror.
func (s *requestPrefixScan) routingField(key []byte, keyEscaped bool) (string, string) {
	fields := []string{"model"}
	if s.needStream {
		fields = append(fields, "stream")
	}
	name := key[1 : len(key)-1]
	if keyEscaped {
		var decoded string
		if err := stdjson.Unmarshal(key, &decoded); err != nil {
			return "", requestBodyReasonInvalidPrefix
		}
		for _, field := range fields {
			if strings.EqualFold(decoded, field) {
				return "", requestBodyReasonNonstandardFields
			}
		}
		return "", ""
	}
	for _, field := range fields {
		// bytes.EqualFold also folds Unicode forms such as U+017F for s.
		// Only an ASCII spelling is known to match the decoder's field.
		if bytes.EqualFold(name, []byte(field)) {
			if !isASCII(name) {
				return "", requestBodyReasonNonstandardFields
			}
			return field, ""
		}
	}
	return "", ""
}

// plainJSONString returns a non-empty JSON string of printable ASCII with no
// escapes. Other model values are left to the complete-body decoder.
func plainJSONString(value []byte) (string, bool) {
	if len(value) < 3 || value[0] != '"' {
		return "", false
	}
	inner := value[1 : len(value)-1]
	for _, c := range inner {
		if c < 0x20 || c > 0x7e || c == '\\' {
			return "", false
		}
	}
	return string(inner), true
}

func isASCII(b []byte) bool {
	for _, c := range b {
		if c >= 0x80 {
			return false
		}
	}
	return true
}

// prefixStringEnd returns the offset after the closing quote of the string that
// starts at buf[start], and whether it contains an escape.
func prefixStringEnd(buf []byte, start int) (int, bool, bool) {
	escaped := false
	for i := start + 1; i < len(buf); i++ {
		switch buf[i] {
		case '\\':
			escaped = true
			i++
		case '"':
			return i + 1, escaped, true
		}
	}
	return 0, escaped, false
}

// jsonMaxDepth is the nesting limit of the complete-body decoders,
// encoding/json and goccy/go-json, for a whole document. The top-level object
// is one level, so a member value nested jsonMaxDepth levels makes the
// document too deep to decode.
const jsonMaxDepth = 10000

// prefixValueEnd returns the offset after the value that starts at buf[start]
// and how many levels it nests. It only locates the end; stdjson.Valid checks
// the syntax afterwards.
func prefixValueEnd(buf []byte, start int) (int, int, bool) {
	if start >= len(buf) {
		return 0, 0, false
	}
	switch buf[start] {
	case '"':
		end, _, complete := prefixStringEnd(buf, start)
		return end, 0, complete
	case '{', '[':
		depth, maxDepth := 0, 0
		for i := start; i < len(buf); i++ {
			switch buf[i] {
			case '"':
				end, _, complete := prefixStringEnd(buf, i)
				if !complete {
					return 0, 0, false
				}
				i = end - 1
			case '{', '[':
				depth++
				maxDepth = max(maxDepth, depth)
			case '}', ']':
				depth--
				if depth == 0 {
					return i + 1, maxDepth, true
				}
			}
		}
		return 0, 0, false
	default:
		for i := start; i < len(buf); i++ {
			switch buf[i] {
			case ',', '}', ']', ' ', '\t', '\n', '\r':
				return i, 0, true
			}
		}
		return 0, 0, false
	}
}

// prefixReplayBody forwards the scanned prefix and then the unread client
// body. The prefix buffer returns to the pool once it has been replayed or the
// body is closed. Transports may close the body from another goroutine, so the
// prefix is guarded. Reads from the client body are not guarded here; the
// server body serializes them with Close, so Close waits for a read already in
// progress to return.
//
// After dispatch, a timer bounds each wait for the client body, so a stalled
// upload ends the upstream request. Only time spent waiting inside a read of
// the client body counts; time the transport spends writing to a slow
// upstream does not. When a read has waited idleTimeout, the timer sets a read
// deadline in the past on the inbound connection, which ends that read. The
// timer runs only while the handler runs, because the server's response
// controller must not be used after the handler returns: the handler stops it
// before returning.
type prefixReplayBody struct {
	mu     sync.Mutex
	prefix *bytebufferpool.ByteBuffer
	offset int
	closed bool
	rest   io.ReadCloser

	// expire sets the inbound read deadline. It is nil when no idle limit
	// applies or the limit has stopped.
	expire       func(time.Time) error
	idleTimeout  time.Duration
	idleTimer    *time.Timer
	waitingSince time.Time // start of the client body read in progress, if any
}

func newPrefixReplayBody(prefix *bytebufferpool.ByteBuffer, rest io.ReadCloser) *prefixReplayBody {
	return &prefixReplayBody{prefix: prefix, rest: rest}
}

// limitStreamedBodyIdle bounds each wait for a streamed primary request body
// by streamedBodyIdleTimeout. The returned function stops it and must run
// before the handler returns.
func limitStreamedBodyIdle(writer http.ResponseWriter, request *http.Request) func() {
	body, ok := request.Body.(*prefixReplayBody)
	if !ok || isShadowRequest(request) || streamedBodyIdleTimeout <= 0 {
		return func() {}
	}
	body.mu.Lock()
	defer body.mu.Unlock()
	if body.closed {
		return func() {}
	}
	body.expire = http.NewResponseController(writer).SetReadDeadline
	body.idleTimeout = streamedBodyIdleTimeout
	body.idleTimer = time.AfterFunc(body.idleTimeout, body.checkIdle)
	return body.stopIdleLimit
}

// checkIdle ends a client body read that has waited idleTimeout, and
// otherwise checks again when the read in progress, or a read that starts
// now, would reach it.
func (b *prefixReplayBody) checkIdle() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.expire == nil {
		return
	}
	next := b.idleTimeout
	if !b.waitingSince.IsZero() {
		waited := time.Since(b.waitingSince)
		if waited >= b.idleTimeout {
			_ = b.expire(time.Unix(1, 0))
			b.expire = nil
			return
		}
		next -= waited
	}
	b.idleTimer.Reset(next)
}

func (b *prefixReplayBody) stopIdleLimit() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.expire = nil
	if b.idleTimer != nil {
		b.idleTimer.Stop()
	}
}

func (b *prefixReplayBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, http.ErrBodyReadAfterClose
	}
	if b.prefix != nil {
		n := copy(p, b.prefix.B[b.offset:])
		b.offset += n
		if b.offset == len(b.prefix.B) {
			b.releasePrefixLocked()
		}
		b.mu.Unlock()
		return n, nil
	}
	if b.expire != nil {
		b.waitingSince = time.Now()
	}
	idle := b.idleTimeout
	b.mu.Unlock()

	n, err := b.rest.Read(p)
	b.mu.Lock()
	b.waitingSince = time.Time{}
	b.mu.Unlock()
	if err == nil {
		return n, nil
	}
	// The upload ended or failed, so no later read needs the limit.
	b.stopIdleLimit()
	if errors.Is(err, io.EOF) {
		return n, err
	}
	if idle > 0 && errors.Is(err, os.ErrDeadlineExceeded) {
		err = fmt.Errorf("no request body bytes for %s: %w", idle, err)
	}
	return n, &clientBodyReadError{err: err}
}

// clientBodyReadError marks a failure to read the client upload after a
// streamed request was dispatched, so the proxy error handler attributes it
// to the client rather than to the upstream.
type clientBodyReadError struct {
	err error
}

func (e *clientBodyReadError) Error() string {
	return "read client request body: " + e.err.Error()
}

func (e *clientBodyReadError) Unwrap() error {
	return e.err
}

func (b *prefixReplayBody) Close() error {
	b.mu.Lock()
	b.closed = true
	b.releasePrefixLocked()
	b.mu.Unlock()
	return b.rest.Close()
}

func (b *prefixReplayBody) releasePrefixLocked() {
	if b.prefix == nil {
		return
	}
	releaseRequestBodyBuffer(b.prefix)
	b.prefix = nil
}

const (
	requestBodyMetricName  = "nvcf_ai_api_gateway_request_body_reads"
	requestBodyModeLabel   = "mode"
	requestBodyReasonLabel = "reason"
)

var requestBodyReadsCounter otelmetric.Int64Counter = metricnoop.Int64Counter{}

var requestBodyModes = []requestBodyMode{
	streamedBodyRead(),
	bufferedBodyRead(requestBodyReasonComplete),
	bufferedBodyRead(requestBodyReasonLateFields),
	bufferedBodyRead(requestBodyReasonFieldsAbsent),
	bufferedBodyRead(requestBodyReasonNonstandardFields),
	bufferedBodyRead(requestBodyReasonInvalidPrefix),
	bufferedBodyRead(requestBodyReasonMultipart),
	bufferedBodyRead(requestBodyReasonModelNotFound),
	bufferedBodyRead(requestBodyReasonFunctionStatus),
	bufferedBodyRead(requestBodyReasonLLMGateway),
	bufferedBodyRead(requestBodyReasonShadow),
}

func setupRequestBodyMetrics() error {
	meter := otel.GetMeterProvider().Meter(shadowMetricsScope)
	counter, err := meter.Int64Counter(
		requestBodyMetricName,
		otelmetric.WithDescription("Number of model API request bodies read, by streamed or buffered mode and the reason for it."),
	)
	if err != nil {
		return fmt.Errorf("create request body reads counter: %w", err)
	}
	requestBodyReadsCounter = counter
	for _, mode := range requestBodyModes {
		addRequestBodyRead(context.Background(), mode, 0)
	}
	return setupRequestBodyLargeMetrics(meter)
}

func addRequestBodyRead(ctx context.Context, mode requestBodyMode, count int64) {
	requestBodyReadsCounter.Add(ctx, count, otelmetric.WithAttributes(
		attribute.String(requestBodyModeLabel, mode.mode),
		attribute.String(requestBodyReasonLabel, mode.reason),
	))
}

// recordRequestBodyMode labels the request span and, for primary requests,
// counts the read. Shadow replays read an in-memory copy and are not counted.
func recordRequestBodyMode(request *http.Request, mode requestBodyMode) {
	trace.SpanFromContext(request.Context()).SetAttributes(
		traceAttrRequestBodyMode.String(mode.mode),
		traceAttrRequestBodyReason.String(mode.reason),
	)
	if !isShadowRequest(request) {
		addRequestBodyRead(request.Context(), mode, 1)
	}
}
