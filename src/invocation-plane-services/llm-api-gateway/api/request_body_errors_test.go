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
	"encoding/json"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	echo "github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/config"
)

func errorMessage(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body), rec.Body.String())
	return body.Error.Message
}

func TestJSONRoutesDescribeBadBodiesWithoutInternals(t *testing.T) {
	t.Parallel()

	const validChat = `{"model":"fn-alpha/company/model","messages":[{"role":"user","content":"hi"}]}`
	tests := []struct {
		name        string
		path        string
		contentType string
		body        string
		wantStatus  int
		wantMessage string
	}{
		{name: "text/plain", path: "/v1/chat/completions", contentType: "text/plain", body: validChat, wantStatus: http.StatusUnsupportedMediaType, wantMessage: "Content-Type must be application/json"},
		{name: "form", path: "/v1/chat/completions", contentType: echo.MIMEApplicationForm, body: validChat, wantStatus: http.StatusUnsupportedMediaType, wantMessage: "Content-Type must be application/json"},
		{name: "xml", path: "/v1/chat/completions", contentType: echo.MIMEApplicationXML, body: validChat, wantStatus: http.StatusUnsupportedMediaType, wantMessage: "Content-Type must be application/json"},
		{name: "multipart", path: "/v1/chat/completions", contentType: "multipart/form-data; boundary=x", body: validChat, wantStatus: http.StatusUnsupportedMediaType, wantMessage: "Content-Type must be application/json"},
		{name: "empty body", path: "/v1/chat/completions", body: "", wantStatus: http.StatusBadRequest, wantMessage: "request body must be a JSON object"},
		{name: "null body", path: "/v1/chat/completions", body: "null", wantStatus: http.StatusBadRequest, wantMessage: "request body must be a JSON object"},
		{name: "array body", path: "/v1/chat/completions", body: "[]", wantStatus: http.StatusBadRequest, wantMessage: "request body must be a JSON object"},
		{name: "number body", path: "/v1/chat/completions", body: "42", wantStatus: http.StatusBadRequest, wantMessage: "request body must be a JSON object"},
		{name: "trailing comma", path: "/v1/chat/completions", body: `{"model":"fn-alpha/company/model",}`, wantStatus: http.StatusBadRequest, wantMessage: "request body must be valid JSON"},
		{name: "truncated", path: "/v1/chat/completions", body: `{"model":`, wantStatus: http.StatusBadRequest, wantMessage: "request body must be valid JSON"},
		{name: "trailing data", path: "/v1/chat/completions", body: `{"model":"fn-alpha/company/model","messages":[{"role":"user","content":"hi"}]}x`, wantStatus: http.StatusBadRequest, wantMessage: "request body must be valid JSON"},
		{name: "embeddings trailing data", path: "/v1/embeddings", body: `{}x`, wantStatus: http.StatusBadRequest, wantMessage: "request body must be valid JSON"},
		{
			name:        "negative max_tokens",
			path:        "/v1/chat/completions",
			body:        `{"model":"fn-alpha/company/model","messages":[{"role":"user","content":"hi"}],"max_tokens":-1}`,
			wantStatus:  http.StatusBadRequest,
			wantMessage: `field "max_tokens" must be an integer from 0 to 4294967295`,
		},
		{
			name:        "nested field type",
			path:        "/v1/chat/completions",
			body:        `{"model":"fn-alpha/company/model","messages":[{"role":"user","content":"hi"}],"stream_options":{"include_usage":"x"}}`,
			wantStatus:  http.StatusBadRequest,
			wantMessage: `field "stream_options.include_usage" must be a boolean`,
		},
		{name: "responses empty body", path: "/v1/responses", body: "", wantStatus: http.StatusBadRequest, wantMessage: "request body must be a JSON object"},
		{name: "embeddings empty body", path: "/v1/embeddings", body: "", wantStatus: http.StatusBadRequest, wantMessage: "request body must be a JSON object"},
		{name: "embeddings array body", path: "/v1/embeddings", body: "[]", wantStatus: http.StatusBadRequest, wantMessage: "request body must be a JSON object"},
		{name: "embeddings without model", path: "/v1/embeddings", body: "{}", wantStatus: http.StatusBadRequest, wantMessage: "model prefix is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newTestAPI(config.Default())
			req := httptest.NewRequest(http.MethodPost, tt.path, strings.NewReader(tt.body))
			if tt.contentType != "" {
				req.Header.Set(echo.HeaderContentType, tt.contentType)
			}
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)

			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
			message := errorMessage(t, rec)
			require.Equal(t, tt.wantMessage, message)
			for _, internal := range []string{"internal=", "code=", "json:", "Go ", "map[string]"} {
				require.NotContains(t, message, internal)
			}
		})
	}
}

func TestUnroutedJSONHandlersCheckBodyBeforeRouting(t *testing.T) {
	t.Parallel()

	handlers := NewHandlers(config.Default(), nil, nil).AsOpenAIProxyHandlers()
	tests := []struct {
		name        string
		handler     echo.HandlerFunc
		body        string
		wantMessage string
	}{
		{name: "reranking array body", handler: handlers.Reranking, body: "[]", wantMessage: "request body must be a JSON object"},
		{name: "text to speech trailing data", handler: handlers.TextToSpeech, body: "{}x", wantMessage: "request body must be valid JSON"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			c := NewGatewayContext(echo.New().NewContext(req, httptest.NewRecorder()))

			err := tt.handler(c)
			var httpErr *echo.HTTPError
			require.ErrorAs(t, err, &httpErr)
			require.Equal(t, http.StatusBadRequest, httpErr.Code)
			require.Equal(t, tt.wantMessage, httpErr.Message)
		})
	}
}

type deadlineReader struct{}

func (deadlineReader) Read([]byte) (int, error) {
	addr := &net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 8080}
	return 0, &net.OpError{Op: "read", Net: "tcp", Source: addr, Addr: addr, Err: os.ErrDeadlineExceeded}
}

func TestTimedOutBodyReadReturns408WithoutAddresses(t *testing.T) {
	t.Parallel()

	for _, limit := range []int64{8, 0} {
		handlerBody := "not called"
		e := newBodyLimitAPI(limit, &handlerBody)

		req := httptest.NewRequest(http.MethodPost, "/", deadlineReader{})
		req.ContentLength = -1
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		require.Equal(t, http.StatusRequestTimeout, rec.Code, "limit %d: %s", limit, rec.Body.String())
		require.Contains(t, rec.Body.String(), "timed out reading request body")
		require.NotContains(t, rec.Body.String(), "10.0.0.1")
		require.Equal(t, "not called", handlerBody)
	}
}

func TestSpeechToSpeechFieldErrorsNameTheField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		field       string
		value       string
		wantMessage string
	}{
		{name: "type mismatch", field: "llm", value: `{"model":42}`, wantMessage: `field "llm.model" must be a string`},
		{name: "malformed", field: "tts", value: `{"voice":`, wantMessage: "`tts` field must be valid JSON"},
		{name: "not an object", field: "stt", value: `[]`, wantMessage: "`stt` field must be a JSON object"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			form := &multipart.Form{Value: map[string][]string{tt.field: {tt.value}}}
			_, err := parseSpeechToSpeechRequest(form)
			require.EqualError(t, err, tt.wantMessage)
		})
	}
}
