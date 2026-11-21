// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	echo "github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestRequestModelIDReadsMissingContentTypeAsJSON(t *testing.T) {
	t.Parallel()

	const jsonBody = `{"model":"fn-alpha/company/model","messages":[]}`
	var multipartBody bytes.Buffer
	writer := multipart.NewWriter(&multipartBody)
	require.NoError(t, writer.WriteField("model", "fn-alpha/company/model"))
	require.NoError(t, writer.Close())

	tests := []struct {
		name        string
		contentType string
		body        string
		wantModel   string
		wantKey     string
	}{
		{name: "missing header", body: jsonBody, wantModel: "fn-alpha/company/model", wantKey: "fn-alpha"},
		{name: "application/json", contentType: "application/json", body: jsonBody, wantModel: "fn-alpha/company/model", wantKey: "fn-alpha"},
		{name: "application/json with charset", contentType: "application/json; charset=utf-8", body: jsonBody, wantModel: "fn-alpha/company/model", wantKey: "fn-alpha"},
		{name: "multipart", contentType: writer.FormDataContentType(), body: multipartBody.String(), wantModel: "fn-alpha/company/model", wantKey: "fn-alpha"},
		{name: "text/plain stays unread", contentType: "text/plain", body: jsonBody},
		{name: "malformed header stays unread", contentType: "application/json; charset", body: jsonBody},
		{name: "missing header with non-JSON body", body: "model=fn-alpha/company/model"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			newRequest := func() *http.Request {
				req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(tt.body))
				if tt.contentType != "" {
					req.Header.Set(echo.HeaderContentType, tt.contentType)
				}
				return req
			}

			require.Equal(t, tt.wantModel, requestModelID(newRequest()))
			require.Equal(t, tt.wantKey, requestRoutingKey(newRequest()))
		})
	}
}

func TestBindReadsMissingContentTypeAsJSONAndKeepsRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		body        string
		wantModel   string
		wantStatus  int
	}{
		{name: "missing header binds JSON", body: `{"model":"fn-alpha/company/model"}`, wantModel: "fn-alpha/company/model"},
		{name: "missing header with invalid JSON", body: `{"model":`, wantStatus: http.StatusBadRequest},
		{name: "text/plain stays unsupported", contentType: "text/plain", body: `{"model":"fn-alpha/company/model"}`, wantStatus: http.StatusUnsupportedMediaType},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(tt.body))
			if tt.contentType != "" {
				req.Header.Set(echo.HeaderContentType, tt.contentType)
			}
			sentContentType := req.Header.Values(echo.HeaderContentType)
			c := NewGatewayContext(echo.New().NewContext(req, httptest.NewRecorder()))

			var got struct {
				Model string `json:"model"`
			}
			err := c.Bind(&got)

			if tt.wantStatus == 0 {
				require.NoError(t, err)
				require.Equal(t, tt.wantModel, got.Model)
			} else {
				var httpErr *echo.HTTPError
				require.ErrorAs(t, err, &httpErr)
				require.Equal(t, tt.wantStatus, httpErr.Code)
			}
			require.Same(t, req, c.Request(), "Bind must leave the original request in the context")
			require.Equal(t, sentContentType, req.Header.Values(echo.HeaderContentType), "Bind must not change the client's headers")
		})
	}
}

// jsonRouteCase is a route that reads the routing key from the body model,
// with a valid client body and an upstream answer the gateway accepts.
type jsonRouteCase struct {
	name, path, body, wantModel string
	upstreamContentType         string
	upstreamBody                string
}

var jsonRouteCases = []jsonRouteCase{
	{
		name:                "decisions",
		path:                decisionsEndpointPath,
		body:                `{"model":"fn-alpha/company/decision-model","input":"The package is delayed.","questions":[{"type":"predicate","name":"escalate"}]}`,
		wantModel:           "company/decision-model",
		upstreamContentType: echo.MIMEApplicationJSON,
		upstreamBody:        `{"answers":[{"name":"escalate","probability":0.99,"type":"predicate"}]}`,
	},
	{
		name:                "systemone",
		path:                systemOneEndpointPath,
		body:                `{"model":"fn-alpha/company/decision-model","state":"hello","questions":{"q1":{"type":"noul"}}}`,
		wantModel:           "company/decision-model",
		upstreamContentType: echo.MIMEApplicationJSON,
		upstreamBody:        `{"answers":{"q1":{"noul":0.99,"type":"noul"}}}`,
	},
	{
		name:                "chat completions",
		path:                "/v1/chat/completions",
		body:                `{"model":"fn-alpha/company-name/model-name","messages":[{"role":"user","content":"hello"}]}`,
		wantModel:           "company-name/model-name",
		upstreamContentType: "text/event-stream",
		upstreamBody:        passthroughChatUpstreamSSE,
	},
	{
		name:                "responses",
		path:                "/v1/responses",
		body:                `{"model":"fn-alpha/company-name/model-name","input":"hello"}`,
		wantModel:           "company-name/model-name",
		upstreamContentType: "text/event-stream",
		upstreamBody:        "event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":0,\"response\":{\"id\":\"resp_1\",\"object\":\"response\",\"status\":\"completed\",\"created_at\":1,\"output\":[]}}\n\n",
	},
	{
		name:                "embeddings",
		path:                "/v1/embeddings",
		body:                `{"model":"fn-alpha/company-name/model-name","input":["hello"]}`,
		wantModel:           "company-name/model-name",
		upstreamContentType: echo.MIMEApplicationJSON,
		upstreamBody:        `{"object":"list","data":[],"model":"company-name/model-name","usage":{"prompt_tokens":1,"total_tokens":1}}`,
	},
	{
		name:                "messages",
		path:                messagesEndpointPath,
		body:                `{"model":"fn-alpha/company-name/model-name","max_tokens":16,"messages":[{"role":"user","content":"hello"}]}`,
		wantModel:           "company-name/model-name",
		upstreamContentType: echo.MIMEApplicationJSON,
		upstreamBody:        `{"type":"message","content":[],"usage":{"input_tokens":1,"output_tokens":1}}`,
	},
}

func serveWithContentType(t *testing.T, e *echo.Echo, path, contentType, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set(echo.HeaderContentType, contentType)
	}
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	return rec
}

// A missing header changes how the gateway reads the body, not what it
// forwards: the upstream gets the same bytes as for application/json.
func TestJSONRoutesServeBodyWithoutContentType(t *testing.T) {
	t.Parallel()

	for _, tt := range jsonRouteCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			forwarded := make(map[string][]byte, 2)
			for _, contentType := range []string{echo.MIMEApplicationJSON, ""} {
				upstream := newPassthroughUpstream(t, tt.upstreamContentType, tt.upstreamBody)
				e := newPassthroughAPI(t, upstream.server.URL)

				rec := serveWithContentType(t, e, tt.path, contentType, tt.body)

				require.Equal(t, http.StatusOK, rec.Code, "Content-Type %q: %s", contentType, rec.Body.String())
				require.Len(t, upstream.requests, 1, "request must reach upstream")
				forwarded[contentType] = <-upstream.requests
			}
			outbound := jsonObject(t, forwarded[""])
			require.JSONEq(t, `"`+tt.wantModel+`"`, string(outbound["model"]))
			require.Equal(t, string(forwarded[echo.MIMEApplicationJSON]), string(forwarded[""]))
		})
	}
}

func TestJSONRoutesStillRejectNonJSONContentType(t *testing.T) {
	t.Parallel()

	for _, tt := range jsonRouteCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			upstream := newPassthroughUpstream(t, tt.upstreamContentType, tt.upstreamBody)
			e := newPassthroughAPI(t, upstream.server.URL)

			rec := serveWithContentType(t, e, tt.path, "text/plain", tt.body)

			require.Equal(t, http.StatusUnsupportedMediaType, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), "Content-Type must be application/json")
			if tt.path == messagesEndpointPath {
				var body anthropicErrorResponse
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				require.Equal(t, "invalid_request_error", body.Error.Type)
			}
			require.Empty(t, upstream.requests, "request must not reach upstream")
		})
	}
}
