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
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	directModel        = "acme/direct-model"
	directFunctionID   = "direct-func-1"
	callerAPIKey       = "caller-api-key"
	callerBearer       = "Bearer " + callerAPIKey
	callerRequestID    = "req-caller-credentials"
	callerTraceparent  = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	callerHeader       = "X-Caller-Feature"
	configuredHeader   = "X-Provider-Feature"
	configuredValue    = "enabled"
	invocationAuthBody = "Unauthorized access"
)

var callerCookies = []string{"qa=1", "session=caller"}

type modelRoute struct {
	name string
	path string
	add  func(*config.GatewayConfig, config.ModelFunctionDetails)
	body func(t *testing.T, model string) (string, string)
}

func jsonModelBody(_ *testing.T, model string) (string, string) {
	return `{"model":"` + model + `"}`, "application/json"
}

func multipartModelBody(t *testing.T, model string) (string, string) {
	t.Helper()
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	require.NoError(t, writer.WriteField("model", model))
	require.NoError(t, writer.Close())
	return body.String(), writer.FormDataContentType()
}

// modelRoutes lists every model API route on the shared host.
func modelRoutes() []modelRoute {
	entries := func(entry config.ModelFunctionDetails) map[string]config.ModelFunctionDetails {
		return map[string]config.ModelFunctionDetails{"m": entry}
	}
	return []modelRoute{
		{"chat completions", "/v1/chat/completions", func(c *config.GatewayConfig, e config.ModelFunctionDetails) { c.OpenAI.ChatCompletions = entries(e) }, jsonModelBody},
		{"completions", "/v1/completions", func(c *config.GatewayConfig, e config.ModelFunctionDetails) { c.OpenAI.Completions = entries(e) }, jsonModelBody},
		{"embeddings", "/v1/embeddings", func(c *config.GatewayConfig, e config.ModelFunctionDetails) { c.OpenAI.Embeddings = entries(e) }, jsonModelBody},
		{"responses", "/v1/responses", func(c *config.GatewayConfig, e config.ModelFunctionDetails) { c.OpenAI.Responses = entries(e) }, jsonModelBody},
		{"messages", messagesPath, func(c *config.GatewayConfig, e config.ModelFunctionDetails) { c.AnthropicMessages = entries(e) }, jsonModelBody},
		{"decisions", decisionsPath, func(c *config.GatewayConfig, e config.ModelFunctionDetails) { c.OpenAI.Decisions = entries(e) }, jsonModelBody},
		{"systemone", systemOnePath, func(c *config.GatewayConfig, e config.ModelFunctionDetails) { c.TypeSafeSystemOne = entries(e) }, jsonModelBody},
		{"image generations", "/v1/images/generations", func(c *config.GatewayConfig, e config.ModelFunctionDetails) { c.OpenAI.ImageGenerations = entries(e) }, jsonModelBody},
		{"image edits", "/v1/images/edits", func(c *config.GatewayConfig, e config.ModelFunctionDetails) { c.OpenAI.ImageEdits = entries(e) }, multipartModelBody},
		{"image variations", "/v1/images/variations", func(c *config.GatewayConfig, e config.ModelFunctionDetails) { c.OpenAI.ImageVariations = entries(e) }, multipartModelBody},
	}
}

// setCallerCredentials sends every credential header a caller may attach,
// with repeated X-Api-Key and Cookie values.
func setCallerCredentials(req *http.Request) {
	req.Header.Set("Authorization", callerBearer)
	req.Header.Add("X-Api-Key", callerAPIKey)
	req.Header.Add("X-Api-Key", callerAPIKey+"-second")
	for _, cookie := range callerCookies {
		req.Header.Add("Cookie", cookie)
	}
}

func setCallerContextHeaders(req *http.Request) {
	req.Header.Set("X-Request-Id", callerRequestID)
	req.Header.Set("Traceparent", callerTraceparent)
	req.Header.Set(callerHeader, "kept")
}

// requireCallerCredentialsDropped checks what a function would see from the
// invocation service: Authorization still arrives for NVCF to authenticate,
// caller X-Api-Key and Cookie do not, and every other header is untouched.
func requireCallerCredentialsDropped(t *testing.T, headers http.Header, contentType string) {
	t.Helper()
	assert.Empty(t, headers.Values("X-Api-Key"), "caller X-Api-Key must not reach the function")
	assert.Empty(t, headers.Values("Cookie"), "caller Cookie must not reach the function")
	assert.Equal(t, []string{callerBearer}, headers.Values("Authorization"), "NVCF authenticates the invocation with Authorization")
	assert.Equal(t, contentType, headers.Get("Content-Type"))
	assert.Equal(t, callerRequestID, headers.Get("X-Request-Id"))
	assert.NotEmpty(t, headers.Get("Traceparent"))
	assert.Equal(t, "kept", headers.Get(callerHeader))
	assert.Equal(t, configuredValue, headers.Get(configuredHeader), "configured customHeaders still apply")
}

func TestDirectModelRoutesDropCallerAPIKeyAndCookie(t *testing.T) {
	for _, route := range modelRoutes() {
		for _, usePexec := range []bool{false, true} {
			name := route.name
			if usePexec {
				name += " pexec"
			}
			t.Run(name, func(t *testing.T) {
				requests := make(chan capturedRequest, 1)
				nvcf := captureServer(t, requests, nil)
				mappings := &config.GatewayConfig{}
				mappings.OpenAI.Host = openAIHost
				route.add(mappings, config.ModelFunctionDetails{
					ModelName:     directModel,
					FunctionID:    directFunctionID,
					UsePexec:      usePexec,
					CustomHeaders: config.CustomHeaders{configuredHeader: configuredValue},
				})
				mux := openAIMux(t, mappings, nvcf.URL, "")

				body, contentType := route.body(t, directModel)
				req := openAIRequest(t, route.path, body)
				req.Header.Set("Content-Type", contentType)
				setCallerCredentials(req)
				setCallerContextHeaders(req)
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, req)

				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				received := awaitRequest(t, requests)
				if usePexec {
					assert.Equal(t, "/v2/nvcf/pexec/functions/"+directFunctionID, received.path)
				} else {
					assert.Equal(t, route.path, received.path)
					assert.Equal(t, directFunctionID, received.headers.Get("function-id"))
				}
				requireCallerCredentialsDropped(t, received.headers, contentType)
			})
		}
	}
}

func TestVanityRoutesDropCallerAPIKeyAndCookie(t *testing.T) {
	for _, usePexec := range []bool{false, true} {
		name := "function-id"
		if usePexec {
			name = "pexec"
		}
		t.Run(name, func(t *testing.T) {
			requests := make(chan capturedRequest, 1)
			nvcf := captureServer(t, requests, nil)
			mappings := &config.GatewayConfig{}
			mappings.Vanity = map[string]config.VanityEntry{
				"example": {
					Host: "vanity.test",
					Paths: map[string]config.PathFunctionDetails{
						"infer": {
							Path:          "/v1/example/infer",
							FunctionID:    "vanity-func",
							UsePexec:      usePexec,
							CustomHeaders: config.CustomHeaders{configuredHeader: configuredValue},
						},
					},
				},
			}
			mux := openAIMux(t, mappings, nvcf.URL, "")

			req := httptest.NewRequest(http.MethodPost, "/v1/example/infer", strings.NewReader(`{}`))
			req.Host = "vanity.test"
			req.Header.Set("Content-Type", "application/json")
			setCallerCredentials(req)
			setCallerContextHeaders(req)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			requireCallerCredentialsDropped(t, awaitRequest(t, requests).headers, "application/json")
		})
	}
}

// Configured customHeaders are operator settings, not caller credentials, so
// they still override the caller's value for the same header.
func TestDirectRoutesStillSendConfiguredAPIKeyAndCookie(t *testing.T) {
	requests := make(chan capturedRequest, 1)
	nvcf := captureServer(t, requests, nil)
	mappings := &config.GatewayConfig{}
	mappings.OpenAI.Host = openAIHost
	mappings.OpenAI.ChatCompletions = map[string]config.ModelFunctionDetails{
		"m": {
			ModelName:  directModel,
			FunctionID: directFunctionID,
			CustomHeaders: config.CustomHeaders{
				"X-Api-Key": "configured-key",
				"Cookie":    "configured=1",
			},
		},
	}
	mux := openAIMux(t, mappings, nvcf.URL, "")

	req := openAIRequest(t, "/v1/chat/completions", `{"model":"`+directModel+`"}`)
	req.Header.Set("Content-Type", "application/json")
	setCallerCredentials(req)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	received := awaitRequest(t, requests)
	assert.Equal(t, []string{"configured-key"}, received.headers.Values("X-Api-Key"))
	assert.Equal(t, []string{"configured=1"}, received.headers.Values("Cookie"))
}

// invocationAuthServer stands in for the invocation service, which accepts a
// request only with exactly one bearer Authorization header and never reads
// X-Api-Key.
func invocationAuthServer(t *testing.T, requests chan<- capturedRequest) *httptest.Server {
	return captureServer(t, requests, func(w http.ResponseWriter, r *http.Request) {
		values := r.Header.Values("Authorization")
		if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(invocationAuthBody))
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}

// Anthropic SDKs send only x-api-key. On the direct path the invocation
// service authenticates, so such a caller is rejected there as before; the
// gateway neither builds Authorization from the key nor forwards the key.
func TestDirectMessagesAPIKeyOnlyCallerGetsInvocationAuthResult(t *testing.T) {
	requests := make(chan capturedRequest, 1)
	nvcf := invocationAuthServer(t, requests)
	mappings := &config.GatewayConfig{}
	mappings.OpenAI.Host = openAIHost
	mappings.AnthropicMessages = map[string]config.ModelFunctionDetails{
		"m": {ModelName: directModel, FunctionID: directFunctionID},
	}
	mux := openAIMux(t, mappings, nvcf.URL, "")

	req := openAIRequest(t, messagesPath, `{"model":"`+directModel+`"}`)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Anthropic-Version", "2023-06-01")
	req.Header.Set("X-Api-Key", callerAPIKey)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Equal(t, invocationAuthBody, rec.Body.String(), "the invocation service answer passes through unchanged")
	received := awaitRequest(t, requests)
	assert.Empty(t, received.headers.Values("Authorization"))
	assert.Empty(t, received.headers.Values("X-Api-Key"))
}

// The LLM Gateway authenticates Messages callers with x-api-key, so LLM routes
// keep forwarding these headers.
func TestLLMGatewayRoutesKeepCallerAPIKeyForGatewayAuth(t *testing.T) {
	for _, route := range modelRoutes() {
		switch route.path {
		case "/v1/chat/completions", messagesPath, decisionsPath, systemOnePath:
		default:
			continue
		}
		t.Run(route.name, func(t *testing.T) {
			requests := make(chan capturedRequest, 1)
			llm := captureServer(t, requests, nil)
			mappings := &config.GatewayConfig{}
			mappings.OpenAI.Host = openAIHost
			route.add(mappings, config.ModelFunctionDetails{
				ModelName:    directModel,
				FunctionID:   directFunctionID,
				FunctionType: config.FunctionTypeLLM,
			})
			mux := openAIMux(t, mappings, "http://nvcf.invalid", llm.URL)

			body, contentType := route.body(t, directModel)
			req := openAIRequest(t, route.path, body)
			req.Header.Set("Content-Type", contentType)
			req.Header.Set("X-Api-Key", callerAPIKey)
			for _, cookie := range callerCookies {
				req.Header.Add("Cookie", cookie)
			}
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			received := awaitRequest(t, requests)
			assert.Equal(t, []string{callerAPIKey}, received.headers.Values("X-Api-Key"))
			assert.Equal(t, callerCookies, received.headers.Values("Cookie"))
		})
	}
}

// Shadow copies are taken before the primary is forwarded. Each copy is
// routed by its target, so only direct targets lose the caller credentials.
func TestShadowCopiesDropCallerCredentialsOnlyOnDirectTargets(t *testing.T) {
	const shadowModel = "acme/shadow-model"
	cases := []struct {
		name        string
		primaryType config.FunctionType
		shadowType  config.FunctionType
	}{
		{"direct primary direct shadow", "", ""},
		{"direct primary llm shadow", "", config.FunctionTypeLLM},
		{"llm primary direct shadow", config.FunctionTypeLLM, ""},
	}
	upstreamFor := func(functionType config.FunctionType) string {
		if functionType == config.FunctionTypeLLM {
			return "llm"
		}
		return "nvcf"
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nvcfRequests := make(chan capturedRequest, 2)
			llmRequests := make(chan capturedRequest, 2)
			nvcf := captureServer(t, nvcfRequests, nil)
			llm := captureServer(t, llmRequests, nil)
			shadowPct := 100
			mappings := &config.GatewayConfig{}
			mappings.OpenAI.Host = openAIHost
			mappings.OpenAI.ChatCompletions = map[string]config.ModelFunctionDetails{
				"primary": {
					ModelName:        directModel,
					FunctionID:       directFunctionID,
					FunctionType:     tc.primaryType,
					ShadowModelName:  shadowModel,
					ShadowPercentage: &shadowPct,
				},
				"shadow": {
					ModelName:    shadowModel,
					FunctionID:   "shadow-func",
					FunctionType: tc.shadowType,
				},
			}
			mux := openAIMux(t, mappings, nvcf.URL, llm.URL)

			req := openAIRequest(t, "/v1/chat/completions", `{"model":"`+directModel+`"}`)
			req.Header.Set("Content-Type", "application/json")
			setCallerCredentials(req)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			// Keyed by whether the request is the shadow copy.
			upstreams := map[bool]string{}
			timeout := time.After(5 * time.Second)
			for len(upstreams) < 2 {
				var received capturedRequest
				var upstream string
				select {
				case received = <-nvcfRequests:
					upstream = "nvcf"
					assert.Empty(t, received.headers.Values("X-Api-Key"), "caller X-Api-Key must not reach the function")
					assert.Empty(t, received.headers.Values("Cookie"), "caller Cookie must not reach the function")
					assert.Equal(t, []string{callerBearer}, received.headers.Values("Authorization"))
				case received = <-llmRequests:
					upstream = "llm"
					assert.Equal(t, []string{callerAPIKey, callerAPIKey + "-second"}, received.headers.Values("X-Api-Key"))
					assert.Equal(t, callerCookies, received.headers.Values("Cookie"))
				case <-timeout:
					t.Fatalf("timed out waiting for primary and shadow requests; seen=%v", upstreams)
				}
				upstreams[received.headers.Get(shadowHeader) == shadowHeaderValue] = upstream
			}
			assert.Equal(t, map[bool]string{false: upstreamFor(tc.primaryType), true: upstreamFor(tc.shadowType)}, upstreams)
		})
	}
}
