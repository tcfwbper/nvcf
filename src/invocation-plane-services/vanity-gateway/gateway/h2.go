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
	"ai-api-gateway-service/router"
	"fmt"
	"math"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/hellofresh/health-go/v5"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	golibversion "github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/version"
)

const (
	defaultMaxRequestSize = 25 * 1024 * 1024 // 25 MiB
	healthPath            = "/health"
	// max nvcf api timeout is one hour
	requestTimeout = 1*time.Hour + 10*time.Second
)

func buildRouter(mappings *config.GatewayConfig, serverConfig Config) (*router.SwappableRouter, error) {
	r, err := buildChiMux(mappings, serverConfig)
	if err != nil {
		return nil, err
	}

	swappableRouter := router.NewSwappableRouter(r)
	return swappableRouter, nil
}

// buildChiMux constructs the router from an already-validated GatewayConfig.
// Callers must ensure mappings have been validated (SetupConfigWithConfigPath
// handles this via WithPostLoadFunc on both initial load and reload).
func buildChiMux(mappings *config.GatewayConfig, serverConfig Config) (*chi.Mux, error) {
	maxRequestSize, err := serverConfig.requestSizeLimit()
	if err != nil {
		return nil, err
	}
	maxStreamedRequestSize, err := serverConfig.streamedRequestSizeLimit()
	if err != nil {
		return nil, err
	}
	// Compile the regex pattern
	re, err := regexp.Compile(serverConfig.PrivateModelNameRegexPattern)
	if err != nil {
		zap.L().Error("Error compiling regex")
		return nil, err
	}
	transport := middleware.TracedRoundTripper(&http.Transport{
		IdleConnTimeout:     30 * time.Second,
		MaxIdleConnsPerHost: 64,
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
	})
	llmGatewayEndpoint := ""
	var llmGatewayDirector *LLMGatewayDirector
	if mappings.HasLLMGatewayRoute() {
		if serverConfig.LLMGatewayEndpoint == "" {
			return nil, fmt.Errorf("LLM_GATEWAY_ENDPOINT is required when a model sets functionType LLM")
		}
		llmGatewayEndpoint = serverConfig.LLMGatewayEndpoint
		llmGatewayDirector, err = NewLLMGatewayDirector(llmGatewayEndpoint, transport)
		if err != nil {
			return nil, err
		}
		if hostWithoutPort(mappings.OpenAI.Host) == llmGatewayDirector.UpstreamHostname() {
			return nil, fmt.Errorf("openai.host %q is the LLM Gateway endpoint; the gateway would proxy to itself", mappings.OpenAI.Host)
		}
	}

	healthManager, err := healthManager(serverConfig.NvcfApiEndpoint, llmGatewayEndpoint, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create health manager: %w", err)
	}

	vanityDirector, err := NewVanityDirector(serverConfig.NvcfApiEndpoint, transport)
	if err != nil {
		return nil, err
	}

	shadowMaxConcurrent := serverConfig.ShadowMaxConcurrent
	if shadowMaxConcurrent <= 0 {
		shadowMaxConcurrent = 256
	}
	shadower := NewTrafficShadower(shadowMaxConcurrent, requestTimeout)

	modelDirector, err := NewModelDirector(mappings, re, vanityDirector, llmGatewayDirector, shadower)
	if err != nil {
		return nil, err
	}
	modelDirector.requestBodyLimit = maxRequestSize
	modelDirector.streamedRequestBodyLimit = maxStreamedRequestSize
	if serverConfig.NvcfControlPlaneEndpoint != "" {
		modelDirector.privateModels, err = sharedPrivateModelAccess(serverConfig.NvcfControlPlaneEndpoint, transport)
		if err != nil {
			return nil, err
		}
	}

	serverTelemetry := middleware.ServerTelemetryMiddleware()

	// Request IDs apply only on the model API host. Pass-through vanity hosts
	// keep the caller's and the function's headers unchanged. The middleware
	// runs on the root router so errors from the root middleware carry an ID.
	requestIDRouter := &middleware.HostRouter{}
	requestIDRouter.Register(mappings.OpenAI.Host, middleware.RequestID)

	r := chi.NewRouter()
	r.Use(requestIDRouter.Handler)
	r.Use(middleware.RejectSpoofedShadowRequests(shadowHeader, serverTelemetry))
	r.Use(chimiddleware.Logger)
	r.Use(chimiddleware.Recoverer)
	r.Use(chimiddleware.Timeout(requestTimeout))
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*.nvidia.com", "vscode-file://vscode-app"},
		AllowedMethods:   []string{"GET", "POST", "OPTIONS"},
		AllowCredentials: true,
		AllowedHeaders:   []string{"*"},
	}))

	hostRouter := &middleware.HostRouter{}

	registerModelAPIs(hostRouter, mappings, modelDirector, healthManager, serverTelemetry, maxRequestSize)
	registerVanity(hostRouter, mappings, vanityDirector, healthManager, serverTelemetry)

	r.Use(hostRouter.Handler)
	r.With(serverTelemetry).Get(healthPath, healthManager.HandlerFunc)
	r.With(serverTelemetry).Get("/info", golibversion.Handler().ServeHTTP)
	return r, nil
}

func createHttp2Server(addr string, swappableRouter *router.SwappableRouter) *http.Server {
	return &http.Server{
		Addr: addr,
		Handler: ignoreH2CUpgradeWithBody(h2c.NewHandler(swappableRouter, &http2.Server{
			MaxConcurrentStreams: math.MaxInt32,
		})),
	}
}

// ignoreH2CUpgradeWithBody serves an HTTP/1 request that has a body and asks
// for an h2c upgrade over HTTP/1.1, as a server may when it ignores Upgrade.
// The h2c handler reads such a body in full before the router runs, so model
// routes could neither bound nor stream it. Requests without a body still
// upgrade, and HTTP/2 with prior knowledge is unchanged.
func ignoreH2CUpgradeWithBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength != 0 && headerHasToken(r.Header, "Upgrade", "h2c") {
			removeH2CUpgrade(r.Header)
		}
		next.ServeHTTP(w, r)
	})
}

// removeH2CUpgrade removes the h2c upgrade request from h: the h2c Upgrade
// token, the HTTP2-Settings header and the Connection tokens that name them.
// Other Upgrade tokens are kept.
func removeH2CUpgrade(h http.Header) {
	h.Del("HTTP2-Settings")
	removeHeaderToken(h, "Connection", "HTTP2-Settings")
	if removeHeaderToken(h, "Upgrade", "h2c") == 0 {
		removeHeaderToken(h, "Connection", "Upgrade")
	}
}

func headerHasToken(h http.Header, name, token string) bool {
	for _, value := range h.Values(name) {
		for _, t := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(t), token) {
				return true
			}
		}
	}
	return false
}

// removeHeaderToken drops token, ignoring case, from the comma-separated
// values of name, deletes name when no token remains, and returns the number
// of tokens left.
func removeHeaderToken(h http.Header, name, token string) int {
	var kept []string
	for _, value := range h.Values(name) {
		for _, t := range strings.Split(value, ",") {
			if t = strings.TrimSpace(t); t != "" && !strings.EqualFold(t, token) {
				kept = append(kept, t)
			}
		}
	}
	if len(kept) == 0 {
		h.Del(name)
		return 0
	}
	h.Set(name, strings.Join(kept, ", "))
	return len(kept)
}

// general pass-through vanity domains
func registerVanity(hostRouter *middleware.HostRouter, mappings *config.GatewayConfig, vanityDirector *VanityDirector, healthManager *health.Health, serverTelemetry func(http.Handler) http.Handler) {
	for _, vanity := range mappings.Vanity {
		r := chi.NewRouter()
		r.Use(serverTelemetry)
		for _, mapping := range vanity.Paths {
			target := VanityExecRequest{
				FunctionID:        mapping.FunctionID,
				FunctionVersionID: mapping.FunctionVersionID,
				PathOverride:      mapping.OutgoingPathOverride,
				UsePexec:          mapping.UsePexec,
				CustomHeaders:     mapping.CustomHeaders,
				EOL:               mapping.EOL,
				OfflineMessage:    mapping.OfflineMessage,
			}
			r.Post(mapping.Path, func(writer http.ResponseWriter, request *http.Request) {
				middleware.AddFunctionIDMetricAttribute(request.Context(), target.FunctionID)
				vanityDirector.ServeExec(target, writer, request)
			})
		}
		r.Get(healthPath, healthManager.HandlerFunc)
		r.Get("/info", golibversion.Handler().ServeHTTP)
		r.Get("/v1/status/{requestId}", vanityDirector.ServePolling)
		hostRouter.Register(vanity.Host, chimiddleware.New(r))
	}
}

// openai specific domain
func registerModelAPIs(hostRouter *middleware.HostRouter, mappings *config.GatewayConfig, modelDirector *ModelDirector, healthManager *health.Health, serverTelemetry func(http.Handler) http.Handler, maxRequestSize int64) {
	r := chi.NewRouter()
	r.Use(serverTelemetry)
	r.Method(http.MethodPost, "/v1/chat/completions", limitModelRequestBody(maxRequestSize)(http.HandlerFunc(modelDirector.ServeChatCompletions)))
	r.Method(http.MethodPost, "/v1/completions", limitModelRequestBody(maxRequestSize)(http.HandlerFunc(modelDirector.ServeCompletions)))
	r.Method(http.MethodPost, "/v1/embeddings", limitModelRequestBody(maxRequestSize)(http.HandlerFunc(modelDirector.ServeEmbeddings)))
	r.Method(http.MethodPost, "/v1/messages", limitModelRequestBody(maxRequestSize)(http.HandlerFunc(modelDirector.ServeMessages)))
	r.Method(http.MethodPost, decisionsPath, limitModelRequestBody(maxRequestSize)(http.HandlerFunc(modelDirector.ServeDecisions)))
	r.Method(http.MethodPost, systemOnePath, limitModelRequestBody(maxRequestSize)(http.HandlerFunc(modelDirector.ServeSystemOne)))
	r.Method(http.MethodPost, "/v1/responses", limitModelRequestBody(maxRequestSize)(http.HandlerFunc(modelDirector.ServeResponses)))
	r.Method(http.MethodPost, "/v1/images/generations", limitModelRequestBody(maxRequestSize)(http.HandlerFunc(modelDirector.ServeImageGenerations)))
	r.Method(http.MethodPost, "/v1/images/edits", limitModelRequestBody(maxRequestSize)(http.HandlerFunc(modelDirector.ServeImageEdits)))
	r.Method(http.MethodPost, "/v1/images/variations", limitModelRequestBody(maxRequestSize)(http.HandlerFunc(modelDirector.ServeImageVariations)))
	r.Get("/v1/models", modelDirector.ListModels)
	r.Get("/v1/models/{model}", modelDirector.GetModel)
	r.Get("/v1/models/{company}/{model}", modelDirector.GetModel)
	r.Get("/v1/models/{prefix}/{company}/{model}", modelDirector.GetModel)

	r.Get(healthPath, healthManager.HandlerFunc)
	r.Get("/info", golibversion.Handler().ServeHTTP)

	// special domain for openai
	hostRouter.Register(mappings.OpenAI.Host, chimiddleware.New(r))
}
