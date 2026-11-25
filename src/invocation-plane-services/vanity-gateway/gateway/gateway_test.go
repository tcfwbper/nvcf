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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewNVCFGatewayRequiresAPIEndpoint(t *testing.T) {
	gateway, err := NewNVCFGateway(nil, Config{MappingPath: "config.yaml"})

	require.Nil(t, gateway)
	require.Error(t, err)
	require.Contains(t, err.Error(), "NVCF_API_ENDPOINT is required")
}

func TestNewNVCFGatewayRejectsNegativeMappingLoadTimeout(t *testing.T) {
	gateway, err := NewNVCFGateway(nil, Config{
		NvcfApiEndpoint:    "https://api.example.com",
		MappingPath:        "config.yaml",
		MappingLoadTimeout: -5 * time.Second,
	})

	require.Nil(t, gateway)
	require.ErrorContains(t, err, "MAPPING_LOAD_TIMEOUT must not be negative")
}

func TestRequestSizeLimitRejectsNonPositiveConfig(t *testing.T) {
	for _, value := range []int64{0, -1} {
		cfg := Config{MaxRequestSizeBytes: &value}
		_, err := cfg.requestSizeLimit()
		require.ErrorContains(t, err, "MAX_REQUEST_SIZE_BYTES must be a positive integer in bytes")
		service, err := NewNVCFGateway(nil, cfg)
		require.Nil(t, service)
		require.ErrorContains(t, err, "MAX_REQUEST_SIZE_BYTES")
	}
}

func TestStreamedRequestSizeLimit(t *testing.T) {
	for _, tc := range []struct {
		name     string
		buffered *int64
		streamed *int64
		want     int64
		wantErr  string
	}{
		{name: "unset follows the default buffered limit", want: defaultMaxRequestSize},
		{name: "unset follows a set buffered limit", buffered: int64Ptr(1024), want: 1024},
		{name: "equal to the buffered limit", buffered: int64Ptr(1024), streamed: int64Ptr(1024), want: 1024},
		{name: "above the buffered limit", streamed: int64Ptr(64 << 20), want: 64 << 20},
		{name: "above a buffered limit equal to the prefix size", buffered: int64Ptr(requestPrefixLimit), streamed: int64Ptr(requestPrefixLimit + 1), want: requestPrefixLimit + 1},
		{name: "zero", streamed: int64Ptr(0), wantErr: "MAX_STREAMED_REQUEST_SIZE_BYTES must be a positive integer in bytes"},
		{name: "negative", streamed: int64Ptr(-1), wantErr: "MAX_STREAMED_REQUEST_SIZE_BYTES must be a positive integer in bytes"},
		{name: "below the buffered limit", streamed: int64Ptr(defaultMaxRequestSize - 1), wantErr: "MAX_STREAMED_REQUEST_SIZE_BYTES must not be below MAX_REQUEST_SIZE_BYTES"},
		{name: "above a buffered limit under the prefix size", buffered: int64Ptr(requestPrefixLimit - 1), streamed: int64Ptr(requestPrefixLimit), wantErr: "MAX_STREAMED_REQUEST_SIZE_BYTES can exceed MAX_REQUEST_SIZE_BYTES only when MAX_REQUEST_SIZE_BYTES is at least 65536"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{MaxRequestSizeBytes: tc.buffered, MaxStreamedRequestSizeBytes: tc.streamed}
			got, err := cfg.streamedRequestSizeLimit()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				service, err := NewNVCFGateway(nil, cfg)
				require.Nil(t, service)
				require.ErrorContains(t, err, "MAX_STREAMED_REQUEST_SIZE_BYTES")
				_, err = buildChiMux(&config.GatewayConfig{}, cfg)
				require.ErrorContains(t, err, "MAX_STREAMED_REQUEST_SIZE_BYTES")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
