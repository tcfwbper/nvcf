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
package provider

import (
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestUpstreamErrorMessageIsBounded(t *testing.T) {
	t.Parallel()

	// A multi-byte rune straddles the cap, so the cut must back up to stay valid UTF-8.
	long := strings.Repeat("a", maxUpstreamErrorMessageBytes-1) + "é" + strings.Repeat("b", 1024)
	for name, body := range map[string]string{
		"plain text":   long,
		"JSON message": `{"error":{"message":"` + long + `"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			message := (&UpstreamError{Status: http.StatusBadGateway, Body: []byte(body)}).Message()
			if len(message) > maxUpstreamErrorMessageBytes {
				t.Fatalf("message length = %d, want at most %d", len(message), maxUpstreamErrorMessageBytes)
			}
			if !utf8.ValidString(message) {
				t.Fatal("message is not valid UTF-8")
			}
			if !strings.HasPrefix(long, message) || len(message) < maxUpstreamErrorMessageBytes-utf8.UTFMax {
				t.Fatalf("message is not a prefix of the upstream text near the cap (length %d)", len(message))
			}
		})
	}

	short := (&UpstreamError{Status: http.StatusBadGateway, Body: []byte("upstream connect error")}).Message()
	if short != "upstream connect error" {
		t.Fatalf("short message = %q", short)
	}
}

func TestUpstreamStreamErrorBodyIsBounded(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("a", MaxUpstreamErrorBodyBytes)
	tests := []struct {
		name  string
		event string
		// wantBody empty means no body, so the client gets the status text.
		wantBody string
	}{
		{
			name:  "capped message and fields",
			event: `{"error":{"message":"` + long + `","type":"invalid_request_error","code":400}}`,
			wantBody: `{"error":{"message":"` + long[:maxUpstreamErrorMessageBytes] +
				`","type":"invalid_request_error","code":"400"}}`,
		},
		{
			name:  "oversized param",
			event: `{"error":{"message":"bad input","param":"` + long + `","code":400}}`,
			wantBody: `{"error":{"message":"bad input","param":"` + long[:maxUpstreamErrorMessageBytes] +
				`","code":"400"}}`,
		},
		{name: "no message", event: `{"error":{"detail":"` + long + `","code":400}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			upstreamErr := upstreamStreamError(tt.event)
			if upstreamErr == nil {
				t.Fatal("error event not detected")
			}
			if upstreamErr.Status != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", upstreamErr.Status, http.StatusBadRequest)
			}
			if string(upstreamErr.Body) != tt.wantBody {
				t.Fatalf("body has %d bytes, want %d", len(upstreamErr.Body), len(tt.wantBody))
			}
		})
	}
}

func TestUpstreamStreamErrorKeepsNullFieldsNil(t *testing.T) {
	t.Parallel()

	upstreamErr := upstreamStreamError(`{"error":{"message":"bad input","type":null,"param":"","code":null}}`)
	if upstreamErr == nil {
		t.Fatal("error event not detected")
	}
	if upstreamErr.Type != nil || upstreamErr.Code != nil {
		t.Fatalf("type = %v, code = %v, want nil for JSON null", upstreamErr.Type, upstreamErr.Code)
	}
	if upstreamErr.Param == nil || *upstreamErr.Param != "" {
		t.Fatalf("param = %v, want the empty string the upstream sent", upstreamErr.Param)
	}
}

func TestUpstreamStreamErrorFieldsAreBounded(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("a", maxUpstreamErrorMessageBytes) + "b"
	event := `{"error":{"message":"bad input","type":"` + long + `","param":"` + long + `","code":"` + long + `"}}`
	upstreamErr := upstreamStreamError(event)
	if upstreamErr == nil {
		t.Fatal("error event not detected")
	}
	fields := map[string]*string{"type": upstreamErr.Type, "param": upstreamErr.Param, "code": upstreamErr.Code}
	for name, field := range fields {
		if field == nil || *field != long[:maxUpstreamErrorMessageBytes] {
			t.Fatalf("%s is not capped at %d bytes", name, maxUpstreamErrorMessageBytes)
		}
	}
}

func TestUpstreamStreamErrorMessageIsBounded(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("a", maxUpstreamErrorMessageBytes) + "b"
	upstreamErr := upstreamStreamError(`{"error":{"message":"` + long + `"}}`)
	if upstreamErr == nil {
		t.Fatal("error event not detected")
	}
	if upstreamErr.Message != long[:maxUpstreamErrorMessageBytes] {
		t.Fatalf("message length = %d, want the first %d bytes", len(upstreamErr.Message), maxUpstreamErrorMessageBytes)
	}
	// The request log gets Error(), which, like UpstreamError, carries no upstream text:
	// engine error events can echo request content.
	if got, want := upstreamErr.Error(), ErrStreamIncomplete.Error()+": upstream error event"; got != want {
		t.Fatalf("log text has %d bytes, want %q", len(got), want)
	}
}
