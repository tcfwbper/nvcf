// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package telemetry

import "net/http"

// TransportWrapper wraps a backend's HTTP transport. operation names what
// the calls through it do, such as "submit" or "status"; it labels the
// calls' metrics, so it comes from a small fixed set and must be a valid
// label value.
type TransportWrapper func(operation string, next http.RoundTripper) http.RoundTripper

// TransportReceiver is implemented by a backend that calls its destination
// over HTTP. The service hands it a TransportWrapper to apply to each HTTP
// client it uses. Every call through a wrapped transport is then a child
// span of the pipeline step that made it, carries the W3C trace context,
// and is counted in BackendCalls and BackendCallDuration by operation and
// status class.
type TransportReceiver interface {
	UseTransport(TransportWrapper)
}
