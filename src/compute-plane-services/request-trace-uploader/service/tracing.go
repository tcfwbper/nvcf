// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
)

// spanPrefix starts every span name.
const spanPrefix = "request-trace-uploader."

// Spans carry only bounded attributes: the backend, an outcome status, and a
// fault's class, scope and reason. None holds an id, a path, or anything
// from a record, and a failure never records its error text.

// startSpan starts a pipeline span named spanPrefix+operation.
func (u *Uploader) startSpan(ctx context.Context, operation string) (context.Context, trace.Span) {
	return u.tracer.Start(ctx, spanPrefix+operation, trace.WithAttributes(attribute.String("backend", u.name)))
}

func attrStatus(status string) attribute.KeyValue { return attribute.String("status", status) }

// failSpan marks span failed with status, and with reason as its bounded
// description when there is one.
func failSpan(span trace.Span, status, reason string) {
	span.SetAttributes(attrStatus(status))
	if reason != "" {
		span.SetAttributes(attribute.String("reason", reason))
	}
	span.SetStatus(codes.Error, reason)
}

// failFault marks span failed with fault's class, scope and reason.
func failFault(span trace.Span, fault backend.Fault) {
	span.SetAttributes(attribute.String("class", string(fault.Class)), attribute.String("scope", string(fault.Scope)))
	failSpan(span, "failure", string(fault.Reason))
}

// knownStatuses are the job statuses a poll span reports as they are. Any
// other value a backend returns is reported as unknown.
var knownStatuses = map[backend.Status]bool{
	backend.StatusPending: true, backend.StatusSucceeded: true,
	backend.StatusFailed: true, backend.StatusNotFound: true,
}

// pollJob reads one job's status inside a poll span.
func (u *Uploader) pollJob(ctx context.Context, id string) (backend.Status, error) {
	ctx, span := u.startSpan(ctx, "poll")
	defer span.End()
	status, err := u.client.Status(ctx, id)
	switch {
	case status == "":
		failFault(span, backend.Classify(u.client, err))
		span.SetAttributes(attrStatus("unreadable"))
	case status == backend.StatusFailed:
		if err != nil {
			failFault(span, backend.Classify(u.client, err))
		} else {
			failSpan(span, "failure", "")
		}
		span.SetAttributes(attrStatus(string(status)))
	case knownStatuses[status]:
		span.SetAttributes(attrStatus(string(status)))
	default:
		span.SetAttributes(attrStatus("unknown"))
	}
	return status, err
}

// roundTripFunc adapts a function to http.RoundTripper.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// wrapTransport is the telemetry.TransportWrapper a backend receives. Each
// call is a client span, a child of the step that made it, carries the W3C
// trace context, and is counted in BackendCalls and BackendCallDuration.
func (u *Uploader) wrapTransport(operation string, next http.RoundTripper) http.RoundTripper {
	if next == nil {
		next = http.DefaultTransport
	}
	if telemetry.ValidLabelValue(operation) != nil {
		operation = "other"
	}
	propagator := propagation.TraceContext{}
	return roundTripFunc(func(req *http.Request) (*http.Response, error) {
		ctx, span := u.tracer.Start(req.Context(), spanPrefix+"backend_call",
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(attribute.String("backend", u.name), attribute.String("operation", operation)))
		defer span.End()
		out := req.Clone(ctx)
		if out.Header == nil {
			out.Header = http.Header{}
		}
		propagator.Inject(ctx, propagation.HeaderCarrier(out.Header))
		started := time.Now()
		resp, err := next.RoundTrip(out)
		status := "error"
		if err == nil && resp != nil {
			status = strconv.Itoa(resp.StatusCode/100) + "xx"
		}
		span.SetAttributes(attrStatus(status))
		if status == "error" || resp.StatusCode >= 400 {
			span.SetStatus(codes.Error, status)
		}
		labels := telemetry.Labels{"operation": operation, "status": status}
		u.metrics.Add(telemetry.BackendCalls, 1, labels)
		u.metrics.Observe(telemetry.BackendCallDuration, time.Since(started).Seconds(), labels)
		return resp, err
	})
}
