// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/url"
	"strings"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
)

// identityFields are the workload identity fields every local log line
// carries when they are known: from the variables NVCF sets in a function
// pod, or from OTEL_RESOURCE_ATTRIBUTES, which the chart sets. OTLP log
// records carry the same identity on their resource instead.
var identityFields = []struct {
	field, env, resourceKey string
}{
	{field: "function_id", env: "NVCF_FUNCTION_ID", resourceKey: "nvcf.function.id"},
	{field: "function_version_id", env: "NVCF_FUNCTION_VERSION_ID", resourceKey: "nvcf.function.version.id"},
	{field: "pod", resourceKey: "k8s.pod.name"},
	{field: "cluster", resourceKey: "k8s.cluster.name"},
}

// newLogger writes one JSON object per line to w, each with the identity
// fields that are known.
func newLogger(w io.Writer, lookup config.LookupFunc) *slog.Logger {
	resource := resourceAttributes(lookup)
	var attrs []slog.Attr
	for _, f := range identityFields {
		value := ""
		if f.env != "" {
			value, _ = lookup(f.env)
		}
		if strings.TrimSpace(value) == "" {
			value = resource[f.resourceKey]
		}
		if value = strings.TrimSpace(value); value != "" {
			attrs = append(attrs, slog.String(f.field, value))
		}
	}
	return slog.New(slog.NewJSONHandler(w, nil).WithAttrs(attrs))
}

// resourceAttributes reads OTEL_RESOURCE_ATTRIBUTES as the OTel SDK does:
// comma-separated key=value pairs with percent-encoded values. An entry
// that does not parse is left out.
func resourceAttributes(lookup config.LookupFunc) map[string]string {
	raw, _ := lookup("OTEL_RESOURCE_ATTRIBUTES")
	out := map[string]string{}
	for _, entry := range strings.Split(raw, ",") {
		key, value, ok := strings.Cut(entry, "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" {
			continue
		}
		if decoded, err := url.PathUnescape(strings.TrimSpace(value)); err == nil {
			out[key] = decoded
		}
	}
	return out
}

// fanout sends each record to every handler that takes its level, such as
// the local JSON lines and the OTLP log exporter.
type fanout []slog.Handler

func (f fanout) Enabled(ctx context.Context, level slog.Level) bool {
	for _, h := range f {
		if h.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (f fanout) Handle(ctx context.Context, record slog.Record) error {
	var errs []error
	for _, h := range f {
		if h.Enabled(ctx, record.Level) {
			errs = append(errs, h.Handle(ctx, record.Clone()))
		}
	}
	return errors.Join(errs...)
}

func (f fanout) WithAttrs(attrs []slog.Attr) slog.Handler {
	out := make(fanout, len(f))
	for i, h := range f {
		out[i] = h.WithAttrs(attrs)
	}
	return out
}

func (f fanout) WithGroup(name string) slog.Handler {
	out := make(fanout, len(f))
	for i, h := range f {
		out[i] = h.WithGroup(name)
	}
	return out
}
