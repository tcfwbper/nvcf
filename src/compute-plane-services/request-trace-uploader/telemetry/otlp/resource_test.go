// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package otlp

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
)

// The deployment names the function and the pod through the standard OTel
// resource variables, so the collector can tell producers apart.
func TestOTLPResourceComesFromTheStandardVariables(t *testing.T) {
	tests := []struct {
		name        string
		attributes  string
		serviceName string
		want        map[string]string
	}{
		{
			name:        "both set",
			attributes:  "nvcf.function.id=fn-1,k8s.pod.name=pod-a",
			serviceName: "uploader-under-test",
			want:        map[string]string{"nvcf.function.id": "fn-1", "k8s.pod.name": "pod-a", "service.name": "uploader-under-test"},
		},
		{
			name:       "attributes only keep the default service name",
			attributes: "k8s.pod.name=pod-b",
			want:       map[string]string{"k8s.pod.name": "pod-b", "service.name": ServiceName},
		},
		{
			name: "unset keeps the default service name",
			want: map[string]string{"service.name": ServiceName},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", tt.attributes)
			t.Setenv("OTEL_SERVICE_NAME", tt.serviceName)
			r, endpoint := httpReceiver(t)
			exporter := newOTLP(t, endpoint, ProtocolHTTP)
			exporter.Add(telemetry.Uploads, 1, telemetry.Labels{"status": "success"})
			flush(t, exporter)
			for key, want := range tt.want {
				if got := r.resourceAttribute(key); got != want {
					t.Errorf("resource %s = %q, want %q", key, got, want)
				}
			}
		})
	}
}

// A malformed OTEL_RESOURCE_ATTRIBUTES costs only the attributes that do not
// parse: the exporter still starts, keeps the rest, and warns without
// printing the value.
func TestOTLPKeepsExportingWithMalformedResourceAttributes(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "k8s.pod.name=pod-c,secret-marker-without-equals")
	t.Setenv("OTEL_SERVICE_NAME", "")

	r, endpoint := httpReceiver(t)
	exporter := newOTLP(t, endpoint, ProtocolHTTP)
	exporter.Add(telemetry.Uploads, 1, telemetry.Labels{"status": "success"})
	flush(t, exporter)
	if got := r.resourceAttribute("k8s.pod.name"); got != "pod-c" {
		t.Errorf("resource k8s.pod.name = %q, want pod-c", got)
	}
	if got := r.resourceAttribute("service.name"); got != ServiceName {
		t.Errorf("resource service.name = %q, want %q", got, ServiceName)
	}
	if !strings.Contains(logs.String(), "OTEL_RESOURCE_ATTRIBUTES") || strings.Contains(logs.String(), "secret-marker") {
		t.Errorf("logs = %s, want a warning that names the variable and not its value", logs.String())
	}
}

// Every exported datapoint names the pod it came from, so series from the
// pods of one function do not collide in a store that drops the resource.
// The pod comes from the k8s.pod.name resource attribute, else HOSTNAME, and
// is fixed when the exporter is built. With neither there is no pod label;
// a set value that is not a short lowercase token is left out, not changed,
// and a warning that does not quote it is logged.
func TestOTLPEveryDatapointCarriesThePodLabel(t *testing.T) {
	tests := []struct {
		name       string
		attributes string
		hostname   string
		want       string
		warns      bool
	}{
		{name: "resource attribute wins over HOSTNAME", attributes: "k8s.pod.name=fn-7d9f-x2kq9", hostname: "other-host", want: "fn-7d9f-x2kq9"},
		{name: "only HOSTNAME", hostname: "fn-7d9f-abcde", want: "fn-7d9f-abcde"},
		{name: "neither"},
		{name: "invalid resource value is left out", attributes: "k8s.pod.name=Not_A_Token", warns: true},
		{name: "invalid resource value does not fall back to HOSTNAME", attributes: "k8s.pod.name=Not_A_Token", hostname: "fn-7d9f-abcde", warns: true},
		{name: "padded HOSTNAME is left out, not trimmed", hostname: " fn-7d9f-abcde ", warns: true},
		{name: "padded resource value is left out, not trimmed", attributes: "k8s.pod.name=%20fn-7d9f-x2kq9%20", warns: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			t.Cleanup(func() { slog.SetDefault(previous) })
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", tt.attributes)
			t.Setenv("HOSTNAME", tt.hostname)
			r, endpoint := httpReceiver(t)
			exporter := newOTLP(t, endpoint, ProtocolHTTP)
			// The label is fixed at construction: a later change is ignored.
			t.Setenv("HOSTNAME", "changed-later")
			exporter.Add(telemetry.Uploads, 1, telemetry.Labels{"status": "success"})
			exporter.Set(telemetry.PendingSegments, 3, nil)
			exporter.Observe(telemetry.UploadDuration, 0.5, telemetry.Labels{"status": "success"})
			flush(t, exporter)

			var points []map[string]string
			for _, name := range []string{telemetry.Uploads, telemetry.PendingSegments, telemetry.UploadDuration} {
				m := r.metric(MetricPrefix + name)
				if m == nil {
					t.Fatalf("metric %s not exported", name)
				}
				for _, p := range m.GetSum().GetDataPoints() {
					points = append(points, attributes(p))
				}
				for _, p := range m.GetGauge().GetDataPoints() {
					points = append(points, attributes(p))
				}
				for _, p := range m.GetHistogram().GetDataPoints() {
					points = append(points, attributes(p))
				}
			}
			for _, attrs := range points {
				got, present := attrs[telemetry.PodLabel]
				if tt.want == "" && present {
					t.Errorf("datapoint %v carries %s=%q, want no pod label", attrs, telemetry.PodLabel, got)
				}
				if tt.want != "" && got != tt.want {
					t.Errorf("datapoint %v %s = %q, want %q", attrs, telemetry.PodLabel, got, tt.want)
				}
				if attrs[telemetry.BackendLabel] != "fake" {
					t.Errorf("datapoint %v lost %s=fake", attrs, telemetry.BackendLabel)
				}
			}
			warned := strings.Count(logs.String(), "no pod label")
			if tt.warns && warned != 1 {
				t.Errorf("logs = %s, want one pod label warning", logs.String())
			}
			if !tt.warns && warned != 0 {
				t.Errorf("logs = %s, want no pod label warning", logs.String())
			}
			if strings.Contains(logs.String(), "Not_A_Token") || strings.Contains(logs.String(), "fn-7d9f") {
				t.Errorf("logs = %s, want the rejected value left out", logs.String())
			}
		})
	}
}
