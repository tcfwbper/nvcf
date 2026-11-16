// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package otlp

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Each signal turns on only when its own endpoint is set, as the platform
// sets each one only when that signal is wanted. An endpoint without a
// protocol stops startup; no endpoint means that signal is off.
const (
	EnvTracesEndpoint = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
	EnvTracesProtocol = "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"
	EnvLogsEndpoint   = "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"
	EnvLogsProtocol   = "OTEL_EXPORTER_OTLP_LOGS_PROTOCOL"

	// instrumentationName names the uploader as the source of its spans
	// and log records.
	instrumentationName = "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader"
	// batchDelay is how long spans and log records wait to be sent in a
	// batch. queueSize and batchSize are the SDK defaults, set here so an
	// OTEL_BSP_* or OTEL_BLRP_* variable cannot change or break them.
	batchDelay = 5 * time.Second
	queueSize  = 2048
	batchSize  = 512
)

// SignalConfig says where one signal is pushed. Endpoint is used as given:
// an http or https URL with a host; for http/protobuf its path is the
// request path.
type SignalConfig struct {
	Endpoint string
	Protocol Protocol
}

// TracesConfigFromEnv reads the traces endpoint and protocol.
func TracesConfigFromEnv(lookup func(string) (string, bool)) (SignalConfig, bool, error) {
	return signalFromEnv(lookup, EnvTracesEndpoint, EnvTracesProtocol)
}

// LogsConfigFromEnv reads the logs endpoint and protocol.
func LogsConfigFromEnv(lookup func(string) (string, bool)) (SignalConfig, bool, error) {
	return signalFromEnv(lookup, EnvLogsEndpoint, EnvLogsProtocol)
}

// Tracing exports spans over OTLP in the background: ending a span never
// waits on the network, and a failed export costs that batch only.
type Tracing struct {
	provider *sdktrace.TracerProvider
}

// NewTracing builds the span exporter. Like the metrics exporter, every
// exporter and batch setting is passed explicitly, so no other
// OTEL_EXPORTER_OTLP_* or OTEL_BSP_* variable changes where or how spans are
// sent, and every span is sampled.
func NewTracing(ctx context.Context, cfg SignalConfig) (*Tracing, error) {
	installDiagnostics()
	var exporter sdktrace.SpanExporter
	var err error
	if cfg.Protocol == ProtocolGRPC {
		exporter, err = otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpointURL(cfg.Endpoint),
			otlptracegrpc.WithTLSCredentials(grpcCredentials(cfg.Endpoint)),
			otlptracegrpc.WithHeaders(map[string]string{}),
			otlptracegrpc.WithTimeout(exportTimeout),
			otlptracegrpc.WithCompressor(""))
	} else {
		exporter, err = otlptracehttp.New(ctx,
			otlptracehttp.WithEndpointURL(cfg.Endpoint),
			otlptracehttp.WithTLSClientConfig(tlsFor(cfg.Endpoint)),
			otlptracehttp.WithHeaders(map[string]string{}),
			otlptracehttp.WithTimeout(exportTimeout),
			otlptracehttp.WithCompression(otlptracehttp.NoCompression))
	}
	if err != nil {
		return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}
	res, err := newResource(ctx)
	if err != nil {
		return nil, fmt.Errorf("build trace resource: %w", err)
	}
	return &Tracing{provider: sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(batchDelay),
			sdktrace.WithExportTimeout(exportTimeout),
			sdktrace.WithMaxQueueSize(queueSize),
			sdktrace.WithMaxExportBatchSize(batchSize)),
	)}, nil
}

// Tracer returns the uploader's tracer.
func (t *Tracing) Tracer() trace.Tracer { return t.provider.Tracer(instrumentationName) }

// Shutdown exports the spans that are left and stops the exporter. ctx
// bounds it. An error names only a bounded reason, so it is safe to log.
func (t *Tracing) Shutdown(ctx context.Context) error { return exportError(t.provider.Shutdown(ctx)) }

// Logs sends log records over OTLP in the background, through an slog
// handler: logging never waits on the network, and a failed export costs
// that batch only.
type Logs struct {
	provider *sdklog.LoggerProvider
	handler  slog.Handler
}

// NewLogs builds the log exporter, with every exporter and batch setting
// passed explicitly, as for the other signals, so no OTEL_BLRP_* variable
// changes them.
func NewLogs(ctx context.Context, cfg SignalConfig) (*Logs, error) {
	installDiagnostics()
	var exporter sdklog.Exporter
	var err error
	if cfg.Protocol == ProtocolGRPC {
		exporter, err = otlploggrpc.New(ctx,
			otlploggrpc.WithEndpointURL(cfg.Endpoint),
			otlploggrpc.WithTLSCredentials(grpcCredentials(cfg.Endpoint)),
			otlploggrpc.WithHeaders(map[string]string{}),
			otlploggrpc.WithTimeout(exportTimeout),
			otlploggrpc.WithCompressor(""))
	} else {
		exporter, err = otlploghttp.New(ctx,
			otlploghttp.WithEndpointURL(cfg.Endpoint),
			otlploghttp.WithTLSClientConfig(tlsFor(cfg.Endpoint)),
			otlploghttp.WithHeaders(map[string]string{}),
			otlploghttp.WithTimeout(exportTimeout),
			otlploghttp.WithCompression(otlploghttp.NoCompression))
	}
	if err != nil {
		return nil, fmt.Errorf("create OTLP log exporter: %w", err)
	}
	res, err := newResource(ctx)
	if err != nil {
		return nil, fmt.Errorf("build log resource: %w", err)
	}
	provider := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter,
			sdklog.WithExportInterval(batchDelay),
			sdklog.WithExportTimeout(exportTimeout),
			sdklog.WithMaxQueueSize(queueSize),
			sdklog.WithExportMaxBatchSize(batchSize))),
	)
	return &Logs{
		provider: provider,
		handler:  otelslog.NewHandler(instrumentationName, otelslog.WithLoggerProvider(provider)),
	}, nil
}

// Handler returns the slog handler that sends records over OTLP.
func (l *Logs) Handler() slog.Handler { return l.handler }

// Shutdown exports the records that are left and stops the exporter. ctx
// bounds it. An error names only a bounded reason, so it is safe to log.
func (l *Logs) Shutdown(ctx context.Context) error { return exportError(l.provider.Shutdown(ctx)) }
