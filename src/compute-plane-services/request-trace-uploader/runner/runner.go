// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package runner is the uploader's process startup, shared by every binary
// built on this module.
//
// A binary's main imports the backends it links, each of which registers
// itself from an init function, and calls Run. Binaries differ only in those
// imports and in the options they pass, so shutdown, logging, metrics export
// and configuration handling cannot drift between them.
package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/atomicfile"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/service"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry/otlp"
)

// telemetryFlushTimeout bounds the final export of traces, metrics and logs
// after the drain, all three together.
const telemetryFlushTimeout = 5 * time.Second

// startRetryFirst and startRetryMax bound the wait between startup
// attempts. It doubles from the first to the max, as a kubelet restart
// backoff does. Tests shorten them.
var (
	startRetryFirst = 10 * time.Second
	startRetryMax   = 5 * time.Minute
)

// Option changes how Run loads configuration.
type Option func(*options)

type options struct {
	defaultBackend config.Backend
	required       []string
}

// WithRequiredSettings makes Run refuse to start when any of names is absent
// from the environment, so a deployment that leaves one out cannot start on
// its default. An empty value counts as set.
func WithRequiredSettings(names ...string) Option {
	return func(o *options) { o.required = append(o.required, names...) }
}

// WithDefaultBackend selects backend when the environment names none or
// leaves it blank. Without it, a backend must be set.
func WithDefaultBackend(backend config.Backend) Option {
	return func(o *options) { o.defaultBackend = backend }
}

// Run loads configuration, starts the service, and blocks until SIGINT or
// SIGTERM. It then drains, sends the last telemetry, and returns the process
// exit code, so a main can pass it to os.Exit. A startup error does not end
// the process: Run logs it and tries again until it starts or is stopped.
// Logs are JSON lines on stderr.
//
// distribution names the build in the start log only. Which backends exist
// is decided by what the calling main linked.
func Run(distribution string, opts ...Option) int {
	slog.SetDefault(newLogger(os.Stderr, os.LookupEnv))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return run(ctx, os.LookupEnv, os.Environ(), distribution, collect(opts))
}

func collect(opts []Option) options {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// run is Run with its environment and shutdown signal passed in. It returns
// only once ctx ends. An attempt that fails before then is retried after a
// wait that doubles from startRetryFirst to startRetryMax: the uploader runs
// as a sidecar, and a sidecar that exits makes its pod unready, which takes
// the frontend it shares the pod with out of serving. Nothing uploads until
// an attempt starts, so a bad setting never runs; the error is logged on
// every attempt, and no uptime is reported until one starts. The exit code is
// that of the last attempt.
func run(ctx context.Context, lookup config.LookupFunc, environ []string, distribution string, o options) int {
	var m processMetrics
	defer m.flushIfLeft()
	wait := startRetryFirst
	for {
		code := attempt(ctx, lookup, environ, distribution, o, &m)
		if ctx.Err() != nil {
			return code
		}
		slog.Warn("request trace uploader did not start; retrying", "retry_in", wait.String())
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return code
		case <-timer.C:
		}
		wait = min(2*wait, startRetryMax)
	}
}

// processMetrics is the metrics export for the life of the process. It
// outlives startup attempts, so startup_failures is one counter that keeps
// growing and is exported on its interval while the process waits to
// retry, and a reset never hides failures from an alert.
type processMetrics struct {
	recorder telemetry.Metrics
	shutdown *signalShutdown
	flushed  bool
}

// start creates the export once, when its endpoint is set. A failure is
// logged and tried again on the next attempt.
func (m *processMetrics) start(cfg otlp.Config, label string) error {
	if m.recorder != nil {
		return nil
	}
	exporter, err := otlp.New(context.Background(), cfg, label)
	if err != nil {
		return err
	}
	m.recorder = telemetry.Safe(exporter)
	m.shutdown = &signalShutdown{name: "metrics", shutdown: exporter.Shutdown}
	return nil
}

// final returns the metrics shutdown for the final export after the drain,
// once; later calls return nothing.
func (m *processMetrics) final() []signalShutdown {
	if m.shutdown == nil || m.flushed {
		return nil
	}
	m.flushed = true
	return []signalShutdown{*m.shutdown}
}

// flushIfLeft sends the final metrics when no attempt did, as when the
// process stops while it waits to retry.
func (m *processMetrics) flushIfLeft() { flushTelemetry(m.final()) }

// failed counts one failed startup attempt under reason and returns its
// exit code.
func (m *processMetrics) failed(reason string) int {
	if m.recorder != nil {
		m.recorder.Add(telemetry.StartupFailures, 1, telemetry.Labels{"reason": reason})
	}
	return 1
}

// attempt loads configuration and runs the service once. It returns instead
// of exiting, so the final telemetry export runs on every path.
//
// Each OTLP signal goes out only when its own endpoint is set. Metrics are
// wrapped in telemetry.Safe; spans and log records are exported in the
// background. Log records also still go to the logger already installed.
// After the drain, a final export of all three runs within
// telemetryFlushTimeout. A failed export never changes the exit code.
func attempt(ctx context.Context, lookup config.LookupFunc, environ []string, distribution string, o options, m *processMetrics) int {
	metricsCfg, exportMetrics, err := otlp.ConfigFromEnv(lookup)
	if err != nil {
		slog.Error("invalid request trace uploader configuration", "error", err)
		return 1
	}
	// Metrics start before the other settings load, so a setting the
	// uploader refuses is still counted in startup_failures.
	if exportMetrics {
		if err := m.start(metricsCfg, backendLabel(lookup, o)); err != nil {
			slog.Error("create request trace uploader metrics exporter", "error", atomicfile.Pathless(err))
			return 1
		}
	}
	failed := m.failed
	// Spans, metrics, then logs go out after the drain. The metrics export
	// outlives the attempt, so it joins only the final flush.
	var traceShutdowns, logShutdowns []signalShutdown
	defer func() {
		final := traceShutdowns
		if ctx.Err() != nil {
			final = append(final, m.final()...)
		}
		flushTelemetry(append(final, logShutdowns...))
	}()
	if err := errors.Join(checkSignal(otlp.TracesConfigFromEnv(lookup)), checkSignal(otlp.LogsConfigFromEnv(lookup))); err != nil {
		slog.Error("invalid request trace uploader configuration", "error", err)
		return failed("config")
	}

	cfg, err := loadConfig(lookup, environ, o)
	if err != nil {
		slog.Error("invalid request trace uploader configuration", "error", err)
		return failed("config")
	}
	if logsCfg, on, _ := otlp.LogsConfigFromEnv(lookup); on {
		logs, err := otlp.NewLogs(context.Background(), logsCfg)
		if err != nil {
			slog.Error("create request trace uploader log exporter", "error", atomicfile.Pathless(err))
			return failed("other")
		}
		local := slog.Default()
		slog.SetDefault(slog.New(fanout{local.Handler(), logs.Handler()}))
		// Logs are flushed last, after the local logger is back, so a
		// warning about the final export still reaches the local log.
		logShutdowns = append(logShutdowns, signalShutdown{name: "logs", shutdown: func(ctx context.Context) error {
			slog.SetDefault(local)
			return logs.Shutdown(ctx)
		}})
	}

	slog.Info("request trace uploader starting",
		"backend", cfg.Backend,
		"compiled_backends", backend.Registered(),
		"distribution", distribution)

	client, err := backend.New(cfg)
	if err != nil {
		slog.Error("create request trace uploader backend", "error", atomicfile.Pathless(err))
		return failed("backend")
	}
	serviceOptions := service.Options{}
	if m.recorder != nil {
		serviceOptions.Metrics = m.recorder
	}
	if tracesCfg, on, _ := otlp.TracesConfigFromEnv(lookup); on {
		tracing, err := otlp.NewTracing(context.Background(), tracesCfg)
		if err != nil {
			slog.Error("create request trace uploader trace exporter", "error", atomicfile.Pathless(err))
			return failed("other")
		}
		traceShutdowns = append(traceShutdowns, signalShutdown{name: "traces", shutdown: tracing.Shutdown})
		serviceOptions.Tracer = tracing.Tracer()
	}
	uploader, err := service.NewUploader(cfg, client, serviceOptions)
	if err != nil {
		slog.Error("create request trace uploader", "error", atomicfile.Pathless(err))
		return failed("config")
	}

	if err := uploader.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("request trace uploader stopped", "error", atomicfile.Pathless(err))
		return failed(service.StartupFailureReason(err))
	}
	return 0
}

// backendLabel is the backend label metrics carry from the first attempt
// on: the backend the environment names, or the default, when this build
// links it, and "unknown" otherwise, so a refused setting never becomes a
// label value.
func backendLabel(lookup config.LookupFunc, o options) string {
	name, _ := lookup(config.EnvBackend)
	if name = strings.TrimSpace(name); name == "" {
		name = string(o.defaultBackend)
	}
	if telemetry.ValidLabelValue(name) != nil || !slices.Contains(backend.Registered(), name) {
		return "unknown"
	}
	return name
}

// checkSignal keeps only the error of a signal's configuration.
func checkSignal(_ otlp.SignalConfig, _ bool, err error) error { return err }

// signalShutdown is the final export of one OTLP signal.
type signalShutdown struct {
	name     string
	shutdown func(context.Context) error
}

// flushTelemetry sends what is left after the drain: spans, then metrics,
// then logs, all within telemetryFlushTimeout. A failed export is logged and
// never changes the exit code. An exporter's error names only a bounded
// reason, so it is safe to log.
func flushTelemetry(shutdowns []signalShutdown) {
	if len(shutdowns) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), telemetryFlushTimeout)
	defer cancel()
	for _, s := range shutdowns {
		if err := s.shutdown(ctx); err != nil {
			slog.Warn("final "+s.name+" export failed", "error", err)
		}
	}
}

// loadConfig reads configuration strictly, so an invalid value or an
// unknown uploader setting in environ stops startup, as does a required
// setting of o that is absent or a backend this build does not link. A
// backend set in the environment wins over the default of o. Errors name
// settings, never their values, and the backend is checked here so nothing
// logs a name that is not a linked backend.
func loadConfig(lookup config.LookupFunc, environ []string, o options) (config.Config, error) {
	for _, name := range o.required {
		if _, ok := lookup(name); !ok {
			return config.Config{}, fmt.Errorf("%s must be set; an empty value is allowed", name)
		}
	}
	if o.defaultBackend != "" {
		base := lookup
		lookup = func(name string) (string, bool) {
			value, ok := base(name)
			if name == config.EnvBackend && (!ok || strings.TrimSpace(value) == "") {
				return string(o.defaultBackend), true
			}
			return value, ok
		}
	}
	cfg, err := config.LoadStrict(lookup, environ)
	if err != nil {
		return config.Config{}, err
	}
	if linked := backend.Registered(); !slices.Contains(linked, string(cfg.Backend)) {
		return config.Config{}, fmt.Errorf("%s names a backend this build does not link (available: %s)", config.EnvBackend, strings.Join(linked, ", "))
	}
	return cfg, nil
}
