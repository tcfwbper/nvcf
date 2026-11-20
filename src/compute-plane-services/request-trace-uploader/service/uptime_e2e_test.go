// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend/backendtest"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/service"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

// Uptime is the signal that the pipeline runs: an uploader that cannot start
// stays alive without it, so an alert on a missing uptime series finds it.
func TestUptimeIsReportedOnlyOnceTheUploaderIsReady(t *testing.T) {
	tests := map[string]struct {
		unreadableJournal bool
		wantUptime        bool
	}{
		"ready":              {wantUptime: true},
		"journal unreadable": {unreadableJournal: true},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			spool, _ := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
			if tt.unreadableJournal {
				segments := filepath.Join(spool.Dir, "request-trace-uploader-state", "segments")
				if err := os.MkdirAll(segments, 0o750); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(segments, "no-segment-name.json"), []byte("{"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			metrics, err := telemetry.NewMemory("fake")
			if err != nil {
				t.Fatal(err)
			}
			logs := &uptimeLogs{}
			captureInto(t, logs)
			listener, base := listen(t)
			uploader := newUploader(t, spool, backendtest.NewFake(), service.Options{Listener: listener, Metrics: metrics}, nil)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- uploader.Run(ctx) }()
			// Registered after newUploader's cleanup, so it runs first: Run
			// stops before the store closes and the spool is removed, on
			// every path.
			t.Cleanup(func() {
				cancel()
				if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("Run() error = %v", err)
				}
			})

			if tt.wantUptime {
				waitFor(t, "readiness", func() bool { return statusOf(base+"/readyz") == http.StatusOK })
				waitFor(t, "the uptime gauge", func() bool {
					_, ok := metrics.Value(telemetry.Uptime, nil)
					return ok
				})
				return
			}
			waitFor(t, "the journal to be rejected", func() bool {
				return strings.Contains(logs.String(), "an operator must look at the journal")
			})
			if got := statusOf(base + "/readyz"); got != http.StatusServiceUnavailable {
				t.Fatalf("/readyz = %d, want 503 while the journal is unreadable", got)
			}
			if value, ok := metrics.Value(telemetry.Uptime, nil); ok {
				t.Fatalf("uptime = %v while not ready, want no series", value)
			}
		})
	}
}

// captureInto sends the default logger to logs for the test.
func captureInto(t *testing.T, logs *uptimeLogs) {
	t.Helper()
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
}

// uptimeLogs is a log buffer the uploader's goroutines may write while the
// test reads it.
type uptimeLogs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *uptimeLogs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *uptimeLogs) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}
