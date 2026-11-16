// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend/backendtest"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/format"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/service"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

// slowPrepare is a backend whose format writer holds the first record it is
// given until release closes, so a test can keep one prepare running with
// its transport half written.
type slowPrepare struct {
	*backendtest.Fake
	started chan struct{}
	release chan struct{}
	once    *sync.Once
	stop    *sync.Once
}

func newSlowPrepare() slowPrepare {
	return slowPrepare{
		Fake:    backendtest.NewFake(),
		started: make(chan struct{}),
		release: make(chan struct{}),
		once:    &sync.Once{},
		stop:    &sync.Once{},
	}
}

func (s slowPrepare) Format() backend.Format { return backend.FormatGzipJSONL }

func (s slowPrepare) NewWriter(target format.Target) (format.Writer, error) {
	w, err := format.GzipJSONL{}.NewWriter(target)
	if err != nil {
		return nil, err
	}
	return slowWriter{Writer: w, s: s}, nil
}

// Release lets the held prepare finish. It is safe to call more than once.
func (s slowPrepare) Release() { s.stop.Do(func() { close(s.release) }) }

type slowWriter struct {
	format.Writer
	s slowPrepare
}

func (w slowWriter) Write(r transform.Record) error {
	w.s.once.Do(func() {
		close(w.s.started)
		<-w.s.release
	})
	return w.Writer.Write(r)
}

// runUploader runs uploader until the test ends.
func runUploader(t *testing.T, uploader *service.Uploader, slow slowPrepare) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- uploader.Run(ctx) }()
	t.Cleanup(func() {
		slow.Release()
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Run() error = %v", err)
		}
	})
}

func waitStarted(t *testing.T, slow slowPrepare) {
	t.Helper()
	select {
	case <-slow.started:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the prepare to start")
	}
}

func TestASlowPrepareDoesNotDelayTheSpoolCheck(t *testing.T) {
	spool, _ := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
	slow := newSlowPrepare()
	metrics, err := telemetry.NewMemory("fake")
	if err != nil {
		t.Fatal(err)
	}
	listener, _ := listen(t)
	uploader := newUploader(t, spool, slow, service.Options{Metrics: metrics, Listener: listener, Volume: roomyVolume(spool.Dir)}, map[string]string{
		config.EnvScanInterval:       "1h",
		config.EnvSpoolCheckInterval: "1s",
	})
	runUploader(t, uploader, slow)
	waitStarted(t, slow)

	// Quarantine past its age appears while the prepare is still running.
	entry := filepath.Join(spool.Dir, "request-trace-uploader-quarantine", "old-entry")
	if err := os.MkdirAll(entry, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(entry, "segment.jsonl.gz"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-8 * 24 * time.Hour)
	if err := os.Chtimes(entry, old, old); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the spool gauges while the prepare runs", func() bool {
		free, ok := metrics.Value(telemetry.SpoolFreeBytes, nil)
		return ok && free > 0
	})
	waitFor(t, "the stale quarantine entry to be removed and counted while the prepare runs", func() bool {
		got, _ := metrics.Value(telemetry.QuarantineEvicted, nil)
		return gone(entry) && got == 1
	})
	select {
	case <-slow.release:
		t.Fatal("the prepare was released before the check ran")
	default:
	}
}

func TestEvictionWaitsForTheNextSegmentBoundary(t *testing.T) {
	spool, closed := spoolWith(t,
		[]string{uploadertest.RequestEnd("a", 1)},
		[]string{uploadertest.RequestEnd("b", 2)},
	)
	slow := newSlowPrepare()
	metrics, err := telemetry.NewMemory("fake")
	if err != nil {
		t.Fatal(err)
	}
	volume := roomyVolume(spool.Dir)
	listener, _ := listen(t)
	uploader := newUploader(t, spool, slow, service.Options{Metrics: metrics, Listener: listener, Volume: volume}, map[string]string{
		config.EnvScanInterval:       "1h",
		config.EnvSpoolCheckInterval: "1s",
	})
	runUploader(t, uploader, slow)
	waitStarted(t, slow)

	// The first segment is being prepared when the spool fills to 95%.
	used := volume.segmentBytes()
	total := used * 20 / 19
	volume.total.Store(total)
	waitFor(t, "the spool check to see the full spool", func() bool {
		free, ok := metrics.Value(telemetry.SpoolFreeBytes, nil)
		return ok && free <= float64(total)
	})
	// Give the check a few more turns: it must never delete mid-prepare.
	time.Sleep(2500 * time.Millisecond)
	if gone(closed[0]) || gone(closed[1]) {
		t.Fatal("a segment was deleted while a prepare was running")
	}
	if got, _ := metrics.Value(telemetry.SegmentsEvicted, nil); got != 0 {
		t.Fatalf("segments_evicted = %v while a prepare was running, want 0", got)
	}

	slow.Release()
	// The next scan is an hour away, so only the segment boundary in this
	// pass can evict. Freeing to 75% takes the oldest closed segment.
	waitFor(t, "an eviction at the next segment boundary", func() bool {
		got, _ := metrics.Value(telemetry.SegmentsEvicted, nil)
		return got == 1
	})
	if !gone(closed[0]) || gone(closed[1]) {
		t.Fatalf("first gone %v second gone %v, want only the oldest evicted", gone(closed[0]), gone(closed[1]))
	}
}
