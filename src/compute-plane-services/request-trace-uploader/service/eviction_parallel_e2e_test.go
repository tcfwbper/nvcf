// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/format"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/service"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

// heldPrepares holds each segment's prepare at its first record until the
// test releases that segment.
type heldPrepares struct {
	mu      sync.Mutex
	started map[int]chan struct{}
	gates   map[int]chan struct{}
	freed   map[int]bool
}

func newHeldPrepares(segments int) *heldPrepares {
	h := &heldPrepares{started: map[int]chan struct{}{}, gates: map[int]chan struct{}{}, freed: map[int]bool{}}
	for i := range segments {
		h.started[i] = make(chan struct{})
		h.gates[i] = make(chan struct{})
	}
	return h
}

func (h *heldPrepares) onWrite(target format.Target, _ transform.Record) error {
	h.mu.Lock()
	started, ok := h.started[target.Segment.Index]
	gate := h.gates[target.Segment.Index]
	if ok {
		delete(h.started, target.Segment.Index)
	}
	h.mu.Unlock()
	if !ok {
		return nil
	}
	close(started)
	<-gate
	return nil
}

func (h *heldPrepares) waitStarted(t *testing.T, indexes ...int) {
	t.Helper()
	for _, i := range indexes {
		h.mu.Lock()
		started, ok := h.started[i]
		h.mu.Unlock()
		if !ok {
			continue
		}
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for segment %d's prepare to start", i)
		}
	}
}

func (h *heldPrepares) release(indexes ...int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, i := range indexes {
		if !h.freed[i] {
			h.freed[i] = true
			close(h.gates[i])
		}
	}
}

func (h *heldPrepares) releaseAll() {
	h.mu.Lock()
	all := make([]int, 0, len(h.gates))
	for i := range h.gates {
		all = append(all, i)
	}
	h.mu.Unlock()
	h.release(all...)
}

// fullSpoolWithHeldPrepares runs an uploader with four workers over the
// given number of closed segments, holds the first four prepares, fills the
// spool to 95% and waits until the spool check has asked for an eviction.
func fullSpoolWithHeldPrepares(t *testing.T, segments int) (closed []string, held *heldPrepares, evicted func() float64, prepared *atomic.Int64) {
	t.Helper()
	var lines [][]string
	for i := range segments {
		lines = append(lines, []string{uploadertest.RequestEnd(fmt.Sprintf("r%d", i), uint64(i+1))})
	}
	spool, closed := spoolWith(t, lines...)
	held = newHeldPrepares(segments)
	metrics, err := telemetry.NewMemory("fake")
	if err != nil {
		t.Fatal(err)
	}
	prepared = &atomic.Int64{}
	volume := roomyVolume(spool.Dir)
	listener, _ := listen(t)
	uploader := newUploader(t, spool, hookedFormat{Fake: batchingFake(), onWrite: held.onWrite}, service.Options{
		Metrics:  metrics,
		Listener: listener,
		Volume:   volume,
		Point: func(name string) {
			if name == service.PointPrepared {
				prepared.Add(1)
			}
		},
	}, map[string]string{
		config.EnvScanInterval:       "1h",
		config.EnvSpoolCheckInterval: "1s",
		config.EnvPrepareWorkers:     "4",
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- uploader.Run(ctx) }()
	t.Cleanup(func() {
		held.releaseAll()
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Run() error = %v", err)
		}
	})
	held.waitStarted(t, 0, 1, 2, 3)

	used := volume.segmentBytes()
	total := used * 20 / 19
	volume.total.Store(total)
	waitFor(t, "the spool check to see the full spool", func() bool {
		free, ok := metrics.Value(telemetry.SpoolFreeBytes, nil)
		return ok && free <= float64(total)
	})
	// One more check, so the eviction is surely due.
	time.Sleep(1100 * time.Millisecond)
	evicted = func() float64 {
		got, _ := metrics.Value(telemetry.SegmentsEvicted, nil)
		return got
	}
	return closed, held, evicted, prepared
}

// When the first of four prepares finishes, intake reaches a segment
// boundary with three still running. The eviction the spool check asked for
// waits until every running prepare is recorded, so nothing is deleted while
// a segment is being prepared.
func TestEvictionWaitsForEveryRunningPrepare(t *testing.T) {
	closed, held, evicted, prepared := fullSpoolWithHeldPrepares(t, 5)

	held.release(0)
	waitFor(t, "the first segment to be recorded prepared", func() bool { return prepared.Load() >= 1 })
	// Give intake time to reach the boundary and the check a few more turns.
	time.Sleep(1500 * time.Millisecond)
	if got := evicted(); got != 0 {
		t.Fatalf("segments_evicted = %v while three prepares were running, want 0", got)
	}
	for i, path := range closed {
		if gone(path) {
			t.Fatalf("segment %d was deleted while three prepares were running", i)
		}
	}

	held.releaseAll()
	waitFor(t, "the eviction once every running prepare is recorded", func() bool { return evicted() >= 1 })
	if !gone(closed[0]) {
		t.Fatal("the oldest segment is still on the spool, want it evicted first")
	}
}

// With four workers, intake can take its last segment before the spool
// fills. An eviction that comes due while those last prepares run happens
// once they are recorded, not at the next pass an hour away.
func TestEvictionDueDuringTheLastPreparesRunsWhenTheyFinish(t *testing.T) {
	_, held, evicted, _ := fullSpoolWithHeldPrepares(t, 4)

	held.releaseAll()
	waitFor(t, "the eviction once the last prepares are recorded", func() bool { return evicted() >= 1 })
}
