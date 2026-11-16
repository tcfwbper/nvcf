// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend/backendtest"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/diskguard"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/service"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

// segmentVolume reports a spool whose used space is the size of its segment
// files, so deleting a segment frees space the way a real volume would.
type segmentVolume struct {
	dir   string
	total atomic.Uint64
	// free, when set, replaces the computed free space.
	free atomic.Int64
}

func (v *segmentVolume) segmentBytes() uint64 {
	entries, _ := os.ReadDir(v.dir)
	var used uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jsonl.gz") {
			continue
		}
		// Counted as a real filesystem counts used space: in the blocks
		// files take, the unit eviction sizes its candidates in.
		if info, err := e.Info(); err == nil {
			used += uint64(diskguard.DiskBytes(filepath.Join(v.dir, e.Name()), info.Size()))
		}
	}
	return used
}

func (v *segmentVolume) Usage() (diskguard.Usage, error) {
	total := v.total.Load()
	used := v.segmentBytes()
	free := total - min(used, total)
	if f := v.free.Load(); f > 0 {
		free = uint64(f)
	}
	return diskguard.Usage{Total: total, Free: free}, nil
}

func roomyVolume(dir string) *segmentVolume {
	v := &segmentVolume{dir: dir}
	v.total.Store(1 << 40)
	return v
}

func TestAboveTheHighWatermarkTheOldestClosedSegmentIsEvictedAndCounted(t *testing.T) {
	spool, closed := spoolWith(t,
		[]string{uploadertest.RequestEnd("a", 1)},
		[]string{uploadertest.RequestEnd("b", 2)},
	)
	volume := &segmentVolume{dir: spool.Dir}
	// The three segments fill 95% of the volume, above the 85% high
	// watermark; removing the oldest brings it under the 75% low watermark.
	volume.total.Store(volume.segmentBytes() * 20 / 19)
	metrics, err := telemetry.NewMemory("fake")
	if err != nil {
		t.Fatal(err)
	}
	fake := backendtest.NewFake()
	uploader := newUploader(t, spool, fake, service.Options{Metrics: metrics, Volume: volume}, nil)

	_ = uploader.Pass(context.Background())
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}

	if !gone(closed[0]) {
		t.Fatal("the oldest closed segment was kept above the high watermark")
	}
	if got, _ := metrics.Value(telemetry.SegmentsEvicted, nil); got != 1 {
		t.Fatalf("segments_evicted = %v, want only the oldest evicted, and counted", got)
	}
	// Back under the high watermark, preparing goes on in the same pass, so
	// the newer segment may leave by upload, never by eviction.
	if gone(closed[1]) && fake.SubmitCalls() == 0 {
		t.Fatal("a newer closed segment was evicted, want only as much as the watermark needs")
	}
}

func TestAboveTheHighWatermarkTheSpoolIsFreedDownToTheConfiguredLowWatermark(t *testing.T) {
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	sent := spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("sent", 1)).Bytes())
	older := spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("older", 2)).Bytes())
	fake := backendtest.NewFake()
	// The first segment's job stays pending, so it is in a batch that may
	// have been sent for the rest of the test.
	fake.Script(backendtest.SubmitStep{Statuses: []backendtest.StatusStep{{Status: backend.StatusPending}}})
	volume := roomyVolume(spool.Dir)
	metrics, err := telemetry.NewMemory("fake")
	if err != nil {
		t.Fatal(err)
	}
	uploader := newUploader(t, spool, fake, service.Options{Metrics: metrics, Volume: volume}, map[string]string{
		config.EnvSpoolHighWatermark: "0.9",
		config.EnvSpoolLowWatermark:  "0.4",
	})
	_ = uploader.Pass(context.Background())
	if got := fake.SubmitCalls(); got != 1 {
		t.Fatalf("submits = %d, want the first segment sent", got)
	}

	newer := spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("newer", 3)).Bytes())
	active := spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("active", 4)).Bytes())
	// Four segments fill 95% of the volume. Freeing to 75% would take one
	// closed segment; freeing to the configured 40% takes every closed
	// segment that is in no batch.
	volume.total.Store(volume.segmentBytes() * 20 / 19)
	_ = uploader.Pass(context.Background())
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}

	if !gone(older) || !gone(newer) {
		t.Fatalf("older gone %v newer gone %v, want both freed down to the low watermark", gone(older), gone(newer))
	}
	if gone(sent) {
		t.Fatal("a segment in a batch that may have been sent was evicted")
	}
	if gone(active) {
		t.Fatal("the active segment was evicted")
	}
	if got, _ := metrics.Value(telemetry.SegmentsEvicted, nil); got != 2 {
		t.Fatalf("segments_evicted = %v, want both deletions counted", got)
	}
}

func TestAPartlySetSpoolPolicyStopsTheUploader(t *testing.T) {
	spool, _ := spoolWith(t)
	cfg := loadEnv(t, spoolEnv(t, spool.Dir, nil))
	cfg.Spool = config.SpoolPolicy{HighWatermark: 0.9}
	uploader, err := service.NewUploader(cfg, backendtest.NewFake(), service.Options{Faults: fastFaults(), Volume: roomyVolume(spool.Dir)})
	if err != nil {
		t.Fatalf("NewUploader() error = %v", err)
	}
	t.Cleanup(func() { _ = uploader.Close() })

	if err := uploader.Pass(context.Background()); err == nil || !strings.Contains(err.Error(), "low watermark") {
		t.Fatalf("Pass() error = %v, want a startup error for the missing low watermark", err)
	}
}

func TestANegativeSpoolCheckIntervalStopsTheUploader(t *testing.T) {
	spool, _ := spoolWith(t)
	cfg := loadEnv(t, spoolEnv(t, spool.Dir, nil))
	cfg.Spool.CheckInterval = -time.Second
	uploader, err := service.NewUploader(cfg, backendtest.NewFake(), service.Options{Faults: fastFaults(), Volume: roomyVolume(spool.Dir)})
	if err != nil {
		t.Fatalf("NewUploader() error = %v", err)
	}
	t.Cleanup(func() { _ = uploader.Close() })

	if err := uploader.Pass(context.Background()); err == nil || !strings.Contains(err.Error(), "check interval") {
		t.Fatalf("Pass() error = %v, want a startup error for the negative check interval", err)
	}
}

func TestQuarantineOlderThanItsMaxAgeIsRemovedOnAPass(t *testing.T) {
	spool, _ := spoolWith(t)
	entry := filepath.Join(spool.Dir, "request-trace-uploader-quarantine", "old-entry")
	if err := os.MkdirAll(entry, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(entry, "segment.jsonl.gz"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	var offset atomic.Int64
	clock := func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }
	metrics, err := telemetry.NewMemory("fake")
	if err != nil {
		t.Fatal(err)
	}
	uploader := newUploader(t, spool, backendtest.NewFake(), service.Options{Metrics: metrics, Volume: roomyVolume(spool.Dir), Now: clock}, nil)

	_ = uploader.Pass(context.Background())
	if gone(entry) {
		t.Fatal("a fresh quarantine entry was removed")
	}
	offset.Store(int64(8 * 24 * time.Hour))
	_ = uploader.Pass(context.Background())
	if !gone(entry) {
		t.Fatal("a quarantine entry past its max age was kept")
	}
	if got, _ := metrics.Value(telemetry.QuarantineEvicted, nil); got != 1 {
		t.Fatalf("quarantine_evicted = %v, want the removal counted", got)
	}
}

func TestPreparingPausesWhileFreeSpaceIsLowAndResumesAfter(t *testing.T) {
	spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
	info, err := os.Stat(closed[0])
	if err != nil {
		t.Fatal(err)
	}
	// A volume ten segments in size, 84% used: under the high watermark,
	// but with less free space than twice the segment to prepare.
	size := info.Size()
	volume := &segmentVolume{dir: spool.Dir}
	volume.total.Store(uint64(10 * size))
	volume.free.Store(16 * size / 10)
	fake := backendtest.NewFake()
	uploader := newUploader(t, spool, fake, service.Options{Volume: volume}, nil)

	_ = uploader.Pass(context.Background())
	if got := fake.SubmitCalls(); got != 0 {
		t.Fatalf("submits = %d while free space is low, want preparing paused", got)
	}
	if gone(closed[0]) {
		t.Fatal("the segment was deleted while preparing was paused")
	}

	volume.total.Store(1 << 40) // the volume grows
	volume.free.Store(0)        // and free space is computed again
	_ = uploader.Pass(context.Background())
	if got := fake.SubmitCalls(); got != 1 {
		t.Fatalf("submits = %d after space freed up, want the segment prepared and sent", got)
	}
}

// A spool budget bounds the spool by the files it holds, so on a large
// shared disk the oldest closed segments are evicted once the spool is
// above its budget's watermark. Without a budget only the disk counts. The
// active segment is never evicted.
func TestASpoolBudgetEvictsOnASharedDiskThatHasRoom(t *testing.T) {
	cases := []struct {
		name        string
		budget      int64
		wantEvicted float64
	}{
		{"no budget", 0, 0},
		// One byte: every closed segment is above the budget's watermark.
		{"budget below the spool", 1, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spool, closed := spoolWith(t,
				[]string{uploadertest.RequestEnd("a", 1)},
				[]string{uploadertest.RequestEnd("b", 2)},
			)
			metrics, err := telemetry.NewMemory("fake")
			if err != nil {
				t.Fatal(err)
			}
			cfg := loadEnv(t, spoolEnv(t, spool.Dir, nil))
			cfg.SpoolBudgetBytes = tc.budget
			fake := backendtest.NewFake()
			opts := service.Options{Metrics: metrics, Volume: roomyVolume(spool.Dir), Faults: fastFaults()}
			uploader, err := service.NewUploader(cfg, fake, opts)
			if err != nil {
				t.Fatalf("NewUploader() error = %v", err)
			}
			t.Cleanup(func() { _ = uploader.Close() })

			_ = uploader.Pass(context.Background())
			if err := uploader.Close(); err != nil {
				t.Fatal(err)
			}

			if got, _ := metrics.Value(telemetry.SegmentsEvicted, nil); got != tc.wantEvicted {
				t.Fatalf("segments_evicted = %v, want %v", got, tc.wantEvicted)
			}
			if left := roomyVolume(spool.Dir).segmentBytes(); left == 0 {
				t.Fatal("every segment was evicted, want the active one kept")
			}
			if tc.budget > 0 && (!gone(closed[0]) || !gone(closed[1])) {
				t.Fatal("a closed segment above the budget was kept")
			}
		})
	}
}

// A small spool under its 85% watermark keeps preparing and exporting, with
// a budget or without one. The fixed 512 MiB free-space floor paused
// preparing on a spool under about 3.3 GiB, and relief only brings a spool
// back to its watermark, so it never resumed.
func TestASmallSpoolKeepsPreparingUnderItsWatermark(t *testing.T) {
	cases := []struct {
		name string
		// budget is the spool budget; zero is none.
		budget int64
		// other is how many bytes of other files the spool holds.
		other int64
		// total and free, when set, size the volume; otherwise it is roomy.
		total uint64
		free  int64
	}{
		{"1 GiB budget holding 600 MiB", 1 << 30, 600 << 20, 0, 0},
		{"2 GiB volume with 500 MiB free, no budget", 0, 0, 2 << 30, 500 << 20},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spool, _ := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
			if tc.other > 0 {
				// Other files under the spool count against the budget at
				// their apparent size; a sparse file takes no real disk space.
				other := filepath.Join(spool.Dir, "other")
				if err := os.MkdirAll(other, 0o750); err != nil {
					t.Fatal(err)
				}
				f, err := os.Create(filepath.Join(other, "data"))
				if err != nil {
					t.Fatal(err)
				}
				if err := f.Truncate(tc.other); err != nil {
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			}
			volume := roomyVolume(spool.Dir)
			if tc.total > 0 {
				volume.total.Store(tc.total)
				volume.free.Store(tc.free)
			}
			metrics, err := telemetry.NewMemory("fake")
			if err != nil {
				t.Fatal(err)
			}
			cfg := loadEnv(t, spoolEnv(t, spool.Dir, nil))
			cfg.SpoolBudgetBytes = tc.budget
			fake := backendtest.NewFake()
			uploader, err := service.NewUploader(cfg, fake, service.Options{Metrics: metrics, Volume: volume, Faults: fastFaults()})
			if err != nil {
				t.Fatalf("NewUploader() error = %v", err)
			}
			t.Cleanup(func() { _ = uploader.Close() })

			_ = uploader.Pass(context.Background())
			if got := fake.SubmitCalls(); got != 1 {
				t.Fatalf("submits = %d, want the segment prepared and sent under the watermark", got)
			}
			if got, _ := metrics.Value(telemetry.SegmentsEvicted, nil); got != 0 {
				t.Fatalf("segments_evicted = %v, want nothing evicted under the watermark", got)
			}
		})
	}
}
