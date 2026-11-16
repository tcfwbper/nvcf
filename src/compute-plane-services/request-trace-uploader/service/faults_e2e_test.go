// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service_test

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend/backendtest"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/journalfs"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/retry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/journal"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/service"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

func batchingFake() *backendtest.Fake {
	fake := backendtest.NewFake()
	fake.SetCapabilities(backend.Capabilities{
		AcceptedFormats: []backend.Format{backend.FormatGzipJSONL},
		Exports:         true,
		MaxBatchFiles:   16,
	})
	return fake
}

func journalBatches(t *testing.T, spool *uploadertest.Spool) []journal.Batch {
	t.Helper()
	store, err := journalfs.Open(context.Background(), journalfs.Config{
		SpoolDir: spool.Dir,
		StateDir: filepath.Join(spool.Dir, "request-trace-uploader-state"),
		Holder:   "inspector",
		LockWait: time.Second,
	}, journalfs.Options{})
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	defer store.Close()
	return store.Batches()
}

// journalSegments opens the journal after the uploader released it and
// lists every segment entry.
func journalSegments(t *testing.T, spool *uploadertest.Spool) []journal.Segment {
	t.Helper()
	store, err := journalfs.Open(context.Background(), journalfs.Config{
		SpoolDir: spool.Dir,
		StateDir: filepath.Join(spool.Dir, "request-trace-uploader-state"),
		Holder:   "inspector",
		LockWait: time.Second,
	}, journalfs.Options{})
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	defer store.Close()
	return store.Segments()
}

// readOnly makes dir unwritable until restore is called or the test ends.
func readOnly(t *testing.T, dir string) (restore func()) {
	t.Helper()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	restore = func() { once.Do(func() { _ = os.Chmod(dir, 0o750) }) }
	t.Cleanup(restore)
	return restore
}

func passUntil(t *testing.T, uploader *service.Uploader, done func() bool) {
	t.Helper()
	waitFor(t, "the pipeline to finish", func() bool {
		_ = uploader.Pass(context.Background())
		return done()
	})
}

func TestARejectArtifactThatCannotBeKeptHoldsTheSegment(t *testing.T) {
	spool, closed := spoolWith(t, []string{"not json", uploadertest.RequestEnd("a", 1)})
	data, err := os.ReadFile(closed[0])
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	id := journal.SegmentID(config.DefaultSegmentPrefix, 0, int64(len(data)), hex.EncodeToString(sum[:]))
	// A directory where the artifact must go makes keeping it fail.
	blocker := filepath.Join(spool.Dir, "request-trace-uploader-quarantine", "rejects", id+".jsonl.gz")
	if err := os.MkdirAll(filepath.Join(blocker, "x"), 0o750); err != nil {
		t.Fatal(err)
	}
	fake := backendtest.NewFake()
	uploader := newUploader(t, spool, fake, service.Options{}, nil)

	_ = uploader.Pass(context.Background())
	if fake.SubmitCalls() != 0 || gone(closed[0]) {
		t.Fatalf("submit calls = %d, source gone = %v; want nothing sent while the rejected line has no durable copy", fake.SubmitCalls(), gone(closed[0]))
	}
	if err := os.RemoveAll(blocker); err != nil {
		t.Fatal(err)
	}
	passUntil(t, uploader, func() bool { return gone(closed[0]) })
	if _, err := os.Stat(blocker); err != nil {
		t.Fatalf("reject artifact: %v, want it kept", err)
	}
}

func TestABatchWhoseMembersCouldNotBeRecordedIsNotSentTwice(t *testing.T) {
	spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)}, []string{uploadertest.RequestEnd("b", 2)})
	segments := filepath.Join(spool.Dir, "request-trace-uploader-state", "segments")
	var restore func()
	prepared := 0
	fake := batchingFake()
	uploader := newUploader(t, spool, fake, service.Options{Point: func(name string) {
		if name == service.PointPrepared {
			if prepared++; prepared == 2 {
				restore = readOnly(t, segments)
			}
		}
	}}, nil)

	_ = uploader.Pass(context.Background())
	restore()
	passUntil(t, uploader, func() bool { return gone(closed[0]) && gone(closed[1]) })
	for range 2 {
		_ = uploader.Pass(context.Background())
	}
	if fake.Duplicates() != 0 {
		t.Fatalf("duplicates = %d, want each segment landed once", fake.Duplicates())
	}
}

func TestARetainedSubmitWaitsForAnOperator(t *testing.T) {
	spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
	fake := backendtest.NewFake()
	fake.Script(backendtest.SubmitStep{Outcome: backendtest.Refuse, Err: &backend.Fault{Class: backend.ClassUncertain, Scope: backend.ScopeUnknown, Reason: backend.ReasonTimeout}})
	faults := fastFaults()
	faults.AmbiguousWithoutID = retry.AmbiguousRetain
	uploader := newUploader(t, spool, fake, service.Options{Faults: faults}, nil)
	for range 3 {
		_ = uploader.Pass(context.Background())
	}
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}
	if fake.SubmitCalls() != 1 || gone(closed[0]) {
		t.Fatalf("submit calls = %d, want the retained batch left alone", fake.SubmitCalls())
	}
	for _, b := range journalBatches(t, spool) {
		if b.NextRetryAt.Before(time.Now().Add(365 * 24 * time.Hour)) {
			t.Fatalf("retained batch retries at %v, want it never retried automatically", b.NextRetryAt)
		}
	}
}

func TestAJobIDThatCouldNotBeSavedIsSavedLaterWithoutAnotherSubmit(t *testing.T) {
	spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
	batches := filepath.Join(spool.Dir, "request-trace-uploader-state", "batches")
	var restore func()
	fake := backendtest.NewFake()
	uploader := newUploader(t, spool, fake, service.Options{Point: func(name string) {
		if name == service.PointSubmitted && restore == nil {
			restore = readOnly(t, batches)
		}
	}}, nil)

	_ = uploader.Pass(context.Background())
	restore()
	passUntil(t, uploader, func() bool { return gone(closed[0]) })
	if fake.SubmitCalls() != 1 {
		t.Fatalf("submit calls = %d, want 1", fake.SubmitCalls())
	}
}

func TestPollingIsNotHeldUpByASlowSubmit(t *testing.T) {
	spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
	fake := backendtest.NewFake()
	fake.Script(
		backendtest.SubmitStep{Statuses: []backendtest.StatusStep{{Status: backend.StatusPending}, {Status: backend.StatusSucceeded}}},
		backendtest.SubmitStep{Delay: 5 * time.Second},
	)
	uploader := newUploader(t, spool, fake, service.Options{}, nil)
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("next-active", 3)).Bytes())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slow := make(chan struct{})
	go func() {
		defer close(slow)
		_ = uploader.Pass(ctx)
	}()
	waitFor(t, "the slow submit to start", func() bool { return fake.SubmitCalls() == 2 })

	started := time.Now()
	if err := uploader.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Poll took %v behind a slow submit, want it independent", elapsed)
	}
	if !gone(closed[0]) {
		t.Fatal("the first job's success was not applied")
	}
	cancel()
	<-slow
}

func TestASegmentWhoseJobsKeepFailingIsQuarantined(t *testing.T) {
	spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
	fake := backendtest.NewFake()
	failing := backendtest.SubmitStep{Statuses: []backendtest.StatusStep{{
		Status: backend.StatusFailed,
		Err:    &backend.Fault{Class: backend.ClassRetryable, Scope: backend.ScopeSegment, Reason: backend.ReasonRejected},
	}}}
	for range 20 {
		fake.Script(failing)
	}
	uploader := newUploader(t, spool, fake, service.Options{}, nil)
	for range 12 {
		_ = uploader.Pass(context.Background())
		time.Sleep(20 * time.Millisecond)
	}
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}
	if state := journalStates(t, spool)[filepath.Base(closed[0])]; state != journal.StateQuarantined {
		t.Fatalf("state = %q after %d failed jobs, want quarantined", state, fake.SubmitCalls())
	}
}

func TestErrorLogsCarryNoSpoolPaths(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file permissions")
	}
	logs := captureLogs(t)
	spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
	if err := os.Chmod(closed[0], 0); err != nil {
		t.Fatal(err)
	}
	uploader := newUploader(t, spool, backendtest.NewFake(), service.Options{}, nil)
	_ = uploader.Pass(context.Background())
	if !strings.Contains(logs.String(), "permission denied") {
		t.Fatalf("logs = %s, want the read failure logged", logs)
	}
	if strings.Contains(logs.String(), spool.Dir) {
		t.Fatalf("logs = %s, want no spool path", logs)
	}
}

func TestASegmentIsRetriedUntilItReachesTheFailureThreshold(t *testing.T) {
	spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
	fake := backendtest.NewFake()
	refused := backendtest.SubmitStep{Outcome: backendtest.Refuse, Err: &backend.Fault{Class: backend.ClassRetryable, Scope: backend.ScopeSegment, Reason: backend.ReasonRejected}}
	faults := fastFaults()
	faults.SegmentScopeFailures = 5
	for range faults.SegmentScopeFailures - 1 {
		fake.Script(refused)
	}
	uploader := newUploader(t, spool, fake, service.Options{Faults: faults}, nil)
	passUntil(t, uploader, func() bool { return gone(closed[0]) || fake.SubmitCalls() > faults.SegmentScopeFailures })
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}
	if state := journalStates(t, spool)[filepath.Base(closed[0])]; state != journal.StateDone {
		t.Fatalf("state = %q after %d refused submits under a threshold of %d, want done", state, faults.SegmentScopeFailures-1, faults.SegmentScopeFailures)
	}
}

func TestIntakeStopsAsSoonAsTheQuarantineBreakerTrips(t *testing.T) {
	bad := make([][]string, 5)
	for i := range bad {
		bad[i] = []string{"not json " + strings.Repeat("x", i)}
	}
	spool, closed := spoolWith(t, bad...)
	faults := fastFaults()
	faults.QuarantineTrip = 2
	uploader := newUploader(t, spool, backendtest.NewFake(), service.Options{Faults: faults}, nil)
	_ = uploader.Pass(context.Background())
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}
	quarantined := 0
	states := journalStates(t, spool)
	for _, path := range closed {
		if states[filepath.Base(path)] == journal.StateQuarantined {
			quarantined++
		}
	}
	if quarantined != faults.QuarantineTrip {
		t.Fatalf("quarantined %d segments with a breaker that trips at %d, want intake to stop at the trip", quarantined, faults.QuarantineTrip)
	}
}

// TestTheQuarantineBreakerResumesAfterTheConfiguredTime runs the breaker
// from the configured settings, with no fault handling override: intake
// stops at the trip, reads new segments again once the resume time has
// passed, and trips again after the same number of quarantines.
func TestTheQuarantineBreakerResumesAfterTheConfiguredTime(t *testing.T) {
	bad := make([][]string, 6)
	for i := range bad {
		bad[i] = []string{"not json " + strings.Repeat("x", i)}
	}
	spool, closed := spoolWith(t, bad...)
	var offset atomic.Int64
	start := time.Now()
	clock := func() time.Time { return start.Add(time.Duration(offset.Load())) }
	metrics, err := telemetry.NewMemory("fake")
	if err != nil {
		t.Fatal(err)
	}
	cfg := loadEnv(t, spoolEnv(t, spool.Dir, map[string]string{
		config.EnvQuarantineTrip:   "2",
		config.EnvQuarantineResume: "1m",
	}))
	uploader, err := service.NewUploader(cfg, backendtest.NewFake(), service.Options{Metrics: metrics, Now: clock})
	if err != nil {
		t.Fatalf("NewUploader() error = %v", err)
	}
	t.Cleanup(func() { _ = uploader.Close() })
	if got, ok := metrics.Value(telemetry.QuarantineResumed, nil); !ok || got != 0 {
		t.Fatalf("quarantine_resumed at start = %v, %v; want 0, true", got, ok)
	}

	counter := func(name string) float64 {
		got, _ := metrics.Value(name, nil)
		return got
	}
	quarantined := func() float64 {
		var n float64
		for _, reason := range telemetry.QuarantineReasons {
			got, _ := metrics.Value(telemetry.SegmentsQuarantined, telemetry.Labels{"reason": reason})
			n += got
		}
		return n
	}
	steps := []struct {
		at                         time.Duration
		quarantined, trips, resume float64
	}{
		{at: 0, quarantined: 2, trips: 1, resume: 0},
		{at: 59 * time.Second, quarantined: 2, trips: 1, resume: 0},
		{at: 60 * time.Second, quarantined: 4, trips: 2, resume: 1},
		{at: 119 * time.Second, quarantined: 4, trips: 2, resume: 1},
		{at: 120 * time.Second, quarantined: 6, trips: 3, resume: 2},
	}
	for _, step := range steps {
		offset.Store(int64(step.at))
		_ = uploader.Pass(context.Background())
		if got := quarantined(); got != step.quarantined {
			t.Fatalf("at %v: quarantined %v segments, want %v", step.at, got, step.quarantined)
		}
		if got := counter(telemetry.QuarantineTripped); got != step.trips {
			t.Fatalf("at %v: quarantine_tripped = %v, want %v", step.at, got, step.trips)
		}
		if got := counter(telemetry.QuarantineResumed); got != step.resume {
			t.Fatalf("at %v: quarantine_resumed = %v, want %v", step.at, got, step.resume)
		}
	}
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}
	states := journalStates(t, spool)
	for _, path := range closed {
		if states[filepath.Base(path)] != journal.StateQuarantined {
			t.Fatalf("segment %s is %q, want every bad segment quarantined after two resumes", filepath.Base(path), states[filepath.Base(path)])
		}
	}
}

func TestAConfirmationWhoseMembersWereNotRecordedIsFinishedWithoutARestart(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
	segments := filepath.Join(spool.Dir, "request-trace-uploader-state", "segments")
	var restore func()
	fake := backendtest.NewFake()
	metrics, err := telemetry.NewMemory("fake")
	if err != nil {
		t.Fatal(err)
	}
	uploader := newUploader(t, spool, fake, service.Options{Metrics: metrics, Point: func(name string) {
		if name == service.PointReceiptSaved && restore == nil {
			restore = readOnly(t, segments)
		}
	}}, nil)

	_ = uploader.Pass(context.Background())
	restore()
	if gone(closed[0]) {
		t.Fatal("source deleted while its confirmation was not recorded")
	}
	passUntil(t, uploader, func() bool { return gone(closed[0]) })
	if fake.SubmitCalls() != 1 {
		t.Fatalf("submit calls = %d, want 1", fake.SubmitCalls())
	}
	if got, _ := metrics.Value(telemetry.Uploads, telemetry.Labels{"status": "success"}); got != 1 {
		t.Errorf("uploads{success} = %v, want 1", got)
	}
}

func TestOneSegmentWhoseJobsKeepFailingIsQuarantinedWithoutHoldingTheBackend(t *testing.T) {
	logs := captureLogs(t)
	spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)}, []string{uploadertest.RequestEnd("b", 2)})
	fake := backendtest.NewFake()
	// A failed job with no reason is an unknown-scope fault.
	failing := backendtest.SubmitStep{Statuses: []backendtest.StatusStep{{Status: backend.StatusFailed}}}
	fake.Script(failing, backendtest.SubmitStep{})
	for range 20 {
		fake.Script(failing)
	}
	uploader := newUploader(t, spool, fake, service.Options{}, nil)
	passUntil(t, uploader, func() bool { return gone(closed[0]) && gone(closed[1]) })
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}
	states := journalStates(t, spool)
	if state := states[filepath.Base(closed[0])]; state != journal.StateQuarantined {
		t.Fatalf("failing segment state = %q after %d submits, want quarantined", state, fake.SubmitCalls())
	}
	if state := states[filepath.Base(closed[1])]; state != journal.StateDone {
		t.Fatalf("healthy segment state = %q, want done", state)
	}
	if strings.Contains(logs.String(), `"action":"hold"`) {
		t.Fatalf("one failing segment held the whole backend:\n%s", logs)
	}
}

// A batch recorded under a larger backend batch limit can be split after a
// restart under a smaller one. Each member then goes alone, and one over the
// limit is quarantined as too large, as a new member over it is.
func TestASplitMemberOverTheBatchLimitIsQuarantined(t *testing.T) {
	spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)}, []string{uploadertest.RequestEnd("b", 1)})
	fake := batchingFake()
	fake.Script(backendtest.SubmitStep{Statuses: []backendtest.StatusStep{
		{Status: backend.StatusPending},
		{Status: backend.StatusFailed, Err: &backend.Fault{Class: backend.ClassPermanent, Scope: backend.ScopeSegment, Reason: backend.ReasonRejected}},
	}})
	first := newUploader(t, spool, fake, service.Options{}, nil)
	_ = first.Pass(context.Background())
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if fake.SubmitCalls() != 1 {
		t.Fatalf("setup: submit calls = %d, want the two segments sent as one batch", fake.SubmitCalls())
	}

	fake.SetCapabilities(backend.Capabilities{
		AcceptedFormats: []backend.Format{backend.FormatGzipJSONL},
		Exports:         true,
		MaxBatchFiles:   16,
		MaxBatchBytes:   1,
	})
	second := newUploader(t, spool, fake, service.Options{}, nil)
	for range 6 {
		_ = second.Pass(context.Background())
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	states := journalStates(t, spool)
	for _, path := range closed {
		if state := states[filepath.Base(path)]; state != journal.StateQuarantined {
			t.Errorf("%s state = %q, want quarantined as too large", filepath.Base(path), state)
		}
	}
}

// A resend moves its batch back to Submitting before it is sent. When that
// record lands but its directory cannot be synced, the batch is still sent:
// later rounds pick up only waiting and ambiguous batches, so stopping there
// would leave it unsent until a restart.
func TestAResendIsSentWhenItsResumeWasNotSynced(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	spool, _ := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
	fake := backendtest.NewFake()
	fake.Script(backendtest.SubmitStep{Outcome: backendtest.Refuse, Err: &backend.Fault{Class: backend.ClassRetryable, Scope: backend.ScopeSegment, Reason: backend.ReasonRejected}})
	// The retry waits long enough that the first pass cannot also resend.
	faults := fastFaults()
	faults.Transient = retry.Backoff{Base: time.Second, Max: time.Second}
	uploader := newUploader(t, spool, fake, service.Options{Faults: faults}, nil)
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if fake.SubmitCalls() != 1 {
		t.Fatalf("setup: submit calls = %d, want the one refused submit", fake.SubmitCalls())
	}

	// A directory that can be written but not opened: a rename into it
	// works and the sync after it fails.
	batches := filepath.Join(spool.Dir, "request-trace-uploader-state", "batches")
	if err := os.Chmod(batches, 0o300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(batches, 0o750) })

	passUntil(t, uploader, func() bool { return fake.SubmitCalls() == 2 })
}

// A dry run keeps the source and deletes only the transports before the
// segment is finished as not exported. Those deletes must be durable first:
// when the transport directory cannot be synced, the segment stays in
// cleanup and is finished on a later pass.
func TestANotExportedSegmentFinishesOnlyAfterItsTransportDeletesAreSynced(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
	fake := backendtest.NewFake()
	fake.SetCapabilities(backend.Capabilities{AcceptedFormats: []backend.Format{backend.FormatGzipJSONL}})
	fake.Script(backendtest.SubmitStep{Statuses: []backendtest.StatusStep{{Status: backend.StatusPending}, {Status: backend.StatusSucceeded}}})
	first := newUploader(t, spool, fake, service.Options{}, nil)
	passUntil(t, first, func() bool { return fake.SubmitCalls() == 1 })

	// The directory can be written but not opened, so a delete in it works
	// and the sync after it fails.
	transport := filepath.Join(spool.Dir, "request-trace-uploader-state", "transport")
	if err := os.Chmod(transport, 0o300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(transport, 0o750) })
	for range 4 {
		_ = first.Pass(context.Background())
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if state := journalStates(t, spool)[filepath.Base(closed[0])]; state != journal.StateCleaning {
		t.Fatalf("state = %q while transport deletes cannot be synced, want cleaning", state)
	}

	if err := os.Chmod(transport, 0o750); err != nil {
		t.Fatal(err)
	}
	second := newUploader(t, spool, fake, service.Options{}, nil)
	for range 2 {
		_ = second.Pass(context.Background())
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if state := journalStates(t, spool)[filepath.Base(closed[0])]; state != journal.StateDone {
		t.Fatalf("state = %q after the directory recovered, want done", state)
	}
}

// timeoutEnv sets a one-second attempt timeout and a two-second operation
// timeout, the smallest values the settings take.
func timeoutEnv() map[string]string {
	return map[string]string{config.EnvAttemptTimeout: "1s", config.EnvOperationTimeout: "2s"}
}

// passWithin runs one pass and reports how long it took; the pass is given up
// after limit, so a call with no deadline shows as a pass that took limit.
func passWithin(t *testing.T, uploader *service.Uploader, limit time.Duration) time.Duration {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	started := time.Now()
	_ = uploader.Pass(ctx)
	return time.Since(started)
}

// A status call that stalls is cut at the attempt timeout, so one slow job
// does not hold the pass.
func TestAStalledStatusCallIsCutAtTheAttemptTimeout(t *testing.T) {
	spool, _ := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
	fake := backendtest.NewFake()
	fake.Script(backendtest.SubmitStep{Statuses: []backendtest.StatusStep{{Status: backend.StatusPending, Delay: time.Minute}}})
	uploader := newUploader(t, spool, fake, service.Options{}, timeoutEnv())
	if took := passWithin(t, uploader, 8*time.Second); took >= 5*time.Second {
		t.Fatalf("pass took %v behind a stalled status call, want it cut after the 1s attempt timeout", took)
	}
	if fake.SubmitCalls() != 1 {
		t.Fatalf("submit calls = %d, want 1", fake.SubmitCalls())
	}
}

// A submit uploads the files, and a full batch can take longer than any core
// timeout on a slow link; a cut submit would fail the same way on every
// retry. So the core sets no deadline on Submit; an upload deadline belongs to
// the backend. A submit longer than both core timeouts still completes.
func TestASubmitIsNotCutByTheCoreTimeouts(t *testing.T) {
	spool, _ := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
	fake := backendtest.NewFake()
	fake.Script(backendtest.SubmitStep{Delay: 2500 * time.Millisecond})
	uploader := newUploader(t, spool, fake, service.Options{}, timeoutEnv())
	passWithin(t, uploader, 8*time.Second)
	if jobs := fake.Jobs(); len(jobs) != 1 || fake.SubmitCalls() != 1 {
		t.Fatalf("jobs = %d after %d submit calls, want the one submit to complete past both timeouts", len(jobs), fake.SubmitCalls())
	}
}

// A Config built by hand, as another module may build one, leaves durations
// zero. NewUploader gives them their defaults: a zero job age would strand
// every job on its first poll, and a zero scan interval would stop Run.
func TestAConfigBuiltByHandGetsTheDefaultDurations(t *testing.T) {
	t.Run("max job age", func(t *testing.T) {
		spool, _ := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
		fake := backendtest.NewFake()
		fake.Script(backendtest.SubmitStep{Statuses: []backendtest.StatusStep{{Status: backend.StatusPending}}})
		cfg := loadEnv(t, spoolEnv(t, spool.Dir, nil))
		cfg.MaxJobAge = 0
		uploader, err := service.NewUploader(cfg, fake, service.Options{Faults: fastFaults()})
		if err != nil {
			t.Fatalf("NewUploader: %v", err)
		}
		for range 2 {
			_ = uploader.Pass(context.Background())
		}
		if err := uploader.Close(); err != nil {
			t.Fatal(err)
		}
		for _, b := range journalBatches(t, spool) {
			if b.State != journal.StateSubmitted {
				t.Fatalf("batch state = %s, want submitted: a pending job is not old", b.State)
			}
		}
	})
	t.Run("scan interval", func(t *testing.T) {
		spool, _ := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
		cfg := loadEnv(t, spoolEnv(t, spool.Dir, nil))
		cfg.ScanInterval = 0
		listener, _ := listen(t)
		uploader, err := service.NewUploader(cfg, backendtest.NewFake(), service.Options{Faults: fastFaults(), Listener: listener})
		if err != nil {
			t.Fatalf("NewUploader: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		if err := uploader.Run(ctx); err != nil {
			t.Fatalf("Run: %v", err)
		}
	})
}

// A failure while the whole backend is held, unavailable or refusing auth,
// says nothing about the segment, so it does not count toward the segment's
// quarantine threshold: a later fault of the segment itself does not
// quarantine a healthy segment.
func TestHeldFailuresDoNotCountTowardTheSegmentThreshold(t *testing.T) {
	for name, reason := range map[string]backend.Reason{"unavailable": backend.ReasonUnavailable, "auth": backend.ReasonAuth} {
		t.Run(name, func(t *testing.T) {
			spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
			fake := backendtest.NewFake()
			faults := fastFaults()
			for range faults.SegmentScopeFailures {
				fake.Script(backendtest.SubmitStep{Outcome: backendtest.Refuse, Err: &backend.Fault{Class: backend.ClassRetryable, Scope: backend.ScopeBackend, Reason: reason}})
			}
			fake.Script(backendtest.SubmitStep{Outcome: backendtest.Refuse, Err: &backend.Fault{Class: backend.ClassRetryable, Scope: backend.ScopeSegment, Reason: backend.ReasonRejected}})
			uploader := newUploader(t, spool, fake, service.Options{Faults: faults}, nil)
			passUntil(t, uploader, func() bool { return gone(closed[0]) })
			if err := uploader.Close(); err != nil {
				t.Fatal(err)
			}
			if state := journalStates(t, spool)[filepath.Base(closed[0])]; state != journal.StateDone {
				t.Fatalf("state = %q after %d held failures and one segment fault, want done", state, faults.SegmentScopeFailures)
			}
		})
	}
}

// A job failure that names no scope, reported while an earlier fault holds
// the whole backend, says nothing about the segment either, so it does not
// count toward the segment's quarantine threshold. Without a hold it counts.
func TestAnUnscopedJobFailureCountsOnlyWhenTheBackendIsNotHeld(t *testing.T) {
	cases := []struct {
		name       string
		second     backendtest.SubmitStep
		wantFaults int
	}{
		{"during a hold", backendtest.SubmitStep{Outcome: backendtest.Refuse, Err: &backend.Fault{Class: backend.ClassRetryable, Scope: backend.ScopeBackend, Reason: backend.ReasonUnavailable}}, 0},
		{"without a hold", backendtest.SubmitStep{}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spool, closed := spoolWith(t,
				[]string{uploadertest.RequestEnd("a", 1)},
				[]string{uploadertest.RequestEnd("b", 2)},
			)
			fake := backendtest.NewFake()
			fake.Script(
				backendtest.SubmitStep{Statuses: []backendtest.StatusStep{{
					Status: backend.StatusFailed,
					Err:    &backend.Fault{Class: backend.ClassRetryable, Scope: backend.ScopeUnknown, Reason: backend.ReasonUnavailable},
				}}},
				tc.second,
			)
			faults := fastFaults()
			// A hold lasts past the poll that reads the failed job.
			faults.Transient = retry.Backoff{Base: time.Minute, Max: time.Minute}
			uploader := newUploader(t, spool, fake, service.Options{Faults: faults}, map[string]string{config.EnvBatchMaxSegments: "1"})
			_ = uploader.Pass(context.Background())
			if err := uploader.Close(); err != nil {
				t.Fatal(err)
			}

			if got := fake.SubmitCalls(); got != 2 {
				t.Fatalf("submits = %d, want one per segment", got)
			}
			segments := journalSegments(t, spool)
			i := slices.IndexFunc(segments, func(seg journal.Segment) bool { return seg.Name == filepath.Base(closed[0]) })
			if i < 0 {
				t.Fatal("segment a is missing from the journal")
			}
			if got := segments[i].Faults; got != tc.wantFaults {
				t.Fatalf("segment a faults = %d, want %d", got, tc.wantFaults)
			}
		})
	}
}

// The segment failure count lives in the journal, so a restart does not give
// a failing segment a fresh threshold.
func TestSegmentFailuresCountAcrossARestart(t *testing.T) {
	spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
	fake := backendtest.NewFake()
	faults := fastFaults()
	segmentFault := backendtest.SubmitStep{Outcome: backendtest.Refuse, Err: &backend.Fault{Class: backend.ClassRetryable, Scope: backend.ScopeSegment, Reason: backend.ReasonRejected}}
	for range faults.SegmentScopeFailures {
		fake.Script(segmentFault)
	}
	first := newUploader(t, spool, fake, service.Options{Faults: faults}, nil)
	passUntil(t, first, func() bool { return fake.SubmitCalls() == 3 })
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := newUploader(t, spool, fake, service.Options{Faults: faults}, nil)
	passUntil(t, second, func() bool { return gone(closed[0]) })
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if state := journalStates(t, spool)[filepath.Base(closed[0])]; state != journal.StateQuarantined || fake.SubmitCalls() != faults.SegmentScopeFailures {
		t.Fatalf("state = %q after %d submits, want quarantined at the %dth segment fault across the restart", state, fake.SubmitCalls(), faults.SegmentScopeFailures)
	}
}

// A segment's failure count lives on its own journal entry, so it carries
// over when its batch is released and sent again in a new batch, and across
// restarts: a segment whose jobs keep failing reaches the threshold even when
// the uploader restarts after every failure.
func TestFailedJobsCountAcrossNewBatchesAndRestarts(t *testing.T) {
	spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
	fake := backendtest.NewFake()
	faults := fastFaults()
	failing := backendtest.SubmitStep{Statuses: []backendtest.StatusStep{{
		Status: backend.StatusFailed,
		Err:    &backend.Fault{Class: backend.ClassRetryable, Scope: backend.ScopeSegment, Reason: backend.ReasonRejected},
	}}}
	for range faults.SegmentScopeFailures + 2 {
		fake.Script(failing)
	}
	var state journal.State
	for restart := 0; restart <= faults.SegmentScopeFailures+1 && state != journal.StateQuarantined; restart++ {
		uploader := newUploader(t, spool, fake, service.Options{Faults: faults}, nil)
		_ = uploader.Pass(context.Background())
		if err := uploader.Close(); err != nil {
			t.Fatal(err)
		}
		state = journalStates(t, spool)[filepath.Base(closed[0])]
	}
	if state != journal.StateQuarantined || fake.SubmitCalls() != faults.SegmentScopeFailures {
		t.Fatalf("state = %q after %d failed jobs with a restart after each, want quarantined at the %dth", state, fake.SubmitCalls(), faults.SegmentScopeFailures)
	}
}

// A record over the configured size is not parsed: its raw bytes go to the
// local reject file and it is counted, and the rest of the segment is still
// exported.
func TestARecordOverTheSizeLimitIsRejectedUnparsed(t *testing.T) {
	big := `{"event_type":"request_end","pad":"` + strings.Repeat("a", 3<<19) + `"}`
	spool, _ := spoolWith(t, []string{big, uploadertest.RequestEnd("b", 1)})
	exporter := newCapture(t, backendtest.NewFake())
	metrics := newMetrics(t)
	uploader := newUploader(t, spool, exporter, service.Options{Metrics: metrics}, map[string]string{config.EnvRecordMaxBytes: "1048576"})

	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatalf("Pass() error = %v", err)
	}
	if got, _ := metrics.Value(telemetry.RecordsUnreadable, telemetry.Labels{"reason": "line_too_long"}); got != 1 {
		t.Errorf("records_unreadable{line_too_long} = %v, want 1", got)
	}
	if got := len(exporter.exported()); got != 1 {
		t.Errorf("exported %d records, want only the record under the limit", got)
	}
	artifacts, err := filepath.Glob(filepath.Join(spool.Dir, "request-trace-uploader-quarantine", "rejects", "*.jsonl.gz"))
	if err != nil || len(artifacts) != 1 {
		t.Fatalf("reject artifacts = %v, %v; want one", artifacts, err)
	}
	file, err := os.Open(artifacts[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	reader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	kept, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(kept) != big+"\n" {
		t.Errorf("reject artifact holds %d bytes, want the %d-byte record unchanged", len(kept), len(big)+1)
	}
}
