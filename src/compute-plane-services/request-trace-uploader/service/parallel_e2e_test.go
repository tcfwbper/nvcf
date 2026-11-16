// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend/backendtest"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/format"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/journalfs"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/journal"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/record"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/service"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

// hookedFormat is a backend that writes gzip JSON lines through a writer
// that calls onWrite before each record, so a test can hold a prepare in the
// middle of a segment.
type hookedFormat struct {
	*backendtest.Fake
	onWrite func(target format.Target, r transform.Record) error
}

func (h hookedFormat) Format() backend.Format { return backend.FormatGzipJSONL }

func (h hookedFormat) NewWriter(target format.Target) (format.Writer, error) {
	w, err := format.GzipJSONL{}.NewWriter(target)
	if err != nil {
		return nil, err
	}
	return hookedWriter{Writer: w, target: target, onWrite: h.onWrite}, nil
}

type hookedWriter struct {
	format.Writer
	target  format.Target
	onWrite func(format.Target, transform.Record) error
}

func (w hookedWriter) Write(r transform.Record) error {
	if err := w.onWrite(w.target, r); err != nil {
		return err
	}
	return w.Writer.Write(r)
}

func workersEnv(n string) map[string]string {
	return map[string]string{config.EnvPrepareWorkers: n}
}

// The first record of each segment waits until the other segment's first
// record arrives: only two prepares running at once get past it.
func TestTwoWorkersPrepareTwoSegmentsAtOnce(t *testing.T) {
	spool, closed := spoolWith(t,
		[]string{uploadertest.RequestEnd("a", 1)},
		[]string{uploadertest.RequestEnd("b", 2)},
	)
	var arrived sync.WaitGroup
	arrived.Add(2)
	both := make(chan struct{})
	go func() { arrived.Wait(); close(both) }()
	var once sync.Map
	client := hookedFormat{Fake: batchingFake(), onWrite: func(target format.Target, _ transform.Record) error {
		if _, seen := once.LoadOrStore(target.Segment.Index, true); seen {
			return nil
		}
		arrived.Done()
		select {
		case <-both:
			return nil
		case <-time.After(5 * time.Second):
			return errors.New("the other segment was not prepared at the same time")
		}
	}}
	uploader := newUploader(t, spool, client, service.Options{}, workersEnv("2"))
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatalf("Pass() error = %v", err)
	}
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range closed {
		if !gone(path) {
			t.Errorf("%s was not exported and cleaned up", filepath.Base(path))
		}
	}
}

// preparedOrder lists segment indexes by when the journal recorded them
// prepared.
func preparedOrder(t *testing.T, spool *uploadertest.Spool) []journal.Segment {
	t.Helper()
	store, err := journalfs.Open(context.Background(), journalfs.Config{
		SpoolDir: spool.Dir,
		StateDir: filepath.Join(spool.Dir, "request-trace-uploader-state"),
		Holder:   "inspector",
		LockWait: time.Second,
	}, journalfs.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	return store.Segments()
}

// The first segment is the slowest to prepare, yet the journal records the
// segments prepared in their order, as one worker would.
func TestWorkersRecordPreparedSegmentsInSegmentOrder(t *testing.T) {
	var segments [][]string
	for i := range 6 {
		segments = append(segments, []string{uploadertest.RequestEnd(strings.Repeat("r", i+1), uint64(i))})
	}
	spool, _ := spoolWith(t, segments...)
	client := hookedFormat{Fake: batchingFake(), onWrite: func(target format.Target, _ transform.Record) error {
		if target.Segment.Index == 0 {
			time.Sleep(300 * time.Millisecond)
		}
		return nil
	}}
	client.Script(backendtest.SubmitStep{Outcome: backendtest.Refuse, Err: errors.New("hold everything prepared")})
	uploader := newUploader(t, spool, client, service.Options{}, workersEnv("4"))
	_ = uploader.Pass(context.Background())
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}
	var last time.Time
	for _, seg := range preparedOrder(t, spool) {
		if seg.Index >= 6 {
			continue
		}
		if seg.PreparedAt.IsZero() {
			t.Fatalf("segment %d was not prepared (state %s)", seg.Index, seg.State)
		}
		if seg.PreparedAt.Before(last) {
			t.Fatalf("segment %d was recorded prepared before an earlier segment", seg.Index)
		}
		last = seg.PreparedAt
	}
}

// With several workers a segment is prepared on a worker and recorded later
// by intake. Its prepare span lasts until that record, so each segment has
// one span and it carries the outcome.
func TestEachParallelPrepareSpanEndsWithItsOutcome(t *testing.T) {
	var segments [][]string
	for i := range 4 {
		segments = append(segments, []string{uploadertest.RequestEnd(strings.Repeat("s", i+1), uint64(i))})
	}
	spool, _ := spoolWith(t, segments...)
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	uploader := newUploader(t, spool, backendtest.NewFake(), service.Options{Tracer: provider.Tracer("test")}, workersEnv("4"))
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	spans := spansNamed(recorder, "request-trace-uploader.prepare")
	if len(spans) != len(segments) {
		t.Fatalf("prepare spans = %d, want one per segment (%d)", len(spans), len(segments))
	}
	for _, span := range spans {
		if status := spanStatus(span); status != "prepared" {
			t.Errorf("prepare span status = %q, want prepared", status)
		}
	}
}

func spanStatus(span sdktrace.ReadOnlySpan) string {
	for _, kv := range span.Attributes() {
		if kv.Key == "status" {
			return kv.Value.AsString()
		}
	}
	return ""
}

// boundWait holds the first submit, so batches stay unsent while intake
// reaches the prepare-ahead bound. Segment 23 waits for segment 24 to
// start, so the bound is reached at segment 25, and segment 24's prepare
// is held at its first record, so it is running when intake first checks
// the bound.
type boundWait struct {
	*backendtest.Fake
	started, held, release, finished, submitted chan struct{}
	holdOnce, submitOnce                        sync.Once
}

func (w *boundWait) Format() backend.Format { return backend.FormatGzipJSONL }

func (w *boundWait) NewWriter(target format.Target) (format.Writer, error) {
	out, err := format.GzipJSONL{}.NewWriter(target)
	if err != nil {
		return out, err
	}
	switch target.Segment.Index {
	case 23:
		return hookedWriter{Writer: out, target: target, onWrite: func(format.Target, transform.Record) error {
			awaitGate(w.started)
			return nil
		}}, nil
	case 24:
		close(w.started)
		return &boundWaitWriter{Writer: out, w: w}, nil
	}
	return out, nil
}

type boundWaitWriter struct {
	format.Writer
	w *boundWait
}

func (b *boundWaitWriter) Write(r transform.Record) error {
	b.w.holdOnce.Do(func() {
		close(b.w.held)
		awaitGate(b.w.release)
	})
	return b.Writer.Write(r)
}

func (b *boundWaitWriter) Close() error {
	defer close(b.w.finished)
	return b.Writer.Close()
}

func (w *boundWait) Submit(ctx context.Context, request backend.SubmitRequest) (string, error) {
	w.submitOnce.Do(func() { awaitGate(w.submitted) })
	return w.Fake.Submit(ctx, request)
}

func awaitGate(ch <-chan struct{}) {
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
	}
}

// Intake reaches the prepare-ahead bound with segment 24 still being
// prepared. While it waits for the batches to be sent, segment 24 finishes
// and the volume is left with room for one segment but not two. Free space
// is then checked for segment 25 without segment 24, so the pass takes it.
func TestFreeSpaceIsCheckedAgainWithoutPreparesThatFinishedDuringTheBoundWait(t *testing.T) {
	var segments [][]string
	for i := range 30 {
		segments = append(segments, []string{uploadertest.RequestEnd(fmt.Sprintf("bound-%03d", i), uint64(i))})
	}
	spool, closed := spoolWith(t, segments...)
	var smallest, largest int64
	for _, path := range closed {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if smallest == 0 || info.Size() < smallest {
			smallest = info.Size()
		}
		largest = max(largest, info.Size())
	}
	// Room for twice the largest segment, as preparing one needs, but not
	// for twice two segments.
	room := 3 * largest
	if 4*smallest <= room {
		t.Fatalf("segments from %d to %d bytes are too uneven for this test", smallest, largest)
	}
	fake := backendtest.NewFake()
	fake.SetCapabilities(backend.Capabilities{
		AcceptedFormats: []backend.Format{backend.FormatGzipJSONL},
		Exports:         true,
		MaxBatchFiles:   16,
	})
	client := &boundWait{Fake: fake, started: make(chan struct{}), held: make(chan struct{}),
		release: make(chan struct{}), finished: make(chan struct{}), submitted: make(chan struct{})}
	volume := roomyVolume(spool.Dir)
	var waited sync.Once
	point := func(name string) {
		if name != service.PointPrepareAheadWait {
			return
		}
		waited.Do(func() {
			awaitGate(client.held)
			// A quarter free keeps the volume under the high watermark.
			volume.total.Store(uint64(4 * room))
			volume.free.Store(room)
			close(client.release)
			awaitGate(client.finished)
			close(client.submitted)
		})
	}
	env := workersEnv("2")
	env[config.EnvBatchMaxSegments] = "8"
	env[config.EnvSpoolMinFreeBytes] = "0"
	uploader := newUploader(t, spool, client, service.Options{Volume: volume, Point: point}, env)
	_ = uploader.Pass(context.Background())
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}
	if state, ok := journalStates(t, spool)[filepath.Base(closed[25])]; !ok || state == journal.StateDiscovered {
		t.Fatalf("segment 25 is %q in the journal: free space was checked as if segment 24 were still being prepared", state)
	}
}

// heldFirstSubmit closes entered when its first submit starts and holds
// that submit until gate closes.
type heldFirstSubmit struct {
	*backendtest.Fake
	entered, gate chan struct{}
	first         sync.Once
}

func (h *heldFirstSubmit) Submit(ctx context.Context, request backend.SubmitRequest) (string, error) {
	h.first.Do(func() {
		close(h.entered)
		awaitGate(h.gate)
	})
	return h.Fake.Submit(ctx, request)
}

// Intake finds the prepare-ahead bound reached, and before it looks at the
// submitter again, the submitter records the next full batch and finishes.
// The batch freed room, so intake checks again and takes the next segment
// in the same pass instead of ending it on its earlier answer.
func TestABatchRecordedJustAfterIntakeReachesTheBoundLetsIntakeGoOn(t *testing.T) {
	var segments [][]string
	for i := range 34 {
		segments = append(segments, []string{uploadertest.RequestEnd(fmt.Sprintf("settle-%03d", i), uint64(i))})
	}
	spool, closed := spoolWith(t, segments...)
	fake := backendtest.NewFake()
	fake.SetCapabilities(backend.Capabilities{
		AcceptedFormats: []backend.Format{backend.FormatGzipJSONL},
		Exports:         true,
		MaxBatchFiles:   16,
	})
	client := &heldFirstSubmit{Fake: fake, entered: make(chan struct{}), gate: make(chan struct{})}
	var prepared atomic.Int64
	// Each round's end reports how many submits had been made by then.
	rounds := make(chan int, 256)
	var once sync.Once
	point := func(name string) {
		switch name {
		case service.PointPrepared:
			prepared.Add(1)
		case service.PointBacklogRoundEnded:
			select {
			case rounds <- fake.SubmitCalls():
			default:
			}
		case service.PointPrepareAheadWait:
			once.Do(func() {
				// Intake may get here before or after the first full batch
				// is recorded; either way that batch's submit starts.
				awaitGate(client.entered)
				// Intake prepares nothing while it is held here. Once the
				// round that sends the last full batch ends, at most one
				// round for a wake already pending follows it; return when
				// no round has ended for a while after that.
				full := int(prepared.Load()) / 16
				close(client.gate)
				deadline := time.After(5 * time.Second)
				sent := false
				for {
					var quiet <-chan time.Time
					if sent {
						quiet = time.After(300 * time.Millisecond)
					}
					select {
					case calls := <-rounds:
						sent = sent || calls >= full
					case <-quiet:
						return
					case <-deadline:
						t.Errorf("the submitter did not send the %d full batches", full)
						return
					}
				}
			})
		}
	}
	uploader := newUploader(t, spool, client, service.Options{Point: point}, nil)
	_ = uploader.Pass(context.Background())
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}
	if state, ok := journalStates(t, spool)[filepath.Base(closed[32])]; !ok || state == journal.StateDiscovered {
		t.Fatalf("segment 32 is %q in the journal: intake ended on a bound the recorded batch had freed", state)
	}
}

// A segment's primary is in a submit that may have landed and its secondary
// was never sent when the record policy changes, so its secondary waits for
// the segment to be prepared again under the new policy. A backlog after
// the restart fills secondary batches beside intake; none of them takes the
// old secondary, so nothing goes out under the old policy.
func TestABacklogBatchNeverSendsADestinationPreparedUnderTheOldPolicy(t *testing.T) {
	spool, closed := spoolWith(t, []string{suppressedLine})
	first := newUploader(t, spool, lostPrimaryBackend{newDualBackend(false, "")}, service.Options{}, nil)
	_ = first.Pass(context.Background())
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd(fmt.Sprintf("backlog-%d", i), uint64(i))).Bytes())
	}

	landing := t.TempDir()
	client := newDualBackend(false, landing)
	caps := client.Capabilities()
	caps.MaxBatchFiles = 2
	client.SetCapabilities(caps)
	var rounds atomic.Int32
	point := func(name string) {
		if name == service.PointBacklogRoundEnded {
			rounds.Add(1)
		}
	}
	restarted := newUploader(t, spool, client, service.Options{Point: point}, map[string]string{config.EnvSuppressNCAIDs: "nca-acme-nca", config.EnvSuppressNCAHeaders: ncaHeaders})
	passUntil(t, restarted, func() bool { return gone(closed[0]) })
	if rounds.Load() == 0 {
		t.Fatal("no full-batch round ran beside intake")
	}

	secondary, err := filepath.Glob(filepath.Join(landing, "secondary-*.jsonl.gz"))
	if err != nil || len(secondary) == 0 {
		t.Fatalf("secondary files sent = %v, %v; want the segments sent", secondary, err)
	}
	for _, file := range secondary {
		if joined := strings.Join(readGzipLines(t, file), "\n"); strings.Contains(joined, "secret") {
			t.Fatalf("secondary sent %s under the old policy", joined)
		}
	}
}

// A Config built by hand takes zero as the default for each prepare
// setting, but a negative one is a mistake and stops construction.
func TestNewUploaderRefusesANegativePrepareSetting(t *testing.T) {
	for name, prepare := range map[string]config.PreparePolicy{
		"workers":      {Workers: -1},
		"memory bytes": {MemoryBytes: -1},
	} {
		t.Run(name, func(t *testing.T) {
			spool, _ := spoolWith(t)
			cfg := loadEnv(t, spoolEnv(t, spool.Dir, nil))
			cfg.Prepare = prepare
			if _, err := service.NewUploader(cfg, backendtest.NewFake(), service.Options{Faults: fastFaults()}); err == nil {
				t.Fatalf("NewUploader() with Prepare %+v error = nil, want it refused", prepare)
			}
		})
	}
}

// openGatedFormat holds segment 0's writer from opening until segment 1's
// writer opens, so the two segments are prepared at once.
type openGatedFormat struct {
	*backendtest.Fake
	second chan struct{}
	once   *sync.Once
}

func (f openGatedFormat) Format() backend.Format { return backend.FormatGzipJSONL }

func (f openGatedFormat) NewWriter(target format.Target) (format.Writer, error) {
	switch target.Segment.Index {
	case 0:
		select {
		case <-f.second:
		case <-time.After(5 * time.Second):
		}
	case 1:
		f.once.Do(func() { close(f.second) })
	}
	return format.GzipJSONL{}.NewWriter(target)
}

// When the quarantine breaker trips on one segment while a later segment
// is still being prepared, the later one is held to be prepared again, and
// its prepare span says so.
func TestAPrepareHeldByABreakerTripHasAHeldSpan(t *testing.T) {
	spool, _ := spoolWith(t, []string{"not json a"}, []string{"not json b"})
	faults := fastFaults()
	faults.QuarantineTrip = 1
	client := openGatedFormat{Fake: backendtest.NewFake(), second: make(chan struct{}), once: &sync.Once{}}
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	uploader := newUploader(t, spool, client, service.Options{Faults: faults, Tracer: provider.Tracer("test")}, workersEnv("4"))
	_ = uploader.Pass(context.Background())
	statuses := map[string]int{}
	for _, span := range spansNamed(recorder, "request-trace-uploader.prepare") {
		statuses[spanStatus(span)]++
	}
	if statuses["quarantined"] != 1 || statuses["held"] != 1 || len(statuses) != 2 {
		t.Fatalf("prepare span statuses = %v, want one quarantined and one held", statuses)
	}
}

// A line past the memory step held by one prepare leaves no room in a small
// budget for another such line, so the second prepare waits, and the wait
// is counted. The first prepare's long record waits in the writer until the
// wait is counted, so the two lines are held at once.
func TestAPrepareWaitingForLineMemoryIsCounted(t *testing.T) {
	long := func(id string) string {
		return `{"schema":"dynamo.request.trace.v1","event_type":"request_end","event_time_unix_ms":1,"request":{"request_id":"` +
			id + strings.Repeat("z", record.DefaultMemoryStep+(1<<20)) + `"}}`
	}
	spool, closed := spoolWith(t, []string{long("a")}, []string{long("b")})
	metrics, err := telemetry.NewMemory("fake")
	if err != nil {
		t.Fatal(err)
	}
	var first sync.Once
	client := hookedFormat{Fake: batchingFake(), onWrite: func(_ format.Target, r transform.Record) error {
		var err error
		first.Do(func() {
			deadline := time.Now().Add(10 * time.Second)
			for {
				if waits, _ := metrics.Value(telemetry.PrepareMemoryWaits, nil); waits >= 1 {
					return
				}
				if time.Now().After(deadline) {
					err = errors.New("the other prepare never waited for line memory")
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		})
		return err
	}}
	env := workersEnv("2")
	env[config.EnvPrepareMemoryBytes] = "67108864"
	uploader := newUploader(t, spool, client, service.Options{Metrics: metrics}, env)
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatalf("Pass() error = %v", err)
	}
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}
	if waits, _ := metrics.Value(telemetry.PrepareMemoryWaits, nil); waits != 1 {
		t.Fatalf("prepare_memory_waits = %v, want one wait", waits)
	}
	for _, path := range closed {
		if !gone(path) {
			t.Errorf("%s was not exported after the wait", filepath.Base(path))
		}
	}
}

func TestPrepareMemoryWaitsStartsAtZero(t *testing.T) {
	spool, _ := spoolWith(t)
	metrics, err := telemetry.NewMemory("fake")
	if err != nil {
		t.Fatal(err)
	}
	newUploader(t, spool, batchingFake(), service.Options{Metrics: metrics}, nil)
	if waits, ok := metrics.Value(telemetry.PrepareMemoryWaits, nil); !ok || waits != 0 {
		t.Fatalf("prepare_memory_waits = %v, %v; want 0 before any prepare", waits, ok)
	}
}

// watchedSubmits closes first when the backend receives its first submit.
type watchedSubmits struct {
	hookedFormat
	first chan struct{}
	once  sync.Once
}

func (w *watchedSubmits) Submit(ctx context.Context, request backend.SubmitRequest) (string, error) {
	w.once.Do(func() { close(w.first) })
	return w.hookedFormat.Submit(ctx, request)
}

// Batches hold two segments. The third segment's prepare waits until the
// backend has a submit, so it finishes only if the first full batch is sent
// while intake is still preparing.
func TestAFullBatchIsSubmittedWhilePreparingGoesOn(t *testing.T) {
	var segments [][]string
	for i := range 4 {
		segments = append(segments, []string{uploadertest.RequestEnd(strings.Repeat("s", i+1), uint64(i))})
	}
	spool, closed := spoolWith(t, segments...)
	fake := backendtest.NewFake()
	fake.SetCapabilities(backend.Capabilities{
		AcceptedFormats: []backend.Format{backend.FormatGzipJSONL},
		Exports:         true,
		MaxBatchFiles:   2,
	})
	client := &watchedSubmits{first: make(chan struct{})}
	client.hookedFormat = hookedFormat{Fake: fake, onWrite: func(target format.Target, _ transform.Record) error {
		if target.Segment.Index != 2 {
			return nil
		}
		select {
		case <-client.first:
			return nil
		case <-time.After(5 * time.Second):
			return errors.New("no batch was submitted while this segment was prepared")
		}
	}}
	uploader := newUploader(t, spool, client, service.Options{}, nil)
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatalf("Pass() error = %v", err)
	}
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range closed {
		if !gone(path) {
			t.Errorf("%s was not exported and cleaned up", filepath.Base(path))
		}
	}
	jobs := fake.Jobs()
	if len(jobs) != 2 {
		t.Fatalf("jobs = %d, want two full batches of two", len(jobs))
	}
	for _, job := range jobs {
		if len(job.Files) != 2 {
			t.Fatalf("job %s has %d files, want full batches only", job.ID, len(job.Files))
		}
	}
}

// Twenty segments and jobs of sixteen: the first job fills at the
// prepare-ahead bound. Its upload waits until the seventeenth segment is
// being prepared, so it finishes only if intake waits for the batch to be
// recorded and goes on, instead of ending the pass at the bound.
func TestABacklogKeepsPreparingPastTheBoundWhileAFullBatchUploads(t *testing.T) {
	var segments [][]string
	for i := range 20 {
		segments = append(segments, []string{uploadertest.RequestEnd(strings.Repeat("u", i+1), uint64(i))})
	}
	spool, closed := spoolWith(t, segments...)
	later := make(chan struct{})
	var once sync.Once
	fake := backendtest.NewFake()
	fake.SetCapabilities(backend.Capabilities{
		AcceptedFormats: []backend.Format{backend.FormatGzipJSONL},
		Exports:         true,
		MaxBatchFiles:   16,
	})
	client := &gatedSubmits{hookedFormat: hookedFormat{Fake: fake, onWrite: func(target format.Target, _ transform.Record) error {
		if target.Segment.Index >= 16 {
			once.Do(func() { close(later) })
		}
		return nil
	}}, gate: later}
	uploader := newUploader(t, spool, client, service.Options{}, workersEnv("2"))
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatalf("Pass() error = %v", err)
	}
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range closed {
		if !gone(path) {
			t.Errorf("%s was not exported in the pass", filepath.Base(path))
		}
	}
}

// gatedSubmits holds its first submit until gate closes.
type gatedSubmits struct {
	hookedFormat
	gate  chan struct{}
	first sync.Once
}

func (g *gatedSubmits) Submit(ctx context.Context, request backend.SubmitRequest) (string, error) {
	var err error
	g.first.Do(func() {
		select {
		case <-g.gate:
		case <-time.After(5 * time.Second):
			err = errors.New("no segment past the prepare-ahead bound was prepared during the upload")
		}
	})
	if err != nil {
		return "", err
	}
	return g.hookedFormat.Submit(ctx, request)
}
