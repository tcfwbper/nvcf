// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service_test

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/retry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/journal"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/service"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

const (
	envCrashSpool   = "RTU_TEST_CRASH_SPOOL"
	envCrashLanding = "RTU_TEST_CRASH_LANDING"
)

// dirBackend lands each job in a directory, so jobs outlive the process
// that created them, as they do on a real destination. A job's files land
// before Submit returns, the way an upload commits before its response
// reaches the caller. batchFiles bounds the files per job; zero means 16.
type dirBackend struct {
	root       string
	batchFiles int
}

func (d dirBackend) Submit(_ context.Context, request backend.SubmitRequest) (string, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	id := hex.EncodeToString(raw[:])
	dir := filepath.Join(d.root, id)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	for i, file := range request.Transports() {
		if err := copyFile(file.Path, filepath.Join(dir, strings.Repeat("f", i+1)+".jsonl.gz")); err != nil {
			return "", err
		}
	}
	return id, os.WriteFile(filepath.Join(dir, "landed"), nil, 0o600)
}

func (d dirBackend) Status(_ context.Context, id string) (backend.Status, error) {
	if _, err := os.Stat(filepath.Join(d.root, id, "landed")); err == nil {
		return backend.StatusSucceeded, nil
	}
	return backend.StatusNotFound, nil
}

func (d dirBackend) Capabilities() backend.Capabilities {
	return backend.Capabilities{
		AcceptedFormats: []backend.Format{backend.FormatGzipJSONL},
		Exports:         true,
		MaxBatchFiles:   cmp.Or(d.batchFiles, 16),
	}
}

// dryRunDirBackend is dirBackend reporting that it does not export, as a dry
// run does.
type dryRunDirBackend struct{ dirBackend }

func (d dryRunDirBackend) Capabilities() backend.Capabilities {
	caps := d.dirBackend.Capabilities()
	caps.Exports = false
	return caps
}

func copyFile(from, to string) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(to)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		return errors.Join(err, out.Close())
	}
	return out.Close()
}

// landedRequests counts, per request id, how often a record landed.
func landedRequests(t *testing.T, root string) map[string]int {
	t.Helper()
	counts := map[string]int{}
	files, err := filepath.Glob(filepath.Join(root, "*", "*.jsonl.gz"))
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		for _, line := range readGzipLines(t, file) {
			var record struct {
				Request struct {
					RequestID string `json:"request_id"`
				} `json:"request"`
			}
			if err := json.Unmarshal([]byte(line), &record); err != nil {
				t.Fatalf("landed line %q: %v", line, err)
			}
			counts[record.Request.RequestID]++
		}
	}
	return counts
}

// A crash right after a run that exports reopened a segment a dry run had
// finished loses nothing: the restart prepares and exports it once.
func TestACrashAfterReopeningADryRunSegmentLosesNothing(t *testing.T) {
	if uploadertest.CrashChild() {
		t.Skip("runs only in the parent")
	}
	landing := t.TempDir()
	spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
	dry := newUploader(t, spool, dryRunDirBackend{dirBackend{root: t.TempDir()}}, service.Options{}, nil)
	for range 2 {
		if err := dry.Pass(context.Background()); err != nil {
			t.Fatalf("dry-run Pass() error = %v", err)
		}
	}
	if err := dry.Close(); err != nil {
		t.Fatal(err)
	}
	if state := journalStates(t, spool)[filepath.Base(closed[0])]; state != journal.StateDone || gone(closed[0]) {
		t.Fatalf("after the dry run: state %q, source gone %v; want done and kept", state, gone(closed[0]))
	}

	result := uploadertest.RunCrash(t, "TestCrashChildRunsThePipeline", service.PointReopened,
		envCrashSpool+"="+spool.Dir, envCrashLanding+"="+landing)
	if !result.Killed || result.Point != service.PointReopened {
		t.Fatalf("child was not killed at %s:\n%s", service.PointReopened, result.Output)
	}

	restarted := newUploader(t, spool, dirBackend{root: landing}, service.Options{}, nil)
	for range 2 {
		if err := restarted.Pass(context.Background()); err != nil {
			t.Fatalf("Pass() after restart error = %v", err)
		}
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
	if n := landedRequests(t, landing)["a"]; n != 1 {
		t.Errorf("request a landed %d times, want once", n)
	}
	if !gone(closed[0]) {
		t.Error("source kept after its upload")
	}
	if state := journalStates(t, spool)[filepath.Base(closed[0])]; state != journal.StateDone {
		t.Errorf("journal state = %q, want done", state)
	}
}

// TestCrashChildRunsThePipeline is the child of the crash tests: it runs one
// pass and is killed at the point the parent named.
func TestCrashChildRunsThePipeline(t *testing.T) {
	if !uploadertest.CrashChild() {
		t.Skip("runs only as a crash child")
	}
	spool := &uploadertest.Spool{Dir: os.Getenv(envCrashSpool), Prefix: config.DefaultSegmentPrefix}
	uploader := newUploader(t, spool, dirBackend{root: os.Getenv(envCrashLanding)}, service.Options{Point: uploadertest.CrashPoint}, nil)
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatalf("Pass() error = %v", err)
	}
}

// TestCrashChildEvictsAboveTheHighWatermark is the child of the eviction
// crash test: the spool reads as 90% full, so its pass evicts the oldest
// closed segment. It is killed at the point the parent named.
func TestCrashChildEvictsAboveTheHighWatermark(t *testing.T) {
	if !uploadertest.CrashChild() {
		t.Skip("runs only as a crash child")
	}
	spool := &uploadertest.Spool{Dir: os.Getenv(envCrashSpool), Prefix: config.DefaultSegmentPrefix}
	volume := &segmentVolume{dir: spool.Dir}
	volume.total.Store(volume.segmentBytes() * 10 / 9)
	uploader := newUploader(t, spool, dirBackend{root: os.Getenv(envCrashLanding)}, service.Options{Point: uploadertest.CrashPoint, Volume: volume}, nil)
	_ = uploader.Pass(context.Background())
}

// transportFiles lists the files in the journal's transport directory.
func transportFiles(t *testing.T, spool *uploadertest.Spool) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(spool.Dir, "request-trace-uploader-state", "transport"))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// A crash after an eviction is recorded and before its files are deleted
// leaves nothing behind: the restart removes the segment's transports, and
// the evicted records are never sent.
func TestACrashAfterAnEvictionIsRecordedLeavesNoTransports(t *testing.T) {
	if uploadertest.CrashChild() {
		t.Skip("runs only in the parent")
	}
	landing := t.TempDir()
	spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
	env := []string{envCrashSpool + "=" + spool.Dir, envCrashLanding + "=" + landing}
	// The first child prepares the segment and is killed before it batches it.
	result := uploadertest.RunCrash(t, "TestCrashChildRunsThePipeline", service.PointPrepared, env...)
	if !result.Killed || result.Point != service.PointPrepared {
		t.Fatalf("child was not killed at %s:\n%s", service.PointPrepared, result.Output)
	}
	if len(transportFiles(t, spool)) == 0 {
		t.Fatal("no transport after the segment was prepared")
	}
	// The second finds the spool above the high watermark and is killed
	// after it recorded the eviction.
	result = uploadertest.RunCrash(t, "TestCrashChildEvictsAboveTheHighWatermark", service.PointEvicted, env...)
	if !result.Killed || result.Point != service.PointEvicted {
		t.Fatalf("child was not killed at %s:\n%s", service.PointEvicted, result.Output)
	}

	restarted := newUploader(t, spool, dirBackend{root: landing}, service.Options{Volume: roomyVolume(spool.Dir)}, nil)
	if err := restarted.Pass(context.Background()); err != nil {
		t.Fatalf("Pass() after restart error = %v", err)
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
	if left := transportFiles(t, spool); len(left) != 0 {
		t.Errorf("transports left after the restart: %d", len(left))
	}
	if n := landedRequests(t, landing)["a"]; n != 0 {
		t.Errorf("evicted request a landed %d times", n)
	}
	if state := journalStates(t, spool)[filepath.Base(closed[0])]; state != journal.StateDone {
		t.Errorf("journal state = %q, want done", state)
	}
}

// crashPoint is a named point a crash test stops the child at.
type crashPoint struct {
	name string
	// mayRepeat reports that a crash at this point can land the same
	// records twice: the job committed but its id was never saved, so
	// the restart cannot tell and submits again (accepted, counted).
	mayRepeat bool
}

var pipelinePoints = []crashPoint{
	{name: service.PointPrepared},
	{name: service.PointBatchRecorded},
	{name: service.PointSubmitted, mayRepeat: true},
	{name: service.PointReceiptSaved},
	{name: service.PointConfirmed},
	{name: service.PointCleaning},
	{name: service.PointSourceDeleted},
}

func TestACrashAtAnyPointLosesAndDoubleDeletesNothing(t *testing.T) {
	if uploadertest.CrashChild() {
		t.Skip("runs only in the parent")
	}
	crashAtEveryPoint(t, "TestCrashChildRunsThePipeline", pipelinePoints, nil,
		[]string{uploadertest.RequestEnd("a", 1), uploadertest.RequestEnd("b", 2)},
		[]string{uploadertest.RequestEnd("c", 3)},
	)
}

// TestCrashChildRunsThePipelineInParallel is the crash child for an
// uploader that prepares two segments at once and sends jobs of two, so a
// full batch is submitted while later segments are still being prepared.
func TestCrashChildRunsThePipelineInParallel(t *testing.T) {
	if !uploadertest.CrashChild() {
		t.Skip("runs only as a crash child")
	}
	spool := &uploadertest.Spool{Dir: os.Getenv(envCrashSpool), Prefix: config.DefaultSegmentPrefix}
	client := dirBackend{root: os.Getenv(envCrashLanding), batchFiles: 2}
	uploader := newUploader(t, spool, client, service.Options{Point: uploadertest.CrashPoint}, workersEnv("2"))
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatalf("Pass() error = %v", err)
	}
}

// With two workers a crash can leave several segments mid-prepare, some
// with a finished transport the journal never recorded, or stop a submit
// that ran beside intake. A restart prepares them again from their sources
// and resumes the batch from the journal.
func TestACrashWhilePreparingInParallelLosesAndDoubleDeletesNothing(t *testing.T) {
	if uploadertest.CrashChild() {
		t.Skip("runs only in the parent")
	}
	points := append([]crashPoint{{name: service.PointPreparingAlongside}}, pipelinePoints...)
	crashAtEveryPoint(t, "TestCrashChildRunsThePipelineInParallel", points, workersEnv("2"),
		[]string{uploadertest.RequestEnd("a", 1), uploadertest.RequestEnd("b", 2)},
		[]string{uploadertest.RequestEnd("c", 3)},
		[]string{uploadertest.RequestEnd("d", 4)},
		[]string{uploadertest.RequestEnd("e", 5)},
	)
}

// crashAtEveryPoint kills child at each point, restarts an uploader with env
// on the same spool, and checks that every request landed once (or more,
// where the point allows it), that every closed source was deleted, and
// that the segment the writer still owns was left alone.
func crashAtEveryPoint(t *testing.T, child string, points []crashPoint, env map[string]string, segments ...[]string) {
	t.Helper()
	requests := []string{"active"}
	for _, lines := range segments {
		for _, line := range lines {
			requests = append(requests, requestIDOf(t, line))
		}
	}
	for _, point := range points {
		t.Run(point.name, func(t *testing.T) {
			landing := t.TempDir()
			spool, closed := spoolWith(t, segments...)
			result := uploadertest.RunCrash(t, child, point.name,
				envCrashSpool+"="+spool.Dir, envCrashLanding+"="+landing)
			if !result.Killed || result.Point != point.name {
				t.Fatalf("child was not killed at %s:\n%s", point.name, result.Output)
			}

			// The writer moved on while the uploader was down: the old
			// active segment is closed now and must be uploaded too.
			oldActive := filepath.Join(spool.Dir, fmt.Sprintf("%s.%06d.jsonl.gz", config.DefaultSegmentPrefix, len(segments)))
			newActive := spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("still-open", 9)).Bytes())

			restarted := newUploader(t, spool, dirBackend{root: landing}, service.Options{}, env)
			for range 2 {
				if err := restarted.Pass(context.Background()); err != nil {
					t.Fatalf("Pass() after restart error = %v", err)
				}
			}
			if err := restarted.Close(); err != nil {
				t.Fatal(err)
			}

			landed := landedRequests(t, landing)
			for _, id := range requests {
				switch n := landed[id]; {
				case n == 0:
					t.Errorf("request %s was lost", id)
				case n > 1 && !point.mayRepeat:
					t.Errorf("request %s landed %d times", id, n)
				}
			}
			if landed["still-open"] != 0 {
				t.Error("the segment the writer still owns was uploaded")
			}
			for _, path := range append(closed, oldActive) {
				if !gone(path) {
					t.Errorf("source %s kept after its upload", filepath.Base(path))
				}
			}
			if gone(newActive) {
				t.Error("the segment the writer still owns was deleted")
			}
			states := journalStates(t, spool)
			for _, path := range append(closed, oldActive) {
				if state := states[filepath.Base(path)]; state != journal.StateDone {
					t.Errorf("journal state of %s = %q, want done", filepath.Base(path), state)
				}
			}
		})
	}
}

func requestIDOf(t *testing.T, line string) string {
	t.Helper()
	var r struct {
		Request struct {
			RequestID string `json:"request_id"`
		} `json:"request"`
	}
	if err := json.Unmarshal([]byte(line), &r); err != nil || r.Request.RequestID == "" {
		t.Fatalf("no request id in %s", line)
	}
	return r.Request.RequestID
}

func retainFaults() *retry.Config {
	faults := fastFaults()
	faults.AmbiguousWithoutID = retry.AmbiguousRetain
	return faults
}

// TestCrashChildRunsThePipelineRetaining is the crash child for an uploader
// configured to retain ambiguous submits.
func TestCrashChildRunsThePipelineRetaining(t *testing.T) {
	if !uploadertest.CrashChild() {
		t.Skip("runs only as a crash child")
	}
	spool := &uploadertest.Spool{Dir: os.Getenv(envCrashSpool), Prefix: config.DefaultSegmentPrefix}
	uploader := newUploader(t, spool, dirBackend{root: os.Getenv(envCrashLanding)}, service.Options{Point: uploadertest.CrashPoint, Faults: retainFaults()}, nil)
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatalf("Pass() error = %v", err)
	}
}

func TestACrashAfterSubmitIsNotResubmittedWhenAmbiguousSubmitsAreRetained(t *testing.T) {
	if uploadertest.CrashChild() {
		t.Skip("runs only in the parent")
	}
	landing := t.TempDir()
	spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
	result := uploadertest.RunCrash(t, "TestCrashChildRunsThePipelineRetaining", service.PointSubmitted,
		envCrashSpool+"="+spool.Dir, envCrashLanding+"="+landing)
	if !result.Killed || result.Point != service.PointSubmitted {
		t.Fatalf("child was not killed at %s:\n%s", service.PointSubmitted, result.Output)
	}

	metrics, err := telemetry.NewMemory("fake")
	if err != nil {
		t.Fatal(err)
	}
	restarted := newUploader(t, spool, dirBackend{root: landing}, service.Options{Faults: retainFaults(), Metrics: metrics}, nil)
	for range 3 {
		_ = restarted.Pass(context.Background())
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
	if n := landedRequests(t, landing)["a"]; n != 1 {
		t.Fatalf("request a landed %d times, want the retained batch not submitted again", n)
	}
	if gone(closed[0]) {
		t.Fatal("source deleted, want it kept for an operator")
	}
	if got, _ := metrics.Value(telemetry.SubmitsAmbiguous, nil); got != 1 {
		t.Errorf("submits_ambiguous = %v, want the retained batch counted once", got)
	}
	for _, b := range journalBatches(t, spool) {
		if b.NextRetryAt.Before(time.Now().Add(365 * 24 * time.Hour)) {
			t.Fatalf("retained batch state %s retries at %v, want it parked for an operator", b.State, b.NextRetryAt)
		}
	}
}
