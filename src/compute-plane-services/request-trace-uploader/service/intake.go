// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/format"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/journal"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/record"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/segment"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform/redact"
)

// fileIdentity tells one spool file from a later one that reuses its name.
type fileIdentity struct {
	name    string
	size    int64
	modTime time.Time
}

// intake reads every closed segment the journal does not know yet. Only one
// intake runs at a time; it shares nothing with polling but the journal. A
// scan stops early once draining starts, except the final one, which the
// drain itself runs. Up to the configured number of segments are prepared
// at once, and recorded in spool order; intake returns once every segment
// it started is recorded.
func (u *Uploader) intake(ctx context.Context, final bool) error {
	scan, err := segment.ScanDir(u.cfg.SourceDir, u.cfg.Prefixes())
	if err != nil {
		return fmt.Errorf("scan the spool: %w", err)
	}
	u.forgetGone(scan)
	pool := u.newPreparePool()
	var errs []error
	for _, item := range scan.Closed {
		// Record what finished, and wait while every worker is busy, so
		// quarantines count toward the breaker below and prepared segments
		// toward prepare-ahead. With one worker this records each segment
		// before the next is read.
		pool.recordReady()
		pool.makeRoom(pool.workers - 1)
		if err := ctx.Err(); err != nil {
			return errors.Join(append(append(pool.finish(), errs...), err)...)
		}
		if !final && u.draining.Load() {
			break
		}
		// Checked per segment, so a large bad backlog stops at the trip
		// instead of being quarantined wholesale.
		tripped := u.policy.Breaker().TrippedAt(u.now())
		u.countResumes()
		if tripped {
			slog.Warn("too many segments quarantined in a row; not reading new segments until the breaker resumes",
				"resume_seconds", int64(u.faults.QuarantineResume/time.Second))
			break
		}
		// An eviction the spool check asked for runs between segments,
		// once every running prepare is recorded.
		u.evictIfDue(pool)
		err := u.take(ctx, item, pool)
		if errors.Is(err, errPreparePaused) {
			break
		}
		if err != nil {
			slog.Error("prepare request trace segment", "segment", item.Index, "error", safeError(err))
			errs = append(errs, err)
		}
	}
	errs = append(pool.finish(), errs...)
	// The end of intake is a boundary too: with several workers, the last
	// segments may have been preparing when the eviction came due.
	u.evictIfDue(pool)
	return errors.Join(errs...)
}

func (u *Uploader) take(ctx context.Context, item segment.Segment, pool *preparePool) error {
	sum, err := u.fingerprint(ctx, item)
	if errors.Is(err, fs.ErrNotExist) {
		// Cleanup removed it after the scan listed it.
		return nil
	}
	if err != nil {
		return err
	}
	seg, _, err := u.store.Discover(journal.SegmentInfo{
		Name:    filepath.Base(item.Path),
		Prefix:  item.Prefix,
		Index:   item.Index,
		Size:    item.Size,
		ModTime: item.ModTime,
		SHA256:  sum,
	})
	if err != nil {
		return err
	}
	if seg.State == journal.StateDone && seg.Reason == journal.ReasonNotExported && u.caps.Exports {
		// A dry run or a diagnostic read finished this segment and kept
		// its source. This backend exports, so the source is sent again.
		if seg, err = u.store.Reopen(seg.ID); err != nil {
			return err
		}
		u.point(PointReopened)
		slog.Info("segment finished without an export is prepared again", "segment", item.Index)
	}
	switch seg.State {
	case journal.StateDiscovered, journal.StatePreparing:
		// Only a segment that needs preparing is checked against the spool,
		// so a large segment already sent never holds back new ones.
		allowed, err := u.roomToPrepare(ctx, item.Size, pool)
		if err != nil {
			return err
		}
		if !allowed {
			return errPreparePaused
		}
		return u.prepare(ctx, item, seg, pool)
	case journal.StateQuarantined:
		// A crash between the journal record and the move left the source
		// on the spool. Admit skips what is already moved.
		return u.quarantine.Admit(seg.ID, item.Path)
	}
	return nil
}

// fingerprint returns the hex sha256 of a closed segment, reading it only
// the first time its name, size and modification time are seen.
func (u *Uploader) fingerprint(ctx context.Context, item segment.Segment) (string, error) {
	key := fileIdentity{name: filepath.Base(item.Path), size: item.Size, modTime: item.ModTime}
	if sum, ok := u.fingerprints[key]; ok {
		return sum, nil
	}
	file, err := record.OpenRegular(item.Path)
	if err != nil {
		return "", fmt.Errorf("open segment: %w", err)
	}
	defer file.Close()
	hash := sha256.New()
	// Checked per chunk, so a drain never waits for a whole large segment.
	if _, err := io.Copy(contextWriter{ctx: ctx, w: hash}, file); err != nil {
		return "", fmt.Errorf("fingerprint segment: %w", err)
	}
	sum := hex.EncodeToString(hash.Sum(nil))
	u.fingerprints[key] = sum
	return sum, nil
}

// contextWriter refuses writes once ctx is done.
type contextWriter struct {
	ctx context.Context
	w   io.Writer
}

func (c contextWriter) Write(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.w.Write(p)
}

func (u *Uploader) forgetGone(scan segment.Scan) {
	present := map[fileIdentity]bool{}
	for _, item := range append(append([]segment.Segment(nil), scan.Closed...), scan.Active...) {
		present[fileIdentity{name: filepath.Base(item.Path), size: item.Size, modTime: item.ModTime}] = true
	}
	for key := range u.fingerprints {
		if !present[key] {
			delete(u.fingerprints, key)
		}
	}
}

// prepare records that one segment's prepare started and starts it in the
// pool. A segment found Preparing is prepared again from the start: its
// earlier writer was aborted. The segment's prepare span lasts until the
// pool records the result.
func (u *Uploader) prepare(ctx context.Context, item segment.Segment, seg journal.Segment, pool *preparePool) error {
	ctx, span := u.startSpan(ctx, "prepare")
	if seg.State == journal.StateDiscovered {
		if err := u.store.BeginPrepare(seg.ID); err != nil {
			failSpan(span, "error", "")
			span.End()
			return err
		}
	}
	if inFlight, _ := pool.inFlight(); inFlight > 0 {
		u.point(PointPreparingAlongside)
	}
	pool.start(ctx, span, item, seg)
	return nil
}

// finishPrepare records the result of one prepare. It runs on the intake
// goroutine, one segment at a time, in spool order.
func (u *Uploader) finishPrepare(job *prepareJob) error {
	item, seg, result, rejects := job.item, job.seg, job.result, job.rejects
	if job.err != nil {
		return job.err
	}
	u.metrics.Observe(telemetry.PrepareDuration, job.elapsed.Seconds(), nil)
	outputs := job.writer.Outputs()
	if hex.EncodeToString(result.Fingerprint) != seg.SHA256 {
		// The file changed after it was fingerprinted, so nothing read here
		// belongs to this journal entry.
		for _, output := range outputs {
			_ = removeFile(output.Path)
		}
		return errors.Join(fmt.Errorf("segment %d changed while it was read", item.Index), rejects.Discard())
	}
	tripped := result.Quarantine != "" && u.policy.Breaker().TrippedAt(u.now())
	u.countResumes()
	if tripped {
		// The breaker tripped while this segment was being prepared. Hold
		// it, as intake holds every segment after a trip; it is prepared
		// again once the breaker resumes.
		job.span.SetAttributes(attrStatus("held"))
		for _, output := range outputs {
			_ = removeFile(output.Path)
		}
		return rejects.Discard()
	}
	if _, err := rejects.Commit(seg.ID); err != nil {
		// The rejected lines exist only in the source until the artifact is
		// kept, so the segment is held and prepared again later.
		for _, output := range outputs {
			_ = removeFile(output.Path)
		}
		return fmt.Errorf("keep rejected lines: %w", err)
	}
	u.recordPrepared(result)
	u.recordRead(item, result)

	span := job.span
	switch {
	case result.Quarantine != "":
		span.SetAttributes(attrStatus("quarantined"), attribute.String("reason", result.Quarantine))
		return u.quarantineSegment(seg.ID, item.Index, result.Quarantine, item.Path)
	case len(outputs) == 0:
		span.SetAttributes(attrStatus("empty"))
		return u.store.MarkEmpty(seg.ID)
	}
	span.SetAttributes(attrStatus("prepared"))
	transports := make(map[string]journal.Transport, len(outputs))
	for _, output := range outputs {
		transports[laneOf(output.Destination)] = journal.Transport{Path: output.Path, SHA256: output.SHA256, Rows: output.Rows, Bytes: output.Bytes, Policy: u.prepPolicy}
	}
	if err := u.store.MarkPrepared(seg.ID, u.name, transports); err != nil {
		return err
	}
	u.point(PointPrepared)
	slog.Info("prepared request trace segment", "segment", item.Index, "records", result.Written)
	if backlog := u.backlog.Load(); backlog != nil {
		backlog.notePrepared()
	}
	return nil
}

// recordSchemaVersion is the version of what the core hands a format writer
// for the same settings. Bump it when a release changes that, such as how
// records are redacted, so unsent transports an older release prepared are
// prepared again.
const recordSchemaVersion = 1

// formatVersioner is implemented by a format factory whose writers can
// change what they write between releases, such as a table writer whose
// schema changes. A new version makes unsent transports an older writer
// wrote be prepared again.
type formatVersioner interface {
	FormatVersion() string
}

// redactionVersion is the redaction algorithm's version. Tests replace it.
var redactionVersion = redact.Version

// recordPolicy fingerprints everything that decides what a transport holds:
// the read bounds, suppression rules and headerless policy, redaction with
// its algorithm's version, the record schema, and the format with its writer
// version. Only the hash is kept or logged, never the configured values.
func recordPolicy(cfg transform.Config, factory format.Factory) (string, error) {
	writerVersion := ""
	if versioned, ok := factory.(formatVersioner); ok {
		writerVersion = versioned.FormatVersion()
	}
	// A redaction that catches more makes transports prepared before it
	// stale. With redaction off the field is left out, so that policy is
	// the one written before the field existed.
	redaction := ""
	if cfg.Redact {
		redaction = redactionVersion
	}
	raw, err := json.Marshal(struct {
		Transform        transform.Config
		RecordSchema     int
		Format           backend.Format
		WriterVersion    string
		RedactionVersion string `json:",omitempty"`
	}{cfg, recordSchemaVersion, factory.Format(), writerVersion, redaction})
	if err != nil {
		return "", fmt.Errorf("fingerprint the record policy: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:16]), nil
}

// reprepareStale discards unsent transports written under a record policy
// other than the one now configured, so newly enabled suppression or
// redaction applies to them. Their segments are prepared again from the
// source. A batch whose submits were only ever refused is released first,
// since nothing in it was sent. Transports in a batch that may have landed
// are left alone. A segment with one destination confirmed and another
// unsent is prepared again as a whole, so nothing goes out under the old
// policy; the confirmed destination is sent again and counted in
// submits_ambiguous as a possible duplicate. A segment whose source is gone
// keeps its transports in quarantine: they are the only copy, and sending
// them would apply settings they were not prepared for.
//
// It runs at startup and again on every pass: a segment with a destination
// in a batch that may have landed waits, its unsent destinations held from
// batching, until that batch is done, and is then prepared again.
func (u *Uploader) reprepareStale() error {
	var errs []error
	for _, b := range u.store.Batches() {
		if b.State != journal.StateRetryWait || b.Receipt != nil || b.MayHaveLanded || b.Backend != u.name || !u.holdsStale(b) {
			continue
		}
		if err := u.store.ReleaseBatch(b.ID); err != nil {
			errs = append(errs, err)
		}
	}
	discarded := 0
	for _, seg := range u.store.Segments() {
		if seg.State != journal.StatePrepared || seg.Backend != u.name {
			continue
		}
		stale, batched, confirmed := false, false, 0
		for _, dest := range seg.Destinations {
			stale = stale || dest.Transport.Policy != u.prepPolicy
			if dest.State == journal.StateConfirmed {
				// Its batch is finished, or gone when it was fenced.
				confirmed++
				continue
			}
			batched = batched || dest.BatchID != "" || dest.State != journal.StatePrepared
		}
		if !stale || batched {
			continue
		}
		same, err := sameSource(filepath.Join(u.cfg.SourceDir, seg.Name), seg)
		if err != nil {
			errs = append(errs, fmt.Errorf("check the source of a stale transport: %w", err))
			continue
		}
		if !same {
			var transports []string
			for _, dest := range seg.Destinations {
				transports = append(transports, dest.Transport.Path)
			}
			errs = append(errs, u.quarantineSegment(seg.ID, seg.Index, reasonSourceMissing, transports...))
			continue
		}
		if err := u.store.Reprepare(seg.ID); err != nil {
			errs = append(errs, err)
			continue
		}
		u.metrics.Add(telemetry.SubmitsAmbiguous, float64(confirmed), nil)
		discarded++
	}
	if discarded > 0 {
		slog.Info("record policy changed; unsent segments are prepared again", "segments", discarded)
	}
	return errors.Join(errs...)
}

// sameSource reports whether path still holds the segment's content: the
// same size and modification time, as cleanup checks, or else the same
// sha256. A missing file, one that is not a regular file, or other content
// under the name is not the source.
func sameSource(path string, seg journal.Segment) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() || info.Size() != seg.Size {
		return false, nil
	}
	if info.ModTime().Equal(seg.ModTime) {
		return true, nil
	}
	file, err := record.OpenRegular(path)
	if err != nil {
		return false, err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return false, err
	}
	return hex.EncodeToString(hash.Sum(nil)) == seg.SHA256, nil
}

// holdsStale reports whether a member of the batch was prepared under a
// record policy other than the one now configured.
func (u *Uploader) holdsStale(b journal.Batch) bool {
	for _, m := range b.Members {
		seg, ok := u.store.Segment(m.SegmentID)
		if !ok {
			continue
		}
		if dest, ok := seg.Destinations[b.Destination]; ok && dest.Transport.Policy != u.prepPolicy {
			return true
		}
	}
	return false
}

var unreadableReasons = []struct {
	reason string
	count  func(transform.Result) int
}{
	{"unparseable", func(r transform.Result) int { return r.Unparseable }},
	{"line_too_long", func(r transform.Result) int { return r.LineTooLong }},
	{"row_too_large", func(r transform.Result) int { return r.RowTooLarge }},
	{"cut", func(r transform.Result) int { return r.Cut }},
}

// unsupportedVersionLabels labels records_dropped for records from a Dynamo
// release older than v1.4.0. They carry no event type, so their record type
// is unknown, as it is for any type outside v1.4.0's set.
func unsupportedVersionLabels() telemetry.Labels {
	return telemetry.Labels{"record_type": "unknown", "reason": "unsupported_version"}
}

func (u *Uploader) recordRead(item segment.Segment, result transform.Result) {
	u.metrics.Add(telemetry.SourceBytes, float64(item.Size), nil)
	for recordType, n := range result.ReceivedByType {
		u.metrics.Add(telemetry.RecordsReceived, float64(n), telemetry.Labels{"record_type": recordType})
	}
	for _, r := range unreadableReasons {
		if n := r.count(result); n > 0 {
			u.metrics.Add(telemetry.RecordsUnreadable, float64(n), telemetry.Labels{"reason": r.reason})
		}
	}
	if result.UnsupportedVersion > 0 {
		u.metrics.Add(telemetry.RecordsDropped, float64(result.UnsupportedVersion), unsupportedVersionLabels())
	}
}
