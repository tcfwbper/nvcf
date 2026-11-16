// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/batch"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/journalfs"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/retry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/journal"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/segment"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
)

// retainedUntil parks a retained batch: it is never due, so only an
// operator moves it.
var retainedUntil = time.Date(9999, time.December, 31, 0, 0, 0, 0, time.UTC)

// quarantineSegment records a quarantine and then moves the files, so a crash
// in between is finished by the next pass. The count is taken once, when the
// quarantine is recorded.
func (u *Uploader) quarantineSegment(id string, index int, reason string, paths ...string) error {
	if err := u.store.Quarantine(id, reason); err != nil {
		return err
	}
	u.metrics.Add(telemetry.SegmentsQuarantined, 1, quarantineLabels(reason))
	tripped := u.policy.Breaker().QuarantinedAt(u.now())
	u.countResumes()
	if tripped {
		u.metrics.Add(telemetry.QuarantineTripped, 1, nil)
	}
	slog.Warn("quarantined request trace segment", "segment", index, "reason", reason)
	return u.quarantine.Admit(id, paths...)
}

// countResumes counts each time the quarantine breaker closed on its own.
func (u *Uploader) countResumes() {
	if n := u.policy.Breaker().Resumed(); n > 0 {
		u.metrics.Add(telemetry.QuarantineResumed, float64(n), nil)
		slog.Info("quarantine breaker resumed; reading new segments again")
	}
}

// quarantineLabels labels a quarantine by its reason. A backend may name
// reasons of its own; any reason outside the known set is counted as unknown,
// so the label stays bounded. The journal and the log keep the reason as
// recorded.
func quarantineLabels(reason string) telemetry.Labels {
	if !slices.Contains(telemetry.QuarantineReasons, reason) {
		reason = "unknown"
	}
	return telemetry.Labels{"reason": reason}
}

// submitPrepared batches every prepared destination that is in no batch and
// submits the batches. Lanes are flushed at the end of each call, so a
// segment waits at most one scan interval, as with the earlier exporter.
func (u *Uploader) submitPrepared(ctx context.Context) error {
	return u.submitBatches(ctx, u.batcher, true)
}

// submitFull submits only the batches that fill up from what is prepared
// now, so a backlog is sent while it is prepared. It batches on a batcher of
// its own and drops the lanes that are not full: their members stay
// prepared and in no batch, for a later call or the end of the pass.
func (u *Uploader) submitFull(ctx context.Context) error {
	batcher, err := u.fullBatcher()
	if err != nil {
		return err
	}
	return u.submitBatches(ctx, batcher, false)
}

// fullOnlyWait stands in for the batch wait in a full-only round, so a lane
// closes only when it is full; the flush at the end of the pass sends what
// has waited.
const fullOnlyWait = 100 * 365 * 24 * time.Hour

// fullBatcher returns the batcher of one full-only round. It never closes a
// lane by age: members that waited would otherwise go one job each.
func (u *Uploader) fullBatcher() (*batch.Batcher, error) {
	cfg := u.batchConfig
	cfg.MaxWait = fullOnlyWait
	return batch.New(cfg, u.batchLimits, batch.Options{Metrics: batchMetrics{u.metrics}})
}

// submitBatches batches every prepared destination that is in no batch on
// batcher and submits the batches that close. flush also closes and submits
// every lane that is not full. A destination prepared under another record
// policy is never batched, by either caller: it waits for reprepareStale to
// prepare its segment again. A release that failed earlier is retried first.
func (u *Uploader) submitBatches(ctx context.Context, batcher *batch.Batcher, flush bool) error {
	u.retryUnsentReleases()
	now := time.Now()
	var batches []batch.Batch
	var errs []error
	for _, seg := range u.store.Segments() {
		if seg.State != journal.StatePrepared && seg.State != journal.StateSubmitting {
			continue
		}
		for lane, dest := range seg.Destinations {
			if dest.State != journal.StatePrepared || dest.BatchID != "" || dest.Transport.Policy != u.prepPolicy {
				continue
			}
			preparedAt := seg.PreparedAt
			if preparedAt.IsZero() {
				preparedAt = now
			}
			member := batch.Member{
				SegmentID:   seg.ID,
				Destination: lane,
				File:        batch.File{Path: dest.Transport.Path, SHA256: dest.Transport.SHA256, Bytes: dest.Transport.Bytes, Rows: dest.Transport.Rows},
				PreparedAt:  preparedAt,
			}
			closed, err := u.batchMember(batcher, seg.ID, member, now)
			if errors.Is(err, batch.ErrMemberTooLarge) {
				errs = append(errs, u.quarantineSegment(seg.ID, seg.Index, reasonTooLarge, filepath.Join(u.cfg.SourceDir, seg.Name), dest.Transport.Path))
				continue
			}
			if err != nil {
				errs = append(errs, err)
				continue
			}
			batches = append(batches, closed...)
		}
	}
	if flush {
		batches = append(batches, batcher.Due(now.Add(batch.DefaultMaxWait))...)
	}
	for _, b := range batches {
		if err := u.submit(ctx, b, false); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// resend submits again every batch whose retry is due, and every batch whose
// earlier submit may have committed without a job id; the second kind can
// land twice and is counted. When ambiguous submits are retained, such a
// batch is parked for an operator instead, as a failed submit would be.
func (u *Uploader) resend(ctx context.Context) error {
	now := time.Now()
	var errs []error
	for _, b := range u.store.Batches() {
		switch {
		case b.State == journal.StateRetryWait && !now.Before(b.NextRetryAt):
		case b.State == journal.StateAmbiguous && b.Receipt == nil && u.retainsAmbiguous():
			// Recovery counted it when it found the submit unfinished.
			slog.Warn("submit retained for an operator", "batch", shortID(b.ID))
			if err := u.store.RetryBatch(b.ID, retainedUntil, reasonRetained); err != nil {
				errs = append(errs, err)
			}
			continue
		case b.State == journal.StateAmbiguous && b.Receipt == nil:
			u.metrics.Add(telemetry.SubmitsAmbiguous, 1, nil)
		default:
			continue
		}
		if err := u.submit(ctx, toBatch(b), true); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// releaseUnsent releases a batch that was recorded but never sent. A
// release the journal refuses is tried again at the start of each submit
// round. Until then the members stay in the batch; a restart would read it
// as a submit that may have landed.
func (u *Uploader) releaseUnsent(id string) error {
	err := u.store.ReleaseBatch(id)
	if err != nil {
		u.stateMu.Lock()
		u.unsent[id] = true
		u.stateMu.Unlock()
	}
	return err
}

// retryUnsentReleases releases the batches releaseUnsent could not. A batch
// the journal already holds as released is done.
func (u *Uploader) retryUnsentReleases() {
	u.stateMu.Lock()
	ids := make([]string, 0, len(u.unsent))
	for id := range u.unsent {
		ids = append(ids, id)
	}
	u.stateMu.Unlock()
	for _, id := range ids {
		if b, ok := u.store.Batch(id); ok && b.State == journal.StateSubmitting {
			if err := u.store.ReleaseBatch(id); err != nil {
				slog.Warn("release a batch that was never sent", "batch", shortID(id), "error", safeError(err))
				continue
			}
		}
		u.stateMu.Lock()
		delete(u.unsent, id)
		u.stateMu.Unlock()
	}
}

// retainsAmbiguous reports whether a submit that may have landed without a
// job id waits for an operator instead of being sent again.
func (u *Uploader) retainsAmbiguous() bool {
	return u.faults.AmbiguousWithoutID == retry.AmbiguousRetain && !u.caps.ResubmitSafe
}

func toBatch(b journal.Batch) batch.Batch {
	out := batch.Batch{ID: b.ID, Destination: b.Destination, ClosedAt: b.CreatedAt}
	for _, m := range b.Members {
		out.Members = append(out.Members, batch.Member{
			SegmentID:   m.SegmentID,
			Destination: b.Destination,
			File:        batch.File{Path: m.Path, SHA256: m.SHA256, Bytes: m.Bytes, Rows: m.Rows},
			PreparedAt:  b.CreatedAt,
		})
		out.Bytes += m.Bytes
	}
	return out
}

// submit waits for the create budget and any hold, records the batch (a new
// one) or moves it back to Submitting (a resend), sends it, and saves the job
// id before it looks at the error.
func (u *Uploader) submit(ctx context.Context, b batch.Batch, resend bool) error {
	if err := u.policy.WaitSubmit(ctx); err != nil {
		return err
	}
	j := batchJournal{store: u.store, point: u.point, exports: u.caps.Exports}
	if resend {
		if err := u.store.ResumeBatch(b.ID); err != nil {
			if !errors.Is(err, journalfs.ErrNotSynced) {
				return err
			}
			// The batch is Submitting on disk and in memory; only the
			// directory sync failed. It is sent now, because later rounds
			// pick up only waiting and ambiguous batches.
			slog.Warn("resume a batch", "batch", shortID(b.ID), "error", safeError(err))
		}
	} else if err := j.OpenBatch(b, u.name); err != nil {
		// A batch recorded with only some members pointing at it must
		// never be sent: a member that does not point at it would be
		// batched and sent again. Release it; nothing was sent.
		if _, recorded := u.store.Batch(b.ID); recorded {
			err = errors.Join(err, u.releaseUnsent(b.ID))
		}
		return fmt.Errorf("record batch before submit: %w", err)
	}
	if backlog := u.backlog.Load(); backlog != nil {
		backlog.batched()
	}
	receipt, err := batch.Send(ctx, j, u.name, b, u.send)
	if receipt != "" {
		if errors.Is(err, batch.ErrReceiptNotSaved) {
			// Keep the id, so a later round saves it instead of
			// submitting again.
			u.stateMu.Lock()
			u.unsaved[b.ID] = receipt
			u.stateMu.Unlock()
			return err
		}
		if err != nil {
			slog.Warn("create returned a job id and an error; the job is polled", "batch", shortID(b.ID), "fault", faultText(backend.Classify(u.client, err)))
		}
		slog.Info("submitted request trace batch", "batch", shortID(b.ID), "segments", len(b.Members), "bytes", b.Bytes)
		return nil
	}
	if err == nil {
		err = errors.New("backend returned no job id")
	}
	return u.submitFailed(b, err)
}

func (u *Uploader) send(ctx context.Context, b batch.Batch) (string, error) {
	request := backend.SubmitRequest{Destination: u.destinationOf(b.Destination)}
	for _, m := range b.Members {
		request.Files = append(request.Files, backend.File{Path: m.File.Path, Bytes: m.File.Bytes, Rows: m.File.Rows})
	}
	if len(b.Members) == 1 {
		request.Path = b.Members[0].File.Path
		if seg, ok := u.store.Segment(b.Members[0].SegmentID); ok {
			request.Segment = segment.Segment{
				Path:    filepath.Join(u.cfg.SourceDir, seg.Name),
				Prefix:  seg.Prefix,
				Index:   seg.Index,
				Size:    seg.Size,
				ModTime: seg.ModTime,
			}
		}
	}
	ctx, span := u.startSpan(ctx, "submit")
	defer span.End()
	if err := u.caps.Check(request); err != nil {
		failFault(span, backend.Classify(u.client, err))
		return "", err
	}
	started := time.Now()
	// No core deadline: a submit uploads the files, and a full batch can take
	// longer than any fixed limit on a slow link, so a cut would fail it on
	// every retry. An upload deadline belongs to the backend.
	id, err := u.client.Submit(ctx, request)
	u.metrics.Observe(telemetry.UploadDuration, time.Since(started).Seconds(), uploadOutcome(id, err))
	switch {
	case id != "" && err == nil:
		span.SetAttributes(attrStatus("success"))
	case err == nil:
		failSpan(span, "failure", "no_job_id")
	default:
		failFault(span, backend.Classify(u.client, err))
	}
	u.point(PointSubmitted)
	return id, err
}

// uploadOutcome labels one backend submit call: success when it returned a
// job id and no error, failure otherwise.
func uploadOutcome(id string, err error) telemetry.Labels {
	if id != "" && err == nil {
		return telemetry.Labels{"status": "success"}
	}
	return telemetry.Labels{"status": "failure"}
}

// submitFailed applies the fault policy to a submit that created no job.
func (u *Uploader) submitFailed(b batch.Batch, err error) error {
	now := time.Now()
	stored, _ := u.store.Batch(b.ID)
	fault := backend.Classify(u.client, err)
	u.countAuth(fault)
	decision, countErr := u.decideFailure(stored, retry.Failure{Key: failureKey(stored.Members), Stage: retry.StageSubmit, Fault: fault}, now)
	class := errorClass(fault)
	slog.Warn("submit failed", "batch", shortID(b.ID), "segments", len(b.Members), "fault", faultText(fault), "action", decision.Action)
	switch decision.Action {
	case retry.ActionResubmit:
		return errors.Join(countErr, u.store.MarkBatchAmbiguous(b.ID, class))
	case retry.ActionQuarantine:
		return errors.Join(countErr, u.split(stored, class))
	case retry.ActionRetain:
		// The create may have landed and the backend cannot take it twice,
		// so the batch waits for an operator and is never sent again
		// automatically, also after a restart. It is recorded as ambiguous
		// first, so a restart never treats it as only refused.
		slog.Warn("submit retained for an operator", "batch", shortID(b.ID))
		if err := u.store.MarkBatchAmbiguous(b.ID, class); err != nil {
			return errors.Join(countErr, err)
		}
		return errors.Join(countErr, u.store.RetryBatch(b.ID, retainedUntil, reasonRetained))
	}
	next := now.Add(u.retryDelay(stored.Attempts))
	if decision.Hold.Until.After(next) {
		next = decision.Hold.Until
	}
	return errors.Join(countErr, u.store.RetryBatch(b.ID, next, class))
}

// decideFailure applies the fault policy to a failure of b and then counts it
// toward each member segment's quarantine threshold in the journal, unless
// its scope turned out to be the whole backend, or it names no segment scope
// and arrived while an earlier fault held the backend: a failure while the
// backend is held says nothing about the segments. The policy sees the
// highest member count, with this failure included when it counts. The
// count lives on the segment entry, so it carries over to a new batch and
// across restarts. The send count, Attempts, still sets the backoff.
func (u *Uploader) decideFailure(b journal.Batch, f retry.Failure, now time.Time) (retry.Decision, error) {
	held := f.Fault.Scope != backend.ScopeSegment && u.policy.Gate().Hold(now).Active(now)
	f.Attempts = u.segmentFaults(b.Members)
	if !held {
		f.Attempts++
	}
	decision := u.policy.Failed(now, u.caps, f)
	if held || decision.Scope == backend.ScopeBackend {
		return decision, nil
	}
	ids := make([]string, 0, len(b.Members))
	for _, m := range b.Members {
		ids = append(ids, m.SegmentID)
	}
	return decision, u.store.CountFaults(ids)
}

func (u *Uploader) retryDelay(attempts int) time.Duration {
	delay := time.Duration(0)
	for range attempts + 1 {
		delay = u.faults.Transient.Doubling(delay)
	}
	return delay
}

// split ends a batch that a segment fault failed. A batch of one quarantines
// its segment; a larger batch is released and its members go alone, so the
// one at fault is found.
func (u *Uploader) split(b journal.Batch, reason string) error {
	if err := u.store.ReleaseBatch(b.ID); err != nil {
		return err
	}
	u.stateMu.Lock()
	defer u.stateMu.Unlock()
	if len(b.Members) > 1 {
		for _, m := range b.Members {
			u.solo[m.SegmentID] = true
		}
		u.metrics.Add(telemetry.BatchSplits, 1, nil)
		return nil
	}
	m := b.Members[0]
	seg, ok := u.store.Segment(m.SegmentID)
	if !ok {
		return nil
	}
	delete(u.solo, m.SegmentID)
	return u.quarantineSegment(seg.ID, seg.Index, reason, filepath.Join(u.cfg.SourceDir, seg.Name), m.Path)
}

// batchMember adds a member to its lane on batcher, or closes a batch of it
// alone when a split sent it alone. Both refuse a member over the batch byte
// limit.
func (u *Uploader) batchMember(batcher *batch.Batcher, segmentID string, member batch.Member, now time.Time) ([]batch.Batch, error) {
	if !u.isSolo(segmentID) {
		return batcher.Add(member, now)
	}
	solo, err := batcher.Solo(member, now)
	if err != nil {
		return nil, err
	}
	return []batch.Batch{solo}, nil
}

func (u *Uploader) isSolo(segmentID string) bool {
	u.stateMu.Lock()
	defer u.stateMu.Unlock()
	return u.solo[segmentID]
}

// failureKey names a batch's work for fault correlation. A batch sent again
// gets a new id, so the key is the member set instead: one segment failing
// under a run of replacement batches counts as one item, never as many
// items failing, which would hold the whole backend.
func failureKey(members []journal.Member) string {
	if len(members) == 1 {
		return members[0].SegmentID
	}
	ids := make([]string, 0, len(members))
	for _, m := range members {
		ids = append(ids, m.SegmentID)
	}
	slices.Sort(ids)
	sum := sha256.Sum256([]byte(strings.Join(ids, ",")))
	return "batch-" + hex.EncodeToString(sum[:])
}

// segmentFaults returns the highest fault count among the member segments.
func (u *Uploader) segmentFaults(members []journal.Member) int {
	highest := 0
	for _, m := range members {
		if seg, ok := u.store.Segment(m.SegmentID); ok {
			highest = max(highest, seg.Faults)
		}
	}
	return highest
}
