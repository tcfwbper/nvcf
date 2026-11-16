// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/diskguard"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/journal"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/segment"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
)

// Eviction keys name either a journal segment or a closed spool file the
// journal has not seen yet, such as one intake skipped while preparing was
// paused.
const (
	evictSegment = "segment:"
	evictFile    = "file:"
)

// errPreparePaused stops intake for this pass: the spool has no room to
// prepare another segment.
var errPreparePaused = errors.New("preparing paused to protect the spool")

// enforceDiskBounds applies the spool bounds: quarantine by age and share
// of the spool, then, above the high watermark, deletes quarantine and then
// the oldest closed segments down to the low watermark. Every deletion is
// counted as loss. Nothing in a batch that may have been sent is ever
// deleted. It runs at the start of every pass and, when the spool check asks,
// at intake's next segment boundary; the caller holds the intake and submit
// locks, so no segment is being prepared or batched.
func (u *Uploader) enforceDiskBounds() {
	u.evictDue.Store(false)
	if _, err := u.guard.Enforce(u.now()); err != nil {
		slog.Warn("enforce spool bounds", "error", safeError(err))
	}
}

// checkSpool runs on the spool check interval, apart from passes, so a long
// prepare or submit never delays it. It bounds quarantine and refreshes the
// spool gauges, but never deletes a segment: when the spool is still above
// the high watermark, it marks an eviction due for intake's next segment
// boundary.
func (u *Uploader) checkSpool(context.Context) error {
	report, err := u.guard.Check(u.now())
	if err != nil {
		slog.Warn("check spool bounds", "error", safeError(err))
	}
	if report.AboveWatermark {
		u.evictDue.Store(true)
	}
	u.updateSpoolGauges(pendingCandidates(u.store.Segments()))
	return nil
}

// evictIfDue runs the spool bounds when the spool check asked for an
// eviction. Intake calls it between segments and at its end, holding the
// intake lock. It first waits for every prepare still running and records
// it, so nothing is deleted while a segment is being prepared.
func (u *Uploader) evictIfDue(pool *preparePool) {
	if !u.evictDue.Load() {
		return
	}
	pool.makeRoom(0)
	_ = u.locked(&u.submitMu, func() error { u.enforceDiskBounds(); return nil })
}

// allowPrepare decides whether preparing one more segment of size bytes
// fits the spool. The caller logs a pause.
func (u *Uploader) allowPrepare(size int64) (diskguard.Decision, error) {
	waiting := 0
	for _, seg := range u.store.Segments() {
		if seg.State == journal.StatePrepared && unbatched(seg) {
			waiting++
		}
	}
	decision, err := u.guard.AllowPrepare(size, waiting)
	if err != nil {
		return diskguard.Decision{}, fmt.Errorf("check spool space: %w", err)
	}
	return decision, nil
}

// updateSpoolGauges reports spool use and the pending segments.
func (u *Uploader) updateSpoolGauges(pending []diskguard.Candidate) {
	if err := u.guard.UpdateGauges(u.now(), pending); err != nil {
		slog.Warn("read spool usage", "error", safeError(err))
	}
}

// spoolGuardConfig is the disk guard configuration for the spool settings. A
// zero policy, as in a Config built by hand, keeps the defaults; one that is
// only partly set is passed on as is, so the guard rejects it at startup.
func spoolGuardConfig(spool config.SpoolPolicy) diskguard.Config {
	cfg := diskguard.DefaultConfig()
	if spool == (config.SpoolPolicy{}) {
		return cfg
	}
	cfg.HighWatermark = spool.HighWatermark
	cfg.LowWatermark = spool.LowWatermark
	cfg.Policy = diskguard.Policy(spool.Eviction)
	cfg.MinFreeBytes = spool.MinFreeBytes
	cfg.QuarantineMaxAge = spool.QuarantineMaxAge
	cfg.QuarantineMaxFraction = spool.QuarantineMaxFraction
	return cfg
}

func unbatched(seg journal.Segment) bool {
	for _, dest := range seg.Destinations {
		if dest.BatchID != "" {
			return false
		}
	}
	return true
}

// spoolSegments is what the disk guard may evict: closed segments that are
// not in any batch, oldest first by the guard. The active (highest index)
// segment of each prefix is never listed.
type spoolSegments struct{ u *Uploader }

func (s spoolSegments) Evictable() ([]diskguard.Candidate, error) {
	scan, err := segment.ScanDir(s.u.cfg.SourceDir, s.u.cfg.Prefixes())
	if err != nil {
		return nil, fmt.Errorf("scan the spool: %w", err)
	}
	byName := map[string][]journal.Segment{}
	for _, seg := range s.u.store.Segments() {
		byName[seg.Name] = append(byName[seg.Name], seg)
	}
	var out []diskguard.Candidate
	for _, item := range scan.Closed {
		name := filepath.Base(item.Path)
		key, ok := evictionKey(byName[name], item)
		if !ok {
			continue
		}
		// Sized as the files take disk, the unit the spool budget measures,
		// so what relief counts as freed is freed.
		bytes := diskguard.DiskBytes(item.Path, item.Size)
		if id, isSegment := strings.CutPrefix(key, evictSegment); isSegment {
			seg, _ := s.u.store.Segment(id)
			for _, dest := range seg.Destinations {
				if dest.Transport.Path != "" {
					bytes += diskguard.DiskBytes(dest.Transport.Path, dest.Transport.Bytes)
				}
			}
		}
		out = append(out, diskguard.Candidate{Key: key, Bytes: bytes, ModTime: item.ModTime})
	}
	return out, nil
}

// evictionKey decides whether the spool file item may be evicted, given every
// journal entry under its name; a writer can reuse a name, so there may be
// several. The file is evictable as its journal entry when the entry that
// matches its size and modification time is unbatched and not yet sent, or as
// a bare file when no entry is still at work. Any entry under the name that
// is in a batch, confirmed or being cleaned keeps the file.
func evictionKey(entries []journal.Segment, item segment.Segment) (string, bool) {
	var match *journal.Segment
	waiting := 0
	for i, seg := range entries {
		switch seg.State {
		case journal.StateDone, journal.StateQuarantined:
			continue
		case journal.StateDiscovered, journal.StatePrepared:
			if !unbatched(seg) {
				return "", false
			}
			waiting++
			if seg.Size == item.Size && seg.ModTime.Equal(item.ModTime) {
				match = &entries[i]
			}
		default:
			return "", false
		}
	}
	switch {
	case match != nil:
		return evictSegment + match.ID, true
	case waiting == 0:
		return evictFile + filepath.Base(item.Path), true
	}
	// Entries wait under the name, but none is this file's content.
	return "", false
}

// Evict records the loss in the journal first and then deletes the files,
// syncing the directory, so a crash in between leaves a file whose entry is
// already done: intake skips it and a later pass evicts it as a bare file.
// The journal removes the transports such a crash or a failed delete left,
// at the next start or with the tombstone.
// A segment is never prepared or sent from files that are gone.
func (s spoolSegments) Evict(key string) error {
	if name, ok := strings.CutPrefix(key, evictFile); ok {
		return removeDurably(filepath.Join(s.u.cfg.SourceDir, name))
	}
	id, ok := strings.CutPrefix(key, evictSegment)
	if !ok {
		return fmt.Errorf("unknown eviction key %q", key)
	}
	seg, ok := s.u.store.Segment(id)
	if !ok {
		return nil
	}
	if (seg.State != journal.StateDiscovered && seg.State != journal.StatePrepared) || !unbatched(seg) {
		return errors.New("segment entered a batch after it was listed for eviction")
	}
	if err := s.u.store.Evict(id); err != nil {
		return err
	}
	s.u.point(PointEvicted)
	var errs []error
	for _, dest := range seg.Destinations {
		errs = append(errs, removeDurably(dest.Transport.Path))
	}
	errs = append(errs, removeDurably(filepath.Join(s.u.cfg.SourceDir, seg.Name)))
	return errors.Join(errs...)
}

// removeDurably deletes path and syncs its directory, so the delete survives
// a power loss. A file already gone is not an error. A failure after the
// delete wraps diskguard.ErrDeletedNotSynced, so the deletion is counted.
func removeDurably(path string) error {
	if err := os.Remove(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err == nil {
		err = errors.Join(dir.Sync(), dir.Close())
	}
	if err != nil {
		return fmt.Errorf("%w: %w", diskguard.ErrDeletedNotSynced, err)
	}
	return nil
}

// gaugesFor maps the disk guard's gauges to telemetry instruments.
func gaugesFor(m telemetry.Metrics) spoolGauges { return spoolGauges{m: m} }

type spoolGauges struct{ m telemetry.Metrics }

func (g spoolGauges) Set(name string, value float64) {
	switch name {
	case diskguard.GaugeSpoolUsedBytes:
		g.m.Set(telemetry.SpoolUsedBytes, value, nil)
	case diskguard.GaugeSpoolFreeBytes:
		g.m.Set(telemetry.SpoolFreeBytes, value, nil)
	case diskguard.GaugePendingSegments:
		g.m.Set(telemetry.PendingSegments, value, nil)
	case diskguard.GaugePendingBytes:
		g.m.Set(telemetry.PendingBytes, value, nil)
	case diskguard.GaugeOldestPendingAge:
		g.m.Set(telemetry.OldestPendingAge, value, nil)
	}
}

// pendingCandidates lists every segment not yet done or quarantined, for the
// pending gauges.
func pendingCandidates(segments []journal.Segment) []diskguard.Candidate {
	var out []diskguard.Candidate
	for _, seg := range segments {
		switch seg.State {
		case journal.StateDone, journal.StateQuarantined:
			continue
		}
		out = append(out, diskguard.Candidate{Key: seg.ID, Bytes: seg.Size, ModTime: seg.ModTime})
	}
	return out
}
