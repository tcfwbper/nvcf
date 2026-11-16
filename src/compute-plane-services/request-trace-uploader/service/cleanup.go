// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/cleanup"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/journal"
)

// cleanupRound finishes every confirmed or empty segment. The round reads
// the spool's highest indexes once, not once per deletion.
func (u *Uploader) cleanupRound(ctx context.Context) error {
	cleaner := u.cleaner.Round()
	var errs []error
	for _, seg := range u.store.Segments() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		switch seg.State {
		case journal.StateConfirmed, journal.StateCleaning:
			err = u.clean(ctx, cleaner, seg)
		case journal.StateEmpty:
			err = u.cleanEmpty(ctx, cleaner, seg)
		}
		if err != nil {
			slog.Warn("clean up request trace segment", "segment", seg.Index, "error", safeError(err))
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func cleanupItem(sourceDir string, seg journal.Segment) cleanup.Item {
	item := cleanup.Item{
		SegmentID: seg.ID,
		Source:    filepath.Join(sourceDir, seg.Name),
		Prefix:    seg.Prefix,
		Index:     seg.Index,
		Size:      seg.Size,
		ModTime:   seg.ModTime,
		SHA256:    seg.SHA256,
	}
	for _, dest := range seg.Destinations {
		item.Transports = append(item.Transports, dest.Transport.Path)
	}
	return item
}

// clean deletes a confirmed segment's files. A segment whose confirmed
// submits did not all export (a diagnostic read, a dry run) never loses its
// source: only the transports go, and the entry is finished as not
// exported. The journal records this per submit, so a restart under a
// backend that exports never reads it as an export; intake reopens it.
// Reopening sends every destination again, including one that a real
// export already reached. No backend here mixes the two within a segment
// (only objectstore has a dry run, with one destination and idempotent
// keys); a backend that does, and is not resubmit safe, would need exports
// tracked per destination.
func (u *Uploader) clean(ctx context.Context, cleaner *cleanup.Cleaner, seg journal.Segment) error {
	item := cleanupItem(u.cfg.SourceDir, seg)
	if exportedEverywhere(seg) {
		_, err := cleaner.Clean(ctx, item)
		return err
	}
	if err := u.store.BeginCleaning(seg.ID); err != nil {
		return err
	}
	for _, path := range item.Transports {
		if err := removeFile(path); err != nil {
			return err
		}
	}
	return u.store.FinishNotExported(seg.ID)
}

func exportedEverywhere(seg journal.Segment) bool {
	if len(seg.Destinations) == 0 {
		return false
	}
	for _, dest := range seg.Destinations {
		if !dest.Exported {
			return false
		}
	}
	return true
}

func (u *Uploader) cleanEmpty(ctx context.Context, cleaner *cleanup.Cleaner, seg journal.Segment) error {
	if u.caps.Exports {
		_, err := cleaner.CleanEmpty(ctx, cleanupItem(u.cfg.SourceDir, seg))
		return err
	}
	// The source is kept, so a run that exports reads it again under its
	// own settings.
	return u.store.FinishNotExported(seg.ID)
}
