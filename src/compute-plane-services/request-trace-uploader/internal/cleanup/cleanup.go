// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package cleanup deletes a segment's files after every destination reported
// success, in an order that a crash can resume at any step:
//
//  1. record Cleaning in the journal,
//  2. delete the transports,
//  3. stat the source, keep it if it is no longer a regular file, and
//     re-hash it only if its size or mtime changed,
//  4. delete the source, never the highest index,
//  5. record Done.
package cleanup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// Counter names. Integration maps them to telemetry instruments.
const (
	CounterSegmentsDone  = "segments_done"
	CounterSegmentsEmpty = "segments_empty"
	CounterSourceChanged = "source_changed"
)

var (
	// ErrSourceChanged reports a source whose content no longer matches the
	// fingerprint that was uploaded. The source is kept.
	ErrSourceChanged = errors.New("source segment changed since it was uploaded")
)

// Result says how far cleanup got.
type Result string

const (
	// ResultDone means the files are gone and the journal says Done.
	ResultDone Result = "done"
	// ResultDeferred means the source is the highest index on the spool and
	// was kept. Cleanup runs again later, once Dynamo has moved on.
	ResultDeferred Result = "deferred"
)

// Journal is the part of the journal cleanup writes.
type Journal interface {
	BeginCleaning(id string) error
	FinishCleaning(id string) error
	FinishEmpty(id string) error
}

// Spool reports the highest segment index for a prefix, which Dynamo may
// still be writing.
type Spool interface {
	HighestIndex(prefix string) (index int, found bool, err error)
}

// Counter receives event counts.
type Counter interface {
	Add(name string, delta int64)
}

// Item is one segment to clean up, as recorded in the journal.
type Item struct {
	SegmentID  string
	Source     string
	Prefix     string
	Index      int
	Size       int64
	ModTime    time.Time
	SHA256     string
	Transports []string
}

// Options holds the cleaner's collaborators. Zero values are safe.
type Options struct {
	Counter Counter
	Logger  *slog.Logger
	// Hash returns the hex sha256 of a file. The default reads the file.
	Hash func(path string) (string, error)
}

// Cleaner runs the ordered cleanup. It is safe for concurrent use on
// different segments.
type Cleaner struct {
	journal Journal
	spool   Spool
	counter Counter
	logger  *slog.Logger
	hash    func(path string) (string, error)
}

// New builds a cleaner.
func New(j Journal, spool Spool, opts Options) *Cleaner {
	c := &Cleaner{journal: j, spool: spool, counter: opts.Counter, logger: opts.Logger, hash: opts.Hash}
	if c.counter == nil {
		c.counter = discardCounter{}
	}
	if c.logger == nil {
		c.logger = slog.Default()
	}
	if c.hash == nil {
		c.hash = hashFile
	}
	for _, name := range []string{CounterSegmentsDone, CounterSegmentsEmpty, CounterSourceChanged} {
		c.counter.Add(name, 0)
	}
	return c
}

// Round returns a cleaner for one cleanup round. It reads the highest index
// of each segment family once, at its first use, and keeps it for the round,
// so a round over a backlog scans the spool once per family instead of once
// per deletion. A kept index is never higher than the spool's: a writer only
// adds higher indexes, and cleanup and eviction never remove the highest.
// So a segment the writer closed during the round is kept until the next
// one, never deleted early. A failed read is not kept.
func (c *Cleaner) Round() *Cleaner {
	round := *c
	round.spool = &roundSpool{spool: c.spool, highest: map[string]highestIndex{}}
	return &round
}

type highestIndex struct {
	index int
	found bool
}

// roundSpool keeps each family's highest index for one round. A round runs
// on one goroutine.
type roundSpool struct {
	spool   Spool
	highest map[string]highestIndex
}

func (r *roundSpool) HighestIndex(prefix string) (int, bool, error) {
	if kept, ok := r.highest[prefix]; ok {
		return kept.index, kept.found, nil
	}
	index, found, err := r.spool.HighestIndex(prefix)
	if err != nil {
		return 0, false, err
	}
	r.highest[prefix] = highestIndex{index: index, found: found}
	return index, found, nil
}

// Clean deletes the files of a segment that every destination confirmed,
// then records Done. Each step can be repeated, so after a crash Clean is
// simply called again for a segment found Confirmed or Cleaning.
//
// The source is kept, and ResultDeferred returned, while it is the highest
// index on the spool. A source whose content changed is kept and reported
// as ErrSourceChanged; the segment stays in Cleaning.
func (c *Cleaner) Clean(ctx context.Context, item Item) (Result, error) {
	if err := validItem(item); err != nil {
		return "", err
	}
	if err := c.journal.BeginCleaning(item.SegmentID); err != nil {
		return "", fmt.Errorf("record cleanup start: %w", err)
	}
	for _, path := range item.Transports {
		if err := removeFile(path); err != nil {
			return "", fmt.Errorf("delete transport: %w", err)
		}
	}
	result, err := c.removeSource(ctx, item)
	if err != nil || result != ResultDone {
		return result, err
	}
	if err := c.journal.FinishCleaning(item.SegmentID); err != nil {
		return "", fmt.Errorf("record cleanup done: %w", err)
	}
	c.counter.Add(CounterSegmentsDone, 1)
	return ResultDone, nil
}

// CleanEmpty deletes the source of a segment that held no records and
// records Done. It follows the same rules as Clean for the source.
func (c *Cleaner) CleanEmpty(ctx context.Context, item Item) (Result, error) {
	if err := validItem(item); err != nil {
		return "", err
	}
	result, err := c.removeSource(ctx, item)
	if err != nil || result != ResultDone {
		return result, err
	}
	if err := c.journal.FinishEmpty(item.SegmentID); err != nil {
		return "", fmt.Errorf("record empty segment done: %w", err)
	}
	c.counter.Add(CounterSegmentsEmpty, 1)
	return ResultDone, nil
}

// removeSource deletes the source if it is not the highest index and still
// holds the content that was fingerprinted. A source already gone counts as
// deleted. Anything at the source's name that is not a regular file, such as
// a pipe or a symlink, is kept as changed.
func (c *Cleaner) removeSource(ctx context.Context, item Item) (Result, error) {
	highest, found, err := c.spool.HighestIndex(item.Prefix)
	if err != nil {
		return "", fmt.Errorf("find the highest segment index: %w", err)
	}
	if found && item.Index >= highest {
		c.logger.Info("kept the highest-index segment after upload", "segment", item.Index)
		return ResultDeferred, nil
	}
	info, err := os.Lstat(item.Source)
	if errors.Is(err, os.ErrNotExist) {
		// An earlier attempt may have deleted it and then failed to sync.
		if err := syncParent(item.Source); err != nil {
			return "", fmt.Errorf("delete source segment: %w", err)
		}
		return ResultDone, nil
	}
	if err != nil {
		return "", fmt.Errorf("stat source segment: %w", err)
	}
	if !info.Mode().IsRegular() {
		// Something else now has the source's name, such as a pipe that
		// would block a read. It is not what was uploaded.
		return "", c.sourceChanged(item)
	}
	if info.Size() != item.Size || !info.ModTime().Equal(item.ModTime) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		sum, err := c.hash(item.Source)
		if err != nil {
			return "", fmt.Errorf("re-hash source segment: %w", err)
		}
		if sum != item.SHA256 {
			return "", c.sourceChanged(item)
		}
	}
	if err := removeFile(item.Source); err != nil {
		return "", fmt.Errorf("delete source segment: %w", err)
	}
	return ResultDone, nil
}

// sourceChanged counts a source that is no longer what was uploaded and
// returns ErrSourceChanged for the caller to log. The source is kept.
func (c *Cleaner) sourceChanged(item Item) error {
	c.counter.Add(CounterSourceChanged, 1)
	return fmt.Errorf("%w: segment %d", ErrSourceChanged, item.Index)
}

func validItem(item Item) error {
	if item.SegmentID == "" || item.Source == "" || item.Prefix == "" || item.SHA256 == "" {
		return errors.New("cleanup item needs a segment id, a source, a prefix and a fingerprint")
	}
	return nil
}

// removeFile deletes path and syncs its directory, so the delete survives a
// power loss. A missing file is not an error, but its directory is still
// synced: an earlier attempt may have deleted it and then failed to sync.
func removeFile(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncParent(path)
}

// syncParent syncs the directory that held path. A directory that is gone
// holds nothing to sync.
func syncParent(path string) error {
	err := syncDir(filepath.Dir(path))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// onSync lets tests observe directory syncs.
var onSync func(dir string)

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := errors.Join(dir.Sync(), dir.Close()); err != nil {
		return err
	}
	if onSync != nil {
		onSync(path)
	}
	return nil
}

func hashFile(path string) (string, error) {
	f, err := openRegular(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// openRegular opens path for reading if it is a regular file. It does not
// follow a final symlink and does not block on a pipe, and it checks the
// type of what it opened, so a file swapped after the stat is refused.
func openRegular(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|openNoFollow, 0)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() {
		f.Close()
		return nil, fmt.Errorf("%s is not a regular file", info.Name())
	}
	return f, nil
}

type discardCounter struct{}

func (discardCounter) Add(string, int64) {}
