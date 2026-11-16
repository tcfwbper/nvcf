// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package diskguard keeps the spool from filling up. It pauses preparing
// when free space or the prepare-ahead bound runs out, bounds quarantine by
// size and age, and above the high watermark deletes per policy, oldest
// first, until the spool is down to the low watermark. Every deletion is
// counted, because each one is data that will not reach its destination.
//
// Usage is measured on the whole filesystem that holds the spool, so the
// spool needs a volume of its own. On a disk it shares with other data,
// such as an emptyDir on the node's disk, that data counts as used: the
// guard would pause preparing and delete segments without bringing usage
// below the watermark.
package diskguard

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"
)

// Metric names. Integration maps them to telemetry instruments.
const (
	CounterSegmentsEvicted   = "segments_evicted"
	CounterQuarantineEvicted = "quarantine_evicted"

	GaugeSpoolUsedBytes   = "spool_used_bytes"
	GaugeSpoolFreeBytes   = "spool_free_bytes"
	GaugePendingSegments  = "pending_segments"
	GaugePendingBytes     = "pending_bytes"
	GaugeOldestPendingAge = "oldest_pending_age_seconds"
)

// Usage is the size of the spool volume.
type Usage struct {
	Total uint64
	Free  uint64
}

// Volume reports the spool volume's size.
type Volume interface {
	Usage() (Usage, error)
}

// Candidate is one thing the guard may delete, or one pending segment for
// the gauges.
type Candidate struct {
	Key     string
	Bytes   int64
	ModTime time.Time
	// Busy marks an item that may still be written, such as a reject
	// artifact a prepare is writing. Only Enforce, which runs while nothing
	// is being prepared, removes it.
	Busy bool
}

// QuarantineStore lists and removes quarantined entries. *Quarantine
// implements it.
type QuarantineStore interface {
	Entries() ([]Candidate, error)
	Remove(key string) error
}

// Segments lists closed segments that may be evicted and evicts one: its
// source and transports are deleted and the journal records the loss. The
// list must never include the highest-index segment of a prefix. Evict wraps
// ErrDeletedNotSynced when the data is gone but a directory sync failed.
type Segments interface {
	Evictable() ([]Candidate, error)
	Evict(key string) error
}

// Counter receives event counts.
type Counter interface {
	Add(name string, delta int64)
}

// Gauges receives the spool gauges.
type Gauges interface {
	Set(name string, value float64)
}

// PauseReason says why preparing is paused.
type PauseReason string

const (
	PauseNone          PauseReason = ""
	PauseFreeSpace     PauseReason = "free_space"
	PausePrepareAhead  PauseReason = "prepare_ahead"
	PauseHighWatermark PauseReason = "high_watermark"
)

// Decision is whether the next segment may be prepared.
type Decision struct {
	Allowed bool
	Reason  PauseReason
}

// Report is what one Enforce pass did.
type Report struct {
	AboveWatermark    bool
	QuarantineEvicted int
	SegmentsEvicted   int
	FreedBytes        int64
}

// Options holds the guard's collaborators. Zero values are safe.
type Options struct {
	Counter Counter
	Gauges  Gauges
	Logger  *slog.Logger
}

// Guard applies the disk rules. Its methods are safe to call at any time;
// Enforce and Check take turns.
type Guard struct {
	// mu keeps Enforce and Check from removing the same entries at once.
	mu         sync.Mutex
	cfg        Config
	volume     Volume
	quarantine QuarantineStore
	segments   Segments
	counter    Counter
	gauges     Gauges
	logger     *slog.Logger
}

// New builds a guard. cfg must already be resolved against the batch size.
func New(cfg Config, volume Volume, quarantine QuarantineStore, segments Segments, opts Options) (*Guard, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	g := &Guard{
		cfg:        cfg,
		volume:     volume,
		quarantine: quarantine,
		segments:   segments,
		counter:    opts.Counter,
		gauges:     opts.Gauges,
		logger:     opts.Logger,
	}
	if g.counter == nil {
		g.counter = discard{}
	}
	if g.gauges == nil {
		g.gauges = discard{}
	}
	if g.logger == nil {
		g.logger = slog.Default()
	}
	for _, name := range []string{CounterSegmentsEvicted, CounterQuarantineEvicted} {
		g.counter.Add(name, 0)
	}
	return g, nil
}

// AllowPrepare reports whether one more segment, whose transport is
// estimated at estimate bytes, may be prepared while waiting segments are
// already prepared and not yet in a submitted batch. A pause only stops
// preparing; submitting what is prepared goes on.
func (g *Guard) AllowPrepare(estimate int64, waiting int) (Decision, error) {
	if waiting >= g.cfg.PrepareAhead {
		return Decision{Reason: PausePrepareAhead}, nil
	}
	usage, err := g.volume.Usage()
	if err != nil {
		return Decision{}, err
	}
	if g.aboveWatermark(usedBytes(usage), usage.Total) {
		return Decision{Reason: PauseHighWatermark}, nil
	}
	if int64(usage.Free) < max(2*estimate, g.cfg.MinFreeBytes) {
		return Decision{Reason: PauseFreeSpace}, nil
	}
	return Decision{Allowed: true}, nil
}

// Check bounds quarantine as Enforce does, but never evicts a segment and
// never removes an item that may still be written, so it may run while a
// segment is being prepared. Report.AboveWatermark says the spool is still
// above the high watermark, so Enforce must run where nothing is being
// prepared. Every deletion is counted.
func (g *Guard) Check(now time.Time) (Report, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	var report Report
	usage, err := g.volume.Usage()
	if err != nil {
		return report, err
	}
	entries, listErr := g.quarantineEntries()
	_, err = g.boundQuarantine(now, usage.Total, entries, true, &report)
	report.AboveWatermark = g.aboveWatermark(usedBytes(usage)-report.FreedBytes, usage.Total)
	g.logDeleted(report)
	return report, errors.Join(listErr, err)
}

// Enforce first bounds quarantine: entries older than QuarantineMaxAge go,
// then the oldest until quarantine fits its share of the spool. Then, above
// the high watermark, it deletes per Policy, oldest first, until the spool
// is down to the low watermark, so a writer that keeps writing does not
// trigger a deletion on every pass. Between the two watermarks nothing is
// deleted. Every deletion is counted. Call it only while no segment is being
// prepared.
func (g *Guard) Enforce(now time.Time) (Report, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	var report Report
	usage, err := g.volume.Usage()
	if err != nil {
		return report, err
	}
	entries, listErr := g.quarantineEntries()
	kept, err := g.boundQuarantine(now, usage.Total, entries, false, &report)
	errs := []error{listErr, err}
	used := usedBytes(usage) - report.FreedBytes
	if g.aboveWatermark(used, usage.Total) {
		report.AboveWatermark = true
		errs = append(errs, g.relieve(used-watermarkBytes(g.cfg.LowWatermark, usage.Total), kept, &report))
	}
	g.logDeleted(report)
	return report, errors.Join(errs...)
}

// quarantineEntries lists quarantine. An unreadable quarantine must not
// stop relief from segments, so the caller goes on with no entries and
// reports the error.
func (g *Guard) quarantineEntries() ([]Candidate, error) {
	entries, err := g.quarantine.Entries()
	if err != nil {
		return nil, fmt.Errorf("list quarantine: %w", err)
	}
	return entries, nil
}

// boundQuarantine removes entries older than QuarantineMaxAge, then the
// oldest until quarantine fits its share of a spool of total bytes. With
// skipBusy it never removes items that may still be written, but still counts
// their bytes toward the share. An entry whose removal failed is still on
// disk: it keeps counting toward the share, so the next entries go instead,
// and it is tried again next pass. It returns what may be removed and is
// kept, oldest first.
func (g *Guard) boundQuarantine(now time.Time, total uint64, entries []Candidate, skipBusy bool, report *Report) ([]Candidate, error) {
	var errs []error
	limit := int64(g.cfg.QuarantineMaxFraction * float64(total))
	var fresh []Candidate
	var held int64
	for _, e := range oldestFirst(entries) {
		if skipBusy && e.Busy {
			held += e.Bytes
			continue
		}
		if now.Sub(e.ModTime) > g.cfg.QuarantineMaxAge {
			gone, err := g.removeQuarantine(e, report)
			errs = append(errs, err)
			if !gone {
				held += e.Bytes
			}
			continue
		}
		fresh = append(fresh, e)
		held += e.Bytes
	}
	var kept []Candidate
	for _, e := range fresh {
		if held <= limit {
			kept = append(kept, e)
			continue
		}
		gone, err := g.removeQuarantine(e, report)
		errs = append(errs, err)
		if gone {
			held -= e.Bytes
		}
	}
	return kept, errors.Join(errs...)
}

func (g *Guard) logDeleted(report Report) {
	if report.QuarantineEvicted > 0 || report.SegmentsEvicted > 0 {
		g.logger.Warn("deleted spool data to stay within disk bounds",
			"quarantine_evicted", report.QuarantineEvicted,
			"segments_evicted", report.SegmentsEvicted,
			"freed_bytes", report.FreedBytes)
	}
}

// relieve frees need bytes, enough to reach the low watermark, per the
// policy: quarantine first, then the oldest closed segments.
func (g *Guard) relieve(need int64, quarantine []Candidate, report *Report) error {
	if g.cfg.Policy == PolicyNone {
		g.logger.Warn("spool above the high watermark; preparing paused and nothing deleted by policy")
		return nil
	}
	freed := int64(0)
	var errs []error
	if g.cfg.Policy == PolicyQuarantineThenOldest {
		for _, e := range quarantine {
			if freed >= need {
				break
			}
			gone, err := g.removeQuarantine(e, report)
			errs = append(errs, err)
			if gone {
				freed += e.Bytes
			}
		}
	}
	if freed < need {
		// A deleted file can free more space than its length, such as a
		// small file that fills a whole block, and a removal that failed part
		// way can free space it does not report, so usage is measured again
		// before a segment, which may not have been sent, is evicted.
		if usage, err := g.volume.Usage(); err != nil {
			errs = append(errs, fmt.Errorf("measure spool volume: %w", err))
		} else {
			need, freed = usedBytes(usage)-watermarkBytes(g.cfg.LowWatermark, usage.Total), 0
		}
	}
	if freed < need {
		segments, err := g.segments.Evictable()
		if err != nil {
			return errors.Join(append(errs, err)...)
		}
		for _, s := range oldestFirst(segments) {
			if freed >= need {
				break
			}
			if err := g.segments.Evict(s.Key); err != nil {
				errs = append(errs, fmt.Errorf("evict segment: %w", err))
				if !errors.Is(err, ErrDeletedNotSynced) {
					continue
				}
			}
			g.counter.Add(CounterSegmentsEvicted, 1)
			report.SegmentsEvicted++
			report.FreedBytes += s.Bytes
			freed += s.Bytes
		}
	}
	if freed < need {
		g.logger.Error("spool still above the low watermark after deleting everything the policy allows",
			"still_needed_bytes", need-freed)
	}
	return errors.Join(errs...)
}

// removeQuarantine deletes one entry and reports whether it is gone. A
// deletion whose directory sync failed still happened, so it is counted and
// its error returned.
func (g *Guard) removeQuarantine(e Candidate, report *Report) (bool, error) {
	err := g.quarantine.Remove(e.Key)
	if err != nil && !errors.Is(err, ErrDeletedNotSynced) {
		return false, fmt.Errorf("evict quarantine entry: %w", err)
	}
	g.counter.Add(CounterQuarantineEvicted, 1)
	report.QuarantineEvicted++
	report.FreedBytes += e.Bytes
	if err != nil {
		return true, fmt.Errorf("evict quarantine entry: %w", err)
	}
	return true, nil
}

// UpdateGauges sets the spool gauges from the volume and the segments not
// yet done.
func (g *Guard) UpdateGauges(now time.Time, pending []Candidate) error {
	usage, err := g.volume.Usage()
	if err != nil {
		return err
	}
	var bytes int64
	var oldest time.Time
	for _, p := range pending {
		bytes += p.Bytes
		if oldest.IsZero() || p.ModTime.Before(oldest) {
			oldest = p.ModTime
		}
	}
	age := 0.0
	if !oldest.IsZero() {
		age = max(now.Sub(oldest).Seconds(), 0)
	}
	g.gauges.Set(GaugeSpoolUsedBytes, float64(usedBytes(usage)))
	g.gauges.Set(GaugeSpoolFreeBytes, float64(usage.Free))
	g.gauges.Set(GaugePendingSegments, float64(len(pending)))
	g.gauges.Set(GaugePendingBytes, float64(bytes))
	g.gauges.Set(GaugeOldestPendingAge, age)
	return nil
}

func (g *Guard) aboveWatermark(used int64, total uint64) bool {
	return used > watermarkBytes(g.cfg.HighWatermark, total)
}

func watermarkBytes(fraction float64, total uint64) int64 {
	return int64(fraction * float64(total))
}

func usedBytes(u Usage) int64 {
	if u.Free > u.Total {
		return 0
	}
	return int64(u.Total - u.Free)
}

// oldestFirst returns a sorted copy, so a store that changes the slice it
// returned while entries are removed cannot change the order.
func oldestFirst(items []Candidate) []Candidate {
	items = slices.Clone(items)
	slices.SortStableFunc(items, func(a, b Candidate) int { return a.ModTime.Compare(b.ModTime) })
	return items
}

type discard struct{}

func (discard) Add(string, int64)   {}
func (discard) Set(string, float64) {}
