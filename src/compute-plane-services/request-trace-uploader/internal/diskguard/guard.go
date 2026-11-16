// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package diskguard keeps the spool from filling up. It pauses preparing
// when free space or the prepare-ahead bound runs out, bounds quarantine by
// size and age, and above the high watermark deletes per policy, oldest
// first, until the spool is down to the low watermark. Every deletion is
// counted, because each one is data that will not reach its destination.
//
// Usage is measured on the whole filesystem that holds the spool. On a disk
// the spool shares with other data, such as an emptyDir on the node's disk,
// that data counts as used, and the spool's own size limit is not seen. A
// budget fixes both: the files the spool holds are measured against the
// spool's own size as well, and the fuller of the budget and the filesystem
// sets the watermark, while the free-space floor holds in both.
//
// The free-space floor is at most half the share of the spool that the high
// watermark leaves free, of the smaller of the filesystem and the budget, so
// a small spool that relief brings back to its watermark prepares again.
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
	// Budget, when set, bounds the spool to its own size next to its
	// filesystem.
	Budget Budget
}

// Guard applies the disk rules. Its methods are safe to call at any time;
// Enforce and Check take turns.
type Guard struct {
	// mu keeps Enforce and Check from removing the same entries at once.
	mu         sync.Mutex
	cfg        Config
	volume     Volume
	budget     Budget
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
		budget:     opts.Budget,
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
	r, err := g.room()
	if err != nil {
		return Decision{}, err
	}
	if r.excess > 0 {
		return Decision{Reason: PauseHighWatermark}, nil
	}
	if r.free < max(2*estimate, r.floor) {
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
	r, measureErr := g.room()
	if measureErr != nil && !errors.As(measureErr, new(budgetError)) {
		return report, measureErr
	}
	errs := []error{measureErr}
	budgetReported := measureErr != nil
	entries, listErr := g.quarantineEntries()
	_, err := g.boundQuarantine(now, r.total, entries, true, &report)
	errs = append(errs, listErr, err)
	l := r.load
	if report.FreedBytes > 0 {
		l = g.measureAgain(l.less(report.FreedBytes), &budgetReported, &errs)
	}
	report.AboveWatermark = l.above() != volumes{}
	g.logDeleted(report)
	return report, errors.Join(errs...)
}

// Enforce first bounds quarantine: entries older than QuarantineMaxAge go,
// then the oldest until quarantine fits its share of the spool. Then, above
// the high watermark, it deletes per Policy, oldest first, until the spool
// is down to the low watermark, so a writer that keeps writing does not
// trigger a deletion on every pass. Between the two watermarks nothing is
// deleted. With a budget, each of the budget and the filesystem that is
// above its high watermark is freed to its own low watermark. Every deletion
// is counted. Call it only while no segment is being prepared.
func (g *Guard) Enforce(now time.Time) (Report, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	var report Report
	r, err := g.room()
	if err != nil && !errors.As(err, new(budgetError)) {
		return report, err
	}
	// A budget that cannot be measured must not stop relief either: the
	// filesystem was measured, so its pass still runs.
	errs := []error{err}
	budgetReported := err != nil
	entries, listErr := g.quarantineEntries()
	kept, err := g.boundQuarantine(now, r.total, entries, false, &report)
	errs = append(errs, listErr, err)
	l := r.load
	if report.FreedBytes > 0 {
		// A deletion can free less than it was sized at, such as a file
		// still linked under another name, so the spool is measured again.
		l = g.measureAgain(l.less(report.FreedBytes), &budgetReported, &errs)
	}
	// A volume still above its high watermark once quarantine is bounded
	// starts relief, which frees it to its low watermark.
	if started := l.above(); started != (volumes{}) {
		_, need := started.of(l)
		report.AboveWatermark = true
		errs = append(errs, g.relieve(need, l, started, kept, &report, &budgetReported))
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

// relieve frees need bytes, enough to bring the volumes that started relief
// down to their low watermarks, per the policy: quarantine first, then the
// oldest closed segments. l is how far each volume is above its watermarks
// now; budgetReported says a budget measuring error was already reported.
func (g *Guard) relieve(need int64, l load, started volumes, quarantine []Candidate, report *Report, budgetReported *bool) error {
	if g.cfg.Policy == PolicyNone {
		g.logger.Warn("spool above the high watermark; preparing paused and nothing deleted by policy")
		return nil
	}
	var errs []error
	// freed counts the bytes deleted since the last measure, by their sizes.
	freed := int64(0)
	// measure measures the spool again and reports whether the target is
	// met. A deletion can free more or less space than it was sized at, such
	// as a small file that fills a whole block, a file still linked under
	// another name, or a removal that failed part way, so sizes alone never
	// end relief or start on segments.
	measure := func() bool {
		l = g.measureAgain(l.less(freed), budgetReported, &errs)
		_, need = started.of(l)
		freed = 0
		return need <= 0
	}
	// settled measures only when something was deleted since the last
	// measure.
	settled := func() bool {
		if freed > 0 {
			return measure()
		}
		return need <= 0
	}
	if g.cfg.Policy == PolicyQuarantineThenOldest {
		for _, e := range quarantine {
			if freed >= need && settled() {
				return errors.Join(errs...)
			}
			gone, err := g.removeQuarantine(e, report)
			errs = append(errs, err)
			if gone {
				freed += e.Bytes
			}
		}
	}
	// A segment may not have been sent, so it goes only once the spool is
	// measured to need it, including space a failed removal freed.
	if measure() {
		return errors.Join(errs...)
	}
	segments, err := g.segments.Evictable()
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	for _, s := range oldestFirst(segments) {
		if freed >= need && settled() {
			return errors.Join(errs...)
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
	if freed >= need && settled() {
		return errors.Join(errs...)
	}
	g.logger.Error("spool still above the low watermark after deleting everything the policy allows",
		"still_needed_bytes", need-freed)
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

// UpdateGauges sets the spool gauges from the fuller volume, the least free
// space, and the segments not yet done.
func (g *Guard) UpdateGauges(now time.Time, pending []Candidate) error {
	r, err := g.room()
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
	g.gauges.Set(GaugeSpoolUsedBytes, float64(r.used))
	g.gauges.Set(GaugeSpoolFreeBytes, float64(r.free))
	g.gauges.Set(GaugePendingSegments, float64(len(pending)))
	g.gauges.Set(GaugePendingBytes, float64(bytes))
	g.gauges.Set(GaugeOldestPendingAge, age)
	return nil
}

// room is the spool's room across the filesystem and, when set, the
// budget.
type room struct {
	// used is the fuller volume's used bytes.
	used int64
	// excess is how far the fuller volume is above its high watermark, in
	// bytes; zero or less is at or below it.
	excess int64
	// load is how far each volume is above its watermarks, for relief.
	load load
	// free is the least free space of the volumes.
	free int64
	// total is the smallest volume, the size quarantine's share is of.
	total uint64
	// floor is the least free space preparing leaves.
	floor int64
}

// room measures the filesystem and the budget. The fuller is the one
// further above, or less far below, its high watermark. When only the
// budget cannot be measured, it returns the filesystem's room with a
// budgetError.
func (g *Guard) room() (room, error) {
	r, err := g.measure(g.volume)
	if err != nil {
		return room{}, err
	}
	var budgetErr error
	if g.budget != nil {
		size, held, err := g.budget.Held()
		if err != nil {
			budgetErr = budgetError{err}
		} else {
			excess := held - watermarkBytes(g.cfg.HighWatermark, uint64(size))
			r.load.budget = pressure{excess: excess, need: held - watermarkBytes(g.cfg.LowWatermark, uint64(size))}
			r.load.haveBudget = true
			if excess > r.excess {
				r.used, r.excess = held, excess
			}
			r.free = min(r.free, max(size-held, 0))
			r.total = min(r.total, uint64(size))
		}
	}
	// Relief may stop at the high watermark, as a deletion that fails part
	// way does, so the floor must sit below the free share that watermark
	// leaves, or a small spool never prepares again.
	r.floor = min(g.cfg.MinFreeBytes, int64((1-g.cfg.HighWatermark)/2*float64(r.total)))
	return r, budgetErr
}

// budgetError is a failure to measure the budget. The room returned with it
// is the filesystem's alone, which relief may still use; preparing does not.
type budgetError struct{ err error }

func (e budgetError) Error() string { return "measure spool budget: " + e.err.Error() }

func (e budgetError) Unwrap() error { return e.err }

func (g *Guard) measure(v Volume) (room, error) {
	usage, err := v.Usage()
	if err != nil {
		return room{}, err
	}
	used := usedBytes(usage)
	excess := used - watermarkBytes(g.cfg.HighWatermark, usage.Total)
	return room{
		used:   used,
		excess: excess,
		load:   load{fs: pressure{excess: excess, need: used - watermarkBytes(g.cfg.LowWatermark, usage.Total)}},
		free:   int64(usage.Free),
		total:  usage.Total,
	}, nil
}

// pressure is how far one volume is above its high watermark (excess) and
// above its low watermark (need), in bytes; zero or less is at or below it.
type pressure struct{ excess, need int64 }

// load is the pressure on the filesystem and, when it was measured, the
// budget.
type load struct {
	fs, budget pressure
	haveBudget bool
}

// less is the load after freed bytes came off both volumes.
func (l load) less(freed int64) load {
	l.fs.excess, l.fs.need = l.fs.excess-freed, l.fs.need-freed
	l.budget.excess, l.budget.need = l.budget.excess-freed, l.budget.need-freed
	return l
}

// volumes says which volumes started relief.
type volumes struct{ fs, budget bool }

// above names the volumes above their high watermarks.
func (l load) above() volumes {
	return volumes{fs: l.fs.excess > 0, budget: l.haveBudget && l.budget.excess > 0}
}

// of is how far the volumes that started relief are still above their high
// and low watermarks: the furthest of them. Relief frees each to its own
// low watermark, so a volume only between its watermarks drives nothing.
func (v volumes) of(l load) (excess, need int64) {
	switch {
	case v.fs && v.budget:
		return max(l.fs.excess, l.budget.excess), max(l.fs.need, l.budget.need)
	case v.fs:
		return l.fs.excess, l.fs.need
	case v.budget:
		return l.budget.excess, l.budget.need
	}
	return 0, 0
}

// measureAgain measures the volumes again where it can. A volume that cannot
// be measured keeps its estimate, so relief never moves to another volume's
// target. A budget error is reported once per pass.
func (g *Guard) measureAgain(estimate load, budgetReported *bool, errs *[]error) load {
	r, err := g.room()
	if err != nil && !errors.As(err, new(budgetError)) {
		*errs = append(*errs, fmt.Errorf("measure spool: %w", err))
		return estimate
	}
	next := estimate
	next.fs = r.load.fs
	if err != nil {
		if !*budgetReported {
			*errs = append(*errs, err)
			*budgetReported = true
		}
		return next
	}
	if r.load.haveBudget {
		next.budget, next.haveBudget = r.load.budget, true
	}
	return next
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
