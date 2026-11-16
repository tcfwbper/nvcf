// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package diskguard_test

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/diskguard"
)

var now = time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)

type fakeVolume struct{ usage diskguard.Usage }

func (v *fakeVolume) Usage() (diskguard.Usage, error) { return v.usage, nil }

// fakeStore serves both quarantine and segments, and records removals in
// order.
type fakeStore struct {
	kind string
	// volume, when set, gets the freed space back on each removal: the
	// item's diskBytes entry, or its logical size.
	volume    *fakeVolume
	diskBytes map[string]int64
	// partial frees this many bytes for a key whose removal fails.
	partial    map[string]int64
	items      []diskguard.Candidate
	removed    *[]string
	unsynced   map[string]bool
	fail       map[string]error
	attempts   map[string]int
	entriesErr error
}

func (s *fakeStore) Entries() ([]diskguard.Candidate, error)   { return s.items, s.entriesErr }
func (s *fakeStore) Evictable() ([]diskguard.Candidate, error) { return s.items, nil }
func (s *fakeStore) Remove(key string) error                   { return s.drop(key) }
func (s *fakeStore) Evict(key string) error                    { return s.drop(key) }

func (s *fakeStore) drop(key string) error {
	if s.attempts == nil {
		s.attempts = map[string]int{}
	}
	s.attempts[key]++
	if err := s.fail[key]; err != nil {
		if s.volume != nil {
			s.volume.usage.Free += uint64(s.partial[key])
		}
		return err
	}
	for i, item := range s.items {
		if item.Key == key {
			s.items = append(s.items[:i], s.items[i+1:]...)
			*s.removed = append(*s.removed, s.kind+":"+key)
			if s.volume != nil {
				freed, ok := s.diskBytes[key]
				if !ok {
					freed = item.Bytes
				}
				s.volume.usage.Free += uint64(freed)
			}
			if s.unsynced[key] {
				return fmt.Errorf("remove %s: %w", key, diskguard.ErrDeletedNotSynced)
			}
			return nil
		}
	}
	return errors.New("unknown key")
}

type metrics struct {
	counts map[string]int64
	gauges map[string]float64
}

func newMetrics() *metrics {
	return &metrics{counts: map[string]int64{}, gauges: map[string]float64{}}
}

func (m *metrics) Add(name string, delta int64)   { m.counts[name] += delta }
func (m *metrics) Set(name string, value float64) { m.gauges[name] = value }

func item(key string, bytes int64, age time.Duration) diskguard.Candidate {
	return diskguard.Candidate{Key: key, Bytes: bytes, ModTime: now.Add(-age)}
}

type world struct {
	volume     *fakeVolume
	quarantine *fakeStore
	segments   *fakeStore
	removed    []string
	metrics    *metrics
}

func newWorld(total, free uint64) *world {
	w := &world{volume: &fakeVolume{usage: diskguard.Usage{Total: total, Free: free}}, metrics: newMetrics()}
	w.quarantine = &fakeStore{kind: "quarantine", removed: &w.removed, volume: w.volume}
	w.segments = &fakeStore{kind: "segment", removed: &w.removed, volume: w.volume}
	return w
}

func (w *world) guard(t *testing.T, cfg diskguard.Config) *diskguard.Guard {
	t.Helper()
	g, err := diskguard.New(cfg, w.volume, w.quarantine, w.segments, diskguard.Options{Counter: w.metrics, Gauges: w.metrics})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return g
}

func TestConfigDefaultsAndValidation(t *testing.T) {
	defaults := diskguard.DefaultConfig()
	if err := defaults.Validate(); err != nil {
		t.Fatalf("defaults invalid: %v", err)
	}
	// Deletion is a near-full backstop: an alert at about 70% and a runbook
	// come first. It starts at 85% and frees down to 75%, as the earlier
	// uploader prunes, which leaves room for a 256 MiB segment on a 2 GiB
	// spool when it starts.
	if defaults.Policy != diskguard.PolicyQuarantineThenOldest || defaults.HighWatermark != 0.85 || defaults.LowWatermark != 0.75 ||
		defaults.MinFreeBytes != 512<<20 || defaults.QuarantineMaxFraction != 0.2 || defaults.QuarantineMaxAge != 7*24*time.Hour ||
		defaults.PrepareAhead != 16 {
		t.Fatalf("defaults = %+v, want the decided values", defaults)
	}
	cases := []struct {
		name   string
		mutate func(*diskguard.Config)
	}{
		{"negative free floor", func(c *diskguard.Config) { c.MinFreeBytes = -1 }},
		{"zero prepare ahead", func(c *diskguard.Config) { c.PrepareAhead = 0 }},
		{"watermark zero", func(c *diskguard.Config) { c.HighWatermark = 0 }},
		{"watermark one", func(c *diskguard.Config) { c.HighWatermark = 1 }},
		{"low watermark zero", func(c *diskguard.Config) { c.LowWatermark = 0 }},
		{"low watermark negative", func(c *diskguard.Config) { c.LowWatermark = -0.1 }},
		{"low watermark equals the high one", func(c *diskguard.Config) { c.LowWatermark = c.HighWatermark }},
		{"low watermark above the high one", func(c *diskguard.Config) { c.LowWatermark = 0.95 }},
		{"low watermark not a number", func(c *diskguard.Config) { c.LowWatermark = math.NaN() }},
		{"unknown policy", func(c *diskguard.Config) { c.Policy = "evict-newest" }},
		{"empty policy", func(c *diskguard.Config) { c.Policy = "" }},
		{"quarantine fraction zero", func(c *diskguard.Config) { c.QuarantineMaxFraction = 0 }},
		{"quarantine fraction above one", func(c *diskguard.Config) { c.QuarantineMaxFraction = 1.5 }},
		{"quarantine age zero", func(c *diskguard.Config) { c.QuarantineMaxAge = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := defaults
			tc.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatalf("Validate accepted %s", tc.name)
			}
			if _, err := diskguard.New(cfg, &fakeVolume{}, &fakeStore{}, &fakeStore{}, diskguard.Options{}); err == nil {
				t.Fatalf("New accepted %s", tc.name)
			}
		})
	}
}

func TestLowPrepareAheadIsRaisedToTheBatchSize(t *testing.T) {
	cfg := diskguard.DefaultConfig()
	cfg.PrepareAhead = 4
	resolved, warnings := cfg.Resolve(16)
	if resolved.PrepareAhead != 16 || len(warnings) != 1 || !strings.Contains(warnings[0], "16") {
		t.Fatalf("Resolve = %d with %q, want 16 and one warning", resolved.PrepareAhead, warnings)
	}
	cfg.PrepareAhead = 32
	if resolved, warnings := cfg.Resolve(16); resolved.PrepareAhead != 32 || len(warnings) != 0 {
		t.Fatalf("Resolve kept %d with %q, want 32 unchanged", resolved.PrepareAhead, warnings)
	}
}

func TestAllowPrepare(t *testing.T) {
	const mib = uint64(1) << 20
	cases := []struct {
		name        string
		total, free uint64
		estimate    int64
		waiting     int
		want        diskguard.PauseReason
	}{
		{"room to prepare", 4096 * mib, 2048 * mib, 30 << 20, 3, diskguard.PauseNone},
		{"below the floor", 2048 * mib, 500 * mib, 30 << 20, 0, diskguard.PauseFreeSpace},
		{"twice the estimate is above the floor", 2048 * mib, 900 * mib, 600 << 20, 0, diskguard.PauseFreeSpace},
		{"prepare ahead is full", 4096 * mib, 2048 * mib, 30 << 20, 16, diskguard.PausePrepareAhead},
		{"between the watermarks", 100 * 1024 * mib, 12 * 1024 * mib, 1, 0, diskguard.PauseNone},
		{"above the high watermark", 100 * 1024 * mib, 5 * 1024 * mib, 1, 0, diskguard.PauseHighWatermark},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(tc.total, tc.free)
			got, err := w.guard(t, watermarks(0.90, 0.80)).AllowPrepare(tc.estimate, tc.waiting)
			if err != nil {
				t.Fatalf("AllowPrepare: %v", err)
			}
			if got.Reason != tc.want || got.Allowed != (tc.want == diskguard.PauseNone) {
				t.Fatalf("AllowPrepare = %+v, want reason %q", got, tc.want)
			}
		})
	}
}

func TestQuarantineOlderThanTheMaxAgeIsEvicted(t *testing.T) {
	w := newWorld(1_000_000, 900_000)
	w.quarantine.items = []diskguard.Candidate{item("fresh", 10, 24*time.Hour), item("stale", 10, 8*24*time.Hour)}

	report, err := w.guard(t, diskguard.DefaultConfig()).Enforce(now)
	if err != nil {
		t.Fatalf("Enforce: %v", err)
	}
	if strings.Join(w.removed, ",") != "quarantine:stale" || report.QuarantineEvicted != 1 {
		t.Fatalf("removed %v (report %+v), want only the stale entry", w.removed, report)
	}
	if w.metrics.counts[diskguard.CounterQuarantineEvicted] != 1 {
		t.Fatalf("%s = %d, want 1", diskguard.CounterQuarantineEvicted, w.metrics.counts[diskguard.CounterQuarantineEvicted])
	}
}

func TestQuarantineAboveItsShareOfTheSpoolIsEvictedOldestFirst(t *testing.T) {
	// The spool is 1000 bytes, so quarantine may hold 200.
	w := newWorld(1000, 700)
	w.quarantine.items = []diskguard.Candidate{
		item("newest", 100, time.Hour),
		item("oldest", 150, 3*time.Hour),
		item("rejects/seg-1.jsonl.gz", 50, 2*time.Hour),
	}

	report, err := w.guard(t, diskguard.DefaultConfig()).Enforce(now)
	if err != nil {
		t.Fatalf("Enforce: %v", err)
	}
	if strings.Join(w.removed, ",") != "quarantine:oldest" || report.FreedBytes != 150 {
		t.Fatalf("removed %v (report %+v), want only the oldest entry", w.removed, report)
	}
}

func TestAboveTheWatermarkQuarantineGoesFirstThenTheOldestSegments(t *testing.T) {
	// F25: 950 of 1000 bytes used is above the 900 high watermark, so the
	// spool is freed down to the 800 low watermark: 150 must go.
	w := newWorld(1000, 50)
	w.quarantine.items = []diskguard.Candidate{item("q-new", 40, time.Hour), item("q-old", 30, 2*time.Hour)}
	w.segments.items = []diskguard.Candidate{item("s-mid", 60, 4*time.Hour), item("s-old", 50, 5*time.Hour), item("s-new", 70, 3*time.Hour)}

	report, err := w.guard(t, watermarks(0.90, 0.80)).Enforce(now)
	if err != nil {
		t.Fatalf("Enforce: %v", err)
	}
	if got := strings.Join(w.removed, ","); got != "quarantine:q-old,quarantine:q-new,segment:s-old,segment:s-mid" {
		t.Fatalf("eviction order = %s, want quarantine oldest first, then the oldest segments down to the low watermark", got)
	}
	if !report.AboveWatermark || report.QuarantineEvicted != 2 || report.SegmentsEvicted != 2 || report.FreedBytes != 180 {
		t.Fatalf("report = %+v, want 2 quarantine and 2 segments evicted, 180 bytes", report)
	}
	if w.metrics.counts[diskguard.CounterSegmentsEvicted] != 2 || w.metrics.counts[diskguard.CounterQuarantineEvicted] != 2 {
		t.Fatalf("counts = %v, want every eviction counted", w.metrics.counts)
	}
}

func TestBetweenTheWatermarksNothingIsEvicted(t *testing.T) {
	// 880 of 1000 bytes used: above the 800 low watermark but not above the
	// 900 high one, so nothing triggers a deletion.
	w := newWorld(1000, 120)
	w.quarantine.items = []diskguard.Candidate{item("q", 40, time.Hour)}
	w.segments.items = []diskguard.Candidate{item("s", 70, time.Hour)}

	report, err := w.guard(t, watermarks(0.90, 0.80)).Enforce(now)
	if err != nil || report.AboveWatermark || len(w.removed) != 0 {
		t.Fatalf("Enforce = %+v, %v, removed %v; want nothing deleted between the watermarks", report, err, w.removed)
	}
}

func TestFreeingToTheLowWatermarkEvictsOnlyWhatIsListed(t *testing.T) {
	// 950 of 1000 bytes used and a low watermark of 500: 450 must go, but
	// only 180 bytes are listed. A segment in a batch that may have been
	// sent is never listed, so the guard stops there and touches nothing else.
	w := newWorld(1000, 50)
	w.quarantine.items = []diskguard.Candidate{item("q", 30, time.Hour)}
	w.segments.items = []diskguard.Candidate{item("s-new", 70, time.Hour), item("s-old", 80, 2*time.Hour)}
	cfg := diskguard.DefaultConfig()
	cfg.LowWatermark = 0.5

	report, err := w.guard(t, cfg).Enforce(now)
	if err != nil {
		t.Fatalf("Enforce: %v", err)
	}
	if got := strings.Join(w.removed, ","); got != "quarantine:q,segment:s-old,segment:s-new" {
		t.Fatalf("eviction order = %s, want every listed item, quarantine first, then oldest first", got)
	}
	if report.QuarantineEvicted != 1 || report.SegmentsEvicted != 2 || report.FreedBytes != 180 {
		t.Fatalf("report = %+v, want 1 quarantine and 2 segments evicted, 180 bytes", report)
	}
	if w.metrics.counts[diskguard.CounterSegmentsEvicted] != 2 || w.metrics.counts[diskguard.CounterQuarantineEvicted] != 1 {
		t.Fatalf("counts = %v, want every eviction counted", w.metrics.counts)
	}
}

// A quarantine that cannot be listed still lets the watermark pass relieve
// the spool from segments; the listing error is reported.
func TestAnUnreadableQuarantineStillLetsTheWatermarkPassRun(t *testing.T) {
	w := newWorld(1000, 50)
	unreadable := errors.New("quarantine entry unreadable")
	w.quarantine.entriesErr = unreadable
	w.segments.items = []diskguard.Candidate{item("s-old", 150, 5*time.Hour), item("s-new", 70, 3*time.Hour)}

	report, err := w.guard(t, watermarks(0.85, 0.80)).Enforce(now)
	if !errors.Is(err, unreadable) {
		t.Fatalf("Enforce error = %v, want the quarantine listing error reported", err)
	}
	if got := strings.Join(w.removed, ","); got != "segment:s-old" || !report.AboveWatermark {
		t.Fatalf("evicted = %q, above watermark %v; want the oldest segment evicted", got, report.AboveWatermark)
	}
}

func TestOldestPolicyKeepsQuarantine(t *testing.T) {
	w := newWorld(1000, 50)
	w.quarantine.items = []diskguard.Candidate{item("q", 40, time.Hour)}
	w.segments.items = []diskguard.Candidate{item("s-new", 70, time.Hour), item("s-old", 60, 2*time.Hour)}
	cfg := diskguard.DefaultConfig()
	cfg.Policy = diskguard.PolicyOldest

	if _, err := w.guard(t, cfg).Enforce(now); err != nil {
		t.Fatalf("Enforce: %v", err)
	}
	if got := strings.Join(w.removed, ","); got != "segment:s-old,segment:s-new" {
		t.Fatalf("eviction order = %s, want segments only, oldest first", got)
	}
}

func TestNonePolicyOnlyPauses(t *testing.T) {
	w := newWorld(1000, 50)
	w.quarantine.items = []diskguard.Candidate{item("q", 40, time.Hour)}
	w.segments.items = []diskguard.Candidate{item("s", 70, time.Hour)}
	cfg := diskguard.DefaultConfig()
	cfg.Policy = diskguard.PolicyNone
	g := w.guard(t, cfg)

	report, err := g.Enforce(now)
	if err != nil || !report.AboveWatermark || len(w.removed) != 0 {
		t.Fatalf("Enforce = %+v, %v, removed %v; want above the watermark with nothing deleted", report, err, w.removed)
	}
	if d, _ := g.AllowPrepare(1, 0); d.Reason != diskguard.PauseHighWatermark {
		t.Fatalf("AllowPrepare = %+v, want paused at the high watermark", d)
	}
}

func TestBelowTheWatermarkNothingIsEvicted(t *testing.T) {
	w := newWorld(1000, 200)
	w.segments.items = []diskguard.Candidate{item("s", 70, time.Hour)}
	report, err := w.guard(t, diskguard.DefaultConfig()).Enforce(now)
	if err != nil || report.AboveWatermark || len(w.removed) != 0 {
		t.Fatalf("Enforce = %+v, %v, removed %v; want nothing", report, err, w.removed)
	}
}

func TestGaugesFollowTheSpool(t *testing.T) {
	w := newWorld(1000, 400)
	g := w.guard(t, diskguard.DefaultConfig())
	pending := []diskguard.Candidate{item("a", 100, 10*time.Minute), item("b", 50, time.Hour)}

	if err := g.UpdateGauges(now, pending); err != nil {
		t.Fatalf("UpdateGauges: %v", err)
	}
	want := map[string]float64{
		diskguard.GaugeSpoolUsedBytes:   600,
		diskguard.GaugeSpoolFreeBytes:   400,
		diskguard.GaugePendingSegments:  2,
		diskguard.GaugePendingBytes:     150,
		diskguard.GaugeOldestPendingAge: 3600,
	}
	for name, value := range want {
		if w.metrics.gauges[name] != value {
			t.Errorf("%s = %v, want %v", name, w.metrics.gauges[name], value)
		}
	}

	w.volume.usage.Free = 550
	if err := g.UpdateGauges(now, pending[:1]); err != nil {
		t.Fatalf("UpdateGauges: %v", err)
	}
	if w.metrics.gauges[diskguard.GaugePendingSegments] != 1 || w.metrics.gauges[diskguard.GaugeOldestPendingAge] != 600 ||
		w.metrics.gauges[diskguard.GaugeSpoolUsedBytes] != 450 {
		t.Fatalf("gauges after a segment left = %v", w.metrics.gauges)
	}
	if err := g.UpdateGauges(now, nil); err != nil || w.metrics.gauges[diskguard.GaugeOldestPendingAge] != 0 {
		t.Fatalf("empty spool age = %v, %v; want 0", w.metrics.gauges[diskguard.GaugeOldestPendingAge], err)
	}
}

func TestQuarantineDirectory(t *testing.T) {
	spool := t.TempDir()
	q := diskguard.NewQuarantine(filepath.Join(spool, ".rtu-quarantine"))
	source := filepath.Join(spool, "request-trace.000003.jsonl.gz")
	transport := filepath.Join(spool, "seg-3.parquet")
	for path, size := range map[string]int{source: 40, transport: 20} {
		if err := os.WriteFile(path, make([]byte, size), 0o640); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if err := q.Admit("seg-3", source, transport); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if _, err := os.Stat(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("quarantined source left in the spool")
	}
	if err := os.MkdirAll(q.RejectsDir(), 0o750); err != nil {
		t.Fatalf("create rejects dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(q.RejectsDir(), "seg-1.jsonl.gz"), make([]byte, 7), 0o640); err != nil {
		t.Fatalf("write reject artifact: %v", err)
	}

	entries, err := q.Entries()
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	if len(entries) != 2 || entries[0].Key != "rejects/seg-1.jsonl.gz" || entries[0].Bytes != 7 ||
		entries[1].Key != "seg-3" || entries[1].Bytes != 60 || entries[1].ModTime.IsZero() {
		t.Fatalf("entries = %+v, want the reject artifact (7 bytes) and seg-3 (60 bytes)", entries)
	}

	for _, bad := range []string{"../escape", "rejects/../../escape", "", "rejects", "a/b/c"} {
		if err := q.Remove(bad); err == nil {
			t.Fatalf("Remove accepted %q", bad)
		}
	}
	for _, e := range entries {
		if err := q.Remove(e.Key); err != nil {
			t.Fatalf("Remove %s: %v", e.Key, err)
		}
	}
	if entries, err := q.Entries(); err != nil || len(entries) != 0 {
		t.Fatalf("entries after removal = %v, %v; want none", entries, err)
	}
	if err := q.Admit("../escape", transport); err == nil {
		t.Fatal("Admit accepted an id that escapes the quarantine")
	}
}

func TestQuarantineThatDoesNotExistYetIsEmpty(t *testing.T) {
	q := diskguard.NewQuarantine(filepath.Join(t.TempDir(), "missing"))
	if entries, err := q.Entries(); err != nil || len(entries) != 0 {
		t.Fatalf("Entries = %v, %v; want none", entries, err)
	}
}

func TestStatVolumeReportsTheSpool(t *testing.T) {
	usage, err := diskguard.StatVolume(t.TempDir()).Usage()
	if err != nil {
		t.Fatalf("Usage: %v", err)
	}
	if usage.Total == 0 || usage.Free > usage.Total {
		t.Fatalf("usage = %+v, want a total with free space within it", usage)
	}
}

func TestADeletionWhoseSyncFailedIsStillCounted(t *testing.T) {
	w := newWorld(1000, 50)
	w.quarantine.items = []diskguard.Candidate{item("q-old", 30, 2*time.Hour)}
	w.quarantine.unsynced = map[string]bool{"q-old": true}
	w.segments.items = []diskguard.Candidate{item("s-old", 80, 5*time.Hour)}
	w.segments.unsynced = map[string]bool{"s-old": true}

	report, err := w.guard(t, diskguard.DefaultConfig()).Enforce(now)
	if !errors.Is(err, diskguard.ErrDeletedNotSynced) {
		t.Fatalf("Enforce error = %v, want the sync failure reported", err)
	}
	if report.QuarantineEvicted != 1 || report.SegmentsEvicted != 1 || report.FreedBytes != 110 {
		t.Fatalf("report = %+v, want both deletions counted", report)
	}
	if w.metrics.counts[diskguard.CounterQuarantineEvicted] != 1 || w.metrics.counts[diskguard.CounterSegmentsEvicted] != 1 {
		t.Fatalf("counts = %v, want both deletions counted", w.metrics.counts)
	}
}

// An entry whose removal failed is still on disk, so it still counts toward
// the quarantine share and the next entries go instead. It is tried once per
// pass, also when the watermark pass runs after it.
func TestAFailedQuarantineRemovalStillCountsTowardTheShare(t *testing.T) {
	cases := []struct {
		name        string
		free        uint64
		quarantine  []diskguard.Candidate
		segments    []diskguard.Candidate
		wantRemoved string
	}{
		{
			// The spool is 1000 bytes, so quarantine may hold 200; it holds 350.
			name:        "oldest over the share",
			free:        900,
			quarantine:  []diskguard.Candidate{item("stuck", 150, 3*time.Hour), item("older", 100, 2*time.Hour), item("newer", 100, time.Hour)},
			wantRemoved: "quarantine:older,quarantine:newer",
		},
		{
			name:        "older than the max age",
			free:        900,
			quarantine:  []diskguard.Candidate{item("stuck", 150, 8*24*time.Hour), item("older", 100, 2*time.Hour), item("newer", 100, time.Hour)},
			wantRemoved: "quarantine:older,quarantine:newer",
		},
		{
			// 950 of 1000 bytes used: after the share pass frees 60, the
			// spool is above the 850 high watermark, and the watermark pass
			// takes a segment to reach the 800 low watermark.
			name:        "over the share and above the watermark",
			free:        50,
			quarantine:  []diskguard.Candidate{item("stuck", 150, 3*time.Hour), item("other", 60, 2*time.Hour)},
			segments:    []diskguard.Candidate{item("s-old", 100, 5*time.Hour)},
			wantRemoved: "quarantine:other,segment:s-old",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(1000, tc.free)
			w.quarantine.items = tc.quarantine
			w.segments.items = tc.segments
			busy := errors.New("entry busy")
			w.quarantine.fail = map[string]error{"stuck": busy}

			_, err := w.guard(t, watermarks(0.85, 0.80)).Enforce(now)
			if !errors.Is(err, busy) {
				t.Fatalf("Enforce error = %v, want the failed removal reported", err)
			}
			if got := strings.Join(w.removed, ","); got != tc.wantRemoved {
				t.Fatalf("removed %s, want %s", got, tc.wantRemoved)
			}
			if n := w.quarantine.attempts["stuck"]; n != 1 {
				t.Fatalf("tried the failing entry %d times in one pass, want 1", n)
			}
		})
	}
}

// Evict and Remove may change the slice the store returned. The guard keeps
// its own copy, so it still deletes oldest first.
func TestEvictionOrderDoesNotDependOnTheStoreSlice(t *testing.T) {
	t.Run("segments", func(t *testing.T) {
		// 950 of 1000 bytes used; freeing to the 850 low watermark takes 100.
		w := newWorld(1000, 50)
		w.segments.items = []diskguard.Candidate{item("old", 50, 5*time.Hour), item("middle", 60, 4*time.Hour), item("new", 70, 3*time.Hour)}

		if _, err := w.guard(t, watermarks(0.90, 0.85)).Enforce(now); err != nil {
			t.Fatalf("Enforce: %v", err)
		}
		if got := strings.Join(w.removed, ","); got != "segment:old,segment:middle" {
			t.Fatalf("evicted %s, want the two oldest segments", got)
		}
	})
	t.Run("quarantine", func(t *testing.T) {
		w := newWorld(1_000_000, 900_000)
		w.quarantine.items = []diskguard.Candidate{item("stale", 10, 8*24*time.Hour), item("staler", 10, 9*24*time.Hour), item("fresh", 10, time.Hour)}

		if _, err := w.guard(t, diskguard.DefaultConfig()).Enforce(now); err != nil {
			t.Fatalf("Enforce: %v", err)
		}
		if got := strings.Join(w.removed, ","); got != "quarantine:staler,quarantine:stale" {
			t.Fatalf("removed %s, want both entries older than the max age", got)
		}
	})
}

// A deleted file can free more space than its length, such as a small reject
// artifact that fills a whole block. Usage is measured again after quarantine
// relief, so no segment, which may not have been sent, is evicted once the
// spool is back under the low watermark.
func TestSegmentsAreNotEvictedOnceQuarantineReliefFreedEnoughSpace(t *testing.T) {
	// 950 of 1000 bytes used; freeing to the 800 low watermark takes 150.
	w := newWorld(1000, 50)
	w.quarantine.items = []diskguard.Candidate{item("q-new", 10, time.Hour), item("q-old", 10, 2*time.Hour)}
	w.quarantine.diskBytes = map[string]int64{"q-new": 80, "q-old": 80}
	w.segments.items = []diskguard.Candidate{item("s-mid", 60, 4*time.Hour), item("s-old", 50, 5*time.Hour)}

	report, err := w.guard(t, watermarks(0.85, 0.80)).Enforce(now)
	if err != nil {
		t.Fatalf("Enforce: %v", err)
	}
	if got := strings.Join(w.removed, ","); got != "quarantine:q-old,quarantine:q-new" {
		t.Fatalf("evicted %s, want only the quarantine: it freed 160 bytes on disk", got)
	}
	if report.SegmentsEvicted != 0 {
		t.Fatalf("report = %+v, want no segment evicted", report)
	}
}

// A quarantine removal that fails part way can still free space, though it
// reports none. Usage is measured before any segment is evicted, so the
// space it freed is seen.
func TestSegmentsAreNotEvictedAfterAQuarantineRemovalThatFailedPartWay(t *testing.T) {
	// 950 of 1000 bytes used; freeing to the 800 low watermark takes 150.
	w := newWorld(1000, 50)
	w.quarantine.items = []diskguard.Candidate{item("q-old", 160, 8*24*time.Hour)}
	w.quarantine.fail = map[string]error{"q-old": errors.New("removed part of the entry")}
	w.quarantine.partial = map[string]int64{"q-old": 160}
	w.segments.items = []diskguard.Candidate{item("s-mid", 60, 4*time.Hour), item("s-old", 50, 5*time.Hour)}

	report, _ := w.guard(t, watermarks(0.85, 0.80)).Enforce(now)
	if report.SegmentsEvicted != 0 {
		t.Fatalf("evicted %v, want no segment: the failed removal freed 160 bytes", w.removed)
	}
}

// Usage measured again after quarantine relief is still freed down to the
// low watermark: a spool that is back under the high watermark but above
// the low one loses its oldest segment.
func TestReliefMeasuredAgainStillFreesToTheLowWatermark(t *testing.T) {
	// 950 of 1000 bytes used. The two entries free 120 bytes on disk,
	// leaving 830: under the 850 high watermark, above the 800 low one.
	w := newWorld(1000, 50)
	w.quarantine.items = []diskguard.Candidate{item("q-new", 10, time.Hour), item("q-old", 10, 2*time.Hour)}
	w.quarantine.diskBytes = map[string]int64{"q-new": 60, "q-old": 60}
	w.segments.items = []diskguard.Candidate{item("s-mid", 60, 4*time.Hour), item("s-old", 50, 5*time.Hour)}

	report, err := w.guard(t, watermarks(0.85, 0.80)).Enforce(now)
	if err != nil {
		t.Fatalf("Enforce: %v", err)
	}
	if got := strings.Join(w.removed, ","); got != "quarantine:q-old,quarantine:q-new,segment:s-old" {
		t.Fatalf("evicted %s, want the quarantine, then the oldest segment to reach the low watermark", got)
	}
	if report.SegmentsEvicted != 1 {
		t.Fatalf("report = %+v, want one segment evicted", report)
	}
}

// watermarks is the default config with these watermarks, for a test whose
// numbers must not move with the defaults.
func watermarks(high, low float64) diskguard.Config {
	cfg := diskguard.DefaultConfig()
	cfg.HighWatermark, cfg.LowWatermark = high, low
	return cfg
}

func TestCheckBoundsQuarantineButNeverEvictsSegments(t *testing.T) {
	// 950 of 1000 bytes used: the stale entry goes, and the spool is still
	// above the 900 high watermark, which Check reports and leaves to
	// Enforce. Check deletes no segment and no quarantine beyond its bounds.
	w := newWorld(1000, 50)
	w.quarantine.items = []diskguard.Candidate{item("stale", 30, 8*24*time.Hour), item("fresh", 40, time.Hour)}
	w.segments.items = []diskguard.Candidate{item("s-old", 60, 2*time.Hour), item("s-new", 70, time.Hour)}

	report, err := w.guard(t, watermarks(0.90, 0.80)).Check(now)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if got := strings.Join(w.removed, ","); got != "quarantine:stale" {
		t.Fatalf("removed %s, want only the stale quarantine entry", got)
	}
	if !report.AboveWatermark || report.QuarantineEvicted != 1 || report.SegmentsEvicted != 0 {
		t.Fatalf("report = %+v, want above the watermark with one quarantine entry removed", report)
	}
	if w.metrics.counts[diskguard.CounterQuarantineEvicted] != 1 || w.metrics.counts[diskguard.CounterSegmentsEvicted] != 0 {
		t.Fatalf("counts = %v, want the removal counted and no segment evicted", w.metrics.counts)
	}
}

func TestCheckBelowTheHighWatermarkReportsNothingDue(t *testing.T) {
	// 880 of 1000 bytes used is under the 900 high watermark.
	w := newWorld(1000, 120)
	report, err := w.guard(t, watermarks(0.90, 0.80)).Check(now)
	if err != nil || report.AboveWatermark || len(w.removed) != 0 {
		t.Fatalf("Check = %+v, %v, removed %v; want nothing due", report, err, w.removed)
	}
}

func TestOnlyEnforceRemovesARejectArtifactStillBeingWritten(t *testing.T) {
	// A busy reject artifact is past its age and over quarantine's 200-byte
	// share. Check runs while a prepare may be writing it, so it keeps it;
	// Enforce runs while nothing is prepared, so a busy item is an orphan.
	w := newWorld(1000, 700)
	busy := item("rejects/.reject.1.tmp", 300, 8*24*time.Hour)
	busy.Busy = true
	w.quarantine.items = []diskguard.Candidate{busy}
	g := w.guard(t, diskguard.DefaultConfig())

	if _, err := g.Check(now); err != nil || len(w.removed) != 0 {
		t.Fatalf("Check removed %v (%v), want the busy artifact kept", w.removed, err)
	}
	if _, err := g.Enforce(now); err != nil || strings.Join(w.removed, ",") != "quarantine:rejects/.reject.1.tmp" {
		t.Fatalf("Enforce removed %v (%v), want the orphaned artifact removed", w.removed, err)
	}
}

func TestCheckCountsARejectArtifactStillBeingWrittenTowardTheQuarantineShare(t *testing.T) {
	// Quarantine may hold 200 of the 1000-byte spool. The busy artifact and
	// the completed entry hold 300 together, so Check removes the completed
	// entry and keeps the busy one.
	w := newWorld(1000, 700)
	busy := item("rejects/.reject.1.tmp", 150, time.Minute)
	busy.Busy = true
	w.quarantine.items = []diskguard.Candidate{busy, item("done", 150, time.Hour)}

	report, err := w.guard(t, diskguard.DefaultConfig()).Check(now)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if got := strings.Join(w.removed, ","); got != "quarantine:done" || report.QuarantineEvicted != 1 {
		t.Fatalf("removed %s (report %+v), want only the completed entry", got, report)
	}
}

func TestQuarantineMarksARejectArtifactStillBeingWrittenBusy(t *testing.T) {
	q := diskguard.NewQuarantine(filepath.Join(t.TempDir(), "quarantine"))
	if err := os.MkdirAll(q.RejectsDir(), 0o750); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"seg-1.jsonl.gz", ".reject.123.tmp"} {
		if err := os.WriteFile(filepath.Join(q.RejectsDir(), name), []byte("x"), 0o640); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := q.Entries()
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	busy := map[string]bool{}
	for _, e := range entries {
		busy[e.Key] = e.Busy
	}
	want := map[string]bool{"rejects/seg-1.jsonl.gz": false, "rejects/.reject.123.tmp": true}
	if !maps.Equal(busy, want) {
		t.Fatalf("busy by key = %v, want %v", busy, want)
	}
}

// Check deletes no segment, but it still measures the spool, so an
// unreadable quarantine does not hide that Enforce is due.
func TestCheckWithAnUnreadableQuarantineStillReportsTheWatermark(t *testing.T) {
	w := newWorld(1000, 50)
	unreadable := errors.New("quarantine entry unreadable")
	w.quarantine.entriesErr = unreadable
	w.segments.items = []diskguard.Candidate{item("s", 70, time.Hour)}

	report, err := w.guard(t, diskguard.DefaultConfig()).Check(now)
	if !errors.Is(err, unreadable) {
		t.Fatalf("Check error = %v, want the quarantine listing error reported", err)
	}
	if !report.AboveWatermark || len(w.removed) != 0 {
		t.Fatalf("report = %+v, removed %v; want the spool reported above the watermark and nothing deleted", report, w.removed)
	}
}
