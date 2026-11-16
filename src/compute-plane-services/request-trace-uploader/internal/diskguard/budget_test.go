// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package diskguard_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/diskguard"
)

type fakeBudget struct {
	size, held int64
	err        error
	// failFrom, when above zero, makes Held fail with err only from that
	// call on.
	failFrom, calls int
}

func (b *fakeBudget) Held() (int64, int64, error) {
	b.calls++
	if b.failFrom > 0 && b.calls < b.failFrom {
		return b.size, b.held, nil
	}
	return b.size, b.held, b.err
}

// When the budget started relief and cannot be read again, relief keeps to
// the budget's target as first measured. It never falls back to the low
// watermark of a filesystem that is only between its watermarks.
func TestAnUnreadableBudgetNeverDrivesAFilesystemBetweenItsWatermarks(t *testing.T) {
	errUnreadable := errors.New("unreadable quarantine subtree")
	// The filesystem is at 80%, between its 75% and 85% watermarks. The
	// budget is at 95%: it needs 200 bytes freed, the filesystem would 500.
	w := newWorld(10_000, 2_000)
	w.budget = &fakeBudget{size: 1000, held: 950, err: errUnreadable, failFrom: 2}
	w.segments.items = []diskguard.Candidate{item("s-new", 250, time.Hour), item("s-old", 250, 2*time.Hour)}
	g := w.guard(t, diskguard.DefaultConfig())

	if _, err := g.Enforce(now); !errors.Is(err, errUnreadable) {
		t.Fatalf("Enforce error = %v, want the budget error reported", err)
	}
	if got := strings.Join(w.removed, ","); got != "segment:s-old" {
		t.Fatalf("evicted %q, want only the oldest: the budget's 200 bytes, not the filesystem's 500", got)
	}
}

// A quarantine deletion can free less than its entry was sized at, such as a
// file still linked under another name, so the spool is measured again
// before Enforce decides whether relief is still needed.
func TestReliefMeasuresAgainAfterQuarantineFreesLessThanCounted(t *testing.T) {
	// The filesystem is 10000 bytes with 9500 used: above its 8500 high
	// watermark, so it is freed to its 7500 low watermark.
	w := newWorld(10_000, 500)
	w.quarantine.items = []diskguard.Candidate{item("q-old", 1500, 30*24*time.Hour)}
	w.quarantine.diskBytes = map[string]int64{"q-old": 0}
	for i := range 5 {
		w.segments.items = append(w.segments.items, item(fmt.Sprintf("s%d", i+1), 600, time.Duration(10-i)*time.Hour))
	}
	g := w.guard(t, diskguard.DefaultConfig())

	report, err := g.Enforce(now)
	if err != nil {
		t.Fatalf("Enforce: %v", err)
	}
	// The aged entry went, but freed nothing on disk: 2000 bytes are still
	// needed, four 600-byte segments.
	if !report.AboveWatermark || report.SegmentsEvicted != 4 {
		t.Fatalf("report = %+v (evicted %q), want relief to go on and evict four segments", report, w.removed)
	}
}

// Relief does not take a deletion's size on trust when that size seems to
// be enough, such as a file still linked under another name: the spool is
// measured again before relief stops, and it goes on if space is still
// needed.
func TestReliefMeasuresAgainBeforeStoppingOnSizes(t *testing.T) {
	cases := []struct {
		name string
		// quarantine is a fresh entry within its share that frees nothing.
		quarantine bool
		// firstFreesNothing makes the oldest segment free nothing.
		firstFreesNothing bool
		want              string
	}{
		// 95000 of 100000 used: 20000 to free to the 75000 low watermark.
		{"quarantine entry frees nothing", true, false, "quarantine:q,segment:s1,segment:s2,segment:s3,segment:s4"},
		{"segment frees nothing", false, true, "segment:s0,segment:s1,segment:s2,segment:s3,segment:s4"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(100_000, 5_000)
			w.quarantine.diskBytes = map[string]int64{}
			w.segments.diskBytes = map[string]int64{}
			if tc.quarantine {
				w.quarantine.items = []diskguard.Candidate{item("q", 20_000, time.Hour)}
				w.quarantine.diskBytes["q"] = 0
			}
			if tc.firstFreesNothing {
				w.segments.items = append(w.segments.items, item("s0", 20_000, 10*time.Hour))
				w.segments.diskBytes["s0"] = 0
			}
			for i := range 5 {
				w.segments.items = append(w.segments.items, item(fmt.Sprintf("s%d", i+1), 5_000, time.Duration(9-i)*time.Hour))
			}
			g := w.guard(t, diskguard.DefaultConfig())
			if _, err := g.Enforce(now); err != nil {
				t.Fatalf("Enforce: %v", err)
			}
			if got := strings.Join(w.removed, ","); got != tc.want {
				t.Fatalf("removed %q, want %q", got, tc.want)
			}
		})
	}
}

// Relief stays quarantine first: when a quarantine deletion seemed to be
// enough but freed nothing, the next quarantine entry goes before any
// segment, which may not have been sent.
func TestReliefFinishesQuarantineBeforeSegmentsWhenADeletionFreesNothing(t *testing.T) {
	// 95000 of 100000 used: 20000 to free to the 75000 low watermark.
	w := newWorld(100_000, 5_000)
	w.quarantine.items = []diskguard.Candidate{item("q1", 20_000, 2*time.Hour), item("q2", 20_000, time.Hour)}
	w.quarantine.diskBytes = map[string]int64{"q1": 0}
	w.segments.items = []diskguard.Candidate{item("s", 20_000, 3*time.Hour)}
	cfg := diskguard.DefaultConfig()
	cfg.QuarantineMaxFraction = 0.5
	if _, err := w.guard(t, cfg).Enforce(now); err != nil {
		t.Fatalf("Enforce: %v", err)
	}
	if got := strings.Join(w.removed, ","); got != "quarantine:q1,quarantine:q2" {
		t.Fatalf("removed %q, want both quarantine entries and no segment", got)
	}
}

// Relief measures the spool once to start, once before it evicts segments,
// and then only to confirm deletions it has not measured yet.
func TestReliefMeasuresOnlyWhatItHasNotMeasured(t *testing.T) {
	w := newWorld(1_000_000_000, 900_000_000).withBudget(100_000, 95_000)
	w.segments.items = []diskguard.Candidate{item("s1", 20_000, 2*time.Hour), item("s2", 20_000, time.Hour)}
	if _, err := w.guard(t, diskguard.DefaultConfig()).Enforce(now); err != nil {
		t.Fatalf("Enforce: %v", err)
	}
	if got := strings.Join(w.removed, ","); got != "segment:s1" {
		t.Fatalf("removed %q, want only the oldest segment", got)
	}
	if w.budget.calls != 3 {
		t.Fatalf("budget measured %d times, want 3: to start, before segments, to confirm", w.budget.calls)
	}
}

// Quarantine is bounded first. Only a volume still above its high watermark
// after that starts relief, which then frees it to its low watermark, so
// cleanup by age alone never costs a segment.
func TestQuarantineCleanupBelowTheHighWatermarkStartsNoRelief(t *testing.T) {
	// 9500 of 10000 used; an aged 1500-byte entry brings it to 8000, below
	// the 8500 high watermark but above the 7500 low one.
	w := newWorld(10_000, 500)
	w.quarantine.items = []diskguard.Candidate{item("q-old", 1500, 30*24*time.Hour)}
	w.segments.items = []diskguard.Candidate{item("s1", 500, time.Hour)}
	report, err := w.guard(t, diskguard.DefaultConfig()).Enforce(now)
	if err != nil {
		t.Fatalf("Enforce: %v", err)
	}
	if report.AboveWatermark || report.SegmentsEvicted != 0 {
		t.Fatalf("report = %+v (removed %q), want quarantine cleanup only", report, w.removed)
	}
}

// A budget that becomes unreadable between the first measure and the
// re-measure before segments are evicted is reported too, and relief keeps
// to the budget's target as first measured.
func TestABudgetThatFailsOnlyWhenMeasuredAgainIsReported(t *testing.T) {
	errUnreadable := errors.New("unreadable quarantine subtree")
	// The filesystem is 1 GB and 10% used; 950 of a 1000-byte budget is used.
	w := newWorld(1_000_000_000, 900_000_000)
	w.budget = &fakeBudget{size: 1000, held: 950, err: errUnreadable, failFrom: 2}
	w.segments.items = []diskguard.Candidate{item("s-mid", 50, 4*time.Hour), item("s-old", 60, 5*time.Hour)}
	g := w.guard(t, diskguard.DefaultConfig())

	_, err := g.Enforce(now)
	if !errors.Is(err, errUnreadable) {
		t.Fatalf("Enforce error = %v, want the budget error from the re-measure", err)
	}
	// 950 of a 1000-byte budget: 200 to free to its low watermark, more
	// than the two segments hold.
	if got := strings.Join(w.removed, ","); got != "segment:s-old,segment:s-mid" {
		t.Fatalf("evicted %q, want both segments for the budget's first-measured target", got)
	}
}

// A budget that cannot be measured, such as a quarantine subtree that cannot
// be read, must not stop relief: the filesystem pass still runs and the
// budget's error is reported. Preparing stays paused on the error.
func TestAnUnreadableBudgetStillLetsTheFilesystemBeRelieved(t *testing.T) {
	errUnreadable := errors.New("unreadable quarantine subtree")
	// The filesystem is 1000 bytes with 950 used: above its 850 high
	// watermark, so it is freed to its 750 low watermark. 200 must go, more
	// than the three segments hold.
	w := newWorld(1000, 50)
	w.budget = &fakeBudget{err: errUnreadable}
	w.segments.items = []diskguard.Candidate{item("s-new", 70, 3*time.Hour), item("s-mid", 50, 4*time.Hour), item("s-old", 60, 5*time.Hour)}
	g := w.guard(t, diskguard.DefaultConfig())

	report, err := g.Enforce(now)
	if !errors.Is(err, errUnreadable) {
		t.Fatalf("Enforce error = %v, want the budget error reported", err)
	}
	if got := strings.Join(w.removed, ","); got != "segment:s-old,segment:s-mid,segment:s-new" {
		t.Fatalf("evicted %q (report %+v), want every segment, oldest first", got, report)
	}
	if _, err := g.AllowPrepare(1, 0); !errors.Is(err, errUnreadable) {
		t.Fatalf("AllowPrepare error = %v, want the budget error", err)
	}
}

// withBudget gives the world a spool budget of size bytes with held bytes
// held.
func (w *world) withBudget(size, held int64) *world {
	w.budget = &fakeBudget{size: size, held: held}
	return w
}

// A spool on a large shared disk, such as an emptyDir with a size limit on
// the node's disk, is pruned at its budget's watermark. Without a budget
// only the filesystem counts, as before.
func TestABudgetBelowTheFilesystemPrunesAtTheBudgetsWatermark(t *testing.T) {
	cases := []struct {
		name        string
		budget      bool
		wantRemoved string
		wantPause   diskguard.PauseReason
	}{
		{"no budget measures the filesystem only", false, "", diskguard.PauseNone},
		// 950 of a 1000-byte budget is used: above its 850 high watermark,
		// so it is freed to its 750 low watermark. 200 must go, more than the
		// three segments hold.
		{"budget", true, "segment:s-old,segment:s-mid,segment:s-new", diskguard.PauseHighWatermark},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The filesystem is 1 GB and 10% used.
			w := newWorld(1_000_000_000, 900_000_000)
			if tc.budget {
				w.withBudget(1000, 950)
			}
			w.segments.items = []diskguard.Candidate{item("s-new", 70, 3*time.Hour), item("s-mid", 50, 4*time.Hour), item("s-old", 60, 5*time.Hour)}
			g := w.guard(t, diskguard.DefaultConfig())

			decision, err := g.AllowPrepare(1, 0)
			if err != nil || decision.Reason != tc.wantPause {
				t.Fatalf("AllowPrepare = %+v, %v; want reason %q", decision, err, tc.wantPause)
			}
			report, err := g.Enforce(now)
			if err != nil {
				t.Fatalf("Enforce: %v", err)
			}
			if got := strings.Join(w.removed, ","); got != tc.wantRemoved {
				t.Fatalf("evicted %q (report %+v), want %q", got, report, tc.wantRemoved)
			}
			if report.AboveWatermark != tc.budget {
				t.Fatalf("report = %+v, want above the watermark only with the budget", report)
			}
		})
	}
}

// Each of the budget and the filesystem that is above its high watermark
// starts relief and is freed to its own low watermark. One that is only
// between its watermarks sets nothing, so a shared node disk at 80% does not
// cost every segment when the budget starts relief.
func TestEachVolumeAboveItsHighWatermarkIsFreedToItsLowWatermark(t *testing.T) {
	cases := []struct {
		name                 string
		fsTotal, fsUsed      uint64
		budgetTotal, budgetU int64
		// wantEvicted counts the 40-byte segments evicted, oldest first.
		wantEvicted int
	}{
		// Only the budget is above its high watermark: it is freed to 750,
		// 200 bytes; the filesystem at 84% sets nothing.
		{"budget only", 10_000, 8_400, 1000, 950, 5},
		// Only the filesystem is above its high watermark: it is freed to
		// 7500, 1100 bytes; the budget at 84% sets nothing.
		{"filesystem only", 10_000, 8_600, 1000, 840, 28},
		// Both are above their high watermarks: each is freed to its low
		// watermark, so the filesystem's 1030 bytes decide.
		{"both", 10_000, 8_530, 1000, 950, 26},
		// The budget starts relief; the filesystem, 500 bytes above its low
		// watermark but below its high one, sets nothing.
		{"filesystem between its watermarks", 10_000, 8_000, 1000, 950, 5},
		{"both below their high watermarks", 10_000, 8_000, 1000, 800, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(tc.fsTotal, tc.fsTotal-tc.fsUsed).withBudget(tc.budgetTotal, tc.budgetU)
			var want []string
			for i := range 30 {
				name := fmt.Sprintf("s%02d", i+1)
				w.segments.items = append(w.segments.items, item(name, 40, time.Duration(30-i)*time.Hour))
				if i < tc.wantEvicted {
					want = append(want, "segment:"+name)
				}
			}
			if _, err := w.guard(t, diskguard.DefaultConfig()).Enforce(now); err != nil {
				t.Fatalf("Enforce: %v", err)
			}
			if got, want := strings.Join(w.removed, ","), strings.Join(want, ","); got != want {
				t.Fatalf("evicted %q, want %q", got, want)
			}
		})
	}
}

// Free space is the least of the budget's remaining room and the
// filesystem's free space, so a transport must fit within the budget.
func TestFreeSpaceCountsTheBudgetsRemainingRoom(t *testing.T) {
	const mib = uint64(1) << 20
	const imib = int64(mib)
	cases := []struct {
		name                string
		fsTotal, fsFree     uint64
		budgetTotal, budgUs int64
		estimate            int64
		want                diskguard.PauseReason
	}{
		{"room in both", 100 * 1024 * mib, 50 * 1024 * mib, 2048 * imib, 1024 * imib, 30 * imib, diskguard.PauseNone},
		// 1700 of 2048 MiB is under the 85% watermark and leaves 348 MiB,
		// less than twice a 200 MiB transport.
		{"budget's room under twice the transport", 100 * 1024 * mib, 50 * 1024 * mib, 2048 * imib, 1700 * imib, 200 * imib, diskguard.PauseFreeSpace},
		{"filesystem below the floor", 100 * 1024 * mib, 500 * mib, 2048 * imib, 1024 * imib, 30 * imib, diskguard.PauseHighWatermark},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(tc.fsTotal, tc.fsFree).withBudget(tc.budgetTotal, tc.budgUs)
			got, err := w.guard(t, diskguard.DefaultConfig()).AllowPrepare(tc.estimate, 0)
			if err != nil {
				t.Fatalf("AllowPrepare: %v", err)
			}
			if got.Reason != tc.want {
				t.Fatalf("AllowPrepare = %+v, want reason %q", got, tc.want)
			}
		})
	}
}

// A small spool that relief brings back to its watermark can prepare again,
// with a budget or without one. The fixed 512 MiB floor is more than the 15%
// the watermark leaves free of a spool under about 3.3 GiB, so with it
// preparing stayed paused while relief kept deleting segments that were
// never exported.
func TestPreparingResumesOnceASmallSpoolIsBackAtItsWatermark(t *testing.T) {
	const gib = int64(1) << 30
	cases := []struct {
		name    string
		fsTotal int64
		// budget is the spool budget; zero is none.
		budget int64
	}{
		{"1 GiB filesystem, no budget", gib, 0},
		{"2 GiB filesystem, no budget", 2 * gib, 0},
		{"1 GiB budget", 1 << 40, gib},
		{"2 GiB budget", 1 << 40, 2 * gib},
		{"3 GiB budget", 1 << 40, 3 * gib},
		// The filesystem is the smaller and fuller volume, so relief brings
		// it, not the budget, back to the watermark.
		{"budget larger than its filesystem", gib, 4 * gib},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			smaller := tc.fsTotal
			if tc.budget > 0 {
				smaller = min(smaller, tc.budget)
			}
			// The spool holds 94% of the smaller volume, in closed segments of
			// 5% each: above 85%, so relief deletes four to get under 75%.
			held := smaller * 94 / 100
			w := newWorld(uint64(tc.fsTotal), uint64(tc.fsTotal-held))
			if tc.budget > 0 {
				w.withBudget(tc.budget, held)
			}
			for i := range 18 {
				w.segments.items = append(w.segments.items, item(fmt.Sprintf("s%02d", i), smaller/20, time.Duration(20-i)*time.Hour))
			}
			g := w.guard(t, diskguard.DefaultConfig())

			report, err := g.Enforce(now)
			if err != nil {
				t.Fatalf("Enforce: %v", err)
			}
			if report.SegmentsEvicted != 4 {
				t.Fatalf("evicted %d segments (%q), want the four oldest", report.SegmentsEvicted, w.removed)
			}
			w.volume.usage.Free += uint64(report.FreedBytes)
			if w.budget != nil {
				w.budget.held -= report.FreedBytes
			}

			decision, err := g.AllowPrepare(1<<20, 0)
			if err != nil || !decision.Allowed {
				t.Fatalf("AllowPrepare = %+v, %v; want preparing to resume at the watermark", decision, err)
			}
		})
	}
}

// Quarantine is bounded to its share of the spool, which with a budget is
// the budget and not the disk under it.
func TestQuarantineShareIsOfTheBudget(t *testing.T) {
	// The budget is 1000 bytes, so quarantine may hold 200.
	w := newWorld(1_000_000_000, 900_000_000).withBudget(1000, 300)
	w.quarantine.items = []diskguard.Candidate{item("newest", 100, time.Hour), item("oldest", 150, 3*time.Hour)}

	if _, err := w.guard(t, diskguard.DefaultConfig()).Enforce(now); err != nil {
		t.Fatalf("Enforce: %v", err)
	}
	if got := strings.Join(w.removed, ","); got != "quarantine:oldest" {
		t.Fatalf("evicted %q, want the oldest entry to bring quarantine under 20%% of the budget", got)
	}
}

// The gauges show the volume the guard bounds: with a budget fuller than
// the disk, the bytes the spool holds and the room left in the budget.
func TestGaugesReportTheFullerVolumeAndTheLeastFreeSpace(t *testing.T) {
	w := newWorld(1_000_000_000, 900_000_000).withBudget(1000, 900)
	if err := w.guard(t, diskguard.DefaultConfig()).UpdateGauges(now, nil); err != nil {
		t.Fatalf("UpdateGauges: %v", err)
	}
	if used, free := w.metrics.gauges[diskguard.GaugeSpoolUsedBytes], w.metrics.gauges[diskguard.GaugeSpoolFreeBytes]; used != 900 || free != 100 {
		t.Fatalf("used = %v, free = %v; want the budget's 900 and 100", used, free)
	}
}

// A spool already above its budget, such as after the budget was lowered,
// has everything above the watermark freed in one pass.
func TestASpoolAboveItsBudgetIsBroughtToTheWatermarkInOnePass(t *testing.T) {
	// 290 bytes held in a 200-byte budget: above its 170 high watermark, so
	// it is freed to its 150 low watermark. 140 must go: all four segments.
	w := newWorld(1_000_000_000, 900_000_000).withBudget(200, 290)
	w.segments.items = []diskguard.Candidate{
		item("s4", 40, time.Hour), item("s3", 40, 2*time.Hour), item("s2", 40, 3*time.Hour), item("s1", 40, 4*time.Hour),
	}
	g := w.guard(t, diskguard.DefaultConfig())
	if _, err := g.Enforce(now); err != nil {
		t.Fatalf("Enforce: %v", err)
	}
	if got := strings.Join(w.removed, ","); got != "segment:s1,segment:s2,segment:s3,segment:s4" {
		t.Fatalf("evicted %q, want all four, oldest first", got)
	}
	if err := g.UpdateGauges(now, nil); err != nil {
		t.Fatalf("UpdateGauges: %v", err)
	}
	// The gauges follow the budget, the fuller volume: 130 of 200 bytes
	// held once the four segments are gone.
	if used, free := w.metrics.gauges[diskguard.GaugeSpoolUsedBytes], w.metrics.gauges[diskguard.GaugeSpoolFreeBytes]; used != 130 || free != 70 {
		t.Fatalf("used = %v, free = %v; want 130 held and 70 left of the budget", used, free)
	}
}
