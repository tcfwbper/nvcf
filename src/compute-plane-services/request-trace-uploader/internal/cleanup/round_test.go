// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cleanup_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/cleanup"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/segment"
)

// scanSpool answers from a real spool directory, as the uploader does, and
// counts its scans.
type scanSpool struct {
	dir   string
	scans map[string]int
	fail  int
}

func (s *scanSpool) HighestIndex(prefix string) (int, bool, error) {
	s.scans[prefix]++
	if s.fail > 0 {
		s.fail--
		return 0, false, errors.New("spool unreadable")
	}
	scan, err := segment.ScanDir(s.dir, []string{prefix})
	if err != nil {
		return 0, false, err
	}
	highest, found := 0, false
	for _, item := range scan.Active {
		if !found || item.Index > highest {
			highest, found = item.Index, true
		}
	}
	return highest, found, nil
}

// confirmedSpool writes n closed segments of prefix and the active one after
// them, and returns an item for each closed one.
func confirmedSpool(t testing.TB, dir, prefix string, n int) []cleanup.Item {
	t.Helper()
	var items []cleanup.Item
	for i := range n + 1 {
		path := filepath.Join(dir, fmt.Sprintf("%s.%06d.jsonl.gz", prefix, i))
		content := fmt.Sprintf("segment %d", i)
		if err := os.WriteFile(path, []byte(content), 0o640); err != nil {
			t.Fatal(err)
		}
		if i == n {
			break
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256([]byte(content))
		items = append(items, cleanup.Item{
			SegmentID: fmt.Sprintf("%s-%d", prefix, i),
			Source:    path,
			Prefix:    prefix,
			Index:     i,
			Size:      info.Size(),
			ModTime:   info.ModTime(),
			SHA256:    hex.EncodeToString(sum[:]),
		})
	}
	return items
}

func TestARoundScansTheSpoolOncePerFamily(t *testing.T) {
	dir := t.TempDir()
	traces := confirmedSpool(t, dir, "request-trace", 5)
	audits := confirmedSpool(t, dir, "request-audit", 2)
	spool := &scanSpool{dir: dir, scans: map[string]int{}}
	round := cleanup.New(&fakeJournal{}, spool, cleanup.Options{}).Round()

	for i, item := range append(traces, audits...) {
		clean := round.Clean
		if i%2 == 1 {
			clean = round.CleanEmpty
		}
		if result, err := clean(t.Context(), item); err != nil || result != cleanup.ResultDone {
			t.Fatalf("clean %s = %s, %v; want done", item.SegmentID, result, err)
		}
		if _, err := os.Stat(item.Source); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("source of %s kept", item.SegmentID)
		}
	}
	if spool.scans["request-trace"] != 1 || spool.scans["request-audit"] != 1 {
		t.Fatalf("scans = %v, want one per family for the round", spool.scans)
	}
}

func TestARoundStillKeepsTheHighestIndex(t *testing.T) {
	dir := t.TempDir()
	items := confirmedSpool(t, dir, "request-trace", 2)
	// The writer moved on and then a restart left only the two closed
	// segments: the last of them is now the highest index.
	if err := os.Remove(filepath.Join(dir, "request-trace.000002.jsonl.gz")); err != nil {
		t.Fatal(err)
	}
	round := cleanup.New(&fakeJournal{}, &scanSpool{dir: dir, scans: map[string]int{}}, cleanup.Options{}).Round()
	if result, err := round.Clean(t.Context(), items[0]); err != nil || result != cleanup.ResultDone {
		t.Fatalf("Clean(first) = %s, %v; want done", result, err)
	}
	if result, err := round.Clean(t.Context(), items[1]); err != nil || result != cleanup.ResultDeferred {
		t.Fatalf("Clean(highest) = %s, %v; want deferred", result, err)
	}
	if _, err := os.Stat(items[1].Source); err != nil {
		t.Fatal("the highest-index source was deleted")
	}
}

func TestARoundAsksAgainAfterTheSpoolCouldNotBeRead(t *testing.T) {
	dir := t.TempDir()
	items := confirmedSpool(t, dir, "request-trace", 2)
	spool := &scanSpool{dir: dir, scans: map[string]int{}, fail: 1}
	round := cleanup.New(&fakeJournal{}, spool, cleanup.Options{}).Round()
	if _, err := round.Clean(t.Context(), items[0]); err == nil {
		t.Fatal("Clean() error = nil while the spool could not be read")
	}
	if _, err := os.Stat(items[0].Source); err != nil {
		t.Fatal("source deleted without knowing the highest index")
	}
	for _, item := range items {
		if result, err := round.Clean(t.Context(), item); err != nil || result != cleanup.ResultDone {
			t.Fatalf("Clean(%s) = %s, %v; want done once the spool reads", item.SegmentID, result, err)
		}
	}
	if spool.scans["request-trace"] != 2 {
		t.Fatalf("scans = %d, want the failed one and one more", spool.scans["request-trace"])
	}
}

// BenchmarkCleanupRound deletes the sources of a backlog of confirmed
// segments: "per-deletion" scans the spool before each one, "per-round"
// once for the round.
func BenchmarkCleanupRound(b *testing.B) {
	for _, n := range []int{100, 1000} {
		for _, mode := range []string{"per-deletion", "per-round"} {
			b.Run(fmt.Sprintf("%s/%d", mode, n), func(b *testing.B) {
				for b.Loop() {
					b.StopTimer()
					dir := b.TempDir()
					items := confirmedSpool(b, dir, "request-trace", n)
					cleaner := cleanup.New(&fakeJournal{}, &scanSpool{dir: dir, scans: map[string]int{}}, cleanup.Options{})
					if mode == "per-round" {
						cleaner = cleaner.Round()
					}
					b.StartTimer()
					for _, item := range items {
						if _, err := cleaner.CleanEmpty(b.Context(), item); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}
