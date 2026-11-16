// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package journalfs

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/journal"
)

type recorded struct {
	kind, path string
}

func record(store *Store) *[]recorded {
	var events []recorded
	store.hooks.event = func(kind, path string) { events = append(events, recorded{kind, path}) }
	return &events
}

func indexOfEvent(events []recorded, kind, path string) int {
	return slices.IndexFunc(events, func(e recorded) bool { return e.kind == kind && e.path == path })
}

func TestAtomicWriteSyncsTheFileBeforeRenameAndTheDirectoryAfter(t *testing.T) {
	h := newHarness(t)
	store := h.open()
	info := h.writeSegment(1, "segment one")
	id := discover(t, store, info)
	events := record(store)

	if err := store.BeginPrepare(id); err != nil {
		t.Fatalf("BeginPrepare: %v", err)
	}
	final := h.segmentFile(info.Name, id)
	want := []recorded{
		{"sync-file", final + ".tmp"},
		{"rename", final},
		{"sync-dir", filepath.Dir(final)},
	}
	if !slices.Equal(*events, want) {
		t.Fatalf("write steps = %v, want %v", *events, want)
	}
}

func TestFailedDirectorySyncIsReportedAndTheRenamedRecordIsKept(t *testing.T) {
	h := newHarness(t)
	store := h.open()
	id := discover(t, store, h.writeSegment(1, "segment one"))

	store.hooks.beforeSyncDir = func(string) error { return errCrash }
	if err := store.BeginPrepare(id); !errors.Is(err, errCrash) {
		t.Fatalf("BeginPrepare error = %v, want the directory sync failure", err)
	}
	store.hooks.beforeSyncDir = nil
	mustState(t, store, id, journal.StatePreparing)
	store = h.restart(store)
	mustState(t, store, id, journal.StatePreparing)
}

func TestNewStateDirectoriesAreLinkedDurably(t *testing.T) {
	h := newHarness(t)
	var events []recorded
	store, err := openWithHooks(h, hooks{event: func(kind, path string) { events = append(events, recorded{kind, path}) }})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, parent := range []string{h.cfg.SpoolDir, h.cfg.StateDir} {
		if indexOfEvent(events, "sync-dir", parent) < 0 {
			t.Errorf("parent %s not synced after creating state directories", filepath.Base(parent))
		}
	}
}

// A state directory whose link sync failed is linked again by the next
// Open, which finds the directory already there.
func TestOpenRetriesTheStateDirectoryLinkThatFailedToSync(t *testing.T) {
	h := newHarness(t)
	failSpool := hooks{beforeSyncDir: func(dir string) error {
		if dir == h.cfg.SpoolDir {
			return errCrash
		}
		return nil
	}}
	if _, err := openWithHooks(h, failSpool); !errors.Is(err, errCrash) {
		t.Fatalf("Open error = %v, want the directory sync failure", err)
	}

	var events []recorded
	store, err := openWithHooks(h, hooks{event: func(kind, path string) { events = append(events, recorded{kind, path}) }})
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if indexOfEvent(events, "sync-dir", h.cfg.SpoolDir) < 0 {
		t.Fatalf("second Open did not sync the spool, where the state directory is linked: %v", events)
	}
}

func TestFenceLinksItsDirectoryBeforeMovingFilesIntoIt(t *testing.T) {
	h := newHarness(t)
	store := h.open()
	info := h.writeSegment(1, "segment one")
	id := discover(t, store, info)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	h.corrupt(h.segmentFile(info.Name, id), "{")

	var events []recorded
	store, err := openWithHooks(h, hooks{event: func(kind, path string) { events = append(events, recorded{kind, path}) }})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	unreadable := filepath.Join(h.cfg.StateDir, "unreadable")
	fence := filepath.Join(unreadable, info.Name+"."+id)
	linked := indexOfEvent(events, "sync-dir", unreadable)
	moved := indexOfEvent(events, "rename", filepath.Join(fence, info.Name))
	fenceSynced := indexOfEvent(events, "sync-dir", fence)
	// Open also syncs the spool, where the state directory is linked, so
	// look for the fence's spool sync after the move.
	spoolSynced := -1
	if moved >= 0 {
		if i := indexOfEvent(events[moved:], "sync-dir", h.cfg.SpoolDir); i >= 0 {
			spoolSynced = moved + i
		}
	}
	if linked < 0 || moved < 0 || fenceSynced < 0 || spoolSynced < 0 {
		t.Fatalf("fence steps missing: %v", events)
	}
	if !(linked < moved && moved < fenceSynced && fenceSynced < spoolSynced) {
		t.Fatalf("fence steps out of order: %v", events)
	}
}

func TestResumedFenceKeepsTheFencedSourceWhenTheNameIsReused(t *testing.T) {
	h := newHarness(t)
	store := h.open()
	info := h.writeSegment(1, "first lifetime")
	id := discover(t, store, info)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	h.corrupt(h.segmentFile(info.Name, id), "{")
	fence := filepath.Join(h.cfg.StateDir, "unreadable", info.Name+"."+id)
	if err := os.MkdirAll(fence, 0o750); err != nil {
		t.Fatalf("create fence: %v", err)
	}
	if err := os.Rename(filepath.Join(h.cfg.SpoolDir, info.Name), filepath.Join(fence, info.Name)); err != nil {
		t.Fatalf("move source: %v", err)
	}
	// Dynamo wrote a new segment under the same name before the restart.
	h.writeSegment(1, "second lifetime")

	h.open()
	fenced, err := os.ReadFile(filepath.Join(fence, info.Name))
	if err != nil || string(fenced) != "first lifetime" {
		t.Fatalf("fenced source = %q, %v; want the first lifetime kept", fenced, err)
	}
	spool, err := os.ReadFile(filepath.Join(h.cfg.SpoolDir, info.Name))
	if err != nil || string(spool) != "second lifetime" {
		t.Fatalf("spool source = %q, %v; want the new segment left for upload", spool, err)
	}
	if !exists(filepath.Join(fence, info.Name+"."+id+".json")) {
		t.Fatal("journal file not moved into the fence")
	}
}

func openWithHooks(h *harness, hk hooks) (*Store, error) {
	return openStore(h.t.Context(), h.cfg, Options{Now: h.clock.Now, Counter: h.counter}, hk)
}

// A write whose rename landed but whose directory sync failed is already
// what a reader of the journal sees, so the store keeps it in memory too:
// later writes start from it, as a restart would.
func TestARecordRenamedIntoPlaceIsKeptWhenTheDirectorySyncFails(t *testing.T) {
	failNextSync := func(store *Store) {
		failed := false
		store.hooks.beforeSyncDir = func(string) error {
			if failed {
				return nil
			}
			failed = true
			return errCrash
		}
	}
	t.Run("batch update", func(t *testing.T) {
		h := newHarness(t)
		store := h.open()
		ids, transports := preparedSegments(t, h, store, 1, "primary")
		createBatch(t, store, batchFor("b1", "primary", ids, transports))
		failNextSync(store)
		if err := store.SetReceipt("b1", journal.Receipt{Backend: "fake", ID: "job-1"}); err == nil {
			t.Fatal("SetReceipt hid the failed sync")
		}
		if b, _ := store.Batch("b1"); b.Receipt == nil || b.Receipt.ID != "job-1" {
			t.Fatalf("receipt in memory = %+v, want job-1 as on disk", b.Receipt)
		}
		if err := store.SetReceipt("b1", journal.Receipt{Backend: "fake", ID: "job-2"}); !errors.Is(err, journal.ErrReceiptExists) {
			t.Fatalf("second receipt error = %v, want ErrReceiptExists", err)
		}
	})
	t.Run("new batch", func(t *testing.T) {
		h := newHarness(t)
		store := h.open()
		ids, transports := preparedSegments(t, h, store, 1, "primary")
		failNextSync(store)
		if err := store.CreateBatch(batchFor("b1", "primary", ids, transports)); err == nil {
			t.Fatal("CreateBatch hid the failed sync")
		}
		if _, ok := store.Batch("b1"); !ok {
			t.Fatal("batch on disk is missing from memory")
		}
	})
	t.Run("new segment", func(t *testing.T) {
		h := newHarness(t)
		store := h.open()
		failNextSync(store)
		info := h.writeSegment(1, "segment one")
		if _, _, err := store.Discover(info); err == nil {
			t.Fatal("Discover hid the failed sync")
		}
		if len(store.Segments()) != 1 {
			t.Fatal("segment on disk is missing from memory")
		}
	})
}

// Directory sync errors reach the startup log, so they carry no paths.
func TestDirectorySyncErrorsCarryNoPaths(t *testing.T) {
	err := syncDir(filepath.Join(t.TempDir(), "missing-dir"))
	if err == nil || strings.Contains(err.Error(), "missing-dir") {
		t.Fatalf("syncDir() = %v, want an error with no path", err)
	}
}
