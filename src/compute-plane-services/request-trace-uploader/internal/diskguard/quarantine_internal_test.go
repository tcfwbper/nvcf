// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package diskguard

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o640); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", filepath.Base(path), err)
	}
	return string(data)
}

func recordSyncs(t *testing.T) *[]string {
	t.Helper()
	var synced []string
	onSync = func(d string) { synced = append(synced, d) }
	t.Cleanup(func() { onSync = nil })
	return &synced
}

// The quarantine directory is linked in the spool, then the entry in the
// quarantine, then the moved file, and only then the source removal. A retry
// after a failed sync finds the directories already made and must still link
// them durably before any file moves.
func TestAdmitLinksTheEntryBeforeMovingFiles(t *testing.T) {
	cases := []struct {
		name     string
		existing bool
	}{
		{"new directories", false},
		{"entry left by a failed attempt", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spool := t.TempDir()
			dir := filepath.Join(spool, ".rtu-quarantine")
			entry := filepath.Join(dir, "seg-3")
			source := filepath.Join(spool, "request-trace.000003.jsonl.gz")
			writeFile(t, source, "segment")
			if tc.existing {
				if err := os.MkdirAll(entry, 0o750); err != nil {
					t.Fatal(err)
				}
			}
			synced := recordSyncs(t)

			if err := NewQuarantine(dir).Admit("seg-3", source); err != nil {
				t.Fatalf("Admit: %v", err)
			}
			// Each MkdirDurable call that makes a level first syncs the link of
			// the deepest directory that already exists.
			want := []string{spool, dir, entry, spool}
			if !tc.existing {
				want = []string{filepath.Dir(spool), spool, spool, dir, entry, spool}
			}
			if !slices.Equal(*synced, want) {
				t.Fatalf("synced directories = %v, want %v", *synced, want)
			}
		})
	}
}

// Admit never replaces a file already in the entry: a name already taken
// gets a numbered suffix, as the journal fence does. A repeated Admit skips
// what it already moved.
func TestAdmitNeverReplacesAQuarantinedFile(t *testing.T) {
	t.Run("same name twice in one call", func(t *testing.T) {
		spool := t.TempDir()
		q := NewQuarantine(filepath.Join(spool, "quarantine"))
		first, second := filepath.Join(spool, "a", "seg.parquet"), filepath.Join(spool, "b", "seg.parquet")
		writeFile(t, first, "first")
		writeFile(t, second, "second")

		for range 2 {
			if err := q.Admit("seg-3", first, second); err != nil {
				t.Fatalf("Admit: %v", err)
			}
		}
		entry := filepath.Join(spool, "quarantine", "seg-3")
		names, err := os.ReadDir(entry)
		if err != nil || len(names) != 2 {
			t.Fatalf("entry holds %v (%v), want two files", names, err)
		}
		if a, b := readFile(t, filepath.Join(entry, "seg.parquet")), readFile(t, filepath.Join(entry, "seg.parquet.1")); a != "first" || b != "second" {
			t.Fatalf("entry files = %q and %q, want first and second", a, b)
		}
	})
	t.Run("file already in the entry", func(t *testing.T) {
		spool := t.TempDir()
		q := NewQuarantine(filepath.Join(spool, "quarantine"))
		entry := filepath.Join(spool, "quarantine", "seg-3")
		writeFile(t, filepath.Join(entry, "seg.parquet"), "earlier")
		source := filepath.Join(spool, "seg.parquet")
		writeFile(t, source, "later")

		if err := q.Admit("seg-3", source); err != nil {
			t.Fatalf("Admit: %v", err)
		}
		if a, b := readFile(t, filepath.Join(entry, "seg.parquet")), readFile(t, filepath.Join(entry, "seg.parquet.1")); a != "earlier" || b != "later" {
			t.Fatalf("entry files = %q and %q, want earlier and later", a, b)
		}
	})
}

// When a later move fails, the moves already made are synced before the
// error returns, so a power loss never keeps a removal without the moved
// file.
func TestAdmitSyncsEarlierMovesWhenALaterMoveFails(t *testing.T) {
	spool := t.TempDir()
	dir := filepath.Join(spool, "quarantine")
	entry := filepath.Join(dir, "seg-3")
	moved, stuck := filepath.Join(spool, "a", "seg.jsonl.gz"), filepath.Join(spool, "b", "seg.parquet")
	writeFile(t, moved, "source")
	writeFile(t, stuck, "transport")
	busy := errors.New("device busy")
	rename = func(from, to string) error {
		if from == stuck {
			return busy
		}
		return os.Rename(from, to)
	}
	t.Cleanup(func() { rename = os.Rename })
	synced := recordSyncs(t)

	err := NewQuarantine(dir).Admit("seg-3", moved, stuck)
	if !errors.Is(err, busy) {
		t.Fatalf("Admit error = %v, want the failed move reported", err)
	}
	if want := []string{filepath.Dir(spool), spool, spool, dir, entry, filepath.Dir(moved)}; !slices.Equal(*synced, want) {
		t.Fatalf("synced directories = %v, want %v", *synced, want)
	}
	if readFile(t, filepath.Join(entry, "seg.jsonl.gz")) != "source" || readFile(t, stuck) != "transport" {
		t.Fatal("want the first file in the entry and the second left in place")
	}
}

// An entry that exists but is not a real directory, such as a link to a
// directory elsewhere, is refused: files moved through it would be outside
// the quarantine, and removing the entry would remove only the link.
func TestAdmitRefusesAnEntryThatIsNotADirectory(t *testing.T) {
	spool := t.TempDir()
	dir := filepath.Join(spool, "quarantine")
	outside := t.TempDir()
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "seg-3")); err != nil {
		t.Skipf("symlink: %v", err)
	}
	source := filepath.Join(spool, "request-trace.000003.jsonl.gz")
	writeFile(t, source, "segment")

	if err := NewQuarantine(dir).Admit("seg-3", source); err == nil {
		t.Fatal("Admit moved files through a linked entry")
	}
	if readFile(t, source) != "segment" {
		t.Fatal("source moved, want it left in place")
	}
	if names, _ := os.ReadDir(outside); len(names) != 0 {
		t.Fatalf("files moved outside the quarantine: %v", names)
	}
}

// A removal whose directory sync failed reports both the deletion and the
// cause, so a caller can match either.
func TestRemoveReportsTheSyncFailure(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "quarantine")
	writeFile(t, filepath.Join(dir, "seg-3", "seg.parquet"), "transport")
	failed := errors.New("input/output error")
	syncFile = func(*os.File) error { return failed }
	t.Cleanup(func() { syncFile = (*os.File).Sync })

	err := NewQuarantine(dir).Remove("seg-3")
	if !errors.Is(err, ErrDeletedNotSynced) || !errors.Is(err, failed) {
		t.Fatalf("Remove error = %v, want ErrDeletedNotSynced wrapping the sync failure", err)
	}
	if _, err := os.Lstat(filepath.Join(dir, "seg-3")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("entry still present after Remove")
	}
}

type halfFullVolume struct{}

func (halfFullVolume) Usage() (Usage, error) { return Usage{Total: 1000, Free: 500}, nil }

// Enforce can run while another goroutine admits a segment. It must never see
// an entry that is half admitted: removing it then would leave the rest of
// the segment's files outside the quarantine and fail the admission.
func TestEnforceNeverSeesAHalfAdmittedEntry(t *testing.T) {
	root := t.TempDir()
	q := NewQuarantine(filepath.Join(root, "quarantine"))
	source, transport := filepath.Join(root, "source"), filepath.Join(root, "transport")
	for _, path := range []string{source, transport} {
		// Each file alone is over the default quarantine share of the volume.
		if err := os.WriteFile(path, make([]byte, 300), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	guard, err := New(DefaultConfig(), halfFullVolume{}, q, nil, Options{})
	if err != nil {
		t.Fatal(err)
	}
	original := rename
	t.Cleanup(func() { rename = original })
	enforced := make(chan error, 1)
	first := true
	rename = func(from, to string) error {
		err := original(from, to)
		if err == nil && first {
			first = false
			go func() { _, err := guard.Enforce(time.Now()); enforced <- err }()
			// Give Enforce time to reach the entry while it is half admitted.
			select {
			case err := <-enforced:
				enforced <- err
			case <-time.After(200 * time.Millisecond):
			}
		}
		return err
	}

	if err := q.Admit("seg", source, transport); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if err := <-enforced; err != nil {
		t.Fatalf("Enforce: %v", err)
	}
	for _, path := range []string{source, transport} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s left outside the quarantine", filepath.Base(path))
		}
	}
}

// A retried Admit finishes when a source's directory was removed after its
// file moved: that directory has nothing left to sync, and a source
// directory that still exists is synced again.
func TestAdmitRetrySkipsASourceDirectoryThatIsGone(t *testing.T) {
	spool := t.TempDir()
	dir := filepath.Join(spool, "quarantine")
	entry := filepath.Join(dir, "seg-3")
	kept, removed := filepath.Join(spool, "a", "seg.jsonl.gz"), filepath.Join(spool, "b", "seg.parquet")
	writeFile(t, kept, "source")
	writeFile(t, removed, "transport")
	q := NewQuarantine(dir)
	if err := q.Admit("seg-3", kept, removed); err != nil {
		t.Fatalf("first Admit: %v", err)
	}
	if err := os.Remove(filepath.Dir(removed)); err != nil {
		t.Fatal(err)
	}
	synced := recordSyncs(t)

	if err := q.Admit("seg-3", kept, removed); err != nil {
		t.Fatalf("retried Admit: %v", err)
	}
	if want := []string{spool, dir, entry, filepath.Dir(kept)}; !slices.Equal(*synced, want) {
		t.Fatalf("synced directories = %v, want %v", *synced, want)
	}
}

// Directory sync errors reach the logs, so they carry no paths.
func TestDirectorySyncErrorsCarryNoPaths(t *testing.T) {
	err := syncDir(filepath.Join(t.TempDir(), "missing-dir"))
	if err == nil || strings.Contains(err.Error(), "missing-dir") {
		t.Fatalf("syncDir() = %v, want an error with no path", err)
	}
}
