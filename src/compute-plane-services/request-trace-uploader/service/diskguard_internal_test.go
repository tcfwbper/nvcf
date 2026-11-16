// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/diskguard"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/journalfs"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/journal"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

// evictionFixture is a spool with one closed segment and the active one, and
// an open journal on it.
type evictionFixture struct {
	u      *Uploader
	closed string
}

func newEvictionFixture(t *testing.T) evictionFixture {
	t.Helper()
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	closed := spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("a", 1)).Bytes())
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("active", 2)).Bytes())
	store, err := journalfs.Open(context.Background(), journalfs.Config{
		SpoolDir: spool.Dir,
		StateDir: filepath.Join(spool.Dir, "request-trace-uploader-state"),
		Holder:   "test",
		LockWait: time.Second,
	}, journalfs.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	cfg := config.Config{SourceDir: spool.Dir, SegmentPrefix: config.DefaultSegmentPrefix}
	return evictionFixture{u: &Uploader{cfg: cfg, store: store, point: func(string) {}}, closed: closed}
}

func (f evictionFixture) discover(t *testing.T, sha string, size int64, mtime time.Time) journal.Segment {
	t.Helper()
	seg, _, err := f.u.store.Discover(journal.SegmentInfo{
		Name: filepath.Base(f.closed), Prefix: config.DefaultSegmentPrefix, Index: 0,
		Size: size, ModTime: mtime, SHA256: sha,
	})
	if err != nil {
		t.Fatal(err)
	}
	return seg
}

// An eviction candidate is sized as the space its files take on disk, the
// unit the spool budget measures, so what relief counts as freed is freed.
func TestEvictionCandidatesAreSizedAsTheyTakeDisk(t *testing.T) {
	f := newEvictionFixture(t)
	size, mtime := f.fileIdentity(t)
	want := diskguard.DiskBytes(f.closed, size)
	if want == size {
		t.Skip("this filesystem allocates a small file exactly its length")
	}
	seg := f.discover(t, strings.Repeat("4", 64), size, mtime)

	candidates, err := spoolSegments{u: f.u}.Evictable()
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].Key != evictSegment+seg.ID || candidates[0].Bytes != want {
		t.Fatalf("Evictable() = %+v, want %s with %d bytes, the space it takes on disk (its length is %d)", candidates, evictSegment+seg.ID, want, size)
	}
}

func (f evictionFixture) fileIdentity(t *testing.T) (int64, time.Time) {
	t.Helper()
	info, err := os.Stat(f.closed)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size(), info.ModTime()
}

func TestEvictionNeverListsAFileWhoseCurrentContentIsInABatch(t *testing.T) {
	// The journal orders entries by id, a hash, so several older entries are
	// tried to cover both orders of the two entries under one name.
	olderSortsLast := false
	for extra := int64(1); extra <= 64; extra++ {
		t.Run(fmt.Sprintf("older entry %d bytes larger", extra), func(t *testing.T) {
			if evictionSkipsBatchedContent(t, extra) {
				olderSortsLast = true
			}
		})
	}
	if !olderSortsLast {
		t.Fatal("no case put the older entry after the batched one; widen the range")
	}
}

// evictionSkipsBatchedContent reports whether the older entry sorted after
// the batched one in the journal.
func evictionSkipsBatchedContent(t *testing.T, extra int64) bool {
	f := newEvictionFixture(t)
	size, mtime := f.fileIdentity(t)
	// An older entry under the same name, from content the writer replaced,
	// still waits to be prepared.
	older := f.discover(t, "1111111111111111111111111111111111111111111111111111111111111111", size+extra, mtime.Add(-time.Hour))
	// The file's current content is prepared and in a batch.
	current := f.discover(t, "2222222222222222222222222222222222222222222222222222222222222222", size, mtime)
	if err := f.u.store.BeginPrepare(current.ID); err != nil {
		t.Fatal(err)
	}
	transport := journalfs.TransportPath(f.u.store.TransportDir(), current.ID, "default", ".jsonl.gz")
	if err := os.WriteFile(transport, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := f.u.store.MarkPrepared(current.ID, "fake", map[string]journal.Transport{"default": {Path: transport, Bytes: 1, Rows: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := f.u.store.CreateBatch(journal.Batch{ID: "b1", Backend: "fake", Destination: "default", Members: []journal.Member{{SegmentID: current.ID, Path: transport}}}); err != nil {
		t.Fatal(err)
	}

	candidates, err := spoolSegments{u: f.u}.Evictable()
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 0 {
		t.Fatalf("Evictable() = %+v, want nothing: the file's current content is in a batch", candidates)
	}
	return older.ID > current.ID
}

func TestEvictionRecordsTheLossBeforeDeleting(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	f := newEvictionFixture(t)
	size, mtime := f.fileIdentity(t)
	seg := f.discover(t, "3333333333333333333333333333333333333333333333333333333333333333", size, mtime)
	// The spool directory refuses deletes, as a failing disk would.
	dir := filepath.Dir(f.closed)
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	if err := (spoolSegments{u: f.u}).Evict(evictSegment + seg.ID); err == nil {
		t.Fatal("Evict() error = nil, want the failed delete reported")
	}
	got, _ := f.u.store.Segment(seg.ID)
	if got.State != journal.StateDone || got.Reason != journal.ReasonEvicted {
		t.Fatalf("segment = %s/%s after a failed delete, want done/evicted so it is never prepared from a missing file", got.State, got.Reason)
	}
}

func TestAnEvictionWhoseDirectorySyncFailedIsStillReportedAsADeletion(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	for _, keyed := range []string{"journal entry", "bare file"} {
		t.Run(keyed, func(t *testing.T) {
			f := newEvictionFixture(t)
			key := evictFile + filepath.Base(f.closed)
			if keyed == "journal entry" {
				size, mtime := f.fileIdentity(t)
				key = evictSegment + f.discover(t, "4444444444444444444444444444444444444444444444444444444444444444", size, mtime).ID
			}
			// The spool directory allows the delete but cannot be opened to
			// sync it, so the file is gone and the sync fails.
			dir := filepath.Dir(f.closed)
			if err := os.Chmod(dir, 0o300); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

			err := (spoolSegments{u: f.u}).Evict(key)
			if !errors.Is(err, diskguard.ErrDeletedNotSynced) {
				t.Fatalf("Evict() error = %v, want %v so the guard counts the deletion", err, diskguard.ErrDeletedNotSynced)
			}
			if _, statErr := os.Stat(f.closed); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("source after Evict: %v, want it deleted", statErr)
			}
			if logged := safeError(err); strings.Contains(logged, dir) {
				t.Fatalf("logged error %q names the spool directory", logged)
			}
		})
	}
}

func TestAQuarantineRemovalWhoseDirectorySyncFailedIsCountedAndLogsNoPath(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	quarantine := diskguard.NewQuarantine(filepath.Join(t.TempDir(), "quarantine"))
	dir := quarantine.RejectsDir()
	artifact := filepath.Join(dir, "seg-1.jsonl.gz")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(artifact, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	// The rejects directory allows the delete but cannot be opened to sync
	// it, so the artifact is gone and the sync fails.
	if err := os.Chmod(dir, 0o300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	err := quarantine.Remove(diskguard.RejectsDirName + "/seg-1.jsonl.gz")
	if !errors.Is(err, diskguard.ErrDeletedNotSynced) {
		t.Fatalf("Remove() error = %v, want %v so the guard counts the deletion", err, diskguard.ErrDeletedNotSynced)
	}
	if _, statErr := os.Stat(artifact); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("artifact after Remove: %v, want it deleted", statErr)
	}
	if logged := safeError(err); strings.Contains(logged, dir) {
		t.Fatalf("logged error %q names the quarantine directory", logged)
	}
}
