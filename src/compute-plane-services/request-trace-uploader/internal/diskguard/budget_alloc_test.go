// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package diskguard_test

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/diskguard"
)

// allocatedUnder is what the kubelet counts for an emptyDir: the blocks of
// every file and directory under root, a hard-linked file once.
func allocatedUnder(t *testing.T, root string) int64 {
	t.Helper()
	seen := map[uint64]bool{}
	var total int64
	err := filepath.WalkDir(root, func(path string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		var st syscall.Stat_t
		if err := syscall.Lstat(path, &st); err != nil {
			return err
		}
		if st.Nlink > 1 && st.Mode&syscall.S_IFMT != syscall.S_IFDIR {
			if seen[uint64(st.Ino)] {
				return nil
			}
			seen[uint64(st.Ino)] = true
		}
		total += int64(st.Blocks) * 512
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return total
}

// Many small files take whole blocks, which is what counts against an
// emptyDir's size limit, so the budget counts the space files and
// directories take on disk, not their lengths.
func TestSpoolBudgetCountsTheSpaceSmallFilesTakeOnDisk(t *testing.T) {
	spool := t.TempDir()
	for i := range 10 {
		if err := os.WriteFile(filepath.Join(spool, fmt.Sprintf("journal-%d", i)), []byte("26 bytes of journal record"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	_, held, err := diskguard.SpoolBudget(1<<30, spool).Held()
	if err != nil {
		t.Fatalf("Held: %v", err)
	}
	if want := allocatedUnder(t, spool); held != want {
		t.Fatalf("held %d bytes, want %d, the space the files and directory take on disk (their lengths are 260)", held, want)
	}
}

// A hard-linked file takes its blocks once, as the kubelet counts it.
func TestSpoolBudgetCountsAHardLinkedFileOnce(t *testing.T) {
	spool := t.TempDir()
	file := filepath.Join(spool, "segment")
	if err := os.WriteFile(file, make([]byte, 64<<10), 0o600); err != nil {
		t.Fatal(err)
	}
	budget := diskguard.SpoolBudget(1<<30, spool)
	_, before, err := budget.Held()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Link(file, filepath.Join(spool, "segment-link")); err != nil {
		t.Fatal(err)
	}
	_, after, err := budget.Held()
	if err != nil {
		t.Fatal(err)
	}
	if want := allocatedUnder(t, spool); after != want || after-before > 64<<10/2 {
		t.Fatalf("held %d after the link (%d before), want %d: the linked file once", after, before, want)
	}
}

// DiskBytes is the space a file takes on disk, the unit eviction frees in.
func TestDiskBytesIsTheSpaceAFileTakesOnDisk(t *testing.T) {
	file := filepath.Join(t.TempDir(), "transport")
	if err := os.WriteFile(file, []byte("small"), 0o600); err != nil {
		t.Fatal(err)
	}
	var st syscall.Stat_t
	if err := syscall.Lstat(file, &st); err != nil {
		t.Fatal(err)
	}
	if got, want := diskguard.DiskBytes(file, 5), int64(st.Blocks)*512; got != want {
		t.Fatalf("DiskBytes = %d, want %d", got, want)
	}
	if got := diskguard.DiskBytes(filepath.Join(t.TempDir(), "gone"), 5); got != 5 {
		t.Fatalf("DiskBytes of a missing file = %d, want the fallback 5", got)
	}
}

// A spool budget holds the space taken under its roots, each counted once
// when one root is inside another, and a root not created yet holds nothing.
func TestSpoolBudgetMeasuresTheFilesUnderItsRoots(t *testing.T) {
	spool := t.TempDir()
	quarantine := t.TempDir()
	write := func(path string, size int) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, make([]byte, size), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(spool, "request-trace.000001.jsonl.gz"), 100)
	write(filepath.Join(spool, "state", "transport", "seg.dest.jsonl.gz"), 50)
	write(filepath.Join(quarantine, "seg-1", "request-trace.000000.jsonl.gz"), 25)
	roots := []string{spool, filepath.Join(spool, "state"), quarantine, filepath.Join(t.TempDir(), "not-yet")}

	size, held, err := diskguard.SpoolBudget(1000, roots...).Held()
	if err != nil {
		t.Fatalf("Held: %v", err)
	}
	if want := allocatedUnder(t, spool) + allocatedUnder(t, quarantine); size != 1000 || held != want {
		t.Fatalf("Held = %d, %d; want 1000 and %d", size, held, want)
	}
}

// A spool reached through a symlinked directory is measured at its target,
// with the state and quarantine directories under it counted once.
func TestSpoolBudgetFollowsASymlinkedSpoolDirectory(t *testing.T) {
	spool := t.TempDir()
	for path, size := range map[string]int{
		filepath.Join(spool, "request-trace.000001.jsonl.gz"):                                  100,
		filepath.Join(spool, "request-trace-uploader-state", "transport", "seg.dest.jsonl.gz"): 50,
		filepath.Join(spool, "request-trace-uploader-quarantine", "seg-1", "request-trace.gz"): 25,
	} {
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, make([]byte, size), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(t.TempDir(), "spool")
	if err := os.Symlink(spool, link); err != nil {
		t.Fatal(err)
	}

	_, held, err := diskguard.SpoolBudget(1000, link, filepath.Join(link, "request-trace-uploader-state"), filepath.Join(link, "request-trace-uploader-quarantine")).Held()
	if err != nil {
		t.Fatalf("Held: %v", err)
	}
	if want := allocatedUnder(t, spool); held != want {
		t.Fatalf("held = %d, want %d from the symlink's target", held, want)
	}
}
