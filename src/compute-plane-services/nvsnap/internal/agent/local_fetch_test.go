// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalFetch_InterruptedFetchIsNotComplete(t *testing.T) {
	root := t.TempDir()
	if err := beginLocalFetch(root, "c1"); err != nil {
		t.Fatal(err)
	}
	// The fetch got the images, then the agent stopped.
	if err := os.WriteFile(filepath.Join(root, "c1", "inventory.img"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if localCheckpointComplete(root, "c1") {
		t.Fatal("an interrupted fetch reads as complete")
	}
	if err := endLocalFetch(root, "c1", true); err != nil {
		t.Fatal(err)
	}
	if !localCheckpointComplete(root, "c1") {
		t.Error("a finished fetch reads as incomplete")
	}
}

func TestLocalFetch_CapturedCheckpointIsComplete(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "c1"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "c1", "inventory.img"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !localCheckpointComplete(root, "c1") {
		t.Error("a checkpoint captured on this node reads as incomplete")
	}
}

// A placeholder may hold the checkpoint directory and its gpushare store
// bind-mounted: a retried fetch keeps both directories (same inode).
func TestLocalFetch_RetryKeepsTheDirectories(t *testing.T) {
	root := t.TempDir()
	store := filepath.Join(root, "c1", "gpushare")
	if err := os.MkdirAll(store, 0o755); err != nil {
		t.Fatal(err)
	}
	before, _ := os.Stat(store)
	if err := beginLocalFetch(root, "c1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store, "chunk-0"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := endLocalFetch(root, "c1", false); err != nil {
		t.Fatal(err)
	}
	if err := beginLocalFetch(root, "c1"); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(store)
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("the gpushare store directory was replaced")
	}
	if _, err := os.Stat(filepath.Join(store, "chunk-0")); !os.IsNotExist(err) {
		t.Error("a failed fetch's file survived the retry")
	}
}

// TestLocalFetch_DoneFetchDropsTheSourceNodesSpan: a tier that copies the
// source's whole directory (the rails, the shared filesystem) brings the
// source node's restore span with it; a done fetch drops it, so this node
// is still new to the checkpoint. A failed fetch empties the directory.
func TestLocalFetch_DoneFetchDropsTheSourceNodesSpan(t *testing.T) {
	root := t.TempDir()
	if err := beginLocalFetch(root, "c1"); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string]string{"inventory.img": "x", restoreSpanFile: "33039\n"} {
		if err := os.WriteFile(filepath.Join(root, "c1", name), []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := endLocalFetch(root, "c1", true); err != nil {
		t.Fatal(err)
	}
	if hasRestoreSpan(filepath.Join(root, "c1")) {
		t.Errorf("the fetched checkpoint kept the source node's %s", restoreSpanFile)
	}
	if !localCheckpointComplete(root, "c1") {
		t.Error("a finished fetch reads as incomplete")
	}
	// A done fetch of a checkpoint without one is fine too.
	if err := beginLocalFetch(root, "c2"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "c2", "inventory.img"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := endLocalFetch(root, "c2", true); err != nil {
		t.Errorf("done fetch with no span file: %v", err)
	}
}

// TestLocalFetch_KeptSpanStillMarksTheFetchDone: when the source node's
// span cannot be dropped, the fetch is still done (a refetch would cost far
// more than one restore budgeted with another node's span), and the error
// says the span stayed.
func TestLocalFetch_KeptSpanStillMarksTheFetchDone(t *testing.T) {
	root := t.TempDir()
	if err := beginLocalFetch(root, "c1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "c1", "inventory.img"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A directory in the span file's place cannot be removed by os.Remove
	// while it holds a file.
	if err := os.MkdirAll(filepath.Join(root, "c1", restoreSpanFile, "x"), 0o755); err != nil {
		t.Fatal(err)
	}
	err := endLocalFetch(root, "c1", true)
	if !errors.Is(err, errSourceSpanKept) {
		t.Fatalf("endLocalFetch = %v, want errSourceSpanKept", err)
	}
	if !localCheckpointComplete(root, "c1") {
		t.Error("the fetch is not marked done; the next restore would fetch the whole checkpoint again")
	}
}
