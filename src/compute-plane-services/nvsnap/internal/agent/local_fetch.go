// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// A checkpoint fetched to this node lands in <CheckpointDir>/<id> in
// place. A restore placeholder may already have that directory (and its
// gpushare store) bind-mounted, created empty by the kubelet, so a fetch
// never replaces the directory or its subdirectories: it fills them.
//
// A fetch marks itself in <CheckpointDir>/.fetching/<id>, outside the
// checkpoint (peers serve the checkpoint's whole tree). The checkpoint is
// complete when its images are there and no fetch is marked: a fetch that
// stopped part way, with the agent, keeps its mark and is redone.

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

const fetchingDirName = ".fetching"

// localFetchLocks serializes the fetches of one checkpoint on this node.
var localFetchLocks sync.Map // checkpoint id -> *sync.Mutex

func lockLocalFetch(id string) func() {
	m, _ := localFetchLocks.LoadOrStore(id, &sync.Mutex{})
	mu := m.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

func fetchMarker(checkpointDir, id string) string {
	return filepath.Join(checkpointDir, fetchingDirName, id)
}

// localCheckpointComplete reports whether checkpoint id is whole on this
// node: a CRIU dump's inventory.img, or a cachedir capture's manifest
// (criu_fallback.go), each written last.
func localCheckpointComplete(checkpointDir, id string) bool {
	if _, err := os.Stat(fetchMarker(checkpointDir, id)); err == nil {
		return false
	}
	fi, err := os.Stat(filepath.Join(checkpointDir, id, "inventory.img"))
	return (err == nil && fi.Size() > 0) || cachedirCaptureComplete(filepath.Join(checkpointDir, id))
}

// beginLocalFetch marks a fetch of id and empties what an earlier fetch
// left, keeping the directories.
func beginLocalFetch(checkpointDir, id string) error {
	if err := os.MkdirAll(filepath.Join(checkpointDir, fetchingDirName), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(fetchMarker(checkpointDir, id), nil, 0o644); err != nil {
		return err
	}
	dir := filepath.Join(checkpointDir, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return clearFiles(dir)
}

// errSourceSpanKept: a done fetch could not drop the source node's restore
// span. The checkpoint is still marked done: fetching it again would cost
// far more than budgeting this restore with that node's span.
var errSourceSpanKept = errors.New("the fetched checkpoint keeps the source node's restore span")

// endLocalFetch records the fetch's outcome: done, or emptied for the
// next try. A done fetch first drops what the source node kept of its
// own (nodeLocalCheckpointFile), whichever tier copied it: the rails and
// the shared filesystem copy a whole directory, and an older peer still
// lists its restore span. It is marked done either way; errSourceSpanKept
// says the span stayed.
func endLocalFetch(checkpointDir, id string, ok bool) error {
	dir := filepath.Join(checkpointDir, id)
	if !ok {
		_ = clearFiles(dir)
		return nil
	}
	spanErr := os.Remove(filepath.Join(dir, restoreSpanFile)) //nolint:gosec // id is one EnsureLocal validated
	if os.IsNotExist(spanErr) {
		spanErr = nil
	}
	if err := os.Remove(fetchMarker(checkpointDir, id)); err != nil {
		return errors.Join(err, spanErr)
	}
	if spanErr != nil {
		return fmt.Errorf("%w: %w", errSourceSpanKept, spanErr)
	}
	return nil
}

// clearFiles removes every non-directory under dir and keeps the
// directories, which a placeholder may hold bind-mounted.
func clearFiles(dir string) error {
	return filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		return os.Remove(p)
	})
}
