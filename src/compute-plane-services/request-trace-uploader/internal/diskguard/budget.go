// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package diskguard

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Budget is the spool's own size, such as its emptyDir size limit, and the
// bytes the spool holds. Unlike a Volume, a spool can hold more than its
// budget. SpoolBudget implements it.
type Budget interface {
	Held() (size, held int64, err error)
}

type spoolBudget struct {
	size  int64
	roots []string
}

// SpoolBudget is a budget of size bytes for what is under roots: the spool,
// its state and its quarantine. It counts the space files and directories
// take on disk, in blocks as the kubelet counts an emptyDir against its
// size limit, a file with several names once. A root reached through a
// symlink is measured at its target, a root inside another is counted once,
// and a root not created yet holds nothing.
func SpoolBudget(size int64, roots ...string) Budget {
	return spoolBudget{size: size, roots: roots}
}

func (b spoolBudget) Held() (int64, int64, error) {
	// Roots are resolved on every call, since one may be created or
	// replaced after the budget is.
	resolved := make([]string, 0, len(b.roots))
	for _, root := range b.roots {
		if target, err := filepath.EvalSymlinks(root); err == nil {
			root = target
		}
		resolved = append(resolved, root)
	}
	var held int64
	seen := map[fileID]bool{}
	for _, root := range outermost(resolved) {
		size, err := treeSize(root, seen)
		if err != nil {
			return 0, 0, fmt.Errorf("measure the spool: %w", err)
		}
		held += size
	}
	return b.size, held, nil
}

// outermost drops every root that is inside, or the same as, another.
func outermost(roots []string) []string {
	var kept []string
	for _, root := range roots {
		root = filepath.Clean(root)
		inside := slices.ContainsFunc(roots, func(other string) bool {
			rel, err := filepath.Rel(filepath.Clean(other), root)
			return err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
		})
		if !inside && !slices.Contains(kept, root) {
			kept = append(kept, root)
		}
	}
	return kept
}

// fileID names a file across its links.
type fileID struct{ dev, ino uint64 }

// DiskBytes is the space the file at path takes on disk, as the budget
// counts it, or fallback when the file cannot be read.
func DiskBytes(path string, fallback int64) int64 {
	info, err := os.Lstat(path)
	if err != nil {
		return fallback
	}
	bytes, _, _ := diskUsage(info)
	return bytes
}
