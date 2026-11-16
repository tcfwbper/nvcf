// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"errors"
	"path/filepath"
	"slices"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
)

// startupDirs returns an uploader whose state and quarantine directories,
// and their parents, do not exist yet. Its secrets file is missing, so
// startup stops right after it creates the directories.
func startupDirs(t *testing.T) (u *Uploader, root, state, quarantine string) {
	t.Helper()
	root = t.TempDir()
	state = filepath.Join(root, "state-parent", "state")
	quarantine = filepath.Join(root, "quarantine-parent", "quarantine")
	u = &Uploader{cfg: config.Config{
		StateDir:      state,
		QuarantineDir: quarantine,
		SecretsFile:   filepath.Join(root, "missing.json"),
	}}
	return u, root, state, quarantine
}

func replaceSyncDir(t *testing.T, sync func(string) error) {
	t.Helper()
	real := syncDir
	syncDir = sync
	t.Cleanup(func() { syncDir = real })
}

// Startup links each directory it creates in its parent durably, so a power
// loss soon after the first start cannot lose the quarantine or the journal.
func TestStartupLinksNewDirectoriesDurably(t *testing.T) {
	u, root, state, quarantine := startupDirs(t)
	var synced []string
	replaceSyncDir(t, func(d string) error {
		synced = append(synced, d)
		return nil
	})

	_ = u.open(t.Context())

	// Each directory's creation first syncs the link of root, the deepest
	// directory that already exists.
	want := []string{filepath.Dir(root), root, filepath.Dir(state), filepath.Dir(root), root, filepath.Dir(quarantine)}
	if !slices.Equal(synced, want) {
		t.Fatalf("synced %q, want %q", synced, want)
	}
}

// A directory that cannot be linked durably stops startup.
func TestStartupStopsWhenADirectoryLinkCannotBeSynced(t *testing.T) {
	u, _, _, _ := startupDirs(t)
	errSync := errors.New("sync failed")
	replaceSyncDir(t, func(string) error { return errSync })

	if err := u.open(t.Context()); !errors.Is(err, errSync) {
		t.Fatalf("open() error = %v, want the directory sync failure", err)
	}
}
