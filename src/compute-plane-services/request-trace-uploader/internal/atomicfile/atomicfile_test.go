// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package atomicfile

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func names(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, entry := range entries {
		out = append(out, entry.Name())
	}
	return out
}

func TestPublishMakesTheFileVisibleOnlyWhenComplete(t *testing.T) {
	dir := t.TempDir()
	file, err := Create(dir, ".out.*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(dir, "out")
	if _, err := os.Stat(final); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("final name before Publish: %v, want absent", err)
	}
	if err := file.Publish(final); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(final)
	if err != nil || string(data) != "hello" {
		t.Fatalf("published = %q, %v", data, err)
	}
	if got := names(t, dir); len(got) != 1 || got[0] != "out" {
		t.Fatalf("dir = %v, want only the published file", got)
	}
}

func TestDiscardRemovesTheTemporaryFileAndIsSafeTwice(t *testing.T) {
	dir := t.TempDir()
	file, err := Create(dir, ".out.*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	if err := file.Discard(); err != nil {
		t.Fatal(err)
	}
	if err := file.Discard(); err != nil {
		t.Fatalf("second Discard() = %v, want nil", err)
	}
	if got := names(t, dir); len(got) != 0 {
		t.Fatalf("dir = %v, want empty", got)
	}
}

// A failure after the rename must not leave a file that looks published but
// was never made durable.
func TestAFailedPublishLeavesNeitherName(t *testing.T) {
	dir := t.TempDir()
	original := syncDir
	t.Cleanup(func() { syncDir = original })
	syncDir = func(string) error { return errors.New("sync failed") }

	file, err := Create(dir, ".out.*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := file.Publish(filepath.Join(dir, "out")); err == nil {
		t.Fatal("Publish() error = nil, want the sync failure")
	}
	if got := names(t, dir); len(got) != 0 {
		t.Fatalf("dir = %v, want neither the temporary nor the final name", got)
	}
}

func TestErrorsCarryNoPaths(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing-dir")
	if _, err := Create(missing, ".out.*.tmp"); err == nil || strings.Contains(err.Error(), "missing-dir") {
		t.Fatalf("Create() error = %v, want an error with no path", err)
	}
	dir := t.TempDir()
	file, err := Create(dir, ".out.*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	err = file.Publish(filepath.Join(missing, "out"))
	if err == nil || strings.Contains(err.Error(), "missing-dir") || strings.Contains(err.Error(), dir) {
		t.Fatalf("Publish() error = %v, want an error with no path", err)
	}
	if got := names(t, dir); len(got) != 0 {
		t.Fatalf("dir = %v, want the temporary file removed after a failed publish", got)
	}
}

var errSync = errors.New("sync failed")

// recordingSync syncs for real and records each directory, except fail,
// which it refuses as a failing disk would.
func recordingSync(synced *[]string, fail string) func(string) error {
	return func(dir string) error {
		if dir == fail {
			return errSync
		}
		*synced = append(*synced, dir)
		return SyncDir(dir)
	}
}

func TestMkdirDurableSyncsTheParentOfEachNewDirectoryTopDown(t *testing.T) {
	root := t.TempDir()
	a, b, c := filepath.Join(root, "a"), filepath.Join(root, "a", "b"), filepath.Join(root, "a", "b", "c")
	var synced []string
	if err := MkdirDurable(c, recordingSync(&synced, "")); err != nil {
		t.Fatal(err)
	}
	// root exists, so its own link is synced first.
	if want := []string{filepath.Dir(root), root, a, b}; !slices.Equal(synced, want) {
		t.Fatalf("synced %q, want %q", synced, want)
	}
	if info, err := os.Stat(c); err != nil || !info.IsDir() {
		t.Fatalf("Stat(c) = %v, %v; want a directory", info, err)
	}
}

// Each new link is synced before the next level is made, so a creation that
// fails part way (a full disk) leaves only levels whose links are durable,
// and the retry, which sees only the missing levels, leaves none unsynced.
func TestMkdirDurableSyncsEachLinkBeforeMakingTheNextLevel(t *testing.T) {
	root := t.TempDir()
	a, b, c := filepath.Join(root, "a"), filepath.Join(root, "a", "b"), filepath.Join(root, "a", "b", "c")
	below := map[string]string{root: b, a: c} // directory synced -> level that must not exist yet
	err := MkdirDurable(c, func(dir string) error {
		if next, ok := below[dir]; ok {
			if _, err := os.Stat(next); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("%s exists while the link in %s is not synced yet", next, dir)
			}
		}
		return SyncDir(dir)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// A directory that is already there may come from an attempt that failed
// before its sync, or from a build that did not sync, so its link is synced
// again.
func TestMkdirDurableSyncsTheParentOfADirectoryThatExists(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "a")
	if err := os.Mkdir(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	var synced []string
	if err := MkdirDurable(dir, recordingSync(&synced, "")); err != nil {
		t.Fatal(err)
	}
	if want := []string{root}; !slices.Equal(synced, want) {
		t.Fatalf("synced %q, want %q", synced, want)
	}
}

// A directory found on the way down may come from a build or an attempt that
// did not sync its link, so that link is synced before anything is made
// below it.
func TestMkdirDurableSyncsTheLinkOfTheDeepestExistingDirectory(t *testing.T) {
	root := t.TempDir()
	a, b := filepath.Join(root, "a"), filepath.Join(root, "a", "b")
	if err := os.Mkdir(a, 0o750); err != nil {
		t.Fatal(err)
	}
	var synced []string
	if err := MkdirDurable(b, recordingSync(&synced, "")); err != nil {
		t.Fatal(err)
	}
	if want := []string{root, a}; !slices.Equal(synced, want) {
		t.Fatalf("synced %q, want %q", synced, want)
	}
}

// A relative path whose deepest existing directory is the working directory
// still syncs that directory's own link, in its parent.
func TestMkdirDurableSyncsTheLinkOfTheWorkingDirectoryForARelativePath(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	var synced []string
	if err := MkdirDurable(filepath.Join("a", "b"), recordingSync(&synced, "")); err != nil {
		t.Fatal(err)
	}
	if len(synced) == 0 || synced[0] != filepath.Dir(root) {
		t.Fatalf("synced %q, want the working directory's parent %q first", synced, filepath.Dir(root))
	}
}

// After a failed sync at any level, the next attempt syncs that level and
// every level below it again, starting with the link of the deepest
// directory that exists, so no link stays unsynced.
func TestMkdirDurableRetrySyncsTheLinkWhoseSyncFailed(t *testing.T) {
	for _, tt := range []struct {
		name      string
		fail      string
		wantRetry []string
	}{
		{name: "link of the top new directory", fail: ".", wantRetry: []string{"..", ".", "a", "a/b"}},
		{name: "link of a middle directory", fail: "a", wantRetry: []string{".", "a", "a/b"}},
		{name: "link of the leaf", fail: "a/b", wantRetry: []string{"a", "a/b"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			abs := func(rel string) string { return filepath.Join(root, filepath.FromSlash(rel)) }
			leaf := abs("a/b/c")
			var synced []string
			if err := MkdirDurable(leaf, recordingSync(&synced, abs(tt.fail))); !errors.Is(err, errSync) {
				t.Fatalf("first MkdirDurable() = %v, want the sync failure", err)
			}

			synced = nil
			if err := MkdirDurable(leaf, recordingSync(&synced, "")); err != nil {
				t.Fatalf("retry: %v", err)
			}
			var want []string
			for _, rel := range tt.wantRetry {
				want = append(want, abs(rel))
			}
			if !slices.Equal(synced, want) {
				t.Fatalf("retry synced %q, want %q", synced, want)
			}
			if info, err := os.Stat(leaf); err != nil || !info.IsDir() {
				t.Fatalf("Stat(leaf) = %v, %v; want a directory", info, err)
			}
		})
	}
}

// A failed sync never removes a directory that was there before the call,
// or anything in it.
func TestMkdirDurableKeepsWhatItDidNotCreate(t *testing.T) {
	root := t.TempDir()
	kept := filepath.Join(root, "a", "kept")
	if err := os.MkdirAll(filepath.Dir(kept), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(kept, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	var synced []string
	if err := MkdirDurable(filepath.Join(root, "a", "b", "c"), recordingSync(&synced, filepath.Join(root, "a"))); !errors.Is(err, errSync) {
		t.Fatalf("MkdirDurable() = %v, want the sync failure", err)
	}
	if _, err := os.Stat(kept); err != nil {
		t.Fatalf("file in an existing directory: %v, want it kept", err)
	}
}

// A file that appears where a level is about to be made, after the scan,
// is refused like MkdirAll refuses it, not taken for a directory.
func TestMkdirDurableRefusesAFileMadeAtALevelMeanwhile(t *testing.T) {
	root := t.TempDir()
	leaf := filepath.Join(root, "a", "b")
	err := MkdirDurable(leaf, func(dir string) error {
		if dir == root {
			if err := os.WriteFile(leaf, nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		return SyncDir(dir)
	})
	if err == nil {
		t.Fatal("MkdirDurable() = nil, want an error for a file at the leaf")
	}
}

func TestMkdirDurableErrorsCarryNoPaths(t *testing.T) {
	plain := filepath.Join(t.TempDir(), "plain-file")
	if err := os.WriteFile(plain, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	err := MkdirDurable(filepath.Join(plain, "sub"), SyncDir)
	if err == nil || strings.Contains(err.Error(), "plain-file") {
		t.Fatalf("MkdirDurable() = %v, want an error with no path", err)
	}
}
