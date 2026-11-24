// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package modelcache

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// modelTree writes a small model tree: nested files, an empty directory,
// an executable, and a relative symlink inside the tree.
func modelTree(t *testing.T) string {
	t.Helper()
	src := t.TempDir()
	must(t, os.MkdirAll(filepath.Join(src, "m/weights"), 0o755))
	must(t, os.MkdirAll(filepath.Join(src, "m/empty"), 0o750))
	must(t, os.WriteFile(filepath.Join(src, "m/config.json"), []byte(`{"a":1}`), 0o644))
	must(t, os.WriteFile(filepath.Join(src, "m/weights/w-0001.safetensors"), bytes.Repeat([]byte{7}, 3<<20), 0o644))
	must(t, os.WriteFile(filepath.Join(src, "m/run.sh"), []byte("#!/bin/sh\n"), 0o755))
	must(t, os.Symlink("weights/w-0001.safetensors", filepath.Join(src, "m/latest")))
	return src
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func harvest(t *testing.T, s *Store, key, src string) string {
	t.Helper()
	gen, err := s.Harvest(key, src, Limits{})
	if err != nil {
		t.Fatalf("harvest: %v", err)
	}
	return gen
}

func TestHarvestThenSeedReproducesTheTree(t *testing.T) {
	s := &Store{Root: t.TempDir()}
	src := modelTree(t)
	harvest(t, s, "k1", src)

	gen, m, ok := s.Latest("k1")
	if !ok {
		t.Fatal("no complete generation after harvest")
	}
	dst := t.TempDir()
	if err := Seed(gen, m, dst); err != nil {
		t.Fatalf("seed: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(dst, "m/config.json"))
	if err != nil || string(b) != `{"a":1}` {
		t.Fatalf("config.json = %q, %v", b, err)
	}
	if fi, err := os.Stat(filepath.Join(dst, "m/run.sh")); err != nil {
		t.Fatalf("run.sh: %v", err)
	} else if fi.Mode().Perm() != 0o755 {
		t.Fatalf("run.sh mode = %v; want 0755", fi.Mode().Perm())
	}
	if fi, err := os.Stat(filepath.Join(dst, "m/empty")); err != nil || !fi.IsDir() || fi.Mode().Perm() != 0o750 {
		t.Fatalf("empty dir = %v, %v; want a 0750 directory", fi, err)
	}
	if tgt, err := os.Readlink(filepath.Join(dst, "m/latest")); err != nil || tgt != "weights/w-0001.safetensors" {
		t.Fatalf("symlink = %q, %v", tgt, err)
	}
	if fi, err := os.Stat(filepath.Join(dst, "m/weights/w-0001.safetensors")); err != nil || fi.Size() != 3<<20 {
		t.Fatalf("weights = %v, %v", fi, err)
	}
}

func TestSeedRefusesACorruptedFile(t *testing.T) {
	s := &Store{Root: t.TempDir()}
	harvest(t, s, "k1", modelTree(t))
	gen, m, _ := s.Latest("k1")

	// Same size, different bytes: only the checksum can tell.
	p := filepath.Join(gen, TreeDir, "m/weights/w-0001.safetensors")
	f, err := os.OpenFile(p, os.O_WRONLY, 0)
	must(t, err)
	_, err = f.WriteAt([]byte{8}, 1<<20)
	must(t, err)
	must(t, f.Close())

	err = Seed(gen, m, t.TempDir())
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("seed of a corrupted generation: err = %v, want ErrCorrupt", err)
	}
}

func TestSeedRefusesAMissingFile(t *testing.T) {
	s := &Store{Root: t.TempDir()}
	harvest(t, s, "k1", modelTree(t))
	gen, m, _ := s.Latest("k1")
	must(t, os.Remove(filepath.Join(gen, TreeDir, "m/config.json")))
	if err := Seed(gen, m, t.TempDir()); err == nil {
		t.Fatal("seed with a missing file succeeded")
	}
}

func TestSeedRefusesANonEmptyTarget(t *testing.T) {
	s := &Store{Root: t.TempDir()}
	harvest(t, s, "k1", modelTree(t))
	gen, m, _ := s.Latest("k1")
	dst := t.TempDir()
	must(t, os.WriteFile(filepath.Join(dst, "partial"), []byte("x"), 0o644))
	if err := Seed(gen, m, dst); !errors.Is(err, ErrTargetNotEmpty) {
		t.Fatalf("seed into a non-empty target: err = %v, want ErrTargetNotEmpty", err)
	}
}

func TestHarvestRejectsAnEscapingSymlink(t *testing.T) {
	s := &Store{Root: t.TempDir()}
	for _, tgt := range []string{"/etc/passwd", "../../outside", "m/../../x"} {
		src := modelTree(t)
		must(t, os.Symlink(tgt, filepath.Join(src, "m/bad")))
		if _, err := s.Harvest("k-"+strings.ReplaceAll(tgt, "/", "_"), src, Limits{}); !errors.Is(err, ErrUnsupported) {
			t.Fatalf("harvest with symlink -> %q: err = %v, want ErrUnsupported", tgt, err)
		}
	}
	if gens, _ := filepath.Glob(filepath.Join(s.Root, "*", GenPrefix+"*")); len(gens) != 0 {
		t.Fatalf("a rejected harvest published %v", gens)
	}
}

func TestHarvestRejectsATreeOverItsLimit(t *testing.T) {
	s := &Store{Root: t.TempDir()}
	if _, err := s.Harvest("k1", modelTree(t), Limits{MaxBytes: 1 << 20}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
	if _, _, ok := s.Latest("k1"); ok {
		t.Fatal("an over-limit harvest published a generation")
	}
}

func TestHarvestLeavesNoAttemptBehindOnFailure(t *testing.T) {
	s := &Store{Root: t.TempDir()}
	_, _ = s.Harvest("k1", modelTree(t), Limits{MaxBytes: 1})
	left, _ := filepath.Glob(filepath.Join(s.Root, "k1", AttemptPrefix+"*"))
	if len(left) != 0 {
		t.Fatalf("failed harvest left %v", left)
	}
}

func TestIncompleteGenerationIsInvisible(t *testing.T) {
	s := &Store{Root: t.TempDir()}
	harvest(t, s, "k1", modelTree(t))
	gen, _, _ := s.Latest("k1")
	must(t, os.Remove(filepath.Join(gen, CompleteFile)))
	if _, _, ok := s.Latest("k1"); ok {
		t.Fatal("a generation without its completion marker was returned")
	}
}

func TestLatestPrefersTheNewestGeneration(t *testing.T) {
	s := &Store{Root: t.TempDir()}
	first := harvest(t, s, "k1", modelTree(t))
	time.Sleep(2 * time.Millisecond)
	second := harvest(t, s, "k1", modelTree(t))
	gen, _, ok := s.Latest("k1")
	if !ok || gen != second || gen == first {
		t.Fatalf("Latest = %s, want %s", gen, second)
	}
}

func TestKeyDependsOnEveryInput(t *testing.T) {
	base := Key("tenant:a", "ngc://m:1", "img@sha256:1", "recipe")
	for _, k := range []string{
		Key("tenant:b", "ngc://m:1", "img@sha256:1", "recipe"),
		Key("tenant:a", "ngc://m:2", "img@sha256:1", "recipe"),
		Key("tenant:a", "ngc://m:1", "img@sha256:2", "recipe"),
		Key("tenant:a", "ngc://m:1", "img@sha256:1", "recipe2"),
	} {
		if k == base {
			t.Fatalf("key %s does not change with an input", k)
		}
	}
	if !ValidKey(base) || ValidKey("../x") || ValidKey("") {
		t.Fatal("ValidKey accepts a bad key or rejects a good one")
	}
}

func TestManifestPathsAreConfined(t *testing.T) {
	for _, p := range []string{"", "/abs", "../x", "a/../../x", "a/./b", "a//b"} {
		if validRel(p) {
			t.Fatalf("validRel(%q) = true", p)
		}
	}
	for _, p := range []string{"a", "a/b.json", ".hidden/x"} {
		if !validRel(p) {
			t.Fatalf("validRel(%q) = false", p)
		}
	}
}

func TestEvictRemovesAGenerationAndUsageCountsIt(t *testing.T) {
	s := &Store{Root: t.TempDir()}
	harvest(t, s, "k1", modelTree(t))
	used, err := s.Usage()
	must(t, err)
	if used < 3<<20 {
		t.Fatalf("usage = %d, want at least the 3 MiB file", used)
	}
	must(t, s.EvictKey("k1"))
	if _, _, ok := s.Latest("k1"); ok {
		t.Fatal("evicted key still has a generation")
	}
	if used, _ := s.Usage(); used != 0 {
		t.Fatalf("usage after evict = %d", used)
	}
}

func TestCleanAttemptsRemovesOnlyAttempts(t *testing.T) {
	s := &Store{Root: t.TempDir()}
	harvest(t, s, "k1", modelTree(t))
	stale := filepath.Join(s.Root, "k1", AttemptPrefix+"dead")
	must(t, os.MkdirAll(filepath.Join(stale, TreeDir), 0o755))
	must(t, s.CleanAttempts())
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("stale attempt survived")
	}
	if _, _, ok := s.Latest("k1"); !ok {
		t.Fatal("CleanAttempts removed a complete generation")
	}
}

func TestFetchVerifiesEachFileAgainstTheManifest(t *testing.T) {
	s := &Store{Root: t.TempDir()}
	harvest(t, s, "k1", modelTree(t))
	gen, m, _ := s.Latest("k1")

	dst := &Store{Root: t.TempDir()}
	open := func(rel string) (ReadCloser, error) { return os.Open(filepath.Join(gen, TreeDir, rel)) }
	if _, err := dst.Fetch("k1", m, open, Limits{}); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if _, _, ok := dst.Latest("k1"); !ok {
		t.Fatal("fetch published nothing")
	}

	// A source serving other bytes than its manifest is refused.
	bad := &Store{Root: t.TempDir()}
	lie := func(rel string) (ReadCloser, error) {
		if strings.HasSuffix(rel, ".json") {
			return nopCloser{strings.NewReader(`{"a":2}`)}, nil
		}
		return open(rel)
	}
	if _, err := bad.Fetch("k1", m, lie, Limits{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("fetch from a lying source: err = %v, want ErrCorrupt", err)
	}
	if _, _, ok := bad.Latest("k1"); ok {
		t.Fatal("a failed fetch published a generation")
	}
}

func TestFetchRejectsAManifestWithAnEscapingPath(t *testing.T) {
	m := Manifest{Version: ManifestVersion, Entries: []Entry{{Path: "../evil", Type: TypeFile, Size: 1, SHA256: strings.Repeat("0", 64)}}}
	s := &Store{Root: t.TempDir()}
	open := func(string) (ReadCloser, error) { return nopCloser{strings.NewReader("x")}, nil }
	if _, err := s.Fetch("k1", m, open, Limits{}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("err = %v, want ErrUnsupported", err)
	}
}

type nopCloser struct{ *strings.Reader }

func (nopCloser) Close() error { return nil }

func init() { testKey = func(k string) bool { return strings.HasPrefix(k, "k") } }

func TestHarvestRefusesATreeWrittenAfterTheDownload(t *testing.T) {
	s := &Store{Root: t.TempDir()}
	src := modelTree(t)
	finished := time.Now().Add(-time.Hour)
	old := finished.Add(-time.Minute)
	ts := []unix.Timeval{unix.NsecToTimeval(old.UnixNano()), unix.NsecToTimeval(old.UnixNano())}
	must(t, filepath.Walk(src, func(p string, _ os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		return unix.Lutimes(p, ts) // the symlink itself, not its target
	}))
	// Untouched since the download: cached.
	if _, err := s.Harvest("k1", src, Limits{NotAfter: finished}); err != nil {
		t.Fatalf("a tree untouched since the download: %v", err)
	}
	// The engine wrote a file into the volume after it started.
	must(t, os.WriteFile(filepath.Join(src, "m/engine.lock"), nil, 0o644))
	if _, err := s.Harvest("k2", src, Limits{NotAfter: finished}); !errors.Is(err, ErrWrittenAfter) {
		t.Fatalf("err = %v, want ErrWrittenAfter", err)
	}
	if _, _, ok := s.Latest("k2"); ok {
		t.Fatal("a tree written after the download was cached")
	}
}

func TestHarvestPublishesOnlyTheTreeTheDownloadFinishedWith(t *testing.T) {
	s := &Store{Root: t.TempDir()}
	src := modelTree(t)
	ents, err := Scan(src, 0)
	must(t, err)
	fp := Fingerprint(ents)
	if _, err := s.Harvest("k1", src, Limits{Fingerprint: fp}); err != nil {
		t.Fatalf("unchanged tree: %v", err)
	}
	// The engine removed a file before the harvest read the volume: every
	// remaining file checks out, but the tree is not the download's.
	must(t, os.Remove(filepath.Join(src, "m/config.json")))
	if _, err := s.Harvest("k2", src, Limits{Fingerprint: fp}); !errors.Is(err, ErrChanged) {
		t.Fatalf("err = %v, want ErrChanged", err)
	}
	if _, _, ok := s.Latest("k2"); ok {
		t.Fatal("a tree missing a downloaded file was cached")
	}
}

// A workload swaps a directory for a symlink to the host while the agent
// walks its volume: the walk must not follow it.
func TestHarvestDoesNotFollowADirectorySwappedForASymlink(t *testing.T) {
	src := t.TempDir()
	must(t, os.MkdirAll(filepath.Join(src, "a"), 0o755))
	must(t, os.WriteFile(filepath.Join(src, "a/x"), []byte("x"), 0o644))
	outside := t.TempDir()
	must(t, os.WriteFile(filepath.Join(outside, "secret"), []byte("host"), 0o600))
	var copied []string
	err := walkNoFollow(src, 0, func(n node) error {
		copied = append(copied, n.rel)
		if n.rel == "a" { // examined; now swapped before it is entered
			must(t, os.Rename(filepath.Join(src, "a"), filepath.Join(src, "a.old")))
			must(t, os.Symlink(outside, filepath.Join(src, "a")))
		}
		return nil
	})
	if err == nil {
		t.Fatalf("the walk entered a swapped directory: %v", copied)
	}
	for _, c := range copied {
		if strings.Contains(c, "secret") {
			t.Fatalf("the walk reached outside the tree: %v", copied)
		}
	}
}

func TestHarvestRefusesAFIFOWithoutBlocking(t *testing.T) {
	src := modelTree(t)
	must(t, unix.Mkfifo(filepath.Join(src, "m/pipe"), 0o644))
	done := make(chan error, 1)
	go func() {
		_, err := (&Store{Root: t.TempDir()}).Harvest("k1", src, Limits{})
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrUnsupported) {
			t.Fatalf("err = %v, want ErrUnsupported", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("harvest blocked on a FIFO")
	}
}

func TestSeedRestoresModesAndMtimes(t *testing.T) {
	s := &Store{Root: t.TempDir()}
	src := modelTree(t)
	when := time.Date(2026, 1, 2, 3, 4, 5, 6000, time.UTC)
	must(t, os.Chmod(filepath.Join(src, "m/config.json"), 0o600))
	must(t, os.Chtimes(filepath.Join(src, "m/config.json"), when, when))
	harvest(t, s, "k1", src)
	gen, m, _ := s.Latest("k1")
	// The cache's own copy stays readable whatever the download's mode.
	if fi, err := os.Stat(filepath.Join(gen, TreeDir, "m/config.json")); err != nil {
		t.Fatalf("cached copy: %v", err)
	} else if fi.Mode().Perm() != 0o644 {
		t.Fatalf("cached copy mode = %v; want 0644", fi.Mode().Perm())
	}
	dst := t.TempDir()
	must(t, Seed(gen, m, dst))
	fi, err := os.Stat(filepath.Join(dst, "m/config.json"))
	must(t, err)
	if fi.Mode().Perm() != 0o600 || !fi.ModTime().Equal(when) {
		t.Fatalf("seeded config.json mode %v mtime %v; want 0600 at %v", fi.Mode().Perm(), fi.ModTime(), when)
	}
	// The seeded tree is the download's in everything but change times.
	a, _ := Scan(src, 0)
	b, _ := Scan(dst, 0)
	for i := range a {
		a[i].ctime = 0
	}
	for i := range b {
		b[i].ctime = 0
	}
	if Fingerprint(a) != Fingerprint(b) {
		t.Fatal("the seeded tree differs from the download in path, size, mode, owner or mtime")
	}
}

func TestSeedRefusesAnotherUsersTreeWhenNotRoot(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("runs as root: any owner can be given")
	}
	s := &Store{Root: t.TempDir()}
	harvest(t, s, "k1", modelTree(t))
	gen, m, _ := s.Latest("k1")
	m.Entries[0].UID = os.Geteuid() + 1
	dst := t.TempDir()
	if err := Seed(gen, m, dst); !errors.Is(err, ErrOwner) {
		t.Fatalf("err = %v, want ErrOwner", err)
	}
	if ents, _ := os.ReadDir(dst); len(ents) != 0 {
		t.Fatal("the refused seed wrote files")
	}
}

// A writer that rewrites a file with same-length bytes and puts its mtime
// back still changes its ctime.
func TestHarvestRefusesAFileRewrittenWithItsMtimeRestored(t *testing.T) {
	s := &Store{Root: t.TempDir()}
	src := modelTree(t)
	ents, err := Scan(src, 0)
	must(t, err)
	fp := Fingerprint(ents)
	p := filepath.Join(src, "m/config.json")
	fi, err := os.Stat(p)
	must(t, err)
	time.Sleep(20 * time.Millisecond)
	must(t, os.WriteFile(p, []byte(`{"a":2}`), 0o644))
	must(t, os.Chtimes(p, fi.ModTime(), fi.ModTime()))
	if _, err := s.Harvest("k1", src, Limits{Fingerprint: fp}); !errors.Is(err, ErrChanged) {
		t.Fatalf("err = %v, want ErrChanged", err)
	}
}

func TestHarvestOfAnEmptyMeasuredTreeCopiesNoBytes(t *testing.T) {
	src := t.TempDir()
	must(t, os.WriteFile(filepath.Join(src, "grew"), []byte("x"), 0o644))
	_, err := (&Store{Root: t.TempDir()}).Harvest("k1", src, Limits{MaxBytes: 0, ExactBytes: true})
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
}

func TestSeedGivesTheVolumeItsRecordedMode(t *testing.T) {
	s := &Store{Root: t.TempDir()}
	src := modelTree(t)
	must(t, unix.Chmod(src, 0o2770))
	harvest(t, s, "k1", src)
	gen, m, _ := s.Latest("k1")
	dst := t.TempDir()
	must(t, Seed(gen, m, dst))
	fi, err := os.Stat(dst)
	must(t, err)
	if fi.Mode()&(os.ModePerm|os.ModeSetgid) != 0o770|os.ModeSetgid {
		t.Fatalf("volume mode = %v, want setgid 0770", fi.Mode())
	}
}

func TestScanStopsAtItsBound(t *testing.T) {
	src := t.TempDir()
	for i := 0; i < 10; i++ {
		must(t, os.WriteFile(filepath.Join(src, fmt.Sprint(i)), nil, 0o644))
	}
	if _, err := Scan(src, 5); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
	if ents, err := Scan(src, 20); err != nil || len(ents) != 11 {
		t.Fatalf("%d entries, %v; want the root and 10", len(ents), err)
	}
}

func TestParallelSeedCopiesEveryShardAndCatchesOneBadOne(t *testing.T) {
	src := t.TempDir()
	for i := 0; i < 40; i++ {
		must(t, os.WriteFile(filepath.Join(src, fmt.Sprintf("shard-%02d", i)), bytes.Repeat([]byte{byte(i)}, 64<<10), 0o644))
	}
	s := &Store{Root: t.TempDir()}
	harvest(t, s, "k1", src)
	gen, m, _ := s.Latest("k1")
	dst := t.TempDir()
	must(t, Seed(gen, m, dst))
	for i := 0; i < 40; i++ {
		b, err := os.ReadFile(filepath.Join(dst, fmt.Sprintf("shard-%02d", i)))
		if err != nil || !bytes.Equal(b, bytes.Repeat([]byte{byte(i)}, 64<<10)) {
			t.Fatalf("shard %d wrong after a parallel seed: %v", i, err)
		}
	}
	f, err := os.OpenFile(filepath.Join(gen, TreeDir, "shard-27"), os.O_WRONLY, 0)
	must(t, err)
	_, err = f.WriteAt([]byte{0xff}, 100)
	must(t, err)
	must(t, f.Close())
	if err := Seed(gen, m, t.TempDir()); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}

// A file whose inode changes during its copy is refused, whatever kept its
// size and mtime: a shard rewritten in place with its mtime set back, and
// a pod torn down mid-harvest (unlinking changes the inode too). Nothing
// is published; the next cold start downloads and harvests again.
func TestHarvestRefusesAFileChangedDuringItsCopy(t *testing.T) {
	for name, change := range map[string]func(src string){
		"rewritten, mtime restored": func(src string) {
			w := filepath.Join(src, "m/weights/w-0001.safetensors")
			fi, err := os.Stat(w)
			must(t, err)
			must(t, os.WriteFile(w, bytes.Repeat([]byte{9}, 3<<20), 0o644))
			must(t, os.Chtimes(w, fi.ModTime(), fi.ModTime()))
		},
		"volume removed": func(src string) { must(t, os.RemoveAll(src)) },
	} {
		src := modelTree(t)
		afterOpen = func() { change(src) }
		s := &Store{Root: t.TempDir()}
		_, err := s.Harvest("k1", src, Limits{})
		afterOpen = nil
		if !errors.Is(err, ErrChanged) {
			t.Errorf("%s: err = %v, want ErrChanged", name, err)
		}
		if _, _, ok := s.Latest("k1"); ok {
			t.Errorf("%s: published a copy", name)
		}
	}
}

// Sizes a peer's manifest gives cannot wrap the totals that bound the copy
// and its disk reservation.
func TestManifestSizesCannotOverflow(t *testing.T) {
	sum := "0000000000000000000000000000000000000000000000000000000000000000"
	big := int64(math.MaxInt64/2 + 1)
	m := Manifest{Version: ManifestVersion, Entries: []Entry{
		{Path: "a", Type: TypeFile, Size: big, SHA256: sum},
		{Path: "b", Type: TypeFile, Size: big, SHA256: sum},
	}}
	m.Bytes = big + big // wraps, as a lying peer would send it
	if err := m.Check(Limits{}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("Check of a wrapping total: %v, want ErrTooLarge", err)
	}
	if got := AllocEstimate(m.Entries); got != math.MaxInt64 {
		t.Fatalf("AllocEstimate = %d, want it to saturate at MaxInt64", got)
	}
	if got := AllocEstimate([]Entry{{Path: "a", Type: TypeFile, Size: math.MaxInt64}}); got != math.MaxInt64 {
		t.Fatalf("AllocEstimate of one huge file = %d", got)
	}
}

func TestWalkReportsADirectoryReadError(t *testing.T) {
	boom := errors.New("getdents failed")
	prev := readDirBatch
	readDirBatch = func(*os.File, int) ([]string, error) { return nil, boom }
	defer func() { readDirBatch = prev }()
	err := walkNoFollow(modelTree(t), 0, func(node) error { return nil })
	if !errors.Is(err, boom) {
		t.Fatalf("walk = %v, want the read error: a failed read is not the end of the listing", err)
	}
}

// foreignGID is a group the test's user is not in, so chown to it fails.
func foreignGID(t *testing.T) int {
	t.Helper()
	groups, err := os.Getgroups()
	must(t, err)
	in := map[int]bool{os.Getegid(): true}
	for _, g := range groups {
		in[g] = true
	}
	for g := 1; g < 1<<20; g++ {
		if !in[g] {
			return g
		}
	}
	t.Skip("the user is in every group")
	return 0
}

func TestOwnerErrorsKeepTheirCause(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("runs as root: any owner or mode can be given")
	}
	s := &Store{Root: t.TempDir()}
	harvest(t, s, "k1", modelTree(t))
	gen, m, _ := s.Latest("k1")
	gid := foreignGID(t)

	entry := m
	entry.Entries = append([]Entry{}, m.Entries...)
	entry.Entries[len(entry.Entries)-1].GID = gid
	if err := Seed(gen, entry, t.TempDir()); !errors.Is(err, ErrOwner) || !errors.Is(err, unix.EPERM) {
		t.Fatalf("an entry's chown: err = %v, want ErrOwner wrapping EPERM", err)
	}

	vol := m
	root := *m.Root
	root.GID = gid
	vol.Root = &root
	if err := Seed(gen, vol, t.TempDir()); !errors.Is(err, ErrOwner) || !errors.Is(err, unix.EPERM) {
		t.Fatalf("the volume's chown: err = %v, want ErrOwner wrapping EPERM", err)
	}

	// A directory the user does not own: its owner matches, its mode
	// cannot be set.
	var st unix.Stat_t
	must(t, unix.Stat("/", &st))
	mode := Manifest{Root: &Entry{Type: TypeDir, UID: int(st.Uid), GID: int(st.Gid), Mode: (st.Mode & 0o7777) ^ 0o001}}
	if err := ApplyMeta("/", mode); !errors.Is(err, ErrOwner) || !errors.Is(err, unix.EPERM) {
		t.Fatalf("the volume's mode: err = %v, want ErrOwner wrapping EPERM", err)
	}
}

func TestPublishSyncsTheAttemptBeforeItsRename(t *testing.T) {
	s := &Store{Root: t.TempDir()}
	prev := syncDir
	defer func() { syncDir = prev }()
	type call struct {
		path     string
		complete bool
	}
	var calls []call
	syncDir = func(p string) error {
		_, err := os.Stat(filepath.Join(p, CompleteFile))
		calls = append(calls, call{p, err == nil})
		return prev(p)
	}
	harvest(t, s, "k1", modelTree(t))
	var attempt, root bool
	for _, c := range calls {
		if strings.HasPrefix(filepath.Base(c.path), AttemptPrefix) && filepath.Dir(c.path) == s.KeyDir("k1") {
			// Synced after .complete was written, while still an attempt.
			attempt = attempt || c.complete
		}
		root = root || c.path == s.Root
	}
	if !attempt {
		t.Fatalf("the attempt directory was not synced with its manifest and marker before the rename: %v", calls)
	}
	if !root {
		t.Fatalf("the key directory's parent was not synced: %v", calls)
	}
}

func TestPublishLeavesNoGenerationWhenTheRenameIsNotDurable(t *testing.T) {
	s := &Store{Root: t.TempDir()}
	prev := syncDir
	defer func() { syncDir = prev }()
	boom := errors.New("fsync failed")
	syncDir = func(p string) error {
		if p == s.KeyDir("k1") {
			return boom
		}
		return prev(p)
	}
	if _, err := s.Harvest("k1", modelTree(t), Limits{}); !errors.Is(err, boom) {
		t.Fatalf("harvest = %v, want the sync error", err)
	}
	if _, _, ok := s.Latest("k1"); ok {
		t.Fatal("a generation whose rename was not made durable is visible, though the harvest failed")
	}
	ents, _ := os.ReadDir(s.KeyDir("k1"))
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), GenPrefix) || strings.HasPrefix(e.Name(), AttemptPrefix) {
			t.Fatalf("the failed harvest left %s", e.Name())
		}
	}
}

func TestUsageCountsAModelDirectoryNamedLikeAnAttempt(t *testing.T) {
	s := &Store{Root: t.TempDir()}
	src := modelTree(t)
	must(t, os.MkdirAll(filepath.Join(src, "m", AttemptPrefix+"shards"), 0o755))
	must(t, os.WriteFile(filepath.Join(src, "m", AttemptPrefix+"shards", "w.bin"), bytes.Repeat([]byte{1}, 2<<20), 0o644))
	harvest(t, s, "k1", src)
	used, err := s.Usage()
	must(t, err)
	if used < 5<<20 {
		t.Fatalf("usage = %d, want at least the 3 MiB and 2 MiB files: a model directory named %s* was left out", used, AttemptPrefix)
	}
	// An attempt in flight still is not counted.
	must(t, os.MkdirAll(filepath.Join(s.KeyDir("k1"), AttemptPrefix+"live", TreeDir), 0o755))
	must(t, os.WriteFile(filepath.Join(s.KeyDir("k1"), AttemptPrefix+"live", TreeDir, "x"), bytes.Repeat([]byte{1}, 4<<20), 0o644))
	if again, _ := s.Usage(); again != used {
		t.Fatalf("usage with an attempt in flight = %d, want %d", again, used)
	}
}
