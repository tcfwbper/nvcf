// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package modelcache

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func sumOf(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

func TestScanLiveReportsInodes(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "sub", "a.bin"), []byte("aaaa"), 0o644); err != nil {
		t.Fatal(err)
	}
	ents, err := ScanLive(root, 0)
	if err != nil {
		t.Fatal(err)
	}
	var a *LiveEntry
	for i := range ents {
		if ents[i].Path == "sub/a.bin" {
			a = &ents[i]
		}
	}
	if a == nil || a.Size != 4 || a.Ino == 0 || a.Ctime == 0 {
		t.Fatalf("sub/a.bin = %+v", a)
	}
	// Renamed, the file keeps its inode: a follower moves its copy.
	if err := os.Rename(filepath.Join(root, "sub", "a.bin"), filepath.Join(root, "b.bin")); err != nil {
		t.Fatal(err)
	}
	ents, err = ScanLive(root, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range ents {
		if e.Path == "b.bin" {
			found = true
			if e.Ino != a.Ino || e.Dev != a.Dev {
				t.Fatalf("renamed file has inode %d/%d, was %d/%d", e.Dev, e.Ino, a.Dev, a.Ino)
			}
		}
	}
	if !found {
		t.Fatal("the renamed file is not in the scan")
	}
}

// The live scan's entries, as Entries, fingerprint the tree as Scan does:
// the helper's fingerprint and the agent's agree.
func TestScanLiveFingerprintsAsScanDoes(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "w.bin"), []byte("w"), 0o644); err != nil {
		t.Fatal(err)
	}
	want, err := Scan(root, 0)
	if err != nil {
		t.Fatal(err)
	}
	fp := Fingerprint(want)
	live, err := ScanLive(root, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := Fingerprint(Entries(live)); got != fp {
		t.Fatalf("live fingerprint %s, want %s", got, fp)
	}
}

func TestOpenBeneathRefusesASymlinkAnywhereInThePath(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("s"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "dir")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "secret"), filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	for _, rel := range []string{"dir/secret", "link", "../" + filepath.Base(outside) + "/secret", "/etc/passwd", ""} {
		if f, _, err := OpenBeneath(root, rel); err == nil {
			f.Close()
			t.Errorf("OpenBeneath(%q) opened it", rel)
		}
	}
	if err := os.MkdirAll(filepath.Join(root, "real"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "real", "ok"), []byte("ok"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, st, err := OpenBeneath(root, "real/ok")
	if err != nil || st.Size != 2 {
		t.Fatalf("real/ok: %v size %d", err, st.Size)
	}
	f.Close()
}

func TestHashOpenRefusesAFileChangedWhileItIsRead(t *testing.T) {
	root := t.TempDir()
	p := filepath.Join(root, "f")
	if err := os.WriteFile(p, []byte("12345678"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, st, err := OpenBeneath(root, "f")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sum, err := HashOpen(f, st)
	if err != nil || sum != sumOf([]byte("12345678")) {
		t.Fatalf("sum %s err %v", sum, err)
	}
	// Same size, mtime set back: only the change time shows it.
	if err := os.WriteFile(p, []byte("87654321"), 0o644); err != nil {
		t.Fatal(err)
	}
	mt := time.Unix(0, st.Mtim.Nano())
	if err := os.Chtimes(p, mt, mt); err != nil {
		t.Fatal(err)
	}
	if _, err := HashOpen(f, st); !errors.Is(err, ErrChanged) {
		t.Fatalf("err %v, want ErrChanged", err)
	}
}
