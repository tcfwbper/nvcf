// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/modelcache"
)

// Streaming (docs/design/model-cache-single-flight.md, 3.3 and 3.4), on
// plain directories: a source over a "download" the test writes, and a
// streamer copying it into another directory through a local client.

func fastStream() streamTiming {
	return streamTiming{settle: 200 * time.Millisecond, poll: 50 * time.Millisecond, stall: 10 * time.Second}
}

func write(t *testing.T, root, rel, content string) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o640); err != nil {
		t.Fatal(err)
	}
}

func sha(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }

// countingClient counts file opens, to tell a rename from a second copy.
type countingClient struct {
	liveClient
	opens atomic.Int64
}

func (c *countingClient) Open(ctx context.Context, rel string) (io.ReadCloser, error) {
	c.opens.Add(1)
	return c.liveClient.Open(ctx, rel)
}

// finish signals the source's download as over, as the helper does, and
// waits for the final manifest.
func finish(t *testing.T, src *liveSource) {
	t.Helper()
	ents, err := modelcache.ScanLive(src.root, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := src.finalize(modelcache.Fingerprint(modelcache.Entries(ents))); err != nil {
		t.Fatalf("finalize: %v", err)
	}
}

func runStream(t *testing.T, client liveClient, dst string) (chan error, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := &streamer{client: client, dst: dst, timing: fastStream()}
	done := make(chan error, 1)
	go func() { done <- s.run(ctx) }()
	return done, cancel
}

func waitDone(t *testing.T, done chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(20 * time.Second):
		t.Fatal("the stream did not end")
		return nil
	}
}

func assertTree(t *testing.T, dst string, want map[string]string) {
	t.Helper()
	got := map[string]string{}
	_ = filepath.Walk(dst, func(p string, fi os.FileInfo, err error) error {
		if err == nil && fi.Mode().IsRegular() {
			rel, _ := filepath.Rel(dst, p)
			b, _ := os.ReadFile(p)
			got[filepath.ToSlash(rel)] = string(b)
		}
		return nil
	})
	if len(got) != len(want) {
		t.Fatalf("tree %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s = %q, want %q (tree %v)", k, got[k], v, got)
		}
	}
}

func TestStreamCopiesAnAppendingDownloadAndEndsWithItsTree(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	ls := newLiveSource(src, "uid-holder", fastStream())
	done, _ := runStream(t, localClient{ls}, dst)
	write(t, src, "a/shard-1.bin", "one")
	waitCopied(t, dst, "a/shard-1.bin") // settles and is copied before the end
	f, err := os.OpenFile(filepath.Join(src, "a", "shard-2.bin"), os.O_CREATE|os.O_WRONLY, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{"tw", "o-", "two"} {
		_, _ = f.WriteString(part)
		time.Sleep(100 * time.Millisecond)
	}
	f.Close()
	if err := os.Symlink("shard-1.bin", filepath.Join(src, "a", "latest")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	finish(t, ls)
	if err := waitDone(t, done); err != nil {
		t.Fatal(err)
	}
	assertTree(t, dst, map[string]string{"a/shard-1.bin": "one", "a/shard-2.bin": "two-two"})
	if tgt, err := os.Readlink(filepath.Join(dst, "a", "latest")); err != nil || tgt != "shard-1.bin" {
		t.Fatalf("symlink = %q %v", tgt, err)
	}
	fi, _ := os.Stat(filepath.Join(dst, "a", "shard-1.bin"))
	if fi.Mode().Perm() != 0o640 {
		t.Fatalf("mode %o, want the download's 0640", fi.Mode().Perm())
	}
}

// A downloader that writes a temporary name and renames it: the stream
// moves its copy instead of copying the file again.
func TestStreamFollowsARenameWithoutASecondCopy(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	ls := newLiveSource(src, "uid-holder", fastStream())
	c := &countingClient{liveClient: localClient{ls}}
	done, _ := runStream(t, c, dst)
	write(t, src, ".incomplete/w.bin", "weights")
	waitCopied(t, dst, ".incomplete/w.bin")
	if err := os.Rename(filepath.Join(src, ".incomplete", "w.bin"), filepath.Join(src, "w.bin")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(src, ".incomplete")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	finish(t, ls)
	if err := waitDone(t, done); err != nil {
		t.Fatal(err)
	}
	assertTree(t, dst, map[string]string{"w.bin": "weights"})
	if _, err := os.Stat(filepath.Join(dst, ".incomplete")); !os.IsNotExist(err) {
		t.Fatal("the downloader's temporary directory is left")
	}
	if n := c.opens.Load(); n != 1 {
		t.Fatalf("the file was read %d times; want once (a rename is not a new file)", n)
	}
}

// A file rewritten after it settled and was copied, and a file deleted
// after it was copied: the end matches the final tree, nothing else.
func TestStreamEndsWithTheFinalTreeAfterRewritesAndDeletes(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	ls := newLiveSource(src, "uid-holder", fastStream())
	done, _ := runStream(t, localClient{ls}, dst)
	write(t, src, "keep.bin", "v1")
	write(t, src, "gone.bin", "temp")
	waitCopied(t, dst, "keep.bin", "gone.bin")
	write(t, src, "keep.bin", "v2-longer")
	if err := os.Remove(filepath.Join(src, "gone.bin")); err != nil {
		t.Fatal(err)
	}
	finish(t, ls) // at once: keep.bin has not settled again
	if err := waitDone(t, done); err != nil {
		t.Fatal(err)
	}
	assertTree(t, dst, map[string]string{"keep.bin": "v2-longer"})
}

// copiedPass runs one copy pass of every file at src, as a stream's pass
// does, and returns once the streamer has recorded each copy.
func copiedPass(t *testing.T, s *streamer, src string) {
	t.Helper()
	root, err := os.OpenRoot(s.dst)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if s.have == nil {
		s.have, s.first = map[string]copied{}, map[string]observed{}
	}
	ents, err := modelcache.ScanLive(src, 0)
	if err != nil {
		t.Fatal(err)
	}
	var todo []modelcache.LiveEntry
	for _, e := range ents {
		if e.Type == modelcache.TypeFile {
			todo = append(todo, e)
		}
	}
	if err := s.copyAll(context.Background(), root, todo, false); err != nil {
		t.Fatal(err)
	}
}

// The source's final manifest is the only truth: a copy whose bytes do
// not match it is copied again.
func TestStreamRecopiesAFileWhoseCopyDoesNotMatchTheFinalManifest(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	ls := newLiveSource(src, "uid-holder", fastStream())
	write(t, src, "w.bin", "right")
	s := &streamer{client: localClient{ls}, dst: dst, timing: fastStream()}
	copiedPass(t, s, src) // the copy is recorded before it is corrupted
	// Corrupt the stream's copy behind its back.
	if err := os.WriteFile(filepath.Join(dst, "w.bin"), []byte("wrong"), 0o640); err != nil {
		t.Fatal(err)
	}
	finish(t, ls)
	if err := s.run(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertTree(t, dst, map[string]string{"w.bin": "right"})
}

// A copy changed after it was recorded, whose source file is then renamed:
// the rename must not carry the changed copy to the new path and record it
// as good. The end has the source's bytes.
func TestStreamDoesNotCarryAChangedCopyAcrossARename(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	ls := newLiveSource(src, "uid-holder", fastStream())
	write(t, src, ".incomplete/w.bin", "good")
	s := &streamer{client: localClient{ls}, dst: dst, timing: fastStream()}
	copiedPass(t, s, src)
	// The copy changes (same size), then the downloader renames the source.
	if err := os.WriteFile(filepath.Join(dst, ".incomplete", "w.bin"), []byte("oops"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(src, ".incomplete", "w.bin"), filepath.Join(src, "w.bin")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(src, ".incomplete")); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	ents, err := modelcache.ScanLive(src, 0)
	if err != nil {
		t.Fatal(err)
	}
	s.sync(context.Background(), root, ents)
	finish(t, ls)
	if err := s.run(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertTree(t, dst, map[string]string{"w.bin": "good"})
}

func TestStreamFailsWhenTheSourceFails(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	ls := newLiveSource(src, "uid-holder", fastStream())
	done, _ := runStream(t, localClient{ls}, dst)
	write(t, src, "w.bin", "x")
	ls.fail()
	if err := waitDone(t, done); err == nil {
		t.Fatal("the stream ended without error on a failed source")
	}
}

func TestStreamFailsOnAStall(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	ls := newLiveSource(src, "uid-holder", fastStream())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tm := fastStream()
	tm.stall = 500 * time.Millisecond
	s := &streamer{client: localClient{ls}, dst: dst, timing: tm}
	done := make(chan error, 1)
	go func() { done <- s.run(ctx) }()
	if err := waitDone(t, done); err == nil {
		t.Fatal("no error after the source made no progress")
	}
}

// Finalizing refuses a tree that is not the one the download reported.
func TestLiveSourceFinalizeRefusesAnotherTree(t *testing.T) {
	src := t.TempDir()
	write(t, src, "w.bin", "x")
	ls := newLiveSource(src, "uid-holder", fastStream())
	if err := ls.finalize("not-this-tree"); err == nil {
		t.Fatal("finalized a tree the download did not report")
	}
	if v := ls.view(); v.State != liveFailed {
		t.Fatalf("state %q, want failed", v.State)
	}
}

// The final manifest carries every file's sha256, and once done the
// source serves only those files, and only while unchanged.
func TestLiveSourceServesOnlyUnchangedManifestFilesOnceDone(t *testing.T) {
	src := t.TempDir()
	write(t, src, "w.bin", "weights")
	ls := newLiveSource(src, "uid-holder", fastStream())
	finish(t, ls)
	v := ls.view()
	if v.State != liveDone || v.Final == nil || len(v.Final.Entries) != 1 || v.Final.Entries[0].SHA256 != sha("weights") {
		t.Fatalf("view %+v", v)
	}
	ctx := context.Background()
	if r, err := ls.open(ctx, "w.bin"); err != nil {
		t.Fatalf("open w.bin: %v", err)
	} else {
		r.Close()
	}
	write(t, src, "engine.log", "the engine wrote this")
	if r, err := ls.open(ctx, "engine.log"); err == nil {
		r.Close()
		t.Fatal("served a file the manifest does not list")
	}
	write(t, src, "w.bin", "changed")
	if r, err := ls.open(ctx, "w.bin"); err == nil {
		r.Close()
		t.Fatal("served a manifest file that changed")
	}
}

// Applying the manifest's owners and modes runs as root on the host: a
// path swapped for a symlink must not lead it outside the volume.
func TestApplyMetaStaysInTheVolume(t *testing.T) {
	dst, outside := t.TempDir(), t.TempDir()
	victim := filepath.Join(outside, "victim")
	if err := os.WriteFile(victim, []byte("v"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victim, filepath.Join(dst, "f")); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	m := modelcache.Manifest{Entries: []modelcache.Entry{{Path: "f", Type: modelcache.TypeFile, Mode: 0o777, UID: os.Getuid(), GID: os.Getgid()}}}
	_ = applyMetaRoot(root, dst, m)
	if fi, _ := os.Stat(victim); fi.Mode().Perm() != 0o600 {
		t.Fatalf("a file outside the volume became %o", fi.Mode().Perm())
	}
}

// A downloader that sizes a file first and fills it in later (as the
// registry's client does, sparse): a half-filled file that pauses is not
// settled. It is copied once, when full, not once half-empty and again.
func TestStreamWaitsForAPreallocatedFileToFill(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	ls := newLiveSource(src, "uid-holder", fastStream())
	c := &countingClient{liveClient: localClient{ls}}
	done, _ := runStream(t, c, dst)
	const size = 1 << 20
	p := filepath.Join(src, "w.bin")
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	half := make([]byte, size/2)
	for i := range half {
		half[i] = 'a'
	}
	if _, err := f.WriteAt(half, 0); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second) // paused, well past the settle time
	if _, err := f.WriteAt(half, size/2); err != nil {
		t.Fatal(err)
	}
	f.Close()
	time.Sleep(700 * time.Millisecond)
	finish(t, ls)
	if err := waitDone(t, done); err != nil {
		t.Fatal(err)
	}
	if n := c.opens.Load(); n != 1 {
		t.Fatalf("the file was read %d times; want once, when full", n)
	}
}

// Two copies whose contents the final manifest swaps: neither is moved
// over the other on a stale index; both end right.
func TestStreamFinishHandlesSwappedContents(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	ls := newLiveSource(src, "uid-holder", fastStream())
	done, _ := runStream(t, localClient{ls}, dst)
	write(t, src, "a.bin", "AAAA")
	write(t, src, "b.bin", "BBBB")
	waitCopied(t, dst, "a.bin", "b.bin")
	// The downloader swaps them (same size, so only the content moves).
	write(t, src, "a.bin", "BBBB")
	write(t, src, "b.bin", "AAAA")
	finish(t, ls)
	if err := waitDone(t, done); err != nil {
		t.Fatal(err)
	}
	assertTree(t, dst, map[string]string{"a.bin": "BBBB", "b.bin": "AAAA"})
}

// A source that sends part of a file and then nothing: the copy gives up
// after the idle bound, not after the stream's hours-long deadline.
func TestStreamCopyGivesUpOnASilentSource(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000")
		_, _ = w.Write([]byte("partial"))
		w.(http.Flusher).Flush()
		select {
		case <-block:
		case <-r.Context().Done():
		}
	}))
	// Deferred last in, first out: unblock the handler, then close.
	defer srv.Close()
	defer close(block)
	dst := t.TempDir()
	root, err := os.OpenRoot(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	tm := fastStream()
	tm.idle = 500 * time.Millisecond
	s := &streamer{dst: dst, timing: tm, client: urlClient{srv.URL}}
	start := time.Now()
	_, err = s.copyOne(context.Background(), root, modelcache.LiveEntry{Entry: modelcache.Entry{Path: "w.bin", Type: modelcache.TypeFile, Size: 1000}})
	if err == nil {
		t.Fatal("a silent source's copy succeeded")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("gave up after %v", d)
	}
}

// urlClient serves every file from one URL.
type urlClient struct{ u string }

func (c urlClient) View(context.Context) (liveView, error) { return liveView{}, nil }
func (c urlClient) Open(ctx context.Context, _ string) (io.ReadCloser, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.u, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// The helper gives up mid-pass: the pass stops before its next file.
func TestStreamStopsMidPassWhenTheHelperDownloads(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	for i := 0; i < 40; i++ {
		write(t, src, fmt.Sprintf("w-%02d.bin", i), "x")
	}
	ls := newLiveSource(src, "uid-holder", fastStream())
	c := &countingClient{liveClient: localClient{ls}}
	root, err := os.OpenRoot(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	var allowed atomic.Int64
	allowed.Store(3)
	s := &streamer{dst: dst, timing: fastStream(), client: c, mayWrite: func() bool { return allowed.Add(-1) >= 0 }}
	s.have, s.first = map[string]copied{}, map[string]observed{}
	var todo []modelcache.LiveEntry
	ents, _ := modelcache.ScanLive(src, 0)
	for _, e := range ents {
		if e.Type == modelcache.TypeFile {
			todo = append(todo, e)
		}
	}
	if err := s.copyAll(context.Background(), root, todo, false); !errors.Is(err, errHelperDownloads) {
		t.Fatalf("copyAll = %v; want %v", err, errHelperDownloads)
	}
	// The guard allowed exactly three files: three opens, three copies.
	if n := c.opens.Load(); n != 3 {
		t.Fatalf("opened %d of 40 files; want the 3 allowed before the helper said it downloads", n)
	}
	if len(s.have) != 3 {
		t.Fatalf("recorded %d copies; want 3", len(s.have))
	}
}

// A source that ends (done or failed) stops its hash workers: they and
// what they hold do not outlive it.
func TestLiveSourceStopsItsHashWorkersWhenItEnds(t *testing.T) {
	for _, end := range []struct {
		name string
		do   func(*testing.T, *liveSource)
	}{
		{"failed", func(_ *testing.T, ls *liveSource) { ls.fail() }},
		{"done", func(t *testing.T, ls *liveSource) { finish(t, ls) }},
	} {
		t.Run(end.name, func(t *testing.T) {
			src := t.TempDir()
			write(t, src, "w.bin", "weights")
			ls := newLiveSource(src, "uid-holder", fastStream())
			// Views a settle apart start the workers and hash the file.
			deadline := time.Now().Add(10 * time.Second)
			for {
				hashed := false
				for _, e := range ls.view().Entries {
					hashed = hashed || (e.Path == "w.bin" && e.SHA256 != "")
				}
				if hashed {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("the file was never hashed")
				}
				time.Sleep(50 * time.Millisecond)
			}
			end.do(t, ls)
			stopped := make(chan struct{})
			go func() { ls.hashers.Wait(); close(stopped) }()
			select {
			case <-stopped:
			case <-time.After(5 * time.Second):
				t.Fatal("the hash workers outlived their source")
			}
			_ = ls.view() // never sends on the closed queue
		})
	}
}

// A failure to give the volume itself its recorded mtime fails the end of
// the stream, as it does for every other entry: the pod must not start on
// a volume that does not match the manifest.
func TestApplyMetaReportsTheVolumesMtimeFailure(t *testing.T) {
	dst := t.TempDir()
	write(t, dst, "w.bin", "w")
	root, err := os.OpenRoot(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	broken := errors.New("chtimes refused")
	orig := rootChtimes
	rootChtimes = func(r *os.Root, name string, a, m time.Time) error {
		if name == "." {
			return broken
		}
		return orig(r, name, a, m)
	}
	defer func() { rootChtimes = orig }()
	m := modelcache.Manifest{
		Root:    &modelcache.Entry{Path: ".", Type: modelcache.TypeDir, Mode: 0o755, UID: os.Getuid(), GID: os.Getgid()},
		Entries: []modelcache.Entry{{Path: "w.bin", Type: modelcache.TypeFile, Mode: 0o640, UID: os.Getuid(), GID: os.Getgid()}},
	}
	if err := applyMetaRoot(root, dst, m); !errors.Is(err, broken) {
		t.Fatalf("applyMetaRoot = %v; want the volume's chtimes failure", err)
	}
}

func TestStreamRestoresASymlinksOwnMtime(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, src, "w.bin", "w")
	if err := os.Symlink("w.bin", filepath.Join(src, "current")); err != nil {
		t.Fatal(err)
	}
	old := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := unix.Lutimes(filepath.Join(src, "current"), []unix.Timeval{unix.NsecToTimeval(old.UnixNano()), unix.NsecToTimeval(old.UnixNano())}); err != nil {
		t.Fatal(err)
	}
	ls := newLiveSource(src, "uid-holder", fastStream())
	done, _ := runStream(t, localClient{ls}, dst)
	finish(t, ls)
	if err := waitDone(t, done); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Lstat(filepath.Join(dst, "current"))
	if err != nil || !fi.ModTime().Equal(old) {
		t.Fatalf("symlink mtime %v (%v), want %v", fi.ModTime(), err, old)
	}
	if wf, _ := os.Stat(filepath.Join(dst, "w.bin")); wf.ModTime().Equal(old) {
		t.Fatal("the link's target got the link's mtime")
	}
}

// waitCopied waits until the stream has put every rel in dst.
func waitCopied(t *testing.T, dst string, rels ...string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for _, rel := range rels {
		for {
			if _, err := os.Stat(filepath.Join(dst, filepath.FromSlash(rel))); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the stream did not copy %s", rel)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}
