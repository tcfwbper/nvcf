// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/modelcache"
)

// Streaming a model download in progress from the node that downloads it
// to nodes that wait (docs/design/model-cache-single-flight.md, 3.3-3.4).
// A liveSource serves a pod's volume while a download writes it; a
// streamer copies it into a waiting pod's volume, file by file as each
// settles, and ends with exactly the tree of the source's final manifest,
// every byte checked against it. This file has no Kubernetes in it: the
// controller (model_cache_flight.go) decides who downloads and who waits.

// streamTiming: settle is how long a file's size, mtime and change time
// must hold before it is copied (or hashed); poll is how often a streamer
// looks at its source; stall is how long a source may show no progress.
// idle bounds how long one file's copy may receive nothing before it is
// given up (0: a minute): a source that stops sending mid-file would
// otherwise hold the stream past its stall check.
type streamTiming struct {
	settle, poll, stall, idle time.Duration
}

var defaultStreamTiming = streamTiming{settle: 10 * time.Second, poll: 2 * time.Second, stall: 5 * time.Minute, idle: time.Minute}

func (t streamTiming) idleBound() time.Duration {
	if t.idle <= 0 {
		return time.Minute
	}
	return t.idle
}

const (
	liveDownloading = "downloading"
	liveDone        = "done"
	liveFailed      = "failed"
	// streamParallel files copy at once, as a fetch does.
	streamParallel = 8
	// streamTmpPrefix names a file being written; a rename puts it in place.
	streamTmpPrefix = ".nvsnap-stream-"
)

var (
	errSourceFailed = errors.New("model cache: the source's download failed")
	errStall        = errors.New("model cache: the source made no progress")
)

// liveView is what a source serves: its pod's uid, its state, its volume's
// entries (with each file's sha256 once known), and when done the final
// manifest, the only thing a streamer trusts.
type liveView struct {
	UID     string                 `json:"uid"`
	State   string                 `json:"state"`
	Entries []modelcache.LiveEntry `json:"entries"`
	Final   *modelcache.Manifest   `json:"final,omitempty"`
}

// liveClient is a streamer's view of its source: over HTTP from a peer, or
// in process for another pod on the source's own node.
type liveClient interface {
	View(ctx context.Context) (liveView, error)
	Open(ctx context.Context, rel string) (io.ReadCloser, error)
}

// liveSource serves the volume at root while the download writes it.
type liveSource struct {
	root, uid string
	timing    streamTiming

	mu      sync.Mutex
	state   string
	seen    map[string]observed // file -> its stat, and since when
	hashes  map[string]hashed   // file -> sha256, valid only for that stat
	hashing map[string]bool
	final   *modelcache.Manifest
	finalAt map[string]modelcache.LiveEntry // file -> its stat in the final manifest
	queue   chan modelcache.LiveEntry       // settled files to hash, bounded
	workers sync.Once
	hashers sync.WaitGroup // the hash workers, which end with the source
	closed  bool           // queue is closed: the source ended
}

type observed struct {
	e     modelcache.LiveEntry
	since time.Time
}

type hashed struct {
	e   modelcache.LiveEntry
	sum string
}

func newLiveSource(root, uid string, tm streamTiming) *liveSource {
	return &liveSource{root: root, uid: uid, timing: tm, state: liveDownloading,
		seen: map[string]observed{}, hashes: map[string]hashed{}, hashing: map[string]bool{}, queue: make(chan modelcache.LiveEntry, 64)}
}

// view scans the volume and returns what it serves. Files that settled are
// hashed in the background, two at a time, so the final manifest takes
// seconds rather than a pass over the whole model.
func (s *liveSource) view() liveView {
	s.mu.Lock()
	state, final := s.state, s.final
	s.mu.Unlock()
	v := liveView{UID: s.uid, State: state, Final: final}
	if state != liveDownloading {
		return v
	}
	ents, err := modelcache.ScanLive(s.root, modelCacheMaxEntries)
	if err != nil {
		return v
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range ents {
		e := &ents[i]
		if e.Type != modelcache.TypeFile {
			continue
		}
		o, ok := s.seen[e.Path]
		if !ok || !modelcache.SameFile(o.e, *e) {
			s.seen[e.Path] = observed{e: *e, since: now}
			continue
		}
		if h, ok := s.hashes[e.Path]; ok && modelcache.SameFile(h.e, *e) {
			e.SHA256 = h.sum
			continue
		}
		// Checked under the lock the shutdown takes: the source may have
		// ended since the state was read above.
		if !s.closed && now.Sub(o.since) >= s.timing.settle && modelcache.Filled(*e) && !s.hashing[e.Path] {
			s.workers.Do(func() {
				for i := 0; i < 2; i++ {
					s.hashers.Add(1)
					go s.hashWorker()
				}
			})
			select {
			case s.queue <- *e:
				s.hashing[e.Path] = true
			default:
				// Full: the next poll queues it.
			}
		}
	}
	v.Entries = ents
	return v
}

// hashWorker hashes queued files; two run per source, until it ends.
func (s *liveSource) hashWorker() {
	defer s.hashers.Done()
	for e := range s.queue {
		s.mu.Lock()
		ended := s.closed
		s.mu.Unlock()
		if ended {
			continue // drain: nothing reads a hash once the source ended
		}
		sum, err := s.hashNow(e)
		s.mu.Lock()
		delete(s.hashing, e.Path)
		if err == nil {
			s.hashes[e.Path] = hashed{e: e, sum: sum}
		}
		s.mu.Unlock()
	}
}

// hashNow hashes file e if it is still in the state e describes.
func (s *liveSource) hashNow(e modelcache.LiveEntry) (string, error) {
	f, st, err := modelcache.OpenBeneath(s.root, e.Path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if uint64(st.Dev) != e.Dev || st.Ino != e.Ino || st.Size != e.Size || st.Mtim.Nano() != e.Mtime || st.Ctim.Nano() != e.Ctime {
		return "", modelcache.ErrChanged
	}
	return modelcache.HashOpen(f, st)
}

// hashAll hashes the files of ents not hashed for their current state,
// streamParallel at a time.
func (s *liveSource) hashAll(ents []modelcache.LiveEntry) error {
	var todo []modelcache.LiveEntry
	s.mu.Lock()
	for _, e := range ents {
		if h, ok := s.hashes[e.Path]; e.Type == modelcache.TypeFile && (!ok || !modelcache.SameFile(h.e, e)) {
			todo = append(todo, e)
		}
	}
	s.mu.Unlock()
	var mu sync.Mutex
	var first error
	work := make(chan modelcache.LiveEntry)
	var wg sync.WaitGroup
	for i := 0; i < streamParallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for e := range work {
				sum, err := s.hashNow(e)
				mu.Lock()
				if err != nil && first == nil {
					first = err
				}
				mu.Unlock()
				if err == nil {
					s.mu.Lock()
					s.hashes[e.Path] = hashed{e: e, sum: sum}
					s.mu.Unlock()
				}
			}
		}()
	}
	for _, e := range todo {
		work <- e
	}
	close(work)
	wg.Wait()
	return first
}

// finalize builds the final manifest once the download is over (the
// helper's signal, holding the tree's fingerprint, fp), while the init is
// held open and nothing else writes the volume. It reuses the hashes made
// as files settled, hashing only the rest. A tree that is not the one the
// download reported fails the source.
func (s *liveSource) finalize(fp string) error {
	ents, err := modelcache.ScanLive(s.root, modelCacheMaxEntries)
	if err == nil && modelcache.Fingerprint(modelcache.Entries(ents)) != fp {
		err = fmt.Errorf("%w: the volume is not the tree the download reported", modelcache.ErrChanged)
	}
	if err != nil {
		s.fail()
		return err
	}
	if err := s.hashAll(ents); err != nil {
		s.fail()
		return err
	}
	m := modelcache.Manifest{Version: modelcache.ManifestVersion}
	at := map[string]modelcache.LiveEntry{}
	for _, e := range ents {
		entry := e.Entry
		if entry.Path == "." {
			root := entry
			m.Root = &root
			continue
		}
		if entry.Type == modelcache.TypeFile {
			s.mu.Lock()
			h, ok := s.hashes[e.Path]
			s.mu.Unlock()
			if ok && modelcache.SameFile(h.e, e) {
				entry.SHA256 = h.sum
			} else if entry.SHA256, err = s.hashNow(e); err != nil {
				s.fail()
				return err
			}
			m.Bytes += entry.Size
			at[e.Path] = e
		}
		m.Entries = append(m.Entries, entry)
	}
	s.mu.Lock()
	s.final, s.finalAt, s.state = &m, at, liveDone
	s.mu.Unlock()
	s.shutdown()
	return nil
}

// setDone serves the volume as done with manifest m: a relay whose stream
// ended, its volume checked against m.
func (s *liveSource) setDone(m modelcache.Manifest) {
	ents, err := modelcache.ScanLive(s.root, modelCacheMaxEntries)
	if err != nil {
		s.fail()
		return
	}
	at := map[string]modelcache.LiveEntry{}
	for _, e := range ents {
		if e.Type == modelcache.TypeFile {
			at[e.Path] = e
		}
	}
	s.mu.Lock()
	s.final, s.finalAt, s.state = &m, at, liveDone
	s.mu.Unlock()
	s.shutdown()
}

// fail ends the source as failed, unless it is done. Every path that
// stops serving a source fails it, so its hash workers end too.
func (s *liveSource) fail() {
	s.mu.Lock()
	if s.state == liveDownloading {
		s.state = liveFailed
	}
	s.mu.Unlock()
	s.shutdown()
}

// shutdown ends the hash workers once the source is done or failed: no
// more files are queued, and the workers drain the queue and return. It
// is idempotent.
func (s *liveSource) shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.queue)
	}
}

// open serves a file of the volume. Once done, only a file of the final
// manifest that is still, when opened, the file manifested: the engine
// that then runs on the volume may write it. A write after the open can
// still reach the reader; a streamer checks every file it copies at the
// end against the manifest's sha256, so such bytes are never kept.
func (s *liveSource) open(_ context.Context, rel string) (io.ReadCloser, error) {
	s.mu.Lock()
	state, at := s.state, s.finalAt
	s.mu.Unlock()
	if state == liveFailed {
		return nil, errSourceFailed
	}
	f, st, err := modelcache.OpenBeneath(s.root, rel)
	if err != nil {
		return nil, err
	}
	if state == liveDone {
		want, ok := at[rel]
		if !ok || uint64(st.Dev) != want.Dev || st.Ino != want.Ino || st.Size != want.Size || st.Mtim.Nano() != want.Mtime || st.Ctim.Nano() != want.Ctime {
			f.Close()
			return nil, fmt.Errorf("%w: %s is not the manifested file", modelcache.ErrChanged, rel)
		}
	}
	return f, nil
}

// localClient reads a source on this node.
type localClient struct{ s *liveSource }

func (c localClient) View(context.Context) (liveView, error) { return c.s.view(), nil }
func (c localClient) Open(ctx context.Context, rel string) (io.ReadCloser, error) {
	return c.s.open(ctx, rel)
}

// streamer copies a source's volume into dst, a waiting pod's empty
// volume. Every write goes through an os.Root at dst, so nothing it
// writes lands outside it.
type streamer struct {
	client liveClient
	dst    string
	timing streamTiming

	have  map[string]copied    // file in dst -> what it was copied from
	first map[string]observed  // file at the source -> its stat, since when
	final *modelcache.Manifest // once done: the tree dst holds
	// mayWrite, when set, is asked before each pass of writes: false
	// ends the stream (the pod's helper downloads on its own).
	mayWrite func() bool
}

var errHelperDownloads = errors.New("model cache: the pod downloads on its own")

type copied struct {
	from  modelcache.LiveEntry // the source's file when it was copied
	sum   string
	local localStat // the copy, as written: any later write changes it
}

// localStat identifies a copy in dst as the streamer left it. Its change
// time moves on any write, chmod or rename, so a copy that still has it
// still has the bytes the streamer hashed as it wrote them.
type localStat struct {
	ino                uint64
	size, mtime, ctime int64
}

func statLocal(dst, rel string) (localStat, error) {
	var st unix.Stat_t
	if err := unix.Lstat(filepath.Join(dst, filepath.FromSlash(rel)), &st); err != nil {
		return localStat{}, err
	}
	return localStat{ino: st.Ino, size: st.Size, mtime: st.Mtim.Nano(), ctime: st.Ctim.Nano()}, nil
}

// intact reports whether rel in dst is still the copy c recorded.
func (s *streamer) intact(rel string, c copied) bool {
	now, err := statLocal(s.dst, rel)
	return err == nil && now == c.local
}

// run streams until the source is done and dst holds its final tree, or
// until the source fails or stalls.
func (s *streamer) run(ctx context.Context) error {
	root, err := os.OpenRoot(s.dst)
	if err != nil {
		return err
	}
	defer root.Close()
	// Kept across runs: a stream that moves to another source keeps its
	// copies, and the final manifest decides which stay.
	if s.have == nil {
		s.have, s.first = map[string]copied{}, map[string]observed{}
	}
	progress, lastSig := time.Now(), ""
	for {
		v, err := s.client.View(ctx)
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case err != nil:
		case v.State == liveFailed:
			return errSourceFailed
		case s.mayWrite != nil && !s.mayWrite():
			return errHelperDownloads
		case v.State == liveDone && v.Final != nil:
			if err := s.finish(ctx, root, *v.Final); err != nil {
				return err
			}
			m := *v.Final
			s.final = &m
			return nil
		default:
			if sig := progressSig(v.Entries); sig != lastSig {
				progress, lastSig = time.Now(), sig
			}
			s.sync(ctx, root, v.Entries)
		}
		if time.Since(progress) > s.timing.stall {
			return errStall
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.timing.poll):
		}
	}
}

// progressSig changes whenever the source's tree does.
func progressSig(ents []modelcache.LiveEntry) string {
	var n, bytes, latest int64
	for _, e := range ents {
		n++
		bytes += e.Size
		if e.Mtime > latest {
			latest = e.Mtime
		}
	}
	return fmt.Sprintf("%d/%d/%d", n, bytes, latest)
}

// sync follows renames and copies the files that settled since they last
// changed. Errors are left for the next poll: a file that changes while it
// is copied is copied again once it settles.
func (s *streamer) sync(ctx context.Context, root *os.Root, ents []modelcache.LiveEntry) {
	at := map[string]modelcache.LiveEntry{}
	for _, e := range ents {
		if e.Type == modelcache.TypeFile {
			at[e.Path] = e
		}
	}
	s.followRenames(root, at)
	now := time.Now()
	var todo []modelcache.LiveEntry
	for p, e := range at {
		if c, ok := s.have[p]; ok && (modelcache.SameFile(c.from, e) || (e.SHA256 != "" && e.SHA256 == c.sum)) {
			continue
		}
		o, ok := s.first[p]
		if !ok || !modelcache.SameFile(o.e, e) {
			s.first[p] = observed{e: e, since: now}
			continue
		}
		if now.Sub(o.since) >= s.timing.settle && modelcache.Filled(e) {
			todo = append(todo, e)
		}
	}
	s.copyAll(ctx, root, todo, false)
}

// followRenames moves a copy whose source file now has another path (the
// same device and inode), instead of copying it again.
func (s *streamer) followRenames(root *os.Root, at map[string]modelcache.LiveEntry) {
	type devIno struct{ dev, ino uint64 }
	byIno := map[devIno]string{}
	for p, e := range at {
		byIno[devIno{e.Dev, e.Ino}] = p
	}
	for old, c := range s.have {
		if _, still := at[old]; still {
			continue
		}
		to, ok := byIno[devIno{c.from.Dev, c.from.Ino}]
		if !ok {
			continue
		}
		if _, taken := s.have[to]; taken {
			continue
		}
		if !s.intact(old, c) {
			// The copy changed since it was recorded: its recorded digest
			// no longer describes it. Forget it; the file is copied again.
			delete(s.have, old)
			continue
		}
		if err := root.MkdirAll(path.Dir(to), 0o755); err != nil {
			continue
		}
		if err := root.Rename(old, to); err != nil {
			continue
		}
		e := at[to]
		// A rename changes only the change time: the copy stays good
		// while the size and mtime are those it was copied at.
		if e.Size == c.from.Size && e.Mtime == c.from.Mtime {
			c.from = e
		}
		local, err := statLocal(s.dst, to)
		if err != nil {
			delete(s.have, old)
			continue
		}
		c.local = local
		delete(s.have, old)
		s.have[to] = c
		s.first[to] = observed{e: e, since: time.Now()}
	}
}

// copyAll copies the files, streamParallel at a time, and returns the
// first error. Each is written under a temporary name and renamed into
// place, hashed as it is written. With exact, each must match its
// manifest sha256.
func (s *streamer) copyAll(ctx context.Context, root *os.Root, todo []modelcache.LiveEntry, exact bool) error {
	sort.Slice(todo, func(i, j int) bool { return todo[i].Path < todo[j].Path })
	var mu sync.Mutex
	var first error
	work := make(chan modelcache.LiveEntry)
	var wg sync.WaitGroup
	for i := 0; i < streamParallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for e := range work {
				if s.mayWrite != nil && !s.mayWrite() {
					mu.Lock()
					if first == nil {
						first = errHelperDownloads
					}
					mu.Unlock()
					continue
				}
				sum, err := s.copyOne(ctx, root, e)
				if err == nil && exact && sum != e.SHA256 {
					err = fmt.Errorf("%w: %s", modelcache.ErrCorrupt, e.Path)
				}
				var local localStat
				if err == nil {
					local, err = statLocal(s.dst, e.Path)
				}
				mu.Lock()
				if err == nil {
					s.have[e.Path] = copied{from: e, sum: sum, local: local}
				} else if first == nil {
					first = err
				}
				mu.Unlock()
			}
		}()
	}
	for _, e := range todo {
		work <- e
	}
	close(work)
	wg.Wait()
	return first
}

func (s *streamer) copyOne(ctx context.Context, root *os.Root, e modelcache.LiveEntry) (string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	idle := time.AfterFunc(s.timing.idleBound(), cancel)
	defer idle.Stop()
	rc, err := s.client.Open(ctx, e.Path)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	r := idleReader{r: rc, t: idle, d: s.timing.idleBound()}
	dir := path.Dir(e.Path)
	if err := root.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	tmp := path.Join(dir, streamTmpPrefix+randHex())
	f, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(r, e.Size+1))
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil && n != e.Size {
		err = fmt.Errorf("%w: %s changed while it was copied", modelcache.ErrChanged, e.Path)
	}
	if err == nil {
		if fi, lerr := root.Lstat(e.Path); lerr == nil && fi.IsDir() {
			err = root.RemoveAll(e.Path)
		}
	}
	if err == nil {
		err = root.Rename(tmp, e.Path)
	}
	if err != nil {
		_ = root.Remove(tmp)
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// idleReader pushes its timer back on every read that returns bytes: the
// copy is cancelled only after d with nothing received.
type idleReader struct {
	r io.Reader
	t *time.Timer
	d time.Duration
}

func (i idleReader) Read(p []byte) (int, error) {
	n, err := i.r.Read(p)
	if n > 0 {
		i.t.Reset(i.d)
	}
	return n, err
}

func randHex() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// finish makes dst exactly the final manifest's tree: it copies every file
// whose copy does not have the manifest's sha256 (moving a copy that has
// it under another path), removes what the manifest does not list, makes
// its directories and symlinks, and gives every entry its recorded owner,
// mode and mtime.
func (s *streamer) finish(ctx context.Context, root *os.Root, m modelcache.Manifest) error {
	if err := m.Check(modelcache.Limits{MaxEntries: modelCacheMaxEntries}); err != nil {
		return err
	}
	want := map[string]modelcache.Entry{}
	for _, e := range m.Entries {
		want[e.Path] = e
	}
	// Directories first, so a rename or a copy has its parent.
	for _, e := range m.Entries {
		if e.Type == modelcache.TypeDir {
			if fi, err := root.Lstat(e.Path); err == nil && !fi.IsDir() {
				if err := root.RemoveAll(e.Path); err != nil {
					return err
				}
			}
			if err := root.MkdirAll(e.Path, 0o755); err != nil {
				return err
			}
		}
	}
	// A copy changed since it was written is not trusted.
	for p, c := range s.have {
		if !s.intact(p, c) {
			delete(s.have, p)
		}
	}
	bySum := map[string]string{}
	for p, c := range s.have {
		if w, ok := want[p]; !ok || w.SHA256 != c.sum {
			bySum[c.sum] = p
		}
	}
	var todo []modelcache.LiveEntry
	for _, e := range m.Entries {
		if e.Type != modelcache.TypeFile {
			continue
		}
		if c, ok := s.have[e.Path]; ok && c.sum == e.SHA256 {
			continue
		}
		// Move a held copy only if it still has that digest and nothing
		// held is in the way; otherwise copy it, checked.
		_, inTheWay := s.have[e.Path]
		if from, ok := bySum[e.SHA256]; ok && !inTheWay && s.have[from].sum == e.SHA256 && s.intact(from, s.have[from]) {
			if root.Rename(from, e.Path) == nil {
				delete(bySum, e.SHA256)
				c := s.have[from]
				delete(s.have, from)
				if local, err := statLocal(s.dst, e.Path); err == nil {
					c.local = local
					s.have[e.Path] = c
					continue
				}
			}
		}
		todo = append(todo, modelcache.LiveEntry{Entry: e})
	}
	if err := s.copyAll(ctx, root, todo, true); err != nil {
		// Once more: a file the engine was not yet done with on the
		// source is served only as manifested, so a retry either gets
		// it or fails for good.
		var again []modelcache.LiveEntry
		for _, e := range todo {
			if c, ok := s.have[e.Path]; !ok || c.sum != e.SHA256 {
				again = append(again, e)
			}
		}
		if err := s.copyAll(ctx, root, again, true); err != nil {
			return err
		}
	}
	if err := s.prune(root, want); err != nil {
		return err
	}
	for _, e := range m.Entries {
		if e.Type != modelcache.TypeSymlink {
			continue
		}
		if t, err := root.Readlink(e.Path); err == nil && t == e.Target {
			continue
		}
		_ = root.RemoveAll(e.Path)
		if err := root.Symlink(e.Target, e.Path); err != nil {
			return err
		}
	}
	return applyMetaRoot(root, s.dst, m)
}

// applyMetaRoot gives every entry of m, and the volume, its recorded owner,
// mode and mtime, as a seed does, through root: the agent runs as root on
// the host, and a path swapped for a symlink under it must not lead a
// chown or chmod outside the volume.
func applyMetaRoot(root *os.Root, dst string, m modelcache.Manifest) error {
	for i := len(m.Entries) - 1; i >= 0; i-- {
		e := m.Entries[i]
		if err := root.Lchown(e.Path, e.UID, e.GID); err != nil {
			return err
		}
		if e.Type == modelcache.TypeSymlink {
			// No mode; its own time, never its target's.
			if err := modelcache.LutimesBeneath(dst, e.Path, e.Mtime); err != nil {
				return err
			}
			continue
		}
		if err := root.Chmod(e.Path, os.FileMode(e.Mode&0o777)|specialBits(e.Mode)); err != nil {
			return err
		}
		t := time.Unix(0, e.Mtime)
		if err := rootChtimes(root, e.Path, t, t); err != nil {
			return err
		}
	}
	if r := m.Root; r != nil {
		if err := root.Lchown(".", r.UID, r.GID); err != nil {
			return err
		}
		if err := root.Chmod(".", os.FileMode(r.Mode&0o777)|specialBits(r.Mode)); err != nil {
			return err
		}
		t := time.Unix(0, r.Mtime)
		if err := rootChtimes(root, ".", t, t); err != nil {
			return err
		}
	}
	return nil
}

// rootChtimes sets a time beneath an os.Root; a test makes it fail.
var rootChtimes = (*os.Root).Chtimes

// specialBits maps setuid, setgid and sticky from a stat mode to Go's.
func specialBits(mode uint32) os.FileMode {
	var m os.FileMode
	if mode&0o4000 != 0 {
		m |= os.ModeSetuid
	}
	if mode&0o2000 != 0 {
		m |= os.ModeSetgid
	}
	if mode&0o1000 != 0 {
		m |= os.ModeSticky
	}
	return m
}

// prune removes from dst every entry the manifest does not list: the
// downloader's temporary files and directories, and streamer leftovers.
func (s *streamer) prune(root *os.Root, want map[string]modelcache.Entry) error {
	ents, err := modelcache.ScanLive(s.dst, modelCacheMaxEntries)
	if err != nil {
		return err
	}
	// Deepest first, so a directory is empty of what goes before it goes.
	sort.Slice(ents, func(i, j int) bool { return strings.Count(ents[i].Path, "/") > strings.Count(ents[j].Path, "/") })
	for _, e := range ents {
		if e.Path == "." {
			continue
		}
		if w, ok := want[e.Path]; ok && w.Type == e.Type {
			continue
		}
		if err := root.RemoveAll(e.Path); err != nil && !os.IsNotExist(err) {
			return err
		}
		delete(s.have, e.Path)
	}
	return nil
}
