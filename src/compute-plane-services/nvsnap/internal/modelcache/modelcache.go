// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package modelcache keeps downloaded model trees on a node's local disk,
// so a later cold pod on that node, or on a node that fetches the tree
// from it, seeds its own download volume instead of downloading again
// (docs/design/node-model-cache.md).
//
// Layout under the cache root, one directory per key:
//
//	<root>/<key>/gen-<id>/tree/      the model files
//	<root>/<key>/gen-<id>/MANIFEST   every entry: path, type, mode, size, sha256
//	<root>/<key>/gen-<id>/.complete  written last; without it the generation does not exist
//	<root>/<key>/.attempt-<id>/      a harvest or fetch in progress
//
// A generation is immutable once complete. A new copy is always a new
// generation, built in an attempt directory nobody reads and renamed into
// place only after every file is written, checked and synced, so no reader
// ever sees a directory replaced under it. Every use of a generation
// checks each file against its manifest while copying, so a file corrupted
// on disk after publication is caught by the pod about to use it.
package modelcache

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	// TreeDir holds a generation's files.
	TreeDir = "tree"
	// ManifestFile lists a generation's entries.
	ManifestFile = "MANIFEST"
	// CompleteFile marks a generation as published.
	CompleteFile = ".complete"
	// GenPrefix starts a generation directory's name.
	GenPrefix = "gen-"
	// AttemptPrefix starts an in-progress harvest or fetch directory.
	AttemptPrefix = ".attempt-"
	// IncomingFile is the heartbeat the agent refreshes while it fetches a
	// key from a peer; a seeding pod waits only while it is fresh.
	IncomingFile = ".incoming"
	// InitSeeded and InitDownloaded are a wrapped download init's
	// termination message: how its volume was filled.
	InitSeeded     = "nvsnap-model-cache: seeded"
	InitDownloaded = "nvsnap-model-cache: downloaded"
	// TreeField precedes the downloaded tree's Fingerprint in the message.
	TreeField = "tree="
	// ManifestVersion is the manifest format.
	ManifestVersion = 1
)

// Entry types.
const (
	TypeDir     = "dir"
	TypeFile    = "file"
	TypeSymlink = "symlink"
)

var (
	// ErrCorrupt: a file does not match its manifest entry.
	ErrCorrupt = errors.New("model cache: file does not match its manifest")
	// ErrUnsupported: the tree holds an entry the cache cannot reproduce
	// exactly (a device, a socket, a symlink leaving the tree, a path
	// leaving the tree). Such a tree is not cached.
	ErrUnsupported = errors.New("model cache: tree holds an entry the cache does not reproduce")
	// ErrTooLarge: the tree is over the limit it was given.
	ErrTooLarge = errors.New("model cache: tree is over its size limit")
	// ErrChanged: a source file changed while it was copied.
	ErrChanged = errors.New("model cache: source changed while it was copied")
	// ErrOwner: the seed cannot give an entry its recorded owner (the
	// helper does not run as root, and the download ran as another user).
	ErrOwner = errors.New("model cache: cannot give the tree its recorded owners")
	// ErrTargetNotEmpty: the volume to seed already has content (an init
	// that restarted after a partial download); the download handles it.
	ErrTargetNotEmpty = errors.New("model cache: target is not empty")
)

// DownloadedTree returns the fingerprint a download init reported in its
// termination message, or "" when it reported none.
func DownloadedTree(msg string) string {
	if !strings.HasPrefix(msg, InitDownloaded) {
		return ""
	}
	for _, f := range strings.Fields(msg[len(InitDownloaded):]) {
		if v, ok := strings.CutPrefix(f, TreeField); ok && len(v) == 64 {
			if _, err := hex.DecodeString(v); err == nil {
				return v
			}
		}
	}
	return ""
}

// Entry is one path in a model tree, relative to the tree root.
type Entry struct {
	Path   string `json:"path"`
	Type   string `json:"type"`
	Mode   uint32 `json:"mode"`
	Size   int64  `json:"size,omitempty"`
	SHA256 string `json:"sha256,omitempty"`
	Target string `json:"target,omitempty"`
	// UID and GID own the entry; Mode holds its permission and special
	// bits (07777); Mtime is its modification time in nanoseconds. A seed
	// gives every entry these, as the download left them.
	UID   int   `json:"uid"`
	GID   int   `json:"gid"`
	Mtime int64 `json:"mtime"`
	// ctime is the entry's change time when it was read: part of a
	// Fingerprint, never stored (a copy has its own).
	ctime int64
}

// Manifest is a generation's list of entries, parents before children.
type Manifest struct {
	Version int `json:"version"`
	// Root is the tree's root directory (path "."): its owner, mode and
	// mtime, given to the volume a seed fills.
	Root    *Entry  `json:"root,omitempty"`
	Entries []Entry `json:"entries"`
	// Bytes is the sum of the file sizes.
	Bytes int64 `json:"bytes"`
}

// Limits bounds what a harvest or fetch writes. Zero means no limit.
type Limits struct {
	MaxBytes   int64
	MaxEntries int
	// ExactBytes makes MaxBytes a bound even when it is 0: a harvest of a
	// tree measured as empty may not copy a byte.
	ExactBytes bool
	// NotAfter, for a harvest, is when the download finished: an entry
	// changed later was written by something else (the engine, once it
	// started), so the tree is not the download's and is not cached.
	NotAfter time.Time
	// Fingerprint, for a harvest, is the tree's Fingerprint as the
	// download left it; the copy is published only if it matches.
	Fingerprint string
}

// ErrWrittenAfter: the tree changed after the download finished.
var ErrWrittenAfter = errors.New("model cache: tree changed after the download finished")

// ReadCloser is a source file, local or from a peer.
type ReadCloser = io.ReadCloser

// Key is the cache key of a download: the tenant it was downloaded for,
// what it downloads and how it lays it out (modelid ShareKey), and the
// downloader itself (its image and its command and settings). A change to
// any of them is another key, so a pod never seeds a tree another recipe
// wrote. The key is hex, safe as a path element and a label value.
func Key(scope, shareKey, image, recipe string) string {
	h := sha256.New()
	for _, s := range []string{scope, shareKey, image, recipe} {
		fmt.Fprintf(h, "%d:%s\n", len(s), s)
	}
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// ValidKey reports whether k is a key Key could have made.
func ValidKey(k string) bool {
	if len(k) != 32 {
		return false
	}
	_, err := hex.DecodeString(k)
	return err == nil
}

// ValidRel reports whether p is a clean path inside a tree.
func ValidRel(p string) bool { return validRel(p) }

// validRel reports whether p is a clean path inside a tree.
func validRel(p string) bool {
	return p != "" && p != "." && !path.IsAbs(p) && path.Clean(p) == p &&
		p != ".." && !strings.HasPrefix(p, "../")
}

// validLink reports whether a symlink at rel pointing to target stays
// inside the tree.
func validLink(rel, target string) bool {
	if target == "" || path.IsAbs(target) || path.Clean(target) != target {
		return false
	}
	return validRel(path.Join(path.Dir(rel), target))
}

// Store is a node's cache root.
type Store struct {
	Root string
}

// KeyDir is where key's generations live.
func (s *Store) KeyDir(key string) string { return filepath.Join(s.Root, key) }

func newID() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%020d-%s", time.Now().UnixNano(), hex.EncodeToString(b[:]))
}

// Latest returns key's newest complete generation and its manifest.
func (s *Store) Latest(key string) (string, Manifest, bool) {
	gens := s.Generations(key)
	for i := len(gens) - 1; i >= 0; i-- {
		if m, err := ReadManifest(gens[i]); err == nil {
			return gens[i], m, true
		}
	}
	return "", Manifest{}, false
}

// Generations lists key's complete generations, oldest first.
func (s *Store) Generations(key string) []string {
	return Generations(s.KeyDir(key))
}

// Generations lists the complete generations in a key directory, oldest
// first. The helper calls it on the key directory mounted into the pod.
func Generations(keyDir string) []string {
	ents, err := os.ReadDir(keyDir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range ents {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), GenPrefix) {
			continue
		}
		dir := filepath.Join(keyDir, e.Name())
		if _, err := os.Stat(filepath.Join(dir, CompleteFile)); err == nil {
			out = append(out, dir)
		}
	}
	sort.Strings(out) // ids start with a zero-padded timestamp
	return out
}

// LatestIn is Latest for a key directory given by path.
func LatestIn(keyDir string) (string, Manifest, bool) {
	gens := Generations(keyDir)
	for i := len(gens) - 1; i >= 0; i-- {
		if m, err := ReadManifest(gens[i]); err == nil {
			return gens[i], m, true
		}
	}
	return "", Manifest{}, false
}

// ReadManifest reads and checks a generation's manifest.
func ReadManifest(gen string) (Manifest, error) {
	var m Manifest
	b, err := os.ReadFile(filepath.Join(gen, ManifestFile))
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return m, fmt.Errorf("manifest %s: %w", gen, err)
	}
	return m, m.Check(Limits{})
}

// Check validates a manifest, typically one received from a peer: a known
// version, confined paths, known types, well-formed checksums, parents
// before children, and the limits.
func (m Manifest) Check(lim Limits) error {
	if m.Version != ManifestVersion {
		return fmt.Errorf("%w: manifest version %d", ErrUnsupported, m.Version)
	}
	if lim.MaxEntries > 0 && len(m.Entries) > lim.MaxEntries {
		return fmt.Errorf("%w: %d entries", ErrTooLarge, len(m.Entries))
	}
	if m.Root != nil && (m.Root.Path != "." || m.Root.Type != TypeDir || m.Root.Mode > 0o7777) {
		return fmt.Errorf("%w: manifest root", ErrUnsupported)
	}
	seen := map[string]string{}
	var total int64
	for _, e := range m.Entries {
		if !validRel(e.Path) {
			return fmt.Errorf("%w: path %q", ErrUnsupported, e.Path)
		}
		if e.Mode > 0o7777 {
			return fmt.Errorf("%w: %q has mode %o", ErrUnsupported, e.Path, e.Mode)
		}
		if _, dup := seen[e.Path]; dup {
			return fmt.Errorf("%w: path %q twice", ErrUnsupported, e.Path)
		}
		if d := path.Dir(e.Path); d != "." && seen[d] != TypeDir {
			return fmt.Errorf("%w: %q comes before its directory", ErrUnsupported, e.Path)
		}
		switch e.Type {
		case TypeDir:
		case TypeFile:
			if e.Size < 0 || len(e.SHA256) != 64 {
				return fmt.Errorf("%w: file %q has no size or checksum", ErrUnsupported, e.Path)
			}
			if _, err := hex.DecodeString(e.SHA256); err != nil {
				return fmt.Errorf("%w: file %q checksum", ErrUnsupported, e.Path)
			}
			if e.Size > math.MaxInt64-total {
				return fmt.Errorf("%w: file sizes overflow", ErrTooLarge)
			}
			total += e.Size
		case TypeSymlink:
			if !validLink(e.Path, e.Target) {
				return fmt.Errorf("%w: symlink %q -> %q", ErrUnsupported, e.Path, e.Target)
			}
		default:
			return fmt.Errorf("%w: %q has type %q", ErrUnsupported, e.Path, e.Type)
		}
		seen[e.Path] = e.Type
	}
	if total != m.Bytes {
		return fmt.Errorf("%w: manifest bytes %d, files sum to %d", ErrUnsupported, m.Bytes, total)
	}
	if lim.MaxBytes > 0 && total > lim.MaxBytes {
		return fmt.Errorf("%w: %d bytes", ErrTooLarge, total)
	}
	return nil
}

// attempt is a directory a harvest or fetch builds a generation in.
type attempt struct {
	keyDir, dir string
}

func (s *Store) newAttempt(key string) (*attempt, error) {
	if !ValidKey(key) && !testKey(key) {
		return nil, fmt.Errorf("model cache: bad key %q", key)
	}
	keyDir := s.KeyDir(key)
	if err := os.MkdirAll(keyDir, 0o755); err != nil {
		return nil, err
	}
	dir := filepath.Join(keyDir, AttemptPrefix+newID())
	if err := os.Mkdir(dir, 0o700); err != nil {
		return nil, err
	}
	if err := os.Mkdir(filepath.Join(dir, TreeDir), 0o755); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	return &attempt{keyDir: keyDir, dir: dir}, nil
}

// testKey lets tests use short readable keys; production keys come from Key.
var testKey = func(string) bool { return false }

func (a *attempt) abort() { _ = os.RemoveAll(a.dir) }

// publish writes the manifest and the completion marker, syncs the tree,
// and renames the attempt to a generation. The attempt directory (its
// manifest, marker and tree entries) and the key directory's parent are
// synced before the rename, and the key directory after it. If that last
// sync fails, the generation is taken back out of view: a harvest or fetch
// that reports an error leaves no generation for a pod to seed from.
func (a *attempt) publish(m Manifest) (string, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	if err := writeSynced(filepath.Join(a.dir, ManifestFile), b); err != nil {
		return "", err
	}
	if err := syncTree(filepath.Join(a.dir, TreeDir)); err != nil {
		return "", err
	}
	// The directory is opened to readers only now: an attempt is 0700 so
	// a pod's mount of the key directory never reads one.
	if err := os.Chmod(a.dir, 0o755); err != nil {
		return "", err
	}
	if err := writeSynced(filepath.Join(a.dir, CompleteFile), nil); err != nil {
		return "", err
	}
	if err := syncDir(a.dir); err != nil {
		return "", err
	}
	// The key directory may be new (newAttempt made it): its own entry
	// lives in the root.
	if err := syncDir(filepath.Dir(a.keyDir)); err != nil {
		return "", err
	}
	gen := filepath.Join(a.keyDir, GenPrefix+newID())
	if err := os.Rename(a.dir, gen); err != nil {
		return "", err
	}
	if err := syncDir(a.keyDir); err != nil {
		// Visible but not durable: back to the attempt's name, which no
		// reader takes and the caller's abort removes.
		if rerr := os.Rename(gen, a.dir); rerr != nil {
			_ = os.RemoveAll(gen)
		}
		return "", err
	}
	return gen, nil
}

// Harvest copies the tree at src (a pod's download volume, read from the
// host) into a new generation of key. It hashes as it copies, and checks
// each file did not change during its copy. Any error leaves nothing.
func (s *Store) Harvest(key, src string, lim Limits) (string, error) {
	a, err := s.newAttempt(key)
	if err != nil {
		return "", err
	}
	m, err := copyIn(src, filepath.Join(a.dir, TreeDir), lim)
	if err != nil {
		a.abort()
		return "", err
	}
	gen, err := a.publish(m)
	if err != nil {
		a.abort()
		return "", err
	}
	return gen, nil
}

// copyIn copies the tree at src to dst, building the manifest. It walks
// without following any symlink (walkNoFollow), and opens every file
// through its parent's descriptor, checking it is the regular file it
// examined, before copying any. The tree is then checked against its
// fingerprint and the bounds, and the files copied Parallel at a time from
// those descriptors. A file that changed during its copy (its size, mtime
// or change time), or anything changed after lim.NotAfter, is refused; so
// is a harvest the pod's teardown overtakes, since unlinking a file changes
// it too.
func copyIn(src, dst string, lim Limits) (Manifest, error) {
	m := Manifest{Version: ManifestVersion}
	root, err := rootEntry(src)
	if err != nil {
		return m, err
	}
	m.Root = &root
	if !lim.NotAfter.IsZero() && time.Unix(0, root.Mtime).After(lim.NotAfter) {
		return m, fmt.Errorf("%w: the volume root", ErrWrittenAfter)
	}
	type pending struct {
		i   int // index in m.Entries
		in  *os.File
		st  unix.Stat_t
		out string
	}
	var files []pending
	defer func() {
		for _, f := range files {
			f.in.Close()
		}
	}()
	bounded := lim.MaxBytes > 0 || lim.ExactBytes
	err = walkNoFollow(src, lim.MaxEntries, func(n node) error {
		if !lim.NotAfter.IsZero() && time.Unix(0, n.st.Mtim.Nano()).After(lim.NotAfter) {
			return fmt.Errorf("%w: %s", ErrWrittenAfter, n.rel)
		}
		var target string
		if n.st.Mode&unix.S_IFMT == unix.S_IFLNK {
			t, err := readLink(n)
			if err != nil {
				return err
			}
			if !validLink(n.rel, t) {
				return fmt.Errorf("%w: symlink %s -> %s", ErrUnsupported, n.rel, t)
			}
			target = t
		}
		e, err := entryOf(n, target)
		if err != nil {
			return err
		}
		out := filepath.Join(dst, filepath.FromSlash(n.rel))
		switch e.Type {
		case TypeDir:
			if err := os.Mkdir(out, 0o755); err != nil {
				return err
			}
		case TypeSymlink:
			if err := os.Symlink(target, out); err != nil {
				return err
			}
		case TypeFile:
			if m.Bytes += e.Size; bounded && m.Bytes > lim.MaxBytes {
				return fmt.Errorf("%w: over %d bytes", ErrTooLarge, lim.MaxBytes)
			}
			if len(files) >= maxOpenFiles {
				return fmt.Errorf("%w: over %d files", ErrTooLarge, maxOpenFiles)
			}
			fd, err := openSame(n.dirfd, n.name, &n.st, 0)
			if err != nil {
				return fmt.Errorf("%s: %w", n.rel, err)
			}
			// Opened non-blocking so a FIFO swapped in could not hang the
			// open; a regular file reads the same either way.
			_ = unix.SetNonblock(fd, false)
			files = append(files, pending{i: len(m.Entries), in: os.NewFile(uintptr(fd), n.rel), st: n.st, out: out})
		}
		m.Entries = append(m.Entries, e)
		return nil
	})
	if err != nil {
		return m, err
	}
	if lim.Fingerprint != "" && Fingerprint(append([]Entry{root}, m.Entries...)) != lim.Fingerprint {
		return m, fmt.Errorf("%w: the tree is not the one the download finished with", ErrChanged)
	}
	if afterOpen != nil {
		afterOpen()
	}
	work := make(chan pending)
	var mu sync.Mutex
	var first error
	var wg sync.WaitGroup
	for w := 0; w < max(Parallel, 1); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range work {
				sum, err := copyFrom(f.in, f.st, f.out)
				mu.Lock()
				if err != nil && first == nil {
					first = fmt.Errorf("%s: %w", m.Entries[f.i].Path, err)
				}
				m.Entries[f.i].SHA256 = sum
				mu.Unlock()
			}
		}()
	}
	for _, f := range files {
		mu.Lock()
		stop := first != nil
		mu.Unlock()
		if stop {
			break
		}
		work <- f
	}
	close(work)
	wg.Wait()
	return m, first
}

// maxOpenFiles bounds the files a harvest holds open at once: a tree of
// more is not cached.
const maxOpenFiles = 65536

// afterOpen, in tests, runs once every file is open and before any copy.
var afterOpen func()

// copyFrom copies the open regular file in, whose status at the walk was
// st, to dst: never more than its size, hashing it, and checking its size
// and mtime did not change.
func copyFrom(in *os.File, st unix.Stat_t, dst string) (string, error) {
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	k, err := io.Copy(io.MultiWriter(out, h), io.LimitReader(in, st.Size+1))
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return "", err
	}
	if k > st.Size {
		return "", ErrTooLarge
	}
	var after unix.Stat_t
	if err := unix.Fstat(int(in.Fd()), &after); err != nil {
		return "", err
	}
	// The change time catches what size and mtime do not: a rewrite in
	// place with its mtime set back, and an unlink (a pod torn down
	// mid-harvest), after which nothing says the open file is still the
	// download's.
	if k != st.Size || after.Size != st.Size || after.Mtim != st.Mtim || after.Ctim != st.Ctim {
		return "", ErrChanged
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Fetch builds a new generation of key from a peer's manifest, reading
// each file through open and checking it against the manifest. The
// manifest is checked first: a path or link leaving the tree, or a tree
// over the limits, is refused before anything is written. The cache's
// own copy is readable by any user (a seeding init may not run as root);
// a seed gives the tree its recorded owners and modes.
func (s *Store) Fetch(key string, m Manifest, open func(rel string) (ReadCloser, error), lim Limits) (string, error) {
	if err := m.Check(lim); err != nil {
		return "", err
	}
	a, err := s.newAttempt(key)
	if err != nil {
		return "", err
	}
	tree := filepath.Join(a.dir, TreeDir)
	if err := materializeAll(tree, m.Entries, func(e Entry) (ReadCloser, error) { return open(e.Path) }); err != nil {
		a.abort()
		return "", err
	}
	gen, err := a.publish(m)
	if err != nil {
		a.abort()
		return "", err
	}
	return gen, nil
}

// Seed copies generation gen, whose manifest is m, into the empty
// directory dst, checking every file against the manifest as it copies.
// Every entry then gets its recorded owner, mode and mtime, children
// before their directory. Running as root it sets any owner; otherwise
// every entry must be the helper's own user, as when the download ran as
// that user, or it refuses before writing. On error dst may hold part of
// the tree; the caller empties it before falling back to the download.
func Seed(gen string, m Manifest, dst string) error {
	if err := m.Check(Limits{}); err != nil {
		return err
	}
	ents, err := os.ReadDir(dst)
	if err != nil {
		return err
	}
	if len(ents) > 0 {
		return ErrTargetNotEmpty
	}
	root := os.Geteuid() == 0
	if !root {
		for _, e := range m.Entries {
			if e.UID != os.Geteuid() {
				return fmt.Errorf("%w: %s is owned by %d, the helper runs as %d", ErrOwner, e.Path, e.UID, os.Geteuid())
			}
		}
	}
	tree := filepath.Join(gen, TreeDir)
	if err := materializeAll(dst, m.Entries, func(e Entry) (ReadCloser, error) {
		return os.OpenFile(filepath.Join(tree, filepath.FromSlash(e.Path)), os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	}); err != nil {
		return err
	}
	return ApplyMeta(dst, m)
}

// ApplyMeta gives every entry of m under dst, and dst itself, its recorded
// owner, mode and mtime, children before their directory. Running as
// root it sets any owner; otherwise it keeps the caller's user and sets
// the group (Seed checked the entries are the caller's).
func ApplyMeta(dst string, m Manifest) error {
	root := os.Geteuid() == 0
	for i := len(m.Entries) - 1; i >= 0; i-- {
		e := m.Entries[i]
		p := filepath.Join(dst, filepath.FromSlash(e.Path))
		var st unix.Stat_t
		if err := unix.Lstat(p, &st); err != nil {
			return err
		}
		// The owner first: chown clears the set-id bits chmod then sets.
		if int(st.Uid) != e.UID || int(st.Gid) != e.GID {
			uid := e.UID
			if !root {
				uid = -1
			}
			if err := unix.Lchown(p, uid, e.GID); err != nil {
				return fmt.Errorf("%w: %s: %w", ErrOwner, e.Path, err)
			}
		}
		if e.Type != TypeSymlink {
			if err := unix.Chmod(p, e.Mode); err != nil {
				return err
			}
		}
		ts := []unix.Timespec{unix.NsecToTimespec(e.Mtime), unix.NsecToTimespec(e.Mtime)}
		if err := unix.UtimesNanoAt(unix.AT_FDCWD, p, ts, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return err
		}
	}
	if r := m.Root; r != nil {
		// The volume itself, as the download left it: files the engine
		// creates later inherit its group and are checked against its mode.
		var st unix.Stat_t
		if err := unix.Stat(dst, &st); err != nil {
			return err
		}
		if int(st.Uid) != r.UID || int(st.Gid) != r.GID {
			uid := r.UID
			if !root {
				uid = -1
			}
			if err := unix.Chown(dst, uid, r.GID); err != nil {
				return fmt.Errorf("%w: the volume: %w", ErrOwner, err)
			}
		}
		if st.Mode&0o7777 != r.Mode {
			if err := unix.Chmod(dst, r.Mode); err != nil {
				return fmt.Errorf("%w: the volume's mode: %w", ErrOwner, err)
			}
		}
		// Its mtime only matters to nothing the engine does; a volume the
		// helper does not own may refuse it.
		ts := []unix.Timespec{unix.NsecToTimespec(r.Mtime), unix.NsecToTimespec(r.Mtime)}
		_ = unix.UtimesNanoAt(unix.AT_FDCWD, dst, ts, 0)
	}
	return nil
}

// Parallel is how many files a seed or a fetch copies at once: a model of
// a terabyte and more in shards of gigabytes copies at disk or network
// speed only with several streams.
var Parallel = 8

// materializeAll creates entries under root: directories and symlinks in
// order (parents first), then the files, Parallel at a time. It returns
// the first error, after the copies in flight have stopped.
func materializeAll(root string, entries []Entry, open func(Entry) (ReadCloser, error)) error {
	var files []Entry
	for _, e := range entries {
		if e.Type == TypeFile {
			files = append(files, e)
			continue
		}
		if err := materialize(root, e, nil); err != nil {
			return fmt.Errorf("%s: %w", e.Path, err)
		}
	}
	n := Parallel
	if n < 1 {
		n = 1
	}
	work := make(chan Entry)
	var mu sync.Mutex
	var first error
	failed := func() bool { mu.Lock(); defer mu.Unlock(); return first != nil }
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for e := range work {
				if failed() {
					continue
				}
				if err := materialize(root, e, func() (ReadCloser, error) { return open(e) }); err != nil {
					mu.Lock()
					if first == nil {
						first = fmt.Errorf("%s: %w", e.Path, err)
					}
					mu.Unlock()
				}
			}
		}()
	}
	for _, e := range files {
		if failed() {
			break
		}
		work <- e
	}
	close(work)
	wg.Wait()
	return first
}

// materialize creates entry e under root, reading a file's bytes from
// open and checking them against e. Files are created readable by all
// and directories open, as the cache keeps them; Seed then applies the
// recorded owner and mode.
func materialize(root string, e Entry, open func() (ReadCloser, error)) error {
	out := filepath.Join(root, filepath.FromSlash(e.Path))
	switch e.Type {
	case TypeDir:
		return os.Mkdir(out, 0o755)
	case TypeSymlink:
		return os.Symlink(e.Target, out)
	case TypeFile:
		in, err := open()
		if err != nil {
			return err
		}
		defer in.Close()
		f, err := os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			return err
		}
		h := sha256.New()
		// One byte past the size shows a source longer than its entry
		// without writing more than that.
		n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(in, e.Size+1))
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return err
		}
		if n != e.Size || !sumIs(h, e.SHA256) {
			return ErrCorrupt
		}
		return nil
	}
	return ErrUnsupported
}

func sumIs(h hash.Hash, want string) bool {
	return hex.EncodeToString(h.Sum(nil)) == want
}

// EvictKey removes every generation of key. The caller holds the key's
// lock and has checked nothing uses it.
func (s *Store) EvictKey(key string) error {
	if !ValidKey(key) && !testKey(key) {
		return fmt.Errorf("model cache: bad key %q", key)
	}
	return os.RemoveAll(s.KeyDir(key))
}

// CleanAttempts removes every attempt directory. Called when the agent
// starts, before it begins any attempt: no attempt then has a live owner.
func (s *Store) CleanAttempts() error {
	keys, err := os.ReadDir(s.Root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, k := range keys {
		if !k.IsDir() {
			continue
		}
		ents, _ := os.ReadDir(filepath.Join(s.Root, k.Name()))
		for _, e := range ents {
			if strings.HasPrefix(e.Name(), AttemptPrefix) {
				if err := os.RemoveAll(filepath.Join(s.Root, k.Name(), e.Name())); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Keys lists the keys with at least one directory under the root.
func (s *Store) Keys() []string {
	ents, err := os.ReadDir(s.Root)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range ents {
		if e.IsDir() {
			out = append(out, e.Name())
		}
	}
	return out
}

// Usage is the disk the cache's complete generations take: allocated
// blocks, directories included. Attempts in flight are left out: their
// owners account for them by reservation.
func (s *Store) Usage() (int64, error) {
	var total int64
	err := filepath.WalkDir(s.Root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		// Attempts are directly in a key directory; a model directory
		// deeper in a tree may have any name.
		if d.IsDir() && strings.HasPrefix(d.Name(), AttemptPrefix) && filepath.Dir(filepath.Dir(p)) == filepath.Clean(s.Root) {
			return filepath.SkipDir
		}
		if p == s.Root {
			return nil
		}
		if fi, err := d.Info(); err == nil {
			if st, ok := fi.Sys().(*syscall.Stat_t); ok {
				total += st.Blocks * 512
			}
		}
		return nil
	})
	return total, err
}

// LastUsed is when key was last seeded or published: the newest mtime of
// its key directory's use marker or generations.
func (s *Store) LastUsed(key string) time.Time {
	var t time.Time
	for _, p := range append(s.Generations(key), filepath.Join(s.KeyDir(key), usedFile)) {
		if fi, err := os.Stat(p); err == nil && fi.ModTime().After(t) {
			t = fi.ModTime()
		}
	}
	return t
}

// usedFile is touched by the agent whenever a pod seeds the key.
const usedFile = ".used"

// Touch records a use of key, for eviction order.
func (s *Store) Touch(key string) {
	p := filepath.Join(s.KeyDir(key), usedFile)
	now := time.Now()
	if err := os.Chtimes(p, now, now); err != nil {
		_ = os.WriteFile(p, nil, 0o644)
	}
}

func writeSynced(p string, b []byte) error {
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// syncDir flushes directory p's entries to disk; a test replaces it.
var syncDir = func(p string) error {
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// syncTree flushes every file and directory under root to disk, so a
// generation marked complete survives a power loss as written.
func syncTree(root string) error {
	return filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type()&fs.ModeSymlink != 0 {
			return nil
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer f.Close()
		return f.Sync()
	})
}
