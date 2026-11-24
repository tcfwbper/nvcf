// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package modelcache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"os"
	"sort"

	"golang.org/x/sys/unix"
)

// node is one entry the walker reached: its path in the tree, its own
// status (not a symlink's target's), and the open directory it is in.
type node struct {
	rel   string
	st    unix.Stat_t
	dirfd int
	name  string
}

// walkNoFollow calls fn for every entry under root, parents before
// children, names in order. It never follows a symlink, anywhere in a
// path: each entry is reached through its parent's open descriptor, and a
// directory is entered only if the one opened is the one examined. A tree
// that a running workload changes under the walk therefore cannot lead it
// outside the tree (a directory swapped for a symlink to /), only to an
// error.
//
// max bounds the entries walked (0: no bound); past it the walk stops with
// ErrTooLarge, before reading the rest, so a tree of millions of entries
// costs neither memory nor time.
func walkNoFollow(root string, max int, fn func(n node) error) error {
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	seen := 0
	return walkDirFD(fd, "", &walkBudget{max: max, seen: &seen}, fn)
}

type walkBudget struct {
	max  int
	seen *int
}

func (b *walkBudget) add(n int) error {
	*b.seen += n
	if b.max > 0 && *b.seen > b.max {
		return fmt.Errorf("%w: over %d entries", ErrTooLarge, b.max)
	}
	return nil
}

func walkDirFD(dirfd int, prefix string, b *walkBudget, fn func(n node) error) error {
	names, err := readNames(dirfd, b)
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		var st unix.Stat_t
		if err := unix.Fstatat(dirfd, name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return err
		}
		rel := name
		if prefix != "" {
			rel = prefix + "/" + name
		}
		if err := fn(node{rel: rel, st: st, dirfd: dirfd, name: name}); err != nil {
			return err
		}
		if st.Mode&unix.S_IFMT != unix.S_IFDIR {
			continue
		}
		sub, err := openSame(dirfd, name, &st, unix.O_DIRECTORY)
		if err != nil {
			return fmt.Errorf("%s: %w", rel, err)
		}
		err = walkDirFD(sub, rel, b, fn)
		unix.Close(sub)
		if err != nil {
			return err
		}
	}
	return nil
}

// openSame opens name in dirfd without following a symlink, and checks it
// is the entry st described.
func openSame(dirfd int, name string, st *unix.Stat_t, flags int) (int, error) {
	fd, err := unix.Openat(dirfd, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC|flags, 0)
	if err != nil {
		return -1, err
	}
	var now unix.Stat_t
	if err := unix.Fstat(fd, &now); err != nil {
		unix.Close(fd)
		return -1, err
	}
	if now.Ino != st.Ino || now.Dev != st.Dev || now.Mode&unix.S_IFMT != st.Mode&unix.S_IFMT {
		unix.Close(fd)
		return -1, ErrChanged
	}
	return fd, nil
}

// readNames reads a directory's names in batches, counting them against
// the walk's budget as it goes.
func readNames(dirfd int, b *walkBudget) ([]string, error) {
	dup, err := unix.Dup(dirfd)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(dup), "")
	defer f.Close()
	var names []string
	for {
		batch, err := readDirBatch(f, 4096)
		names = append(names, batch...)
		if berr := b.add(len(batch)); berr != nil {
			return nil, berr
		}
		// Only io.EOF ends the listing: any other error, with or without
		// names, leaves it incomplete.
		if err == io.EOF {
			return names, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

// readDirBatch reads up to n names; a test makes it fail.
var readDirBatch = func(f *os.File, n int) ([]string, error) { return f.Readdirnames(n) }

// entryOf describes n, without its checksum.
func entryOf(n node, target string) (Entry, error) {
	e := Entry{Path: n.rel, Mode: n.st.Mode & 0o7777, UID: int(n.st.Uid), GID: int(n.st.Gid), Mtime: n.st.Mtim.Nano(), ctime: n.st.Ctim.Nano()}
	switch n.st.Mode & unix.S_IFMT {
	case unix.S_IFDIR:
		e.Type = TypeDir
	case unix.S_IFREG:
		e.Type, e.Size = TypeFile, n.st.Size
	case unix.S_IFLNK:
		e.Type, e.Target, e.Mode = TypeSymlink, target, 0
	default:
		return e, fmt.Errorf("%w: %s has file type %o", ErrUnsupported, n.rel, n.st.Mode&unix.S_IFMT)
	}
	return e, nil
}

func readLink(n node) (string, error) {
	buf := make([]byte, 4096)
	k, err := unix.Readlinkat(n.dirfd, n.name, buf)
	if err != nil {
		return "", err
	}
	if k >= len(buf) {
		return "", fmt.Errorf("%w: symlink %s target too long", ErrUnsupported, n.rel)
	}
	return string(buf[:k]), nil
}

// rootEntry describes the tree's root directory itself ("."): the volume
// a download may have given its own owner and mode.
func rootEntry(root string) (Entry, error) {
	var st unix.Stat_t
	if err := unix.Lstat(root, &st); err != nil {
		return Entry{}, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR {
		return Entry{}, fmt.Errorf("%w: %s is not a directory", ErrUnsupported, root)
	}
	return Entry{Path: ".", Type: TypeDir, Mode: st.Mode & 0o7777, UID: int(st.Uid), GID: int(st.Gid), Mtime: st.Mtim.Nano(), ctime: st.Ctim.Nano()}, nil
}

// Scan describes the tree at root without reading file contents: the root
// (path ".") and every entry's path, type, size, mode, owner, mtime and
// link target, at most max entries (0: no bound).
func Scan(root string, max int) ([]Entry, error) {
	r, err := rootEntry(root)
	if err != nil {
		return nil, err
	}
	out := []Entry{r}
	err = walkNoFollow(root, max, func(n node) error {
		var target string
		if n.st.Mode&unix.S_IFMT == unix.S_IFLNK {
			t, err := readLink(n)
			if err != nil {
				return err
			}
			target = t
		}
		e, err := entryOf(n, target)
		if err != nil {
			return err
		}
		out = append(out, e)
		return nil
	})
	return out, err
}

// Fingerprint identifies a tree as it lies on disk: every entry's path,
// type, size, mode, owner, mtime, change time and link target, in walk
// order. The download init records it when its download finishes; a
// harvest publishes only a copy with the same fingerprint, so nothing
// changed afterwards (a file removed, added, rewritten) is cached. The
// change time is what makes that hold against a writer: any write, chmod
// or chown sets it, and no unprivileged call sets it back, as one can an
// mtime.
func Fingerprint(entries []Entry) string {
	h := sha256.New()
	for _, e := range entries {
		fmt.Fprintf(h, "%s\t%d:%s\t%d\t%o\t%d\t%d\t%d\t%d\t%d:%s\n",
			e.Type, len(e.Path), e.Path, e.Size, e.Mode, e.UID, e.GID, e.Mtime, e.ctime, len(e.Target), e.Target)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// AllocEstimate is the disk a tree takes, rounding each file to 4 KiB and
// counting 4 KiB per entry for its inode and directory slot, so a tree of
// many small files is not reserved as if it were their byte count. It
// saturates at MaxInt64, which no budget admits, rather than wrap.
func AllocEstimate(entries []Entry) int64 {
	const block = 4096
	total := int64(block) // the manifest
	for _, e := range entries {
		if e.Size < 0 || e.Size > math.MaxInt64-2*block {
			return math.MaxInt64
		}
		n := block + (e.Size+block-1)/block*block
		if n > math.MaxInt64-total {
			return math.MaxInt64
		}
		total += n
	}
	return total
}
