// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package modelcache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

// Streaming a download in progress (docs/design/model-cache-single-flight.md).

// SignalFile is the file the helper writes into the pod's signal volume
// (an in-memory emptyDir only the wrapped init mounts) once the download
// exited 0, holding the tree's Fingerprint. It tells the agent the
// download is over while the init is held open. It is not written into
// the model volume, whose directories' times are part of the tree.
const SignalFile = "downloaded"

// SignalDownloading is what the signal file holds while the pod's own
// download runs: the agent never streams into that volume.
const SignalDownloading = "downloading"

// LiveEntry is an entry of a tree being written: an Entry, plus what
// tells a file renamed apart from a new one (device and inode) and its
// change time. A follower copies by these.
type LiveEntry struct {
	Entry
	Dev   uint64 `json:"dev"`
	Ino   uint64 `json:"ino"`
	Ctime int64  `json:"ctime"`
	// Allocated is the bytes the filesystem holds for the file. A
	// downloader that sizes a file first and fills it in later leaves it
	// short of Size until it is full.
	Allocated int64 `json:"allocated"`
}

// Filled reports whether a file's blocks cover its size: one sized ahead
// and still being filled is not. A filesystem that compresses reports
// fewer blocks than bytes; its files then count as filled only at the end
// of the download, when a stream copies whatever is left.
func Filled(e LiveEntry) bool {
	return e.Type != TypeFile || e.Allocated >= e.Size
}

// Entries returns the Entries of live, change times kept, for Fingerprint.
func Entries(live []LiveEntry) []Entry {
	out := make([]Entry, len(live))
	for i, l := range live {
		out[i] = l.Entry
		out[i].ctime = l.Ctime
	}
	return out
}

// SameFile reports whether a and b describe the same file in the same
// state: same inode, size, mtime and change time.
func SameFile(a, b LiveEntry) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Size == b.Size && a.Mtime == b.Mtime && a.Ctime == b.Ctime && a.Type == b.Type
}

// ScanLive describes the tree at root as Scan does, with each entry's
// device, inode and change time.
func ScanLive(root string, max int) ([]LiveEntry, error) {
	r, err := rootEntry(root)
	if err != nil {
		return nil, err
	}
	var rst unix.Stat_t
	if err := unix.Lstat(root, &rst); err != nil {
		return nil, err
	}
	out := []LiveEntry{{Entry: r, Dev: uint64(rst.Dev), Ino: rst.Ino, Ctime: rst.Ctim.Nano(), Allocated: rst.Blocks * 512}}
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
		out = append(out, LiveEntry{Entry: e, Dev: uint64(n.st.Dev), Ino: n.st.Ino, Ctime: n.st.Ctim.Nano(), Allocated: n.st.Blocks * 512})
		return nil
	})
	return out, err
}

// OpenBeneath opens the regular file rel under root, component by
// component through each parent's descriptor, following no symlink
// anywhere, so a tree a workload changes under it cannot lead it outside
// root. It returns the file and its status.
func OpenBeneath(root, rel string) (*os.File, unix.Stat_t, error) {
	var st unix.Stat_t
	if !validRel(rel) {
		return nil, st, fmt.Errorf("%w: path %q", ErrUnsupported, rel)
	}
	dirfd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, st, err
	}
	parts := strings.Split(rel, "/")
	for _, p := range parts[:len(parts)-1] {
		next, err := unix.Openat(dirfd, p, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		unix.Close(dirfd)
		if err != nil {
			return nil, st, err
		}
		dirfd = next
	}
	fd, err := unix.Openat(dirfd, parts[len(parts)-1], unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	unix.Close(dirfd)
	if err != nil {
		return nil, st, err
	}
	if err := unix.Fstat(fd, &st); err != nil {
		unix.Close(fd)
		return nil, st, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG {
		unix.Close(fd)
		return nil, st, fmt.Errorf("%w: %s is not a regular file", ErrUnsupported, rel)
	}
	return os.NewFile(uintptr(fd), rel), st, nil
}

// LutimesBeneath sets the mtime (and atime) of rel under root without
// following a symlink anywhere: not the entry itself, which may be one,
// nor any directory on the way.
func LutimesBeneath(root, rel string, mtime int64) error {
	if !validRel(rel) {
		return fmt.Errorf("%w: path %q", ErrUnsupported, rel)
	}
	dirfd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer func() { unix.Close(dirfd) }()
	parts := strings.Split(rel, "/")
	for _, p := range parts[:len(parts)-1] {
		next, err := unix.Openat(dirfd, p, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return err
		}
		unix.Close(dirfd)
		dirfd = next
	}
	ts := []unix.Timespec{unix.NsecToTimespec(mtime), unix.NsecToTimespec(mtime)}
	return unix.UtimesNanoAt(dirfd, parts[len(parts)-1], ts, unix.AT_SYMLINK_NOFOLLOW)
}

// HashOpen hashes the open file f from its start, whose status was st, and
// checks it did not change while it was read (size, mtime, change time).
func HashOpen(f *os.File, st unix.Stat_t) (string, error) {
	h := sha256.New()
	n, err := io.Copy(h, io.NewSectionReader(f, 0, st.Size+1))
	if err != nil {
		return "", err
	}
	var after unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &after); err != nil {
		return "", err
	}
	if n != st.Size || after.Size != st.Size || after.Mtim != st.Mtim || after.Ctim != st.Ctim {
		return "", ErrChanged
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
