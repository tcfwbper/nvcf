// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package diskguard

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/atomicfile"
)

// ErrDeletedNotSynced reports data that was deleted but whose directory
// could not be synced. The deletion happened and is counted; the error is
// still returned so the failing volume is seen. Segments.Evict implementations
// wrap it in the same case.
var ErrDeletedNotSynced = errors.New("deleted, but the directory sync failed")

// Test seams: onSync observes the order of directory syncs; rename and
// syncFile let a test fail one move or one sync.
var (
	onSync   func(dir string)
	rename   = os.Rename
	syncFile = (*os.File).Sync
)

// RejectsDirName holds reject artifacts: raw lines that could not be read,
// kept per segment. They count against the quarantine bound and outlive the
// source.
const RejectsDirName = "rejects"

// rejectTempSuffix ends the name of a reject artifact still being written;
// transform.RejectFile writes under such a name and renames it when done.
const rejectTempSuffix = ".tmp"

// Quarantine is the quarantine directory on the spool volume:
//
//	<dir>/<segment id>/          a quarantined segment and its files
//	<dir>/rejects/<segment id>.* reject artifacts
//
// Admit, Entries and Remove hold one lock, so a listing never shows an entry
// that an admission is still filling, and nothing removes it mid-admission.
type Quarantine struct {
	dir string
	mu  sync.Mutex
}

// NewQuarantine returns the quarantine at dir.
func NewQuarantine(dir string) *Quarantine {
	return &Quarantine{dir: dir}
}

// RejectsDir is where reject artifacts are written.
func (q *Quarantine) RejectsDir() string {
	return filepath.Join(q.dir, RejectsDirName)
}

// Admit moves files into the quarantine entry for id, by rename, so they
// must be on the same volume. The directories are linked durably first, on
// every call, and each source directory is synced only after the entry, so a
// power loss never keeps a removal without the moved file. When a move fails,
// the moves already made are synced before the error returns. A file never
// replaces one already in the entry: a name already taken gets a numbered
// suffix. A path already gone is skipped, and so is a source directory that
// is gone, so an interrupted Admit can be repeated.
func (q *Quarantine) Admit(id string, paths ...string) error {
	if !validEntryName(id) {
		return fmt.Errorf("quarantine id %q is not a plain name", id)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	entry := filepath.Join(q.dir, id)
	if err := linkDurable(entry); err != nil {
		return err
	}
	var sources []string
	var moveErr error
	for _, path := range paths {
		if moveErr = moveInto(path, entry); moveErr != nil {
			break
		}
		if dir := filepath.Dir(path); !slices.Contains(sources, dir) {
			sources = append(sources, dir)
		}
	}
	if err := syncDir(entry); err != nil {
		return errors.Join(moveErr, err)
	}
	for _, dir := range sources {
		// A source directory removed since its file left has no removal
		// left to make durable.
		if err := syncDir(dir); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return errors.Join(moveErr, err)
		}
	}
	return moveErr
}

// moveInto renames path into dir without replacing anything there: a name
// already taken gets a numbered suffix, as the journal fence does. A path
// already gone is skipped.
func moveInto(path, dir string) error {
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("stat file to quarantine: %w", err)
	}
	target := filepath.Join(dir, filepath.Base(path))
	for n := 1; ; n++ {
		if _, err := os.Lstat(target); errors.Is(err, os.ErrNotExist) {
			break
		} else if err != nil {
			return fmt.Errorf("stat quarantine file: %w", err)
		}
		target = filepath.Join(dir, filepath.Base(path)+"."+strconv.Itoa(n))
	}
	if err := rename(path, target); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("move file into quarantine: %w", err)
	}
	return nil
}

// Entries lists every quarantined segment and every reject artifact, with
// its size and modification time. A reject artifact still being written is
// marked Busy. A quarantine that does not exist yet is empty.
func (q *Quarantine) Entries() ([]Candidate, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	items, err := os.ReadDir(q.dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read quarantine: %w", err)
	}
	var entries []Candidate
	for _, item := range items {
		if item.Name() == RejectsDirName && item.IsDir() {
			rejects, err := q.rejects()
			if err != nil {
				return nil, err
			}
			entries = append(entries, rejects...)
			continue
		}
		info, err := item.Info()
		if err != nil {
			return nil, fmt.Errorf("stat quarantine entry: %w", err)
		}
		size, err := treeSize(filepath.Join(q.dir, item.Name()), map[fileID]bool{})
		if err != nil {
			return nil, fmt.Errorf("measure quarantine entry: %w", err)
		}
		entries = append(entries, Candidate{Key: item.Name(), Bytes: size, ModTime: info.ModTime()})
	}
	return entries, nil
}

func (q *Quarantine) rejects() ([]Candidate, error) {
	items, err := os.ReadDir(q.RejectsDir())
	if err != nil {
		return nil, fmt.Errorf("read quarantine rejects: %w", err)
	}
	entries := make([]Candidate, 0, len(items))
	for _, item := range items {
		info, err := item.Info()
		if err != nil {
			return nil, fmt.Errorf("stat reject artifact: %w", err)
		}
		size, err := treeSize(filepath.Join(q.RejectsDir(), item.Name()), map[fileID]bool{})
		if err != nil {
			return nil, fmt.Errorf("measure quarantine entry: %w", err)
		}
		entries = append(entries, Candidate{
			Key:     RejectsDirName + "/" + item.Name(),
			Bytes:   size,
			ModTime: info.ModTime(),
			// A reject artifact is written under a temporary name and renamed
			// once complete, so a temporary one may still be written.
			Busy: strings.HasSuffix(item.Name(), rejectTempSuffix),
		})
	}
	return entries, nil
}

// Remove deletes one entry by the key Entries returned: "<id>" or
// "rejects/<name>". Any other key is refused, so a key can never reach
// outside the quarantine.
func (q *Quarantine) Remove(key string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	parts := strings.Split(key, "/")
	valid := (len(parts) == 1 && validEntryName(parts[0])) ||
		(len(parts) == 2 && parts[0] == RejectsDirName && validEntryName(parts[1]))
	if !valid {
		return fmt.Errorf("quarantine key %q is not an entry", key)
	}
	path := filepath.Join(q.dir, filepath.FromSlash(key))
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("remove quarantine entry: %w", err)
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("%w: %w", ErrDeletedNotSynced, err)
	}
	return nil
}

// linkDurable creates entry and any missing parents with
// atomicfile.MkdirDurable. The entry and the quarantine directory that holds
// it are linked on every call, even when they exist, so a retry after a
// failed sync still makes them durable. An entry that is not a real
// directory, such as a link, is refused: files moved through it would leave
// the quarantine.
func linkDurable(entry string) error {
	for _, dir := range []string{filepath.Dir(entry), entry} {
		if err := atomicfile.MkdirDurable(dir, syncDir); err != nil {
			return fmt.Errorf("create quarantine directory: %w", err)
		}
	}
	if info, err := os.Lstat(entry); err != nil {
		return fmt.Errorf("stat quarantine entry: %w", atomicfile.Pathless(err))
	} else if !info.IsDir() {
		return errors.New("quarantine entry exists and is not a directory")
	}
	return nil
}

func validEntryName(name string) bool {
	return name != "" && name != "." && name != ".." && name != RejectsDirName && !strings.ContainsAny(name, `/\`)
}

// treeSize adds up the space root and everything under it take on disk,
// as the kubelet counts an emptyDir: files, directories and links, with a
// file seen under another name counted once. A file or directory that is
// gone by the time the walk reaches it, such as one deleted meanwhile,
// holds nothing.
func treeSize(root string, seen map[fileID]bool) (int64, error) {
	var size int64
	err := filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		info, err := d.Info()
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		bytes, id, linked := diskUsage(info)
		if linked {
			if seen[id] {
				return nil
			}
			seen[id] = true
		}
		size += bytes
		return nil
	})
	return size, err
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open directory for sync: %w", atomicfile.Pathless(err))
	}
	syncErr := syncFile(d)
	closeErr := d.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return fmt.Errorf("sync directory: %w", atomicfile.Pathless(err))
	}
	if onSync != nil {
		onSync(dir)
	}
	return nil
}
