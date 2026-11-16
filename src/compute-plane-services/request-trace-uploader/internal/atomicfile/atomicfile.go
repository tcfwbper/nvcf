// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package atomicfile writes a file under a temporary name and publishes it
// under its final name only once it is complete and synced, so a reader never
// sees a partial file. Every writer that publishes a local file uses it.
// MkdirDurable creates the directories those files go into, durably.
//
// Errors carry no file system paths, because callers log them.
package atomicfile

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// SyncDir syncs a directory, so a link made in it by a rename or a mkdir
// survives a power loss.
func SyncDir(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return Pathless(err)
	}
	return Pathless(errors.Join(handle.Sync(), handle.Close()))
}

// syncDir is the sync Publish uses. Tests replace it.
var syncDir = SyncDir

// MkdirDurable creates dir and any missing parents one level at a time, top
// down, and syncs each new directory's parent before it makes the next level,
// so every new link survives a power loss before anything is made below it.
// A creation that fails part way therefore leaves only durable links. When
// dir already exists it syncs dir's parent anyway: an earlier attempt, or an
// older build, may have made dir without a sync. For the same reason, when
// only part of the path exists, it first syncs the link of the deepest
// directory that does.
//
// When a sync fails, MkdirDurable removes the directory it just made, which
// is still empty, so the next attempt makes it again and syncs its link. It
// never removes a directory it did not make. A concurrent caller that found
// that directory in the meantime gets an error when it writes there; if the
// removal fails, the next attempt cannot tell, and this attempt's error is
// the only sign. sync is the caller's directory sync, so callers keep their
// own error text and test hooks.
func MkdirDurable(dir string, sync func(dir string) error) error {
	dir = filepath.Clean(dir)
	var missing []string // deepest first
	for d := dir; ; d = filepath.Dir(d) {
		if _, err := os.Stat(d); err == nil {
			break
		} else if !errors.Is(err, fs.ErrNotExist) {
			return Pathless(err)
		}
		missing = append(missing, d)
		if filepath.Dir(d) == d {
			break
		}
	}
	if len(missing) == 0 {
		// Fails when dir is not a directory.
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return Pathless(err)
		}
		return sync(filepath.Dir(dir))
	}
	// The deepest directory that exists may come from an attempt that failed
	// before its sync, or from a build that did not sync, so its link is
	// synced before anything is made below it.
	// It is made absolute first: a relative path can stop at ".", whose
	// parent filepath.Dir cannot name.
	if existing := filepath.Dir(missing[len(missing)-1]); existing != missing[len(missing)-1] {
		existing, err := filepath.Abs(existing)
		if err != nil {
			return Pathless(err)
		}
		if err := sync(filepath.Dir(existing)); err != nil {
			return err
		}
	}
	for i := len(missing) - 1; i >= 0; i-- {
		d := missing[i]
		err := os.Mkdir(d, 0o750)
		made := err == nil
		if errors.Is(err, fs.ErrExist) {
			// Made by someone else since the scan: accepted only if it is a
			// directory.
			err = os.MkdirAll(d, 0o750)
		}
		if err != nil {
			return Pathless(err)
		}
		if err := sync(filepath.Dir(d)); err != nil {
			if made {
				if rmErr := os.Remove(d); rmErr != nil && !errors.Is(rmErr, fs.ErrNotExist) {
					return errors.Join(err, Pathless(rmErr))
				}
			}
			return err
		}
	}
	return nil
}

// File is a file being written under a temporary name.
type File struct {
	file *os.File
}

// Create starts a file in dir. pattern is the temporary name, as for
// os.CreateTemp; callers choose one that recovery can find and remove.
func Create(dir, pattern string) (*File, error) {
	file, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, Pathless(err)
	}
	return &File{file: file}, nil
}

// Write appends p.
func (f *File) Write(p []byte) (int, error) {
	n, err := f.file.Write(p)
	return n, Pathless(err)
}

// Publish syncs and closes the file, then renames it to path and syncs the
// directory. On any failure it removes the temporary file, and the file at
// path if the rename happened, so a failed Publish leaves neither name.
func (f *File) Publish(path string) error {
	err := f.file.Sync()
	if closeErr := f.file.Close(); err == nil {
		err = closeErr
	}
	renamed := false
	if err == nil {
		err = os.Rename(f.file.Name(), path)
		renamed = err == nil
	}
	if err == nil {
		err = syncDir(filepath.Dir(path))
	}
	if err == nil {
		return nil
	}
	cleanup := removeIfPresent(f.file.Name())
	if renamed {
		cleanup = errors.Join(cleanup, removeIfPresent(path))
	}
	return Pathless(errors.Join(err, cleanup))
}

// Discard closes and removes an unpublished file. It is safe to call more
// than once and after a Publish that failed.
func (f *File) Discard() error {
	closeErr := f.file.Close()
	if errors.Is(closeErr, os.ErrClosed) {
		closeErr = nil
	}
	return Pathless(errors.Join(closeErr, removeIfPresent(f.file.Name())))
}

func removeIfPresent(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// Pathless drops the path from a file system error, keeping the operation
// and the cause, so the error can be logged. Joined errors are rewritten one
// by one.
func Pathless(err error) error {
	if err == nil {
		return nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		parts := make([]error, 0, len(joined.Unwrap()))
		for _, part := range joined.Unwrap() {
			parts = append(parts, Pathless(part))
		}
		return errors.Join(parts...)
	}
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) {
		return fmt.Errorf("%s: %w", pathErr.Op, pathErr.Err)
	}
	var linkErr *os.LinkError
	if errors.As(err, &linkErr) {
		return fmt.Errorf("%s: %w", linkErr.Op, linkErr.Err)
	}
	return err
}
