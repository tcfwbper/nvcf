// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package record

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
)

// DefaultMaxLineBytes is the default bound on one raw line held in memory.
const DefaultMaxLineBytes = 512 << 20

// DefaultMemoryStep is the line size at which a LineMemory is first asked.
const DefaultMemoryStep = 16 << 20

// LineMemory accounts for the memory long lines take, so concurrent passes
// can share a budget.
type LineMemory interface {
	// Hold is called before the line held in memory grows past its last
	// hold: first past the memory step, then past each doubling of it.
	// size is what the line may reach before the next call, never more than
	// the line limit. Hold may block. An error ends the pass, and Err
	// reports it.
	Hold(size int64) error
	// Release is called once the held line is no longer needed: at the next
	// Next, when it is rejected or cut, and at Close.
	Release()
}

// LineOptions bounds a Lines pass.
type LineOptions struct {
	// MaxLineBytes bounds one raw line held in memory. A longer line is
	// drained to its end, streamed to Reject, and counted as oversized.
	// Zero means DefaultMaxLineBytes.
	MaxLineBytes int64
	// Reject receives the raw bytes of every oversized line and every line
	// cut short by a damaged member, each followed by a newline. Nil drops
	// them. The bytes can hold user data, so Reject must stay local.
	Reject io.Writer
	// Memory, when set, is asked before a line held in memory grows past
	// MemoryStep. Nil asks nothing.
	Memory LineMemory
	// MemoryStep is the line size at which Memory is first asked. Zero
	// means DefaultMemoryStep.
	MemoryStep int64
}

// LineStats summarizes one pass.
type LineStats struct {
	// Lines counts non-empty lines delivered by Next.
	Lines int
	// Oversized counts lines longer than MaxLineBytes.
	Oversized int
	// Cut counts partial lines lost where a gzip member was damaged.
	Cut int
	// LostBytes counts compressed bytes that could not be decompressed:
	// a truncated tail, a damaged member, or anything that is not gzip. A
	// damaged member is counted from its start to the next gzip header, so
	// the count can include bytes whose lines were kept; it says how much of
	// the file was in damaged members, and Cut says how many lines were
	// lost.
	LostBytes int64
	// Resyncs counts restarts at a later gzip header after a damaged member.
	Resyncs int
	// ChecksumFailures counts gzip members that decoded but failed their
	// checksum. Their lines are delivered before the checksum at the member
	// end is read, so a caller that sees one must not trust the pass.
	ChecksumFailures int
}

// Lines reads the lines of a gzip JSON-lines segment and computes the
// SHA-256 of the whole file in the same pass.
//
// A segment is a series of gzip members, one per writer flush. A damaged
// member does not end the pass: the reader keeps every line before it,
// restarts at the next gzip header if there is one, and counts the bytes it
// skipped. A line ends at "\n", "\r\n" or a lone "\r", the way Python reads
// a text file, and blank lines are skipped.
type Lines struct {
	src     *members
	reject  io.Writer
	maxLine int64
	memory  LineMemory
	step    int64
	// held is the size the last Hold covered, or zero when no line is
	// held.
	held int64

	buf       []byte
	r, w      int
	line      []byte
	oversized bool
	pendingCR bool
	broken    bool
	eof       bool
	number    int
	current   []byte

	stats LineStats
	sum   []byte
	err   error
	done  bool
}

// OpenLines starts a pass over the segment at path. It refuses a path that
// is not a regular file, without following a symlink or waiting on a pipe.
func OpenLines(path string, opts LineOptions) (*Lines, error) {
	if opts.MaxLineBytes < 0 {
		return nil, fmt.Errorf("max line bytes must not be negative, got %d", opts.MaxLineBytes)
	}
	if opts.MemoryStep < 0 {
		return nil, fmt.Errorf("memory step must not be negative, got %d", opts.MemoryStep)
	}
	step := opts.MemoryStep
	if step == 0 {
		step = DefaultMemoryStep
	}
	maxLine := opts.MaxLineBytes
	if maxLine == 0 {
		maxLine = DefaultMaxLineBytes
	}
	file, err := OpenRegular(path)
	if err != nil {
		return nil, fmt.Errorf("open segment: %w", err)
	}
	l := &Lines{
		reject:  opts.Reject,
		maxLine: maxLine,
		memory:  opts.Memory,
		step:    step,
		buf:     make([]byte, 64<<10),
	}
	l.src = newMembers(file, &l.stats)
	return l, nil
}

// Next advances to the next non-empty line, reporting whether there is one.
func (l *Lines) Next() bool {
	l.release()
	for !l.done {
		if line, ok := l.scan(); ok {
			return l.deliver(line)
		}
		if l.err != nil {
			return l.finish()
		}
		if l.broken {
			// The damaged member ended mid-line; that line is incomplete.
			l.broken = false
			l.cut()
			// A failed reject write ends the pass before the next read.
			if l.err != nil {
				return l.finish()
			}
		}
		if l.eof {
			return l.finish()
		}
		n, err := l.src.Read(l.buf)
		l.r, l.w = 0, n
		switch {
		case errors.Is(err, errBreak):
			l.broken = true
		case errors.Is(err, io.EOF):
			l.eof = true
		case err != nil:
			l.err = fmt.Errorf("read segment: %w", err)
		}
	}
	return false
}

// scan consumes buffered bytes up to the next line end. It reports a line
// when one ended, and ok false when it needs more input.
func (l *Lines) scan() (line []byte, ok bool) {
	for l.r < l.w {
		if l.pendingCR {
			l.pendingCR = false
			if l.buf[l.r] == '\n' {
				l.r++
				continue
			}
		}
		chunk := l.buf[l.r:l.w]
		i := bytes.IndexAny(chunk, "\r\n")
		if i < 0 {
			l.appendLine(chunk)
			l.r = l.w
			return nil, false
		}
		l.appendLine(chunk[:i])
		if l.err != nil {
			return nil, false
		}
		if chunk[i] == '\r' {
			l.pendingCR = true
		}
		l.r += i + 1
		l.number++
		if l.oversized {
			l.endReject()
			l.stats.Oversized++
			l.oversized = false
			continue
		}
		if len(l.line) == 0 {
			continue
		}
		return l.line, true
	}
	return nil, false
}

func (l *Lines) deliver(line []byte) bool {
	l.current = line
	l.line = l.line[:0:0]
	l.stats.Lines++
	return true
}

func (l *Lines) appendLine(chunk []byte) {
	if len(chunk) == 0 {
		return
	}
	if l.oversized {
		l.writeReject(chunk)
		return
	}
	size := int64(len(l.line)) + int64(len(chunk))
	if size > l.maxLine {
		l.oversized = true
		l.writeReject(l.line)
		l.writeReject(chunk)
		l.line = l.line[:0]
		l.release()
		return
	}
	if !l.hold(size) {
		return
	}
	l.line = append(l.line, chunk...)
}

// hold asks the line memory before the held line grows to size bytes. It
// covers the line up to the next doubling of the step, so a growing line
// asks a few times, not on every read. It reports false when the memory
// refused, which ends the pass.
func (l *Lines) hold(size int64) bool {
	if l.memory == nil || size <= max(l.held, l.step) {
		return true
	}
	covered := max(l.held, l.step)
	for covered < size {
		covered *= 2
	}
	covered = min(covered, l.maxLine)
	if err := l.memory.Hold(covered); err != nil {
		l.err = fmt.Errorf("hold memory for a long line: %w", err)
		return false
	}
	l.held = covered
	return true
}

// release drops the held line, delivered or not, and then tells the line
// memory, so the memory is free when another pass is let in.
func (l *Lines) release() {
	if l.held == 0 {
		return
	}
	l.held = 0
	l.current = nil
	l.line = nil
	l.memory.Release()
}

// cut drops a line left open by a damaged member. The fragment still takes
// a line number, so later line numbers match the lines that were written.
func (l *Lines) cut() {
	l.pendingCR = false
	defer l.release()
	if l.oversized {
		l.number++
		l.endReject()
		l.stats.Oversized++
		l.oversized = false
		l.line = l.line[:0]
		return
	}
	if len(l.line) == 0 {
		return
	}
	l.number++
	l.writeReject(l.line)
	l.endReject()
	l.stats.Cut++
	l.line = l.line[:0]
}

func (l *Lines) writeReject(chunk []byte) {
	if l.reject == nil || len(chunk) == 0 || l.err != nil {
		return
	}
	if _, err := l.reject.Write(chunk); err != nil {
		l.err = fmt.Errorf("write rejected line: %w", err)
	}
}

func (l *Lines) endReject() {
	if l.reject == nil || l.err != nil {
		return
	}
	if _, err := l.reject.Write([]byte{'\n'}); err != nil {
		l.err = fmt.Errorf("write rejected line: %w", err)
	}
}

// finish ends the pass. A line still open at a clean end of file is the
// last record, written without a trailing newline.
func (l *Lines) finish() bool {
	if l.done {
		return false
	}
	l.done = true
	if l.err != nil {
		return false
	}
	if l.oversized {
		l.endReject()
		l.stats.Oversized++
		l.oversized = false
	}
	sum, err := l.src.drain()
	if err != nil {
		l.err = err
		return false
	}
	l.sum = sum
	if len(l.line) > 0 {
		l.number++
		line := l.line
		l.line = nil
		l.current = line
		l.stats.Lines++
		return true
	}
	return false
}

// Line returns the line read by the last successful Next, without its line
// end. It is valid until the next call to Next.
func (l *Lines) Line() []byte { return l.current }

// Number returns the 1-based line number of the current line, counting
// blank lines.
func (l *Lines) Number() int { return l.number }

// Stats returns the running totals for this pass.
func (l *Lines) Stats() LineStats { return l.stats }

// Sum returns the SHA-256 of the whole file once Next has returned false
// without an error, and nil before that.
func (l *Lines) Sum() []byte {
	if !l.done || l.err != nil {
		return nil
	}
	if l.sum == nil {
		sum, err := l.src.drain()
		if err != nil {
			l.err = err
			return nil
		}
		l.sum = sum
	}
	return l.sum
}

// Err returns the I/O error that ended the pass, if any. Damaged gzip data
// is counted in Stats, not reported here.
func (l *Lines) Err() error { return l.err }

// Close releases the segment and any line still held.
func (l *Lines) Close() error {
	l.release()
	return l.src.close()
}

// OpenRegular opens path for reading if it is a regular file. It does not
// follow a final symlink and does not block on a pipe, and it checks the
// type of what it opened, so a file swapped after a scan is refused. Every
// read of a discovered segment opens it this way.
func OpenRegular(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|openNoFollow, 0)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() {
		file.Close()
		return nil, fmt.Errorf("%s is not a regular file", info.Name())
	}
	return file, nil
}

// errBreak reports that the decompressed stream jumped over damaged bytes,
// so a line open at that point is incomplete.
var errBreak = errors.New("gzip stream break")

// members decompresses a series of gzip members from a file and hashes every
// file byte exactly once, even when it rewinds to look for the next header.
type members struct {
	file  *hashedFile
	br    *bufio.Reader
	gz    gzip.Reader
	stats *LineStats

	started     bool
	inMember    bool
	memberStart int64
	eof         bool
}

func newMembers(file *os.File, stats *LineStats) *members {
	hf := &hashedFile{file: file, hash: sha256.New()}
	return &members{file: hf, br: bufio.NewReaderSize(hf, 64<<10), stats: stats}
}

func (m *members) consumed() int64 { return m.file.pos - int64(m.br.Buffered()) }

func (m *members) seek(offset int64) {
	m.file.pos = offset
	m.br.Reset(m.file)
}

// Read returns decompressed bytes. It returns errBreak, possibly with bytes,
// when it skipped damaged input.
func (m *members) Read(p []byte) (int, error) {
	for {
		if m.eof {
			return 0, io.EOF
		}
		if !m.inMember {
			start := m.consumed()
			if err := m.gz.Reset(m.br); err != nil {
				if errors.Is(err, io.EOF) {
					m.eof = true
					return 0, io.EOF
				}
				if ioErr := m.file.err; ioErr != nil {
					return 0, ioErr
				}
				return 0, m.resync(start)
			}
			m.started = true
			// Members are walked one at a time so a damaged one can be
			// skipped without losing the members after it.
			m.gz.Multistream(false)
			m.memberStart = start
			m.inMember = true
		}
		n, err := m.gz.Read(p)
		if errors.Is(err, io.EOF) {
			m.inMember = false
			if n > 0 {
				return n, nil
			}
			continue
		}
		if err != nil {
			if errors.Is(err, gzip.ErrChecksum) {
				// The member decoded, but its content is in doubt. A cut
				// trailer also ends here, with the next member's first bytes
				// read as the trailer, so the next header is searched for
				// from the member start like any other damage.
				m.stats.ChecksumFailures++
			}
			if ioErr := m.file.err; ioErr != nil {
				return n, ioErr
			}
			m.inMember = false
			return n, m.resync(m.memberStart)
		}
		return n, nil
	}
}

// resync looks for the next gzip header after the damaged member that starts
// at from, counting every skipped byte as lost.
func (m *members) resync(from int64) error {
	m.seek(from + 1)
	for {
		head, err := m.br.Peek(3)
		if len(head) == 3 && head[0] == 0x1f && head[1] == 0x8b && head[2] == 0x08 {
			m.stats.LostBytes += m.consumed() - from
			m.stats.Resyncs++
			return errBreak
		}
		if err != nil {
			if ioErr := m.file.err; ioErr != nil {
				return ioErr
			}
			if _, discardErr := m.br.Discard(len(head)); discardErr != nil {
				return discardErr
			}
			m.stats.LostBytes += m.consumed() - from
			m.eof = true
			return errBreak
		}
		if _, err := m.br.Discard(1); err != nil {
			return err
		}
	}
}

// drain hashes whatever the decompressor did not read and returns the file
// hash.
func (m *members) drain() ([]byte, error) {
	m.file.pos = m.file.hashed
	if _, err := io.Copy(io.Discard, m.file); err != nil {
		return nil, fmt.Errorf("hash segment: %w", err)
	}
	return m.file.hash.Sum(nil), nil
}

func (m *members) close() error {
	if m.started {
		// Closing a gzip reader only releases its decompressor; it cannot
		// fail in a way that matters for a file opened read-only.
		_ = m.gz.Close()
	}
	if err := m.file.file.Close(); err != nil {
		return fmt.Errorf("close segment: %w", err)
	}
	return nil
}

// hashedFile reads a file at a movable offset and hashes each byte the first
// time any read reaches it. Moving back never rehashes; reads only move
// forward from there, so the hashed prefix stays contiguous.
type hashedFile struct {
	file   *os.File
	pos    int64
	hashed int64
	hash   hash.Hash
	err    error
}

func (h *hashedFile) Read(p []byte) (int, error) {
	n, err := h.file.ReadAt(p, h.pos)
	if n > 0 {
		end := h.pos + int64(n)
		if end > h.hashed {
			h.hash.Write(p[h.hashed-h.pos : n])
			h.hashed = end
		}
		h.pos = end
	}
	if errors.Is(err, io.EOF) {
		if n > 0 {
			return n, nil
		}
		return 0, io.EOF
	}
	if err != nil {
		h.err = err
	}
	return n, err
}
