// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package record

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// memoryLog records what a Lines pass asked of its LineMemory, in order,
// with the lines it delivered in between.
type memoryLog struct {
	events  []string
	holds   []int64
	holding bool
	fail    error
}

func (m *memoryLog) Hold(size int64) error {
	if m.fail != nil {
		return m.fail
	}
	m.events = append(m.events, fmt.Sprintf("hold %d", size))
	m.holds = append(m.holds, size)
	m.holding = true
	return nil
}

func (m *memoryLog) Release() {
	m.events = append(m.events, "release")
	m.holding = false
}

func readWithMemory(t *testing.T, path string, opts LineOptions, m *memoryLog) []string {
	t.Helper()
	opts.Memory = m
	lines, err := OpenLines(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer lines.Close()
	var got []string
	for lines.Next() {
		m.events = append(m.events, fmt.Sprintf("line %d", len(lines.Line())))
		got = append(got, string(lines.Line()))
	}
	if err := lines.Err(); err != nil {
		t.Fatalf("Err() = %v", err)
	}
	return got
}

func TestLinesHoldMemoryBeforeALongLineGrowsAndReleaseItAfter(t *testing.T) {
	const step = 16 << 10
	long := noisy("long", 60_000) // about 200 KiB: several reads of the 64 KiB buffer
	path := writeRaw(t, member(t, `{"a":1}`, long, `{"b":2}`))
	m := &memoryLog{}

	got := readWithMemory(t, path, LineOptions{MemoryStep: step}, m)

	if len(got) != 3 || got[1] != long {
		t.Fatalf("lines = %d, want the three lines unchanged", len(got))
	}
	if len(m.holds) < 2 {
		t.Fatalf("events = %v, want several holds while the long line grew past %d", m.events, step)
	}
	for i, size := range m.holds {
		if size <= step || (i > 0 && size <= m.holds[i-1]) {
			t.Fatalf("holds = %v, want sizes above the step, each larger than the last", m.holds)
		}
	}
	if last := m.holds[len(m.holds)-1]; last < int64(len(long)) {
		t.Fatalf("last hold %d is below the line's %d bytes", last, len(long))
	}
	want := fmt.Sprintf("line %d", len(`{"a":1}`))
	if m.events[0] != want {
		t.Fatalf("events = %v, want the short first line delivered before any hold", m.events)
	}
	tail := m.events[len(m.events)-3:]
	if tail[0] != fmt.Sprintf("line %d", len(long)) || tail[1] != "release" || tail[2] != fmt.Sprintf("line %d", len(`{"b":2}`)) {
		t.Fatalf("events end %v, want the long line, then release, then the next line", tail)
	}
}

func TestLinesReleaseALongLineHeldAtTheEnd(t *testing.T) {
	long := strings.Repeat("x", 100<<10)
	tests := []struct {
		name string
		stop func(*Lines)
	}{
		{"pass read to the end", func(l *Lines) {
			for l.Next() {
			}
		}},
		{"closed after the line", func(l *Lines) {}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeRaw(t, member(t, `{"a":1}`, `"`+long+`"`))
			m := &memoryLog{}
			lines, err := OpenLines(path, LineOptions{MemoryStep: 16 << 10, Memory: m})
			if err != nil {
				t.Fatal(err)
			}
			for lines.Next() && len(lines.Line()) < len(long) {
			}
			if !m.holding {
				t.Fatalf("events = %v, want the long line held while it is current", m.events)
			}
			tt.stop(lines)
			if err := lines.Close(); err != nil {
				t.Fatal(err)
			}
			if m.holding {
				t.Fatalf("events = %v, want the long line released", m.events)
			}
		})
	}
}

func TestLinesReleaseARejectedLongLine(t *testing.T) {
	long := noisy("long", 60_000)
	path := writeRaw(t, member(t, long, `{"b":2}`))
	m := &memoryLog{}
	var rejected strings.Builder

	got := readWithMemory(t, path, LineOptions{MemoryStep: 16 << 10, MaxLineBytes: 100 << 10, Reject: &rejected}, m)

	if len(got) != 1 || got[0] != `{"b":2}` {
		t.Fatalf("lines = %q, want only the short line", got)
	}
	if rejected.Len() != len(long)+1 {
		t.Fatalf("rejected %d bytes, want the long line and its newline", rejected.Len())
	}
	if len(m.holds) == 0 || m.events[len(m.events)-2] != "release" {
		t.Fatalf("events = %v, want the line held while it grew and released before the next line", m.events)
	}
	for _, size := range m.holds {
		if size > 100<<10 {
			t.Fatalf("holds = %v, want none above the line limit", m.holds)
		}
	}
}

func TestLinesStopWhenMemoryCannotBeHeld(t *testing.T) {
	refused := errors.New("no memory for you")
	path := writeRaw(t, member(t, `{"a":1}`, strings.Repeat("y", 100<<10), `{"b":2}`))
	m := &memoryLog{fail: refused}
	lines, err := OpenLines(path, LineOptions{MemoryStep: 16 << 10, Memory: m})
	if err != nil {
		t.Fatal(err)
	}
	defer lines.Close()
	var got []string
	for lines.Next() {
		got = append(got, string(lines.Line()))
	}
	if len(got) != 1 {
		t.Fatalf("lines = %d, want only the line before the refused one", len(got))
	}
	if !errors.Is(lines.Err(), refused) {
		t.Fatalf("Err() = %v, want the refusal", lines.Err())
	}
	if lines.Sum() != nil {
		t.Fatal("Sum() is set after a pass that stopped early")
	}
}

func TestLinesAskNothingOfMemoryForShortLines(t *testing.T) {
	path := writeRaw(t, member(t, `{"a":1}`, noisy("mid", 3000), `{"b":2}`))
	m := &memoryLog{}
	readWithMemory(t, path, LineOptions{MemoryStep: 16 << 10}, m)
	for _, event := range m.events {
		if !strings.HasPrefix(event, "line ") {
			t.Fatalf("events = %v, want no hold or release below the step", m.events)
		}
	}
}

// releaseCheck runs check on every Release.
type releaseCheck struct {
	memoryLog
	check func()
}

func (r *releaseCheck) Release() {
	r.check()
	r.memoryLog.Release()
}

// When a line's memory is returned, the reader must no longer hold the line,
// or a waiting prepare would be let in while the memory is still in use.
func TestLinesDropALineBeforeReleasingItsMemory(t *testing.T) {
	const step = 16 << 10
	long := noisy("long", 60_000)
	cutLong := noisy("cut", 60_000)
	tests := []struct {
		name  string
		parts func(t *testing.T) [][]byte
		opts  LineOptions
	}{
		{"delivered line, at the next Next", func(t *testing.T) [][]byte {
			return [][]byte{member(t, long, `{"b":2}`)}
		}, LineOptions{}},
		{"rejected oversized line", func(t *testing.T) [][]byte {
			return [][]byte{member(t, long, `{"b":2}`)}
		}, LineOptions{MaxLineBytes: 100 << 10}},
		{"line cut by a damaged member", func(t *testing.T) [][]byte {
			// The line compresses about 23 times, so only a late cut
			// leaves enough of it to be held.
			damaged := member(t, cutLong)
			return [][]byte{damaged[:len(damaged)*97/100], member(t, `{"b":2}`)}
		}, LineOptions{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeRaw(t, tt.parts(t)...)
			var lines *Lines
			m := &releaseCheck{}
			m.check = func() {
				if len(lines.current) > step || cap(lines.line) > step {
					t.Errorf("released while the reader still holds %d bytes of line and %d of buffer", len(lines.current), cap(lines.line))
				}
			}
			opts := tt.opts
			opts.MemoryStep = step
			opts.Memory = m
			var err error
			if lines, err = OpenLines(path, opts); err != nil {
				t.Fatal(err)
			}
			for lines.Next() {
			}
			if err := lines.Close(); err != nil {
				t.Fatal(err)
			}
			if len(m.holds) == 0 {
				t.Fatal("the long line was never held")
			}
		})
	}
}
