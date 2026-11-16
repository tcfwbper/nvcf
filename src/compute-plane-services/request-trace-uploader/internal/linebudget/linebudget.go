// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package linebudget shares a memory budget among concurrent prepares for
// the long lines they hold. Short lines cost each prepare a small, fixed
// amount and are not counted; a line past the reader's memory step is held
// against the budget at Factor times its size, and a prepare whose line
// does not fit waits until another prepare lets go.
//
// The line that started holding first never waits, even past the budget.
// So two growing lines can never wait on each other, and a lone line
// larger than the whole budget is still read, as it would be with one
// prepare. Memory is then bounded by the budget plus that one line.
package linebudget

import (
	"context"
	"errors"
	"sync"
)

// Factor is the memory one held line takes per byte of the line: the line
// itself, its parsed form, and the copies redaction and the format writer
// make. Measured at about five times the line for lines of 133 MiB and
// 507 MiB.
const Factor = 5

// Options holds the budget's collaborators. Zero values are safe.
type Options struct {
	// OnWait is called once for every hold that has to wait.
	OnWait func()
}

// Budget is the memory every prepare's long lines share. It is safe for
// concurrent use.
type Budget struct {
	limit  int64
	onWait func()

	mu   sync.Mutex
	used int64
	next uint64
	// active holds the tickets of leases that hold or wait, so the oldest
	// can be found.
	active map[uint64]struct{}
	// wake is closed and replaced whenever room is made.
	wake chan struct{}
}

// New builds a budget of limit bytes.
func New(limit int64, opts Options) (*Budget, error) {
	if limit < 1 {
		return nil, errors.New("line memory budget must be at least one byte")
	}
	return &Budget{
		limit:  limit,
		onWait: opts.OnWait,
		active: map[uint64]struct{}{},
		wake:   make(chan struct{}),
	}, nil
}

// Lease starts the accounting for one prepare. A wait ends when ctx does.
func (b *Budget) Lease(ctx context.Context) *Lease {
	return &Lease{budget: b, ctx: ctx}
}

// Held returns the bytes held now.
func (b *Budget) Held() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

// oldestLocked returns the lowest active ticket.
func (b *Budget) oldestLocked() uint64 {
	var oldest uint64
	for ticket := range b.active {
		if oldest == 0 || ticket < oldest {
			oldest = ticket
		}
	}
	return oldest
}

func (b *Budget) wakeLocked() {
	close(b.wake)
	b.wake = make(chan struct{})
}

// Lease is one prepare's share of the budget. It implements
// record.LineMemory, and is used by one goroutine at a time.
type Lease struct {
	budget *Budget
	ctx    context.Context
	ticket uint64
	held   int64
}

// Hold reserves memory for the current line to reach size bytes, waiting
// while the budget has no room and this line is not the oldest one held. A
// wait cut short by the context returns its error; what the lease already
// held stays held until Release.
func (l *Lease) Hold(size int64) error {
	b := l.budget
	want := size * Factor
	b.mu.Lock()
	if l.ticket == 0 {
		b.next++
		l.ticket = b.next
		b.active[l.ticket] = struct{}{}
	}
	waited := false
	for {
		extra := want - l.held
		if extra <= 0 || b.used+extra <= b.limit || b.oldestLocked() == l.ticket {
			if extra > 0 {
				b.used += extra
				l.held = want
			}
			b.mu.Unlock()
			return nil
		}
		wake := b.wake
		b.mu.Unlock()
		if !waited {
			waited = true
			if b.onWait != nil {
				b.onWait()
			}
		}
		select {
		case <-wake:
		case <-l.ctx.Done():
			b.mu.Lock()
			if l.held == 0 {
				// Nothing is held, so nothing will call Release: leave the
				// line, and let the next lease become the oldest.
				l.leaveLocked()
			}
			b.mu.Unlock()
			return l.ctx.Err()
		}
		b.mu.Lock()
	}
}

// Release returns everything the lease holds. It is safe to call when
// nothing is held.
func (l *Lease) Release() {
	b := l.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if l.ticket == 0 {
		return
	}
	b.used -= l.held
	l.held = 0
	l.leaveLocked()
}

func (l *Lease) leaveLocked() {
	delete(l.budget.active, l.ticket)
	l.ticket = 0
	l.budget.wakeLocked()
}
