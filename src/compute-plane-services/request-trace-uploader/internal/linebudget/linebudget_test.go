// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package linebudget_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/linebudget"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/record"
)

// A line of size bytes takes Factor times that in memory, so these sizes
// are in units of the budget divided by Factor.
const unit = 100 / linebudget.Factor

var _ record.LineMemory = (*linebudget.Lease)(nil)

// testBudget counts the holds that wait and signals each one, so a test
// knows a hold is waiting before it frees room.
type testBudget struct {
	*linebudget.Budget
	waits   atomic.Int64
	waiting chan struct{}
}

func newBudget(t *testing.T) *testBudget {
	t.Helper()
	tb := &testBudget{waiting: make(chan struct{}, 16)}
	b, err := linebudget.New(100, linebudget.Options{OnWait: func() {
		tb.waits.Add(1)
		select {
		case tb.waiting <- struct{}{}:
		default:
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	tb.Budget = b
	return tb
}

// holdAsync starts Hold and returns a channel that receives its result.
func holdAsync(lease *linebudget.Lease, size int64) <-chan error {
	done := make(chan error, 1)
	go func() { done <- lease.Hold(size) }()
	return done
}

// requireWaiting returns once the hold behind done has started to wait.
func (b *testBudget) requireWaiting(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case <-b.waiting:
	case err := <-done:
		t.Fatalf("Hold() returned %v, want it to wait", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Hold() neither waited nor returned")
	}
}

func requireReturns(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Hold() still waiting")
		return nil
	}
}

func TestNewRejectsABudgetBelowOneByte(t *testing.T) {
	for _, limit := range []int64{0, -1} {
		if _, err := linebudget.New(limit, linebudget.Options{}); err == nil {
			t.Errorf("New(%d) error = nil", limit)
		}
	}
}

func TestHoldsWithinTheBudgetDoNotWait(t *testing.T) {
	b := newBudget(t)
	ctx := context.Background()
	first, second := b.Lease(ctx), b.Lease(ctx)
	if err := requireReturns(t, holdAsync(first, unit/2)); err != nil {
		t.Fatal(err)
	}
	if err := requireReturns(t, holdAsync(second, unit/2)); err != nil {
		t.Fatal(err)
	}
	if b.Held() != 2*(unit/2)*linebudget.Factor {
		t.Fatalf("Held() = %d, want both lines", b.Held())
	}
	if b.waits.Load() != 0 {
		t.Fatalf("waits = %d, want none", b.waits.Load())
	}
}

func TestAHoldWaitsUntilTheBudgetHasRoom(t *testing.T) {
	b := newBudget(t)
	ctx := context.Background()
	first, second := b.Lease(ctx), b.Lease(ctx)
	if err := first.Hold(unit / 2); err != nil {
		t.Fatal(err)
	}
	done := holdAsync(second, unit*3/4)
	b.requireWaiting(t, done)
	first.Release()
	if err := requireReturns(t, done); err != nil {
		t.Fatal(err)
	}
	if b.waits.Load() != 1 {
		t.Fatalf("waits = %d, want one", b.waits.Load())
	}
	second.Release()
	if b.Held() != 0 {
		t.Fatalf("Held() = %d after every release, want 0", b.Held())
	}
}

// The line that started holding first may grow past the budget, so two
// growing lines can never wait on each other, and a lone line larger than
// the whole budget still gets read.
func TestTheOldestHolderNeverWaits(t *testing.T) {
	b := newBudget(t)
	ctx := context.Background()
	older, younger := b.Lease(ctx), b.Lease(ctx)
	if err := older.Hold(unit / 2); err != nil {
		t.Fatal(err)
	}
	if err := younger.Hold(unit / 2); err != nil {
		t.Fatal(err)
	}
	if err := requireReturns(t, holdAsync(older, unit*3)); err != nil {
		t.Fatal(err)
	}
	done := holdAsync(younger, unit)
	b.requireWaiting(t, done)
	older.Release()
	if err := requireReturns(t, done); err != nil {
		t.Fatal(err)
	}
	younger.Release()

	lone := b.Lease(ctx)
	if err := requireReturns(t, holdAsync(lone, unit*50)); err != nil {
		t.Fatal(err)
	}
	lone.Release()
}

func TestAWaitingHoldEndsWithItsContext(t *testing.T) {
	b := newBudget(t)
	first := b.Lease(context.Background())
	if err := first.Hold(unit); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	waiting := b.Lease(ctx)
	done := holdAsync(waiting, unit/2)
	b.requireWaiting(t, done)
	cancel()
	if err := requireReturns(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("Hold() = %v, want the context error", err)
	}
	// The canceled lease holds nothing and is not in line, so the next
	// lease is the oldest once the first lets go.
	first.Release()
	next := b.Lease(context.Background())
	if err := requireReturns(t, holdAsync(next, unit*2)); err != nil {
		t.Fatal(err)
	}
	next.Release()
	if b.Held() != 0 {
		t.Fatalf("Held() = %d, want 0", b.Held())
	}
}

func TestReleaseWithoutAHoldIsHarmless(t *testing.T) {
	b := newBudget(t)
	lease := b.Lease(context.Background())
	lease.Release()
	if err := lease.Hold(unit / 4); err != nil {
		t.Fatal(err)
	}
	lease.Release()
	lease.Release()
	if b.Held() != 0 {
		t.Fatalf("Held() = %d, want 0", b.Held())
	}
}

func TestConcurrentLeasesNeverDeadlockAndReturnEverything(t *testing.T) {
	b := newBudget(t)
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(worker), 7))
			lease := b.Lease(context.Background())
			for range 200 {
				size := int64(0)
				for range 1 + rng.IntN(4) {
					size += 1 + rng.Int64N(unit)
					if err := lease.Hold(size); err != nil {
						t.Error(err)
						return
					}
				}
				lease.Release()
			}
		}()
	}
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(20 * time.Second):
		t.Fatal("leases deadlocked")
	}
	if b.Held() != 0 {
		t.Fatalf("Held() = %d, want 0", b.Held())
	}
}
