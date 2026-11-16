// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/diskguard"
)

// backlogSubmits coordinates intake with the full-batch submits that run
// beside it. Intake reports each segment it records prepared; the submitter
// runs a round per report and reports each batch it records, so intake can
// wait at the prepare-ahead bound for the next batch instead of ending the
// pass.
type backlogSubmits struct {
	// wake holds one pending request for a round. Only intake sends.
	wake chan struct{}

	mu sync.Mutex
	// changed is closed and replaced when a round starts or ends, or a batch
	// is recorded.
	changed chan struct{}
	// prepared counts segments intake recorded prepared; seen is that count
	// when the latest round started.
	prepared, seen uint64
	// batches counts the batches recorded.
	batches uint64
	running bool
}

func newBacklogSubmits() *backlogSubmits {
	return &backlogSubmits{wake: make(chan struct{}, 1), changed: make(chan struct{})}
}

func (s *backlogSubmits) notePrepared() {
	s.mu.Lock()
	s.prepared++
	s.mu.Unlock()
	s.wakeUp()
}

func (s *backlogSubmits) wakeUp() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// askForRound asks the submitter for a round while intake waits at the
// prepare-ahead bound, only when a prepared segment is not yet seen by a
// round. A round that saw them all needs no other, and asking anyway would
// queue rounds with nothing new and keep the submitter from settling.
func (s *backlogSubmits) askForRound() {
	s.mu.Lock()
	unseen := s.prepared > s.seen
	s.mu.Unlock()
	if unseen {
		s.wakeUp()
	}
}

func (s *backlogSubmits) broadcastLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *backlogSubmits) roundStarted() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = true
	s.seen = s.prepared
	s.broadcastLocked()
}

func (s *backlogSubmits) roundEnded() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = false
	s.broadcastLocked()
}

func (s *backlogSubmits) batched() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches++
	s.broadcastLocked()
}

// recorded returns how many batches have been recorded.
func (s *backlogSubmits) recorded() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.batches
}

// settled reports that no round runs or is asked for and the latest round
// saw every prepared segment, so no submit will record another batch until
// intake prepares more. It also returns the channel the next change closes
// and how many batches have been recorded.
func (s *backlogSubmits) settled() (<-chan struct{}, bool, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.changed, !s.running && s.seen >= s.prepared && len(s.wake) == 0, s.batches
}

// intakeSubmitting runs intake and, beside it, submits each batch that
// fills up from what intake prepares, so a backlog is sent while it is
// prepared instead of after. A batch that is not full waits for the end of
// the pass, as before. The caller holds the intake lock; submits take the
// submit lock, so they never run beside another submit.
func (u *Uploader) intakeSubmitting(ctx context.Context) error {
	s := newBacklogSubmits()
	u.backlog.Store(s)
	submitted := make(chan error, 1)
	go func() {
		var errs []error
		for range s.wake {
			s.roundStarted()
			errs = append(errs, u.locked(&u.submitMu, func() error { return u.submitFull(ctx) }))
			s.roundEnded()
			u.point(PointBacklogRoundEnded)
		}
		submitted <- errors.Join(errs...)
	}()
	err := u.intake(ctx, false)
	close(s.wake)
	err = errors.Join(err, <-submitted)
	u.backlog.Store(nil)
	return err
}

// roomToPrepare reports whether one more segment of size bytes may be
// prepared. At the prepare-ahead bound, while full batches are submitted
// beside intake, it waits for the next batch to be recorded instead of
// ending the pass. It gives up when the submitter has seen every prepared
// segment and recorded no batch since the last check, and when the
// uploader drains. It logs a pause.
func (u *Uploader) roomToPrepare(ctx context.Context, size int64, pool *preparePool) (bool, error) {
	backlog := u.backlog.Load()
	for {
		// Finished prepares count toward prepare-ahead and can fill the next
		// batch; segments still being prepared count toward free space.
		pool.recordReady()
		var before uint64
		if backlog != nil {
			before = backlog.recorded()
		}
		_, inFlightBytes := pool.inFlight()
		decision, err := u.allowPrepare(size + inFlightBytes)
		if err != nil || decision.Allowed {
			return decision.Allowed, err
		}
		waitable := decision.Reason == diskguard.PausePrepareAhead && backlog != nil && !u.draining.Load()
		var changed <-chan struct{}
		if waitable {
			u.point(PointPrepareAheadWait)
			var settled bool
			var after uint64
			changed, settled, after = backlog.settled()
			if settled && after != before {
				// A batch was recorded after the check above and may have
				// freed room, so check again.
				continue
			}
			waitable = !settled
		}
		if !waitable {
			slog.Warn("preparing paused to protect the spool; prepared work is still sent", "reason", decision.Reason)
			return false, nil
		}
		backlog.askForRound()
		select {
		case <-changed:
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(time.Second):
			// Look at draining again.
		}
	}
}
