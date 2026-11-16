// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/format"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/journal"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/segment"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform"
)

// preparePool reads and writes up to workers segments at once, and records
// each result in the journal in the order the segments were started, which
// is their order on the spool. With one worker it records each segment
// before it looks at the next, as a single loop would.
type preparePool struct {
	u       *Uploader
	workers int
	// window holds the started segments whose result is not recorded yet,
	// oldest first.
	window []*prepareJob
	errs   []error
}

// prepareJob is one segment being prepared. The fields after done are
// written by the job's goroutine and read only after done is closed.
type prepareJob struct {
	item segment.Segment
	seg  journal.Segment
	span trace.Span
	done chan struct{}

	writer  format.Writer
	rejects *transform.RejectFile
	result  transform.Result
	elapsed time.Duration
	err     error
}

func (u *Uploader) newPreparePool() *preparePool {
	return &preparePool{u: u, workers: max(u.cfg.Prepare.Workers, 1)}
}

// inFlight returns how many started segments are not recorded yet and their
// total size.
func (p *preparePool) inFlight() (int, int64) {
	var bytes int64
	for _, job := range p.window {
		bytes += job.item.Size
	}
	return len(p.window), bytes
}

// makeRoom records results, oldest first, until at most limit segments are
// in flight, waiting for each.
func (p *preparePool) makeRoom(limit int) {
	for len(p.window) > max(limit, 0) {
		p.recordOldest()
	}
}

// recordReady records the results that are done at the head of the window.
func (p *preparePool) recordReady() {
	for len(p.window) > 0 {
		select {
		case <-p.window[0].done:
			p.recordOldest()
		default:
			return
		}
	}
}

// finish waits for every started segment and records it, and returns the
// errors seen since the pool started.
func (p *preparePool) finish() []error {
	p.makeRoom(0)
	return p.errs
}

func (p *preparePool) recordOldest() {
	job := p.window[0]
	<-job.done
	p.window = p.window[1:]
	defer job.span.End()
	if err := p.u.finishPrepare(job); err != nil {
		failSpan(job.span, "error", "")
		slog.Error("prepare request trace segment", "segment", job.item.Index, "error", safeError(err))
		p.errs = append(p.errs, err)
	}
}

// start prepares item on its own goroutine. The caller has made room. The
// pool ends span once it records the result.
func (p *preparePool) start(ctx context.Context, span trace.Span, item segment.Segment, seg journal.Segment) {
	job := &prepareJob{item: item, seg: seg, span: span, done: make(chan struct{})}
	p.window = append(p.window, job)
	go func() {
		defer close(job.done)
		p.u.runPrepare(ctx, job)
	}()
}

// runPrepare reads the segment into the backend's transport format. It
// writes nothing to the journal, so any number can run at once.
func (u *Uploader) runPrepare(ctx context.Context, job *prepareJob) {
	started := time.Now()
	defer func() { job.elapsed = time.Since(started) }()
	writer, err := u.factory.NewWriter(format.Target{
		Segment:      job.item,
		SegmentID:    job.seg.ID,
		Dir:          u.store.TransportDir(),
		Destinations: u.caps.AllDestinations(),
	})
	if err != nil {
		job.err = fmt.Errorf("open format writer: %w", err)
		return
	}
	job.writer = writer
	job.rejects = transform.NewRejectFile(u.quarantine.RejectsDir())
	job.result, err = u.preparer.Prepare(ctx, job.item.Path, writer, job.rejects)
	if err != nil {
		job.err = errors.Join(fmt.Errorf("prepare segment: %w", err), job.rejects.Discard())
	}
}
