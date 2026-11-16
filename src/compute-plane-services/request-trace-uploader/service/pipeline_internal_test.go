// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend/backendtest"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/retry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/journal"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

// newInternalUploader builds an uploader on spool, with fast fault waits,
// that the test closes when it ends.
func newInternalUploader(t *testing.T, spool *uploadertest.Spool, client backend.Client) *Uploader {
	t.Helper()
	secrets := filepath.Join(t.TempDir(), "secrets.json")
	if err := os.WriteFile(secrets, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{config.EnvSourceDir: spool.Dir, config.EnvBackend: string(config.BackendDebug), config.EnvSecretsFile: secrets}
	cfg, err := config.LoadStrict(func(name string) (string, bool) {
		value, ok := env[name]
		return value, ok
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	faults := retry.DefaultConfig()
	faults.Transient = retry.Backoff{Base: 10 * time.Millisecond, Max: 50 * time.Millisecond}
	faults.ResumeJitter = 0
	u, err := NewUploader(cfg, client, Options{Faults: &faults})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = u.Close() })
	return u
}

// A batch that was recorded but never sent, and whose release the journal
// refused, is released on a later submit round once the journal takes
// writes again. Its segment is then sent without waiting for a restart,
// which would read the batch as a submit that may have landed.
func TestAnUnsentBatchWhoseReleaseFailedIsReleasedOnALaterRound(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	closed := spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("a", 1)).Bytes())
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("active", 2)).Bytes())
	fake := backendtest.NewFake()
	u := newInternalUploader(t, spool, fake)
	ctx := context.Background()
	if err := u.start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := u.intake(ctx, false); err != nil {
		t.Fatal(err)
	}
	segments := u.store.Segments()
	if len(segments) != 1 || segments[0].State != journal.StatePrepared {
		t.Fatalf("segments = %+v, want one prepared", segments)
	}
	seg := segments[0]
	var lane string
	var member journal.Member
	for name, dest := range seg.Destinations {
		lane, member = name, journal.Member{SegmentID: seg.ID, Path: dest.Transport.Path}
	}
	if err := u.store.CreateBatch(journal.Batch{ID: "unsent", Backend: u.name, Destination: lane, Members: []journal.Member{member}}); err != nil {
		t.Fatal(err)
	}

	batches := filepath.Join(u.store.StateDir(), "batches")
	if err := os.Chmod(batches, 0o500); err != nil {
		t.Fatal(err)
	}
	releaseErr := u.releaseUnsent("unsent")
	if err := os.Chmod(batches, 0o700); err != nil {
		t.Fatal(err)
	}
	if releaseErr == nil {
		t.Fatal("releaseUnsent() error = nil with the batch directory read-only, want the failed release reported")
	}

	for range 2 {
		_ = u.Pass(ctx)
	}
	if b, _ := u.store.Batch("unsent"); b.State != journal.StateReleased {
		t.Fatalf("unsent batch state = %s, want released", b.State)
	}
	if got := fake.SubmitCalls(); got != 1 {
		t.Fatalf("submits = %d, want the segment sent once in a new batch", got)
	}
	if _, err := os.Stat(closed); !os.IsNotExist(err) {
		t.Fatalf("source after its upload: %v, want removed", err)
	}
}

// Only a stranded batch's last poll is read, so an entry is dropped once its
// batch is no longer polled. A failed job leaves none behind.
func TestLastPollTimesAreDroppedOnceTheBatchIsNoLongerPolled(t *testing.T) {
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	closed := spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("a", 1)).Bytes())
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("active", 2)).Bytes())
	fake := backendtest.NewFake()
	fake.Script(backendtest.SubmitStep{Statuses: []backendtest.StatusStep{{
		Status: backend.StatusFailed,
		Err:    &backend.Fault{Class: backend.ClassRetryable, Scope: backend.ScopeSegment, Reason: backend.ReasonRejected},
	}}})
	u := newInternalUploader(t, spool, fake)
	ctx := context.Background()
	deadline := time.Now().Add(10 * time.Second)
	for {
		_ = u.Pass(ctx)
		if _, err := os.Stat(closed); os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the segment was never exported")
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = u.Pass(ctx)

	if got := fake.SubmitCalls(); got != 2 {
		t.Fatalf("submits = %d, want the failed job and its resend", got)
	}
	u.stateMu.Lock()
	defer u.stateMu.Unlock()
	if len(u.lastPolled) != 0 {
		t.Fatalf("last poll times kept for %d batches no longer polled, want none", len(u.lastPolled))
	}
}
