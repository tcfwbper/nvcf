// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"fmt"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend/backendtest"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/batch"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

// While intake waits at the prepare-ahead bound, it asks the submitter for
// a round only when a prepared segment is not yet seen by one. A round that
// has seen every prepared segment needs no other: an extra one would find
// nothing new, and asking again for each would keep intake from seeing the
// submitter settle.
func TestIntakeAtTheBoundAsksForARoundOnlyForAnUnseenSegment(t *testing.T) {
	s := newBacklogSubmits()
	s.notePrepared()
	<-s.wake
	s.roundStarted()
	s.askForRound()
	if len(s.wake) != 0 {
		t.Fatal("intake asked for a round although the running round has seen every prepared segment")
	}
	s.roundEnded()
	s.notePrepared()
	<-s.wake
	s.askForRound()
	if len(s.wake) != 1 {
		t.Fatal("intake did not ask for a round for a prepared segment no round has seen")
	}
}

// A full-only round sends only lanes that fill up: members that waited past
// the batch wait still go together in one full batch, never one job each.
// Partial lanes wait for the flush at the end of the pass.
func TestAFullOnlyRoundNeverClosesALaneByAge(t *testing.T) {
	fake := backendtest.NewFake()
	fake.SetCapabilities(backend.Capabilities{
		AcceptedFormats: []backend.Format{backend.FormatGzipJSONL},
		Exports:         true,
		MaxBatchFiles:   4,
	})
	u := newInternalUploader(t, uploadertest.NewSpool(t, config.DefaultSegmentPrefix), fake)
	batcher, err := u.fullBatcher()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	old := now.Add(-10 * batch.DefaultMaxWait)
	for i := range 4 {
		member := batch.Member{
			SegmentID:   fmt.Sprintf("segment-%d", i),
			Destination: defaultLane,
			File:        batch.File{Path: fmt.Sprintf("transport-%d", i), Bytes: 100, Rows: 1},
			PreparedAt:  old,
		}
		closed, err := batcher.Add(member, now)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case i < 3 && len(closed) != 0:
			t.Fatalf("adding member %d closed %d batch(es) of %d member(s), want none before the lane is full", i, len(closed), len(closed[0].Members))
		case i == 3 && (len(closed) != 1 || len(closed[0].Members) != 4):
			t.Fatalf("adding the fourth member closed %d batch(es), want one full batch of 4", len(closed))
		}
	}
}
