// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package transform

import (
	"context"
	"strings"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/record"
)

type ctxKey struct{}

type lineMemory struct {
	ctx      context.Context
	holds    []int64
	releases int
}

func (m *lineMemory) Hold(size int64) error { m.holds = append(m.holds, size); return nil }
func (m *lineMemory) Release()              { m.releases++ }

func TestPrepareHoldsLineMemoryForLongLines(t *testing.T) {
	long := `{"schema":"dynamo.request.trace.v1","event_type":"request_end","event_time_unix_ms":1,"request":{"request_id":"` +
		strings.Repeat("a", record.DefaultMemoryStep+1) + `"}}`
	path := segmentFile(t, textLine, long, textLine)
	var leases []*lineMemory
	preparer, err := NewWithOptions(DefaultConfig(), Options{LineMemory: func(ctx context.Context) record.LineMemory {
		m := &lineMemory{ctx: ctx}
		leases = append(leases, m)
		return m
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), ctxKey{}, "prepare")
	writer := &memoryWriter{}
	result, err := preparer.Prepare(ctx, path, writer, nil)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if result.Written != 3 {
		t.Fatalf("Written = %d, want every line", result.Written)
	}
	if len(leases) != 1 || leases[0].ctx.Value(ctxKey{}) != "prepare" {
		t.Fatalf("leases = %d, want one, for the Prepare call's context", len(leases))
	}
	m := leases[0]
	if len(m.holds) == 0 || m.holds[len(m.holds)-1] < int64(len(long)) || m.releases != 1 {
		t.Fatalf("holds = %v releases = %d, want the long line held and released once", m.holds, m.releases)
	}
}

func TestNewKeepsPreparingWithoutLineMemory(t *testing.T) {
	preparer, err := New(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := preparer.Prepare(context.Background(), segmentFile(t, textLine), &memoryWriter{}, nil); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
}
