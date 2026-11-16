// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package uploadertest_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform/suppress"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

type countingWriter struct {
	records    int
	suppressed int
	redacted   int
}

func (w *countingWriter) Write(r transform.Record) error {
	w.records++
	if r.Suppression.Action == suppress.StripPayload {
		w.suppressed++
	}
	w.redacted += r.RedactedParts
	return nil
}
func (w *countingWriter) Close() error { return nil }
func (w *countingWriter) Abort() error { return nil }

func smallProfile() uploadertest.LoadProfile {
	p := uploadertest.DefaultLoadProfile()
	p.Uncompressed = 4 << 20
	p.MediaShare = 0.2
	return p
}

func TestLoadSegmentIsReadableDynamoTraffic(t *testing.T) {
	p := smallProfile()
	path := filepath.Join(t.TempDir(), "load.jsonl.gz")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := uploadertest.WriteLoadSegment(file, p)
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("WriteLoadSegment() error = %v", err)
	}
	if stats.Uncompressed < p.Uncompressed {
		t.Fatalf("Uncompressed = %d, want at least %d", stats.Uncompressed, p.Uncompressed)
	}

	cfg := transform.DefaultConfig()
	cfg.Suppression = suppress.FromIDs(p.Suppressed(), p.NCAHeader)
	preparer, err := transform.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	w := &countingWriter{}
	result, err := preparer.Prepare(context.Background(), path, w, nil)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if result.Received != stats.Lines || result.Bad() != 0 || result.UnsupportedVersion != 0 {
		t.Fatalf("Prepare() = %d received, %d bad, %d unsupported; want %d lines, all readable", result.Received, result.Bad(), result.UnsupportedVersion, stats.Lines)
	}
	if w.suppressed != stats.Suppressed || stats.Suppressed == 0 {
		t.Fatalf("suppressed %d records, want %d (and more than none)", w.suppressed, stats.Suppressed)
	}
	if w.redacted != stats.MediaParts || stats.MediaParts == 0 {
		t.Fatalf("redacted %d media parts, want %d (and more than none)", w.redacted, stats.MediaParts)
	}
	for _, kind := range []string{"request_payload", "request_end", "tool_start", "tool_end"} {
		if result.ReceivedByType[kind] == 0 {
			t.Fatalf("ReceivedByType = %v, want %s records", result.ReceivedByType, kind)
		}
	}
}

func TestLoadSegmentIsReproducible(t *testing.T) {
	p := smallProfile()
	p.Uncompressed = 256 << 10
	var first, second bytes.Buffer
	if _, err := uploadertest.WriteLoadSegment(&first, p); err != nil {
		t.Fatal(err)
	}
	if _, err := uploadertest.WriteLoadSegment(&second, p); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Bytes(), second.Bytes()) {
		t.Fatal("two segments from one profile differ")
	}
	p.Seed++
	var other bytes.Buffer
	if _, err := uploadertest.WriteLoadSegment(&other, p); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first.Bytes(), other.Bytes()) {
		t.Fatal("segments from two seeds are equal")
	}
}

func TestLoadProfileRejectsBadShapes(t *testing.T) {
	tests := []struct {
		name   string
		change func(*uploadertest.LoadProfile)
	}{
		{"no size", func(p *uploadertest.LoadProfile) { p.Uncompressed = 0 }},
		{"no lines per member", func(p *uploadertest.LoadProfile) { p.LinesPerMember = 0 }},
		{"shares above one", func(p *uploadertest.LoadProfile) { p.AgenticShare, p.MediaShare = 0.7, 0.4 }},
		{"suppressed beyond the ids", func(p *uploadertest.LoadProfile) { p.SuppressedIDs = len(p.NCAIDs) + 1 }},
		{"no header", func(p *uploadertest.LoadProfile) { p.NCAHeader = "" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := smallProfile()
			tt.change(&p)
			if _, err := uploadertest.WriteLoadSegment(&bytes.Buffer{}, p); err == nil {
				t.Fatal("WriteLoadSegment() error = nil")
			}
		})
	}
}
