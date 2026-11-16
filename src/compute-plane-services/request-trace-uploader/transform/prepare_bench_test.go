// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package transform_test

import (
	"context"
	"os"
	"runtime"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/format"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/journal"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/segment"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform/suppress"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

// BenchmarkPrepare reads one 256 MiB segment, suppresses and redacts it, and
// writes the gzip JSON-lines transport: the prepare pass on its own. Set
// RTU_BENCH_DIR to keep the generated segment between runs.
func BenchmarkPrepare(b *testing.B) {
	profiles := map[string]func(*uploadertest.LoadProfile){
		"mixed": func(*uploadertest.LoadProfile) {},
		"text":  func(p *uploadertest.LoadProfile) { p.MediaShare = 0 },
	}
	for _, name := range []string{"mixed", "text"} {
		b.Run(name, func(b *testing.B) {
			p := uploadertest.DefaultLoadProfile()
			profiles[name](&p)
			path, stats := uploadertest.LoadSegmentFile(b, p)
			cfg := transform.DefaultConfig()
			cfg.Suppression = suppress.FromIDs(p.Suppressed(), p.NCAHeader)
			preparer, err := transform.New(cfg)
			if err != nil {
				b.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil {
				b.Fatal(err)
			}
			dir := b.TempDir()
			target := format.Target{
				Segment:      segment.Segment{Path: path, Prefix: "request-trace", Index: 1, Size: info.Size()},
				SegmentID:    journal.SegmentID("request-trace", 1, info.Size(), "bench"),
				Dir:          dir,
				Destinations: []backend.Destination{""},
			}
			b.SetBytes(stats.Uncompressed)
			runtime.GC()
			b.ResetTimer()
			usage := uploadertest.StartUsage()
			for b.Loop() {
				w, err := format.GzipJSONL{}.NewWriter(target)
				if err != nil {
					b.Fatal(err)
				}
				result, err := preparer.Prepare(context.Background(), path, w, nil)
				if err != nil {
					b.Fatal(err)
				}
				if result.Written == 0 {
					b.Fatal("prepared no records")
				}
				for _, out := range w.Outputs() {
					if err := os.Remove(out.Path); err != nil {
						b.Fatal(err)
					}
				}
			}
			usage.Stop().Report(b, stats.Uncompressed)
		})
	}
}
