// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/service"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

// benchBackend is an exporting backend that does not read what it is sent:
// backendtest.Fake reads every transport into memory to fingerprint it,
// which would dominate the peak memory a benchmark reports. Each submit
// waits as long as uploading its bytes at rate would take, and each job
// succeeds on its first poll.
type benchBackend struct {
	caps  backend.Capabilities
	rate  float64 // bytes per second; zero waits not at all
	start time.Time

	mu          sync.Mutex
	submits     int
	firstSubmit time.Duration
	jobs        int
}

func (f *benchBackend) Submit(ctx context.Context, request backend.SubmitRequest) (string, error) {
	var bytes int64
	for _, file := range request.Transports() {
		bytes += file.Bytes
	}
	f.mu.Lock()
	f.submits++
	if f.submits == 1 {
		f.firstSubmit = time.Since(f.start)
	}
	f.jobs++
	id := fmt.Sprintf("job-%06d", f.jobs)
	f.mu.Unlock()
	if f.rate > 0 {
		timer := time.NewTimer(time.Duration(float64(bytes) / f.rate * float64(time.Second)))
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-timer.C:
		}
	}
	return id, nil
}

func (f *benchBackend) Status(context.Context, string) (backend.Status, error) {
	return backend.StatusSucceeded, nil
}

func (f *benchBackend) Capabilities() backend.Capabilities { return f.caps }

func envInt(b *testing.B, name string, fallback int) int {
	b.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 0 {
		b.Fatalf("%s must be a whole number", name)
	}
	return n
}

// loadSegments returns n distinct generated 256 MiB segments, the
// uncompressed size of each, and the profile they share but for the seed.
func loadSegments(b *testing.B, n int) ([]string, []int64, uploadertest.LoadProfile) {
	b.Helper()
	var paths []string
	var sizes []int64
	p := uploadertest.DefaultLoadProfile()
	for i := range n {
		q := p
		q.Seed = uint64(i + 1)
		path, stats := uploadertest.LoadSegmentFile(b, q)
		paths = append(paths, path)
		sizes = append(sizes, stats.Uncompressed)
	}
	return paths, sizes, p
}

// BenchmarkFingerprint is the first pass over a segment: the hash that
// names it in the journal.
func BenchmarkFingerprint(b *testing.B) {
	paths, _, _ := loadSegments(b, 1)
	info, err := os.Stat(paths[0])
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(info.Size())
	usage := uploadertest.StartUsage()
	for b.Loop() {
		file, err := os.Open(paths[0])
		if err != nil {
			b.Fatal(err)
		}
		h := sha256.New()
		if _, err := io.Copy(h, file); err != nil {
			b.Fatal(err)
		}
		_ = file.Close()
	}
	usage.Stop().Report(b, info.Size())
}

// BenchmarkBacklog drains a backlog of 256 MiB segments through the whole
// pipeline: fingerprint, prepare, batch, submit, poll and clean up, one
// pass after another as Run does while each pass outlasts the scan
// interval. The backend takes 16 files or 1 GiB per job and uploads at
// RTU_BENCH_UPLOAD_MIBPS mebibytes of transport a second (0 for no wait).
// RTU_BENCH_BACKLOG sets the segment count (default 24, above the
// prepare-ahead bound of 16), and RTU_BENCH_DISTINCT how many distinct
// segments are generated and linked in turn (default 4).
func BenchmarkBacklog(b *testing.B) {
	count := envInt(b, "RTU_BENCH_BACKLOG", 24)
	distinct := min(envInt(b, "RTU_BENCH_DISTINCT", 4), count)
	if count > 0 && distinct < 1 {
		b.Fatal("RTU_BENCH_DISTINCT must be at least 1")
	}
	rate := float64(envInt(b, "RTU_BENCH_UPLOAD_MIBPS", 100)) * (1 << 20)
	sources, sizes, profile := loadSegments(b, distinct)
	var total int64
	for i := range count {
		total += sizes[i%distinct]
	}
	env := map[string]string{
		config.EnvSuppressNCAIDs:     strings.Join(profile.Suppressed(), ","),
		config.EnvSuppressNCAHeaders: profile.NCAHeader,
	}
	for name, value := range benchSettings() {
		env[name] = value
	}

	b.SetBytes(total)
	var firstSubmit, wall time.Duration
	// Usage covers only the timed drains, not the setup and shutdown
	// around them.
	var measured uploadertest.UsageReport
	runtime.GC()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		spool := uploadertest.NewSpool(b, config.DefaultSegmentPrefix)
		var closed []string
		for i := range count {
			path := filepath.Join(spool.Dir, fmt.Sprintf("%s.%06d.jsonl.gz", spool.Prefix, i))
			if err := linkOrCopy(sources[i%distinct], path); err != nil {
				b.Fatal(err)
			}
			closed = append(closed, path)
		}
		spool.WriteAt(count, uploadertest.NewSegment().Member(uploadertest.RequestEnd("active", 1)).Bytes())
		fake := &benchBackend{
			caps: backend.Capabilities{
				AcceptedFormats: []backend.Format{backend.FormatGzipJSONL},
				Exports:         true,
				MaxBatchFiles:   16,
				MaxBatchBytes:   1 << 30,
			},
			rate: rate,
		}
		settings := spoolBenchEnv(b, spool.Dir, env)
		cfg, _, err := config.Load(func(name string) (string, bool) {
			value, ok := settings[name]
			return value, ok
		})
		if err != nil {
			b.Fatal(err)
		}
		uploader, err := service.NewUploader(cfg, fake, service.Options{Faults: fastFaults(), Volume: roomyVolume(spool.Dir)})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		usage := uploadertest.StartUsage()
		fake.start = time.Now()
		for remaining(closed) > 0 {
			if err := uploader.Pass(context.Background()); err != nil {
				b.Fatalf("Pass() error = %v", err)
			}
		}
		wall += time.Since(fake.start)
		firstSubmit += fake.firstSubmit
		measured = measured.Add(usage.Stop())
		b.StopTimer()
		if err := uploader.Close(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	measured.Report(b, total)
	b.ReportMetric(firstSubmit.Seconds()/float64(b.N), "first-submit-s")
	b.ReportMetric(float64(total)/(1<<20)/(wall.Seconds()/float64(b.N)), "MiB/wall-s")
}

// benchSettings passes RTU_BENCH_SET_<setting> variables through as uploader
// settings, so a run can try a setting without a code change.
func benchSettings() map[string]string {
	out := map[string]string{}
	for _, entry := range os.Environ() {
		name, value, _ := strings.Cut(entry, "=")
		if setting, ok := strings.CutPrefix(name, "RTU_BENCH_SET_"); ok {
			out[setting] = value
		}
	}
	return out
}

func spoolBenchEnv(b *testing.B, dir string, extra map[string]string) map[string]string {
	b.Helper()
	secrets := filepath.Join(b.TempDir(), "secrets.json")
	if err := os.WriteFile(secrets, []byte("{}"), 0o600); err != nil {
		b.Fatal(err)
	}
	env := map[string]string{
		config.EnvSourceDir:   dir,
		config.EnvBackend:     "bench",
		config.EnvSecretsFile: secrets,
	}
	for name, value := range extra {
		env[name] = value
	}
	return env
}

// linkOrCopy links a cached segment into the spool, or copies it when the
// cache in RTU_BENCH_DIR is on another filesystem. The timer is stopped.
func linkOrCopy(src, dst string) error {
	if err := os.Link(src, dst); err == nil {
		return nil
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	if closeErr := out.Close(); err == nil {
		err = closeErr
	}
	return err
}

func remaining(paths []string) int {
	n := 0
	for _, path := range paths {
		if !gone(path) {
			n++
		}
	}
	return n
}
