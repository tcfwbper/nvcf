// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package uploadertest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime/metrics"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/atomicfile"
)

// Usage samples the process while a benchmark runs: CPU time from the
// operating system, and the peak of the Go heap and of the memory the Go
// runtime holds, sampled every few milliseconds without stopping the world.
type Usage struct {
	start    time.Time
	cpuStart time.Duration
	stop     chan struct{}
	done     sync.WaitGroup

	mu       sync.Mutex
	peakHeap uint64
	peakHeld uint64
}

// UsageReport is what a Usage saw.
type UsageReport struct {
	Wall time.Duration
	// CPU is user plus system time of the whole process.
	CPU time.Duration
	// PeakHeap is the largest heap of live and not yet swept objects.
	PeakHeap uint64
	// PeakHeld is the largest amount of memory the Go runtime held from the
	// operating system and had not released, close to resident size.
	PeakHeld uint64
}

// StartUsage starts sampling.
func StartUsage() *Usage {
	u := &Usage{start: time.Now(), cpuStart: processCPU(), stop: make(chan struct{})}
	u.sample()
	u.done.Add(1)
	go func() {
		defer u.done.Done()
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-u.stop:
				return
			case <-ticker.C:
				u.sample()
			}
		}
	}()
	return u
}

var usageSamples = []string{
	"/memory/classes/heap/objects:bytes",
	"/memory/classes/total:bytes",
	"/memory/classes/heap/released:bytes",
}

func (u *Usage) sample() {
	samples := make([]metrics.Sample, len(usageSamples))
	for i, name := range usageSamples {
		samples[i].Name = name
	}
	metrics.Read(samples)
	heap := samples[0].Value.Uint64()
	held := samples[1].Value.Uint64() - samples[2].Value.Uint64()
	u.mu.Lock()
	u.peakHeap = max(u.peakHeap, heap)
	u.peakHeld = max(u.peakHeld, held)
	u.mu.Unlock()
}

// Stop ends sampling and returns what was seen.
func (u *Usage) Stop() UsageReport {
	wall := time.Since(u.start)
	cpu := processCPU() - u.cpuStart
	close(u.stop)
	u.done.Wait()
	u.sample()
	u.mu.Lock()
	defer u.mu.Unlock()
	return UsageReport{Wall: wall, CPU: cpu, PeakHeap: u.peakHeap, PeakHeld: u.peakHeld}
}

// Add sums the time of r and other and keeps the larger peaks, so the
// timed parts of several iterations report as one.
func (r UsageReport) Add(other UsageReport) UsageReport {
	return UsageReport{
		Wall:     r.Wall + other.Wall,
		CPU:      r.CPU + other.CPU,
		PeakHeap: max(r.PeakHeap, other.PeakHeap),
		PeakHeld: max(r.PeakHeld, other.PeakHeld),
	}
}

// Report adds the usage to b's results: mebibytes of input per CPU second
// (one core's throughput), CPU and wall seconds per iteration, and the peak
// memory. inputBytes is the input of one iteration.
func (r UsageReport) Report(b *testing.B, inputBytes int64) {
	b.Helper()
	const mib = 1 << 20
	if r.CPU > 0 && inputBytes > 0 {
		b.ReportMetric(float64(inputBytes)*float64(b.N)/mib/r.CPU.Seconds(), "MiB/cpu-s")
	}
	b.ReportMetric(r.CPU.Seconds()/float64(b.N), "cpu-s/op")
	b.ReportMetric(r.Wall.Seconds()/float64(b.N), "wall-s/op")
	b.ReportMetric(float64(r.PeakHeap)/mib, "peak-heap-MiB")
	b.ReportMetric(float64(r.PeakHeld)/mib, "peak-held-MiB")
}

func processCPU() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// LoadSegmentFile returns the path of a segment shaped by p and its stats.
// The segment is written under the directory in RTU_BENCH_DIR when that is
// set, named by a hash of p so later runs reuse it, or under a temporary
// directory otherwise. A cached segment is used only when its stats file,
// written last, is there. Both are written under a temporary name and
// renamed into place, so a benchmark in another process that builds the
// same segment at once never truncates one being read.
func LoadSegmentFile(tb testing.TB, p LoadProfile) (string, LoadStats) {
	tb.Helper()
	dir := os.Getenv("RTU_BENCH_DIR")
	if dir == "" {
		dir = tb.TempDir()
	}
	key, err := json.Marshal(p)
	if err != nil {
		tb.Fatalf("uploadertest: key load profile: %v", err)
	}
	sum := sha256.Sum256(key)
	path := filepath.Join(dir, "load-"+hex.EncodeToString(sum[:8])+".jsonl.gz")
	statsPath := path + ".stats.json"
	if data, err := os.ReadFile(statsPath); err == nil {
		var stats LoadStats
		if json.Unmarshal(data, &stats) == nil {
			if _, err := os.Stat(path); err == nil {
				return path, stats
			}
		}
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		tb.Fatalf("uploadertest: create bench dir: %v", err)
	}
	file, err := atomicfile.Create(dir, "load-*.tmp")
	if err != nil {
		tb.Fatalf("uploadertest: create load segment: %v", err)
	}
	stats, err := WriteLoadSegment(file, p)
	if err == nil {
		err = file.Publish(path)
	} else {
		_ = file.Discard()
	}
	if err != nil {
		tb.Fatalf("uploadertest: write load segment: %v", err)
	}
	data, err := json.Marshal(stats)
	if err != nil {
		tb.Fatalf("uploadertest: encode load stats: %v", err)
	}
	statsFile, err := atomicfile.Create(dir, "load-*.stats.tmp")
	if err == nil {
		if _, err = statsFile.Write(data); err == nil {
			err = statsFile.Publish(statsPath)
		} else {
			_ = statsFile.Discard()
		}
	}
	if err != nil {
		tb.Fatalf("uploadertest: write load stats: %v", err)
	}
	return path, stats
}
