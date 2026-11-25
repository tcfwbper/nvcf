/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gateway

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/metrics"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// generatedJSONBody serves head, size filler bytes inside a JSON string, and
// tail without holding the body in memory, like a client upload.
type generatedJSONBody struct {
	head, tail string
	fill       int
	off        int
}

func (g *generatedJSONBody) Read(p []byte) (int, error) {
	total := len(g.head) + g.fill + len(g.tail)
	if g.off >= total {
		return 0, io.EOF
	}
	n := 0
	for n < len(p) && g.off < total {
		switch {
		case g.off < len(g.head):
			c := copy(p[n:], g.head[g.off:])
			n += c
			g.off += c
		case g.off < len(g.head)+g.fill:
			c := min(len(p)-n, len(g.head)+g.fill-g.off)
			for i := range c {
				p[n+i] = 'a'
			}
			n += c
			g.off += c
		default:
			c := copy(p[n:], g.tail[g.off-len(g.head)-g.fill:])
			n += c
			g.off += c
		}
	}
	return n, nil
}

func (g *generatedJSONBody) Close() error { return nil }

type profileScenario struct {
	name  string
	model string
	early bool
	mode  string
}

func (s profileScenario) body(size int) *generatedJSONBody {
	if s.early {
		return &generatedJSONBody{head: `{"model":"` + s.model + `","stream":false,"input":"`, tail: `"}`, fill: size}
	}
	return &generatedJSONBody{head: `{"input":"`, tail: `","model":"` + s.model + `","stream":false}`, fill: size}
}

var profileScenarios = []profileScenario{
	{"direct early fields", prefixTestModel, true, "streamed/early_fields"},
	{"direct late fields", prefixTestModel, false, "buffered/late_fields"},
	{"shadowed early fields", "acme/shadowed", true, "buffered/shadow"},
	{"LLM Gateway early fields", "acme/llm", true, "buffered/llm_gateway"},
}

type profileResult struct {
	liveHeapBytes   uint64
	peakHeapBytes   uint64
	allocatedBytes  uint64
	allocatedPerReq uint64
}

func heapObjectBytes(sample []metrics.Sample) uint64 {
	metrics.Read(sample)
	return sample[0].Value.Uint64()
}

// profileRequestBodies sends concurrency requests of size filler bytes
// through the model director. Each upstream reads all but the last KiB of its
// body and then waits until every primary request has done the same. The live
// heap is measured there, after two collections so pooled buffers that are no
// longer referenced do not count. The sampled peak includes garbage and pooled
// buffers.
func profileRequestBodies(t testing.TB, scenario profileScenario, size, concurrency int) profileResult {
	t.Helper()
	return profileRequestBodiesWithLimits(t, scenario, size, concurrency, 64<<20, 64<<20)
}

// profileRequestBodiesWithLimits is profileRequestBodies with the buffered
// and streamed body limits given.
func profileRequestBodiesWithLimits(t testing.TB, scenario profileScenario, size, concurrency int, limit, streamedLimit int64) profileResult {
	t.Helper()
	var (
		arrivedMu sync.Mutex
		arrived   int
		release   = make(chan struct{})
		liveHeap  uint64
	)
	var baseline runtime.MemStats
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		defer req.Body.Close()
		if isShadowRequest(req) {
			_, _ = io.Copy(io.Discard, req.Body)
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: http.NoBody, Request: req}, nil
		}
		if _, err := io.CopyN(io.Discard, req.Body, int64(size-1024)); err != nil {
			return nil, err
		}
		arrivedMu.Lock()
		arrived++
		if arrived == concurrency {
			runtime.GC()
			runtime.GC()
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)
			if stats.HeapAlloc > baseline.HeapAlloc {
				liveHeap = stats.HeapAlloc - baseline.HeapAlloc
			}
			close(release)
		}
		arrivedMu.Unlock()
		select {
		case <-release:
		case <-time.After(time.Minute):
			return nil, errors.New("not every request reached the upstream")
		}
		if _, err := io.Copy(io.Discard, req.Body); err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: http.NoBody, Request: req}, nil
	})
	_, handler := newPrefixTestDirectorWithStreamedLimit(t, limit, streamedLimit, transport)

	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&baseline)
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	baseObjects := heapObjectBytes(sample)
	peak := baseObjects
	stopSampling := make(chan struct{})
	samplingDone := make(chan struct{})
	go func() {
		defer close(samplingDone)
		local := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
		ticker := time.NewTicker(200 * time.Microsecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopSampling:
				return
			case <-ticker.C:
				peak = max(peak, heapObjectBytes(local))
			}
		}
	}()

	var wg sync.WaitGroup
	codes := make([]int, concurrency)
	for i := range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", scenario.body(size))
			req.ContentLength = -1
			req.Header.Set(contentTypeHeader, "application/json")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			codes[i] = rec.Code
		}()
	}
	wg.Wait()
	close(stopSampling)
	<-samplingDone

	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	for _, code := range codes {
		require.Equal(t, http.StatusOK, code)
	}
	allocated := after.TotalAlloc - baseline.TotalAlloc
	return profileResult{
		liveHeapBytes:   liveHeap,
		peakHeapBytes:   peak - min(peak, baseObjects),
		allocatedBytes:  allocated,
		allocatedPerReq: allocated / uint64(concurrency),
	}
}

func mib(b uint64) string {
	return fmt.Sprintf("%.2f", float64(b)/(1<<20))
}

// TestRequestBodyLiveHeapStaysBoundedWhenStreamed is a regression bound: the
// streamed path keeps far less than one body per request live while the
// upstream reads, and the buffered path keeps at least one.
func TestRequestBodyLiveHeapStaysBoundedWhenStreamed(t *testing.T) {
	const size, concurrency = 4 << 20, 4
	streamed := profileRequestBodies(t, profileScenarios[0], size, concurrency)
	buffered := profileRequestBodies(t, profileScenarios[1], size, concurrency)
	assert.Less(t, streamed.liveHeapBytes, uint64(size), "streamed live heap %s MiB", mib(streamed.liveHeapBytes))
	// The buffered path holds each body until the upstream finishes reading
	// it. Allow for buffer sizing differences below one body per request.
	assert.GreaterOrEqual(t, buffered.liveHeapBytes, uint64(size*concurrency*3/4), "buffered live heap %s MiB", mib(buffered.liveHeapBytes))

	// A streamed body four times the buffered limit passes under a larger
	// streamed limit and keeps the same bound.
	const bufferedLimit = size / 4
	aboveBuffered := profileRequestBodiesWithLimits(t, profileScenarios[0], size, concurrency, bufferedLimit, 64<<20)
	assert.Less(t, aboveBuffered.liveHeapBytes, uint64(size), "streamed above the buffered limit: live heap %s MiB", mib(aboveBuffered.liveHeapBytes))
}

// TestRequestBodyMemoryProfile prints allocation and heap measurements for
// each read path as body size and concurrency grow. Set
// VANITY_GATEWAY_BODY_PROFILE=1 to run it.
func TestRequestBodyMemoryProfile(t *testing.T) {
	if os.Getenv("VANITY_GATEWAY_BODY_PROFILE") == "" {
		t.Skip("set VANITY_GATEWAY_BODY_PROFILE=1 to print the request body memory profile")
	}
	t.Logf("| Scenario | Read mode | Body MiB | Concurrency | Live heap MiB | Sampled peak MiB | Allocated MiB per request |")
	t.Logf("| --- | --- | ---: | ---: | ---: | ---: | ---: |")
	for _, scenario := range profileScenarios {
		for _, size := range []int{1 << 20, 4 << 20, 16 << 20} {
			for _, concurrency := range []int{1, 4, 16} {
				result := profileRequestBodies(t, scenario, size, concurrency)
				t.Logf("| %s | %s | %d | %d | %s | %s | %s |", scenario.name, scenario.mode, size>>20, concurrency,
					mib(result.liveHeapBytes), mib(result.peakHeapBytes), mib(result.allocatedPerReq))
			}
		}
	}
}

func BenchmarkReadModelRequestBody(b *testing.B) {
	director, _ := newPrefixTestDirector(b, 64<<20, http.DefaultTransport)
	for _, scenario := range profileScenarios[:2] {
		for _, size := range []int{256 << 10, 1 << 20, 8 << 20} {
			b.Run(fmt.Sprintf("%s/%dKiB", scenario.mode, size>>10), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(size))
				for b.Loop() {
					req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", scenario.body(size))
					req.ContentLength = -1
					_, _, err := director.readModelRequestBody(req, director.chatCompletions.modelNameToNVCFUrl)
					if err != nil {
						b.Fatal(err)
					}
					_, _ = io.Copy(io.Discard, req.Body)
					_ = req.Body.Close()
				}
			})
		}
	}
}
