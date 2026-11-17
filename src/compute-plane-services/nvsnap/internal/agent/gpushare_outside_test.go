// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"slices"
	"testing"
)

// The GPU processes under the library are found whatever their session:
// an engine started with setsid is one, a helper without GPU state or a
// GPU process without the library is not.
func TestShimGPUProcesses(t *testing.T) {
	base := t.TempDir()
	fakeGPUProc(t, base, 100, 1, 100, []string{testShim}, nil)                 // init: library, no GPU
	fakeGPUProc(t, base, 200, 2, 200, []string{testShim, "/dev/nvidia0"}, nil) // engine in its own session
	fakeGPUProc(t, base, 300, 3, 200, []string{"/dev/nvidia1"}, nil)           // GPU without the library
	got := shimGPUProcesses(base, []int{100, 200, 300, 999})
	if !slices.Equal(got, []string{"200"}) {
		t.Fatalf("shimGPUProcesses = %v, want [200]", got)
	}
	if got := shimGPUProcesses(base, []int{100, 300}); len(got) != 0 {
		t.Fatalf("shimGPUProcesses = %v, want none", got)
	}
}
