// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/containerd"
)

func recordingTail(group, gpu, netUnlock bool, fail map[string]bool) (*restoreTail, *[]string) {
	var calls []string
	step := func(name string) error {
		calls = append(calls, name)
		if fail[name] {
			return fmt.Errorf("%s: exit status 1", name)
		}
		return nil
	}
	return &restoreTail{
		group: group, gpu: gpu, netUnlock: netUnlock,
		placeholder: &containerd.ContainerInfo{Labels: map[string]string{
			"io.kubernetes.pod.namespace": "sr-1", "io.kubernetes.pod.name": "fn-0",
			"io.kubernetes.pod.uid": "u1", "io.kubernetes.container.name": "inference"}},
		resumeGPU: func(hold bool) error {
			if hold {
				return step("gpu-resume-held")
			}
			return step("gpu-resume")
		},
		continueTasks: func() error { return step("continue") },
		awaitOpen:     func() error { return step("open") },
		unlockNet:     func() error { return step("net-unlock") },
		resumeNIXL:    func() error { return step("nixl-resume") },
		resumeMX:      func() { _ = step("mx-resume") },
	}, &calls
}

// The restore's tail, in order: the NIXL KV connector comes back only
// once the restored processes run (after the GPU resume, and for a group
// after open and the network unlock), and before ModelExpress. A failed
// connector resume fails the restore; outside a group it is marked for
// the placeholder's disposal.
func TestRestoreTail_Order(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		group, gpu, netUnlock bool
		fail                  string
		want                  []string
		wantErr, notServing   bool
	}{
		{name: "single pod", gpu: true,
			want: []string{"gpu-resume", "continue", "nixl-resume", "mx-resume"}},
		{name: "single pod, connector does not resume", gpu: true, fail: "nixl-resume", wantErr: true, notServing: true,
			want: []string{"gpu-resume", "continue", "nixl-resume"}},
		{name: "single pod, GPU resume fails", gpu: true, fail: "gpu-resume", wantErr: true,
			want: []string{"gpu-resume"}},
		{name: "single pod without the library", want: nil},
		{name: "group member", group: true, gpu: true, netUnlock: true,
			want: []string{"gpu-resume-held", "continue", "open", "net-unlock", "nixl-resume", "mx-resume"}},
		{name: "group member, connector does not resume", group: true, gpu: true, netUnlock: true, fail: "nixl-resume", wantErr: true,
			want: []string{"gpu-resume-held", "continue", "open", "net-unlock", "nixl-resume"}},
		{name: "group member, open fails", group: true, gpu: true, fail: "open", wantErr: true,
			want: []string{"gpu-resume-held", "continue", "open"}},
		{name: "group companion", group: true, netUnlock: true,
			want: []string{"open", "net-unlock", "nixl-resume", "mx-resume"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tail, calls := recordingTail(tc.group, tc.gpu, tc.netUnlock, map[string]bool{tc.fail: true})
			err := tail.run()
			if (err != nil) != tc.wantErr {
				t.Errorf("err = %v, want err %v", err, tc.wantErr)
			}
			var ns *restoredNotServingError
			if got := errors.As(err, &ns); got != tc.notServing {
				t.Errorf("marked not serving = %v, want %v (err %v)", got, tc.notServing, err)
			} else if got && (ns.Pod != "fn-0" || ns.UID != "u1") {
				t.Errorf("not serving names %+v, want the placeholder", ns)
			}
			if !slices.Equal(*calls, tc.want) {
				t.Errorf("steps = %v\n want %v", *calls, tc.want)
			}
		})
	}
}

// A single-pod restore whose connector does not resume keeps its
// placeholder's restore marker, and hands its removal to whoever disposes
// of the placeholder: an agent that stops first replaces the pod on its
// next run (capture_marker.go). Any other outcome leaves the marker to
// the restore.
func TestRestoreTail_KeepsTheMarkerUntilTheDisposal(t *testing.T) {
	for _, tc := range []struct {
		name string
		fail string
		kept bool
	}{
		{"connector does not resume", "nixl-resume", true},
		{"restored", "", false},
		{"GPU resume fails", "gpu-resume", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tail, _ := recordingTail(false, true, false, map[string]bool{tc.fail: true})
			released := false
			tail.releaseMarker = func() { released = true }
			err := tail.run()
			if tail.markerKept != tc.kept {
				t.Errorf("marker kept = %v, want %v", tail.markerKept, tc.kept)
			}
			releaseNotServing(err)
			if released != tc.kept {
				t.Errorf("released by the disposer = %v, want %v", released, tc.kept)
			}
		})
	}
}
