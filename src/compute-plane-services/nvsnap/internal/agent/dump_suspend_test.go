// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/containerd"
)

// dumpSteps records, in order, every step dumpAroundSuspend takes on the
// workload: the tool's verbs (through gpushareMX and gpushareNIXL), the
// suspend, the dump, the GPU resume and the store's harvest.
type dumpSteps struct {
	calls []string
	fail  map[string]bool
}

func (s *dumpSteps) step(name string) error {
	s.calls = append(s.calls, name)
	if s.fail[name] {
		return fmt.Errorf("%s: exit status 1", name)
	}
	return nil
}

// fakeDumpSource is a session (init 5000) whose engine (5100) is under the
// gpushare library and opted in to nixl-pause. (nixl-pause refuses a pod
// that also loads through ModelExpress: fakeMXDumpSource.)
func fakeDumpSource(t *testing.T) (base string, pids []int) {
	t.Helper()
	return fakeDumpSourceEnv(t, "NVSNAP_NIXL_PAUSE=1")
}

// fakeMXDumpSource is fakeDumpSource whose engine also loads through
// ModelExpress.
func fakeMXDumpSource(t *testing.T) (base string, pids []int) {
	t.Helper()
	return fakeDumpSourceEnv(t, "NVSNAP_NIXL_PAUSE=1", "MODEL_EXPRESS_URL=http://mx:8001")
}

func fakeDumpSourceEnv(t *testing.T, engineEnv ...string) (base string, pids []int) {
	t.Helper()
	base = t.TempDir()
	fakeGPUProc(t, base, 5000, 1, 5000, []string{testShim}, nil)
	fakeGPUProc(t, base, 5100, 2, 5000, []string{testShim, "/dev/nvidia0"}, nil)
	fakeMXProc(t, base, 5000, nil, []string{"/bin/sh"})
	fakeMXProc(t, base, 5100, engineEnv, []string{"python3", "-m", "vllm"})
	pids, _, ok, err := gpushareTargets(base, 5000)
	if err != nil || !ok {
		t.Fatalf("gpushareTargets = %v, %v, %v", pids, ok, err)
	}
	return base, pids
}

func (s *dumpSteps) install(t *testing.T) {
	t.Helper()
	fakeControlSockets(t, nil)
	prevMX, prevNIXL := gpushareMX, gpushareNIXL
	gpushareMX = func(_ context.Context, _ int, verb, _ string, _ []int) (string, error) {
		return "pid=2 mx: ok\n", s.step(verb)
	}
	gpushareNIXL = func(_ context.Context, _ int, verb, _ string, pids []int) (string, error) {
		var out strings.Builder
		for _, p := range pids {
			fmt.Fprintf(&out, "pid=%d nixl %s: ok engine paused, NIXL connector released on 1 worker(s)\n", p, strings.TrimPrefix(verb, "nixl-"))
		}
		return out.String(), s.step(verb)
	}
	t.Cleanup(func() { gpushareMX, gpushareNIXL = prevMX, prevNIXL })
}

func (s *dumpSteps) source(base string, pids []int, leaveRunning bool, dumpRuns bool) sourceDump {
	return sourceDump{
		on: true, nixlOptIn: true, leaveRunning: leaveRunning, procBase: base, hostPID: 5000, targetHostPID: 5000, pids: pids,
		toolText: nixlToolText, checkpointDir: base,
		suspend: func(context.Context, int, []int, string, *logrus.Entry) (string, bool, error) {
			if err := s.step("suspend"); err != nil {
				return "", false, err
			}
			return "GPU-0", true, nil
		},
		resume: func(context.Context, int, []int, string, time.Duration, *logrus.Entry) error {
			return s.step("gpu-resume")
		},
		watchFaults: func(*logrus.Entry) *gpuFaultWatch { return nil },
		closeFabric: func() {},
		collect:     func(string) error { return s.step("collect") },
		dump: func() (bool, bool, func(), error) {
			err := s.step("dump")
			return dumpRuns, err == nil && dumpRuns, func() {}, err
		},
	}
}

// The source's steps around its dump, in order, on every path: the pauses
// before the suspend (ModelExpress, then the NIXL KV connector), and, on a
// source that runs on, the GPU resume first, the connector's resume (checked)
// next, ModelExpress last. A source whose GPU resume failed is replaced and
// gets neither.
func TestDumpAroundSuspend_StepsInOrder(t *testing.T) {
	for _, tc := range []struct {
		name         string
		leaveRunning bool
		dumpRuns     bool // false: a preparation fails before CRIU
		fail         []string
		want         []string
		wantErr      bool
		wantReplaced bool
	}{
		{name: "capture", dumpRuns: true,
			want: []string{"nixl-pause", "suspend", "dump", "collect"}},
		{name: "capture, dump fails", dumpRuns: true, fail: []string{"dump"}, wantErr: true,
			want: []string{"nixl-pause", "suspend", "dump", "gpu-resume", "nixl-resume"}},
		{name: "capture, fails before CRIU", dumpRuns: false, fail: []string{"dump"}, wantErr: true,
			want: []string{"nixl-pause", "suspend", "dump", "gpu-resume", "nixl-resume"}},
		{name: "suspend fails", dumpRuns: true, fail: []string{"suspend"}, wantErr: true,
			want: []string{"nixl-pause", "suspend", "nixl-resume"}},
		{name: "nixl-pause refused", dumpRuns: true, fail: []string{"nixl-pause"}, wantErr: true,
			want: []string{"nixl-pause", "nixl-resume"}},
		{name: "GPU resume fails", dumpRuns: true, fail: []string{"dump", "gpu-resume"}, wantErr: true, wantReplaced: true,
			want: []string{"nixl-pause", "suspend", "dump", "gpu-resume"}},
		{name: "leave running", leaveRunning: true, dumpRuns: true,
			want: []string{"nixl-pause", "suspend", "dump", "gpu-resume", "nixl-resume", "collect"}},
		{name: "leave running, connector does not resume", leaveRunning: true, dumpRuns: true, fail: []string{"nixl-resume"},
			wantErr: true, wantReplaced: true,
			want: []string{"nixl-pause", "suspend", "dump", "gpu-resume", "nixl-resume"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, pids := fakeDumpSource(t)
			s := &dumpSteps{fail: map[string]bool{}}
			for _, f := range tc.fail {
				s.fail[f] = true
			}
			s.install(t)
			a := &Agent{config: Config{Webhook: WebhookConfig{NIXLPause: true}}}
			info, err := a.dumpAroundSuspend(context.Background(), s.source(base, pids, tc.leaveRunning, tc.dumpRuns), testLog())
			if (err != nil) != tc.wantErr || sourceMustBeReplaced(err) != tc.wantReplaced {
				t.Errorf("err = %v; want err %v, replaced %v", err, tc.wantErr, tc.wantReplaced)
			}
			if !slices.Equal(s.calls, tc.want) {
				t.Errorf("steps = %v\n want %v", s.calls, tc.want)
			}
			if err == nil && (info == nil || !info.NIXL || !slices.Equal(info.NIXLPIDs, pids)) {
				t.Errorf("info = %+v, want the pauses recorded, with the pids the NIXL pause released (%v)", info, pids)
			}
		})
	}
}

// A source that runs on resumes the pids its pause released even when
// the socket scan no longer finds them: the leave-running resume and the
// resume after a failed dump both address them, rather than skip them and
// leave the engine paused.
func TestDumpAroundSuspend_ResumesThePausedPIDs(t *testing.T) {
	for _, tc := range []struct {
		name         string
		leaveRunning bool
		fail         []string
		wantErr      bool
	}{
		{name: "leave running", leaveRunning: true},
		{name: "dump fails", fail: []string{"dump"}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, pids := fakeDumpSource(t)
			s := &dumpSteps{fail: map[string]bool{}}
			for _, f := range tc.fail {
				s.fail[f] = true
			}
			s.install(t)
			// After the pause, no process shows a control socket.
			controlSocketPIDs = func(_ int, pids []int) []int {
				if slices.Contains(s.calls, "nixl-pause") {
					return nil
				}
				return pids
			}
			a := &Agent{config: Config{Webhook: WebhookConfig{NIXLPause: true}}}
			_, err := a.dumpAroundSuspend(context.Background(), s.source(base, pids, tc.leaveRunning, true), testLog())
			if (err != nil) != tc.wantErr || sourceMustBeReplaced(err) {
				t.Errorf("err = %v; want err %v, the source not replaced", err, tc.wantErr)
			}
			if !slices.Contains(s.calls, "nixl-resume") {
				t.Errorf("steps = %v: the paused pids were not resumed", s.calls)
			}
		})
	}
}

// The dump of a capture that paused the NIXL KV connector runs fenced:
// dumpAroundSuspend reports the pause (sourceDump.nixlPaused, which dumpV2
// turns into the fenced marker name) before the dump runs; a capture that
// paused nothing does not.
func TestDumpAroundSuspend_FencesAPausedDump(t *testing.T) {
	for _, optedIn := range []bool{true, false} {
		base, pids := fakeDumpSource(t)
		s := &dumpSteps{}
		s.install(t)
		d := s.source(base, pids, false, true)
		d.nixlOptIn = optedIn
		fenced, fencedAtDump := false, false
		d.nixlPaused = func() { fenced = true }
		dump := d.dump
		d.dump = func() (bool, bool, func(), error) {
			fencedAtDump = fenced
			return dump()
		}
		if _, err := (&Agent{config: Config{Webhook: WebhookConfig{NIXLPause: true}}}).dumpAroundSuspend(context.Background(), d, testLog()); err != nil {
			t.Fatal(err)
		}
		if fencedAtDump != optedIn {
			t.Errorf("opted in %v: the dump ran fenced %v", optedIn, fencedAtDump)
		}
	}
}

// A pod that did not opt in (nixlOptIn) gets no nixl-pause, and the
// checkpoint records no paused connector, whatever its processes' marker.
func TestDumpAroundSuspend_NotOptedInPausesNoConnector(t *testing.T) {
	base, pids := fakeMXDumpSource(t)
	s := &dumpSteps{}
	s.install(t)
	d := s.source(base, pids, false, true)
	d.nixlOptIn = false
	info, err := (&Agent{config: Config{Webhook: WebhookConfig{NIXLPause: true}}}).dumpAroundSuspend(context.Background(), d, testLog())
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"mx-pause", "suspend", "dump", "collect"}; !slices.Equal(s.calls, want) {
		t.Errorf("steps = %v, want %v", s.calls, want)
	}
	if info.NIXL {
		t.Error("the checkpoint records a paused connector")
	}
}

// The capture refuses nixl-pause for an engine that spans nodes itself,
// not only in the preflight (a capture through POST /v1/checkpoints runs
// none): GPU ranks on more than one node, a session whose driver does not
// say, or a container one of whose processes says so. Nothing is paused
// or suspended; ModelExpress, paused first, is resumed. One GPU rank with
// its companions, captured in a session on one node, is paused.
func TestDumpAroundSuspend_RefusesNIXLPauseAcrossNodes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		session string
		nodes   int
		initEnv []string
		want    []string
	}{
		{"ranks on two nodes", "s1", 2, nil, nil},
		{"session, driver does not say", "s1", 0, nil, nil},
		{"fabric marker on the init", "", 0, []string{"NVSNAP_GPUSHARE_FABRIC=1"}, nil},
		{"group of two", "", 0, []string{"LWS_GROUP_SIZE=2"}, nil},
		{"one rank with companions", "s1", 1, nil, []string{"nixl-pause", "suspend", "dump", "collect"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, pids := fakeDumpSource(t)
			fakeMXProc(t, base, 5000, tc.initEnv, []string{"/bin/sh"})
			s := &dumpSteps{}
			s.install(t)
			a := &Agent{config: Config{Webhook: WebhookConfig{NIXLPause: true}}}
			d := s.source(base, pids, false, true)
			d.fabricSession, d.rankNodes = tc.session, tc.nodes
			_, err := a.dumpAroundSuspend(context.Background(), d, testLog())
			refused := !slices.Contains(tc.want, "nixl-pause")
			if refused && (err == nil || !strings.Contains(err.Error(), "spans nodes") || sourceMustBeReplaced(err)) {
				t.Errorf("err = %v, want a refusal naming the limit, source kept", err)
			}
			if !refused && err != nil {
				t.Errorf("err = %v, want the capture", err)
			}
			if !slices.Equal(s.calls, tc.want) {
				t.Errorf("steps = %v, want %v", s.calls, tc.want)
			}
		})
	}
}

// sourceDumpFor, dumpV2's own completion of the dump, carries the pod's
// opt-in (read from the pod as admitted) and the capture's session and
// rank nodes into the dump: through it, a pod that opted in is paused on
// one node and refused across two, and one that did not is not paused.
func TestSourceDumpFor_CarriesTheNIXLChoices(t *testing.T) {
	info := &containerd.ContainerInfo{Labels: map[string]string{"io.kubernetes.pod.namespace": "ns",
		"io.kubernetes.pod.name": "w", "io.kubernetes.container.name": "engine"}}
	for _, tc := range []struct {
		name  string
		pod   *corev1.Pod
		nodes int
		want  []string
	}{
		{"opted in, one node", nixlPod(true, true), 1, []string{"nixl-pause", "suspend", "dump", "collect"}},
		{"opted in, two nodes", nixlPod(true, true), 2, nil},
		{"not opted in", nixlPod(false, false), 1, []string{"suspend", "dump", "collect"}},
		// The pod's spec says its engine spans nodes, which its processes
		// do not show: refused before anything is paused.
		{"opted in, spec spans nodes", spansBySpec(nixlPod(true, true)), 1, nil},
		// Its opt-in cannot be read: refused, fail closed.
		{"pod cannot be read", nil, 1, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, pids := fakeDumpSource(t)
			s := &dumpSteps{}
			s.install(t)
			kc := fake.NewSimpleClientset()
			if tc.pod != nil {
				kc = fake.NewSimpleClientset(tc.pod)
			}
			a := &Agent{config: Config{Webhook: WebhookConfig{NIXLPause: true}}, kubeClient: kc}
			steps := s.source(base, pids, false, true)
			d := a.sourceDumpFor(context.Background(), info, base, dumpV2Opts{FabricSession: "s1", RankNodes: tc.nodes},
				sourceDump{on: true, procBase: base, hostPID: 5000, targetHostPID: 5000, pids: pids, toolText: nixlToolText,
					closeFabric: func() {}, dump: steps.dump}, testLog())
			if d.suspend == nil || d.resume == nil || d.watchFaults == nil || d.collect == nil {
				t.Fatal("sourceDumpFor left a step unset")
			}
			// The steps that act on the workload are the recorder's.
			d.suspend, d.resume, d.watchFaults, d.collect = steps.suspend, steps.resume, steps.watchFaults, steps.collect
			_, err := a.dumpAroundSuspend(context.Background(), d, testLog())
			if !slices.Equal(s.calls, tc.want) {
				t.Errorf("steps = %v, want %v", s.calls, tc.want)
			}
			if len(tc.want) == 0 && !errors.Is(err, errNIXLPauseUnsupported) {
				t.Errorf("err = %v, want the capture refused before nixl-pause", err)
			}
		})
	}
}

// spansBySpec marks p's engine container as a GPU container in a
// LeaderWorkerSet group of two: its spec says it spans nodes.
func spansBySpec(p *corev1.Pod) *corev1.Pod {
	p.Spec.Containers[1].Resources.Limits = corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}
	p.Annotations["leaderworkerset.sigs.k8s.io/size"] = "2"
	return p
}

// An agent whose nixl-pause switch is off captures as before when the
// pod cannot be read: the refusal is only for an agent that may pause.
func TestSourceDumpFor_SwitchOffDoesNotRefuseAnUnreadPod(t *testing.T) {
	info := &containerd.ContainerInfo{Labels: map[string]string{"io.kubernetes.pod.namespace": "ns",
		"io.kubernetes.pod.name": "w", "io.kubernetes.container.name": "engine"}}
	a := &Agent{kubeClient: fake.NewSimpleClientset()}
	d := a.sourceDumpFor(context.Background(), info, t.TempDir(), dumpV2Opts{}, sourceDump{on: true}, testLog())
	if d.nixlRefusal != "" || d.nixlOptIn {
		t.Errorf("refusal %q, opted in %v; want neither", d.nixlRefusal, d.nixlOptIn)
	}
}

// A pod that opted in and also loads through ModelExpress is refused
// before nixl-pause on every capture, as the preflight refuses it: not
// supported together. ModelExpress, paused first, is resumed.
func TestDumpAroundSuspend_RefusesNIXLPauseWithModelExpress(t *testing.T) {
	base, pids := fakeMXDumpSource(t)
	s := &dumpSteps{}
	s.install(t)
	a := &Agent{config: Config{Webhook: WebhookConfig{NIXLPause: true}}}
	_, err := a.dumpAroundSuspend(context.Background(), s.source(base, pids, false, true), testLog())
	if !errors.Is(err, errNIXLPauseUnsupported) || !strings.Contains(err.Error(), "ModelExpress") || sourceMustBeReplaced(err) {
		t.Errorf("err = %v, want a refusal naming ModelExpress, source kept", err)
	}
	if want := []string{"mx-pause", "mx-resume"}; !slices.Equal(s.calls, want) {
		t.Errorf("steps = %v, want %v", s.calls, want)
	}
}
