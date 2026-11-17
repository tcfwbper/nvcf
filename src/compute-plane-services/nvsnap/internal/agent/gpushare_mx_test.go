// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
)

const mxToolText = "  mx-pause      Before suspend: ...\n  mx-resume     After open ...\n"

// fakeMX records the tool calls in order and fails the verbs in fail.
func fakeMX(t *testing.T, fail map[string]bool) *[]string {
	t.Helper()
	var calls []string
	prev := gpushareMX
	gpushareMX = func(_ context.Context, hostPID int, verb, host string, pids []int) (string, error) {
		c := fmt.Sprintf("%s@%d %v", verb, hostPID, pids)
		if host != "" {
			c = "--host " + host + " " + c
		}
		calls = append(calls, c)
		if fail[verb] {
			// As gpushareTool fails: a toolError carrying the output.
			out := "pid=7 mx pause: err 2 RDMA device file(s) still open or mapped\n"
			return out, &toolError{args: append([]string{verb}, intsToStrings(pids)...), err: errors.New("exit status 1"), out: out}
		}
		return "ok", nil
	}
	t.Cleanup(func() { gpushareMX = prev })
	return &calls
}

func testLog() *logrus.Entry { return logrus.NewEntry(logrus.New()) }

func TestGPUShareMXSupported(t *testing.T) {
	for text, want := range map[string]bool{
		mxToolText:                  true,
		"  open   After resume ...": false,
		"  mx-pause only":           false,
		"":                          false,
	} {
		if got := gpushareMXSupported(text); got != want {
			t.Errorf("gpushareMXSupported(%q) = %v, want %v", text, got, want)
		}
	}
}

func fakeMXProc(t *testing.T, base string, pid int, environ, cmdline []string) {
	t.Helper()
	dir := filepath.Join(base, fmt.Sprint(pid))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for f, v := range map[string][]string{"environ": environ, "cmdline": cmdline} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte(strings.Join(v, "\x00")+"\x00"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// A worker uses ModelExpress when one of its processes names the server
// in its environment or loads with --load-format mx; an empty URL, another
// load format or a process that cannot be read does not count.
func TestUsesModelExpress(t *testing.T) {
	base := t.TempDir()
	fakeMXProc(t, base, 10, []string{"PATH=/bin", "MODEL_EXPRESS_URL=http://mx:8001"}, []string{"python3", "-m", "vllm"})
	fakeMXProc(t, base, 11, []string{"PATH=/bin"}, []string{"vllm", "serve", "m", "--load-format", "mx"})
	fakeMXProc(t, base, 12, []string{"PATH=/bin"}, []string{"vllm", "serve", "m", "--load-format=mx"})
	fakeMXProc(t, base, 13, []string{"PATH=/bin"}, []string{"python3", "-m", "dynamo.vllm", "--load-format", "modelexpress"})
	fakeMXProc(t, base, 14, []string{"PATH=/bin"}, []string{"python3", "-m", "dynamo.vllm", "--load-format=modelexpress"})
	fakeMXProc(t, base, 20, []string{"MODEL_EXPRESS_URL="}, []string{"vllm", "serve", "m", "--load-format", "safetensors"})
	fakeMXProc(t, base, 21, []string{"MY_MODEL_EXPRESS_URL=x"}, []string{"vllm", "serve", "mx"})
	for _, tc := range []struct {
		pids []int
		want bool
	}{
		{[]int{10}, true},
		{[]int{11}, true},
		{[]int{12}, true},
		{[]int{13}, true}, // as Dynamo workers name it
		{[]int{14}, true},
		{[]int{20, 21}, false},
		{[]int{99}, false}, // gone
		{[]int{20, 11}, true},
		{nil, false},
	} {
		if got := usesModelExpress(base, tc.pids); got != tc.want {
			t.Errorf("usesModelExpress(%v) = %v, want %v", tc.pids, got, tc.want)
		}
	}
}

func TestSessionPIDs(t *testing.T) {
	base := t.TempDir()
	fakeGPUProc(t, base, 5000, 1, 5000, nil, nil)
	fakeGPUProc(t, base, 5100, 2, 5000, nil, nil)
	fakeGPUProc(t, base, 7000, 1, 7000, nil, nil)
	got := sessionPIDs(base, 5000)
	slices.Sort(got)
	if !slices.Equal(got, []int{5000, 5100}) {
		t.Fatalf("sessionPIDs = %v, want [5000 5100]", got)
	}
}

// The pause runs only for a ModelExpress worker whose tool has mx-pause;
// any other capture goes on as before, without a call.
func TestPauseModelExpress_OnlyWhenUsedAndSupported(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mx       bool
		text     string
		pids     []int
		wantCall bool
	}{
		{"not ModelExpress", false, mxToolText, []int{7}, false},
		{"tool without mx-pause", true, "  open  ...", []int{7}, false},
		{"no GPU processes", true, mxToolText, nil, false},
		{"ModelExpress, tool has it", true, mxToolText, []int{7, 8}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := fakeMX(t, nil)
			paused, err := pauseModelExpress(context.Background(), tc.mx, tc.text, 42, tc.pids, testLog())
			if err != nil {
				t.Fatal(err)
			}
			if paused != tc.wantCall {
				t.Errorf("paused = %v, want %v", paused, tc.wantCall)
			}
			want := []string(nil)
			if tc.wantCall {
				want = []string{"mx-pause@42 [7 8]"}
			}
			if !slices.Equal(*calls, want) {
				t.Errorf("calls = %v, want %v", *calls, want)
			}
		})
	}
}

// A refused pause puts back what it paused (some processes may have
// released before one refused) and says why, as errMXPauseRefused.
func TestPauseModelExpress_RefusedResumesWhatItPaused(t *testing.T) {
	calls := fakeMX(t, map[string]bool{"mx-pause": true})
	paused, err := pauseModelExpress(context.Background(), true, mxToolText, 42, []int{7, 8}, testLog())
	if paused {
		t.Error("paused = true after a refused pause")
	}
	if !errors.Is(err, errMXPauseRefused) {
		t.Fatalf("err = %v, want errMXPauseRefused", err)
	}
	if !strings.Contains(err.Error(), "RDMA device file(s) still open") {
		t.Errorf("err %q lacks the tool's reason", err)
	}
	if want := []string{"mx-pause@42 [7 8]", "mx-resume@42 [7 8]"}; !slices.Equal(*calls, want) {
		t.Errorf("calls = %v, want %v", *calls, want)
	}
}

// A source runs on, and so needs ModelExpress back, unless its dump
// completed and killed it.
func TestSourceRunsOn(t *testing.T) {
	for _, tc := range []struct{ dumped, leaveRunning, want bool }{
		{false, false, true}, // failed before or in the dump
		{false, true, true},
		{true, true, true}, // leave-running dump
		{true, false, false},
	} {
		if got := sourceRunsOn(tc.dumped, tc.leaveRunning); got != tc.want {
			t.Errorf("sourceRunsOn(%v, %v) = %v, want %v", tc.dumped, tc.leaveRunning, got, tc.want)
		}
	}
}

// A restore resumes ModelExpress only when its capture paused it, through
// the restored tree's tool, and a failure there does not fail the restore.
func TestResumeRestoredModelExpress(t *testing.T) {
	for _, tc := range []struct {
		name string
		gs   *GPUShareInfo
		text string
		host string
		fail bool
		want []string
	}{
		{"no gpushare", nil, mxToolText, "", false, nil},
		{"capture did not pause", &GPUShareInfo{PIDs: []int{7}}, mxToolText, "", false, nil},
		{"paused", &GPUShareInfo{PIDs: []int{7, 8}, ModelExpress: true}, mxToolText, "", false, []string{"mx-resume@99 [7 8]"}},
		{"paused, new pod address", &GPUShareInfo{PIDs: []int{7}, ModelExpress: true}, mxToolText, "10.1.2.3", false, []string{"--host 10.1.2.3 mx-resume@99 [7]"}},
		{"paused, resume fails", &GPUShareInfo{PIDs: []int{7}, ModelExpress: true}, mxToolText, "", true, []string{"mx-resume@99 [7]"}},
		{"paused, tool without mx", &GPUShareInfo{PIDs: []int{7}, ModelExpress: true}, "  open ...", "", false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := fakeMX(t, map[string]bool{"mx-resume": tc.fail})
			resumeRestoredModelExpress(context.Background(), tc.gs, tc.text, 99, tc.host, testLog())
			if !slices.Equal(*calls, tc.want) {
				t.Errorf("calls = %v, want %v", *calls, tc.want)
			}
		})
	}
}

// Only the RDMA findings of processes mx-pause addresses stop blocking:
// another process's RDMA (a helper outside the library) still blocks, as
// the dump would fail on it after the instance is stopped.
func TestReleasedByMXPause(t *testing.T) {
	in := []PreflightFinding{
		{Pod: "w", Check: "rdma", Severity: preflightBlock, PID: 7, Detail: "process 7 holds RDMA devices /dev/infiniband/uverbs0"},
		{Pod: "w", Check: "rdma", Severity: preflightBlock, PID: 9, Detail: "process 9 holds RDMA devices /dev/infiniband/uverbs0"},
		{Pod: "w", Check: "socket", Severity: preflightBlock, Detail: "mptcp"},
		{Pod: "w", Check: "ghost", Severity: preflightWarn, Detail: "g"},
	}
	out := releasedByMXPause(in, map[int]bool{7: true})
	if out[0].Severity != preflightInfo || !strings.Contains(out[0].Detail, "mx-pause") {
		t.Errorf("rdma finding of a pausable process = %+v, want info naming mx-pause", out[0])
	}
	if out[1] != in[1] || out[2] != in[2] || out[3] != in[3] {
		t.Errorf("other findings changed: %+v", out[1:])
	}
	if in[0].Severity != preflightBlock {
		t.Error("the input was changed")
	}
	if len(blocking(out)) != 2 {
		t.Errorf("blocking = %v, want the other process's RDMA and the socket", blocking(out))
	}
}

// mx-pause addresses the session's GPU processes under the library; of
// those, the ones using ModelExpress are pausable. A helper outside the
// library, a process without GPU state, or one without ModelExpress is not.
func TestMXPausable(t *testing.T) {
	base := t.TempDir()
	mxEnv := []string{"MODEL_EXPRESS_URL=http://mx:8001"}
	fakeGPUProc(t, base, 5000, 1, 5000, []string{testShim}, nil)                 // API server: no GPU state
	fakeGPUProc(t, base, 5100, 2, 5000, []string{testShim, "/dev/nvidia0"}, nil) // worker, ModelExpress
	fakeGPUProc(t, base, 5200, 3, 5000, []string{"/dev/nvidia1"}, nil)           // GPU helper outside the library
	fakeGPUProc(t, base, 5300, 4, 5000, []string{testShim, "/dev/nvidia2"}, nil) // worker without ModelExpress
	fakeGPUProc(t, base, 7100, 2, 7000, []string{testShim, "/dev/nvidia3"}, nil) // another session
	for _, pid := range []int{5000, 5100, 5200, 7100} {
		fakeMXProc(t, base, pid, mxEnv, []string{"python3"})
	}
	got := mxPausable(base, 5000)
	if len(got) != 1 || !got[5100] {
		t.Fatalf("mxPausable = %v, want only 5100", got)
	}
}

func TestLastBytes(t *testing.T) {
	if got := lastBytes("abc", 5); got != "abc" {
		t.Errorf("lastBytes short = %q", got)
	}
	if got := lastBytes("Traceback… ValueError: boom", 16); got != "… ValueError: boom" {
		t.Errorf("lastBytes = %q, want the end kept", got)
	}
}

// The capture records that it paused ModelExpress, for its restore.
func TestGPUShareInfo_ModelExpressRoundTrip(t *testing.T) {
	for _, gs := range []GPUShareInfo{{PIDs: []int{7}}, {PIDs: []int{7}, ModelExpress: true}} {
		raw, err := json.Marshal(gs)
		if err != nil {
			t.Fatal(err)
		}
		var b GPUShareInfo
		if err := json.Unmarshal(raw, &b); err != nil {
			t.Fatal(err)
		}
		if !gs.ModelExpress && strings.Contains(string(raw), "modelExpress") {
			t.Errorf("%s: an unpaused capture's record names modelExpress", raw)
		}
		if b.ModelExpress != gs.ModelExpress {
			t.Errorf("ModelExpress %v came back %v", gs.ModelExpress, b.ModelExpress)
		}
	}
}

// A restored worker advertises its pod's own address only when the pod
// moved off the captured one and the tool can pass it (--host).
func TestRestoredMXHost(t *testing.T) {
	withHost := mxToolText + "  --host H: (mx-resume) the address peers pull the weights from\n"
	for _, tc := range []struct {
		name, pod, captured, text, want string
	}{
		{"moved, tool takes --host", "10.1.253.45", "10.1.245.116", withHost, "10.1.253.45"},
		{"same address", "10.1.245.116", "10.1.245.116", withHost, ""},
		{"pod address unknown", "", "10.1.245.116", withHost, ""},
		{"tool without --host", "10.1.253.45", "10.1.245.116", mxToolText, ""},
	} {
		if got := restoredMXHost(tc.pod, tc.captured, tc.text); got != tc.want {
			t.Errorf("%s: restoredMXHost = %q, want %q", tc.name, got, tc.want)
		}
	}
}
