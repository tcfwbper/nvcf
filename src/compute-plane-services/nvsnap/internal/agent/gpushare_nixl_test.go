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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

const nixlToolText = mxToolText + "  nixl-pause   (before suspend: ...)\n  nixl-resume  (after open: ...)\n"

// fakeNIXL records the tool calls in order, answers each with out[verb]
// (default: one engine that released its connector), and fails the verbs
// in fail.
func fakeNIXL(t *testing.T, out map[string]string, fail map[string]bool) *[]string {
	t.Helper()
	fakeControlSockets(t, nil) // every pid has a control socket
	var calls []string
	prev := gpushareNIXL
	gpushareNIXL = func(_ context.Context, hostPID int, verb, host string, pids []int) (string, error) {
		c := fmt.Sprintf("%s@%d %v", verb, hostPID, pids)
		if host != "" {
			c = "--host " + host + " " + c
		}
		calls = append(calls, c)
		o, ok := out[verb]
		if !ok {
			// One engine that released, or brought back, its connector.
			var b strings.Builder
			for _, p := range pids {
				if verb == "nixl-pause" {
					fmt.Fprintf(&b, "pid=%d nixl pause: ok engine paused, NIXL connector released on 1 worker(s), UCX 1.22.0\n", p)
				} else {
					fmt.Fprintf(&b, "pid=%d nixl resume: ok resumed as engine e2, side channel 10.0.0.9:5600, 0 request(s) prefilled here\n", p)
				}
			}
			o = b.String()
		}
		if fail[verb] {
			o = "pid=7 nixl pause: err 2 RDMA device file(s) still open or mapped\nnixl-pause failed: resuming every pid\n"
			return o, &toolError{args: append([]string{verb}, intsToStrings(pids)...), err: errors.New("exit status 1"), out: o}
		}
		return o, nil
	}
	t.Cleanup(func() { gpushareNIXL = prev })
	return &calls
}

func TestGPUShareNIXLSupported(t *testing.T) {
	for text, want := range map[string]bool{
		nixlToolText:        true,
		mxToolText:          false,
		"  nixl-pause only": false,
		"":                  false,
	} {
		if got := gpushareNIXLSupported(text); got != want {
			t.Errorf("gpushareNIXLSupported(%q) = %v, want %v", text, got, want)
		}
	}
}

// A connector was released when any process answered ok with something
// The pause released nothing only when every pid says so ("ok no ...").
func TestNIXLNothingPaused(t *testing.T) {
	for out, want := range map[string]bool{
		"pid=7 nixl pause: ok engine paused, NIXL connector released on 2 worker(s), UCX 1.22.0\npid=8 nixl pause: ok TP worker: NIXL connector released, UCX 1.22.0\n": false,
		"pid=7 nixl pause: ok no NIXL KV connector\npid=8 nixl pause: ok no Python\n":                                                                                   true,
		"pid=7 nixl pause: ok no NIXL KV connector\npid=8 nixl pause: ok TP worker: NIXL connector released":                                                            false,
		"pid=7 nixl pause: ok no NIXL KV connector\n":                                                                                                                   false, // pid 8 did not answer
		"": false,
		"warning: ok no thing\npid=8 nixl pause: ok no Python\n": false, // not a reply line for pid 7
	} {
		if got := nixlNothingPaused(out, []int{7, 8}); got != want {
			t.Errorf("nixlNothingPaused(%q) = %v, want %v", out, got, want)
		}
	}
}

func TestPauseNIXL(t *testing.T) {
	noConn := "pid=7 nixl pause: ok no NIXL KV connector\npid=8 nixl pause: ok no NIXL KV connector\n"
	for _, tc := range []struct {
		name       string
		optedIn    bool
		text       string
		out        string
		fail       bool
		wantPaused bool
		wantErr    bool
		wantCalls  []string
	}{
		{"not opted in", false, nixlToolText, "", false, false, false, nil},
		{"tool without nixl", true, mxToolText, "", false, false, false, nil},
		{"opted in, connector released", true, nixlToolText, "", false, true, false, []string{"nixl-pause@42 [7 8]"}},
		{"opted in, no connector", true, nixlToolText, noConn, false, false, false, []string{"nixl-pause@42 [7 8]"}},
		// The tool resumes every pid itself, but may be stopped before it
		// does: the agent resumes them too.
		{"refused", true, nixlToolText, "", true, false, true, []string{"nixl-pause@42 [7 8]", "nixl-resume@42 [7 8]"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := map[string]string{}
			if tc.out != "" {
				out["nixl-pause"] = tc.out
			}
			calls := fakeNIXL(t, out, map[string]bool{"nixl-pause": tc.fail})
			paused, err := pauseNIXL(context.Background(), tc.optedIn, tc.text, 42, []int{7, 8}, testLog())
			if (len(paused) > 0) != tc.wantPaused || (err != nil) != tc.wantErr {
				t.Errorf("pauseNIXL = %v, %v; want paused %v, err %v", paused, err, tc.wantPaused, tc.wantErr)
			}
			if tc.wantErr && (!errors.Is(err, errNIXLPauseRefused) || !strings.Contains(err.Error(), "RDMA device file") || sourceMustBeReplaced(err)) {
				t.Errorf("err = %v, want errNIXLPauseRefused with the tool's reason, source kept", err)
			}
			if !slices.Equal(*calls, tc.wantCalls) {
				t.Errorf("calls = %v, want %v", *calls, tc.wantCalls)
			}
		})
	}
}

// A refused pause whose resume also fails leaves an engine that takes no
// requests: the source is replaced.
func TestPauseNIXL_RefusedAndNotResumed(t *testing.T) {
	calls := fakeNIXL(t, nil, map[string]bool{"nixl-pause": true, "nixl-resume": true})
	paused, err := pauseNIXL(context.Background(), true, nixlToolText, 42, []int{7}, testLog())
	if len(paused) > 0 || !errors.Is(err, errNIXLPauseRefused) || !sourceMustBeReplaced(err) {
		t.Errorf("pauseNIXL = %v, %v; want errNIXLPauseRefused and the source replaced", paused, err)
	}
	if !slices.Equal(*calls, []string{"nixl-pause@42 [7]", "nixl-resume@42 [7]"}) {
		t.Errorf("calls = %v", *calls)
	}
}

// A source that runs on gets its connector back; when that fails it is
// reported as left suspended, so the caller replaces it, and the capture's
// own error is kept.
func TestResumeNIXLSource(t *testing.T) {
	captureErr := errors.New("dump failed")
	calls := fakeNIXL(t, nil, nil)
	if err := resumeNIXLSource(context.Background(), 42, []int{7}, []int{7}, captureErr, testLog()); !errors.Is(err, captureErr) || errors.Is(err, errSourceLeftSuspended) {
		t.Errorf("resume ok: err = %v, want the capture's own", err)
	}
	if !slices.Equal(*calls, []string{"nixl-resume@42 [7]"}) {
		t.Errorf("calls = %v (no --host on the source)", *calls)
	}

	fakeNIXL(t, nil, map[string]bool{"nixl-resume": true})
	err := resumeNIXLSource(context.Background(), 42, []int{7}, []int{7}, captureErr, testLog())
	if !errors.Is(err, errSourceLeftSuspended) || !errors.Is(err, captureErr) || !sourceMustBeReplaced(err) {
		t.Errorf("resume failed: err = %v, want errSourceLeftSuspended joined with the capture's error", err)
	}
	if err := resumeNIXLSource(context.Background(), 42, []int{7}, []int{7}, nil, testLog()); !errors.Is(err, errSourceLeftSuspended) {
		t.Errorf("resume failed after a good leave-running dump: err = %v, want errSourceLeftSuspended", err)
	}
}

// A restore resumes the connector only when its capture paused it, at the
// new pod's address, and fails when it cannot: the engine would take no
// requests.
func TestResumeRestoredNIXL(t *testing.T) {
	for _, tc := range []struct {
		name    string
		gs      *GPUShareInfo
		text    string
		host    string
		fail    bool
		wantErr bool
		want    []string
	}{
		{"no gpushare", nil, nixlToolText, "", false, false, nil},
		{"capture did not pause", &GPUShareInfo{PIDs: []int{7}, ModelExpress: true}, nixlToolText, "", false, false, nil},
		{"paused", &GPUShareInfo{PIDs: []int{7, 8}, NIXL: true}, nixlToolText, "", false, false, []string{"nixl-resume@99 [7 8]"}},
		{"paused, new pod address", &GPUShareInfo{PIDs: []int{7}, NIXL: true}, nixlToolText, "10.1.2.3", false, false, []string{"--host 10.1.2.3 nixl-resume@99 [7]"}},
		{"paused, resume fails", &GPUShareInfo{PIDs: []int{7}, NIXL: true}, nixlToolText, "", true, true, []string{"nixl-resume@99 [7]"}},
		{"paused, tool without nixl", &GPUShareInfo{PIDs: []int{7}, NIXL: true}, mxToolText, "", false, true, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := fakeNIXL(t, nil, map[string]bool{"nixl-resume": tc.fail})
			err := resumeRestoredNIXL(context.Background(), tc.gs, tc.text, 99, tc.host, testLog())
			if (err != nil) != tc.wantErr {
				t.Errorf("err = %v, want err %v", err, tc.wantErr)
			}
			if !slices.Equal(*calls, tc.want) {
				t.Errorf("calls = %v, want %v", *calls, tc.want)
			}
		})
	}
}

// The restored engine listens at its pod's new address, keeps the
// captured one only when the pod kept it, and is not resumed when the
// pod's address is unknown: the restore fails and starts cold.
func TestRestoredNIXLHost(t *testing.T) {
	for _, tc := range []struct {
		pod, captured, want string
		wantErr             bool
	}{
		{"10.1.253.45", "10.1.245.116", "10.1.253.45", false},
		{"10.1.245.116", "10.1.245.116", "", false},
		{"10.1.253.45", "", "10.1.253.45", false},
		{"", "10.1.245.116", "", true},
	} {
		got, err := restoredNIXLHost(tc.pod, tc.captured)
		if got != tc.want || (err != nil) != tc.wantErr {
			t.Errorf("restoredNIXLHost(%q, %q) = %q, %v; want %q, err %v", tc.pod, tc.captured, got, err, tc.want, tc.wantErr)
		}
	}
}

// Without the capture's pause there is nothing to resume, address or not;
// with it, an unknown address fails the restore before any resume.
// Before CRIU runs, a restore whose capture paused the connector checks
// it can bring it back: a GPU tool with nixl-resume, and the restored pod's
// address. Either missing is a restore that did not run here
// (errCheckpointUnavailable), which says nothing about the checkpoint.
func TestNIXLRestorePrecheck(t *testing.T) {
	a := &Agent{}
	gs := &GPUShareInfo{PIDs: []int{7}, NIXL: true}
	moved := restoreV2Group{InetAddrMap: "10.1.245.116=10.1.253.45"}
	for _, tc := range []struct {
		name     string
		gs       *GPUShareInfo
		tool     string
		group    restoreV2Group
		wantHost string
		notRun   bool
	}{
		{"not paused", &GPUShareInfo{PIDs: []int{7}}, mxToolText, restoreV2Group{}, "", false},
		{"paused, moved", gs, nixlToolText, moved, "10.1.253.45", false},
		{"paused, tool without nixl-resume", gs, mxToolText, moved, "", true},
		{"paused, pod address unknown", gs, nixlToolText, restoreV2Group{}, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			host, err := a.nixlRestorePrecheck(context.Background(), tc.gs, tc.tool, "10.1.245.116", tc.group)
			if host != tc.wantHost || (err != nil) != tc.notRun {
				t.Errorf("precheck = %q, %v; want %q, err %v", host, err, tc.wantHost, tc.notRun)
			}
			if tc.notRun && (!errors.Is(err, errCheckpointUnavailable) || !errors.Is(err, errNIXLResumeUnavailable)) {
				t.Errorf("err = %v, want a restore that did not run (errCheckpointUnavailable, errNIXLResumeUnavailable)", err)
			}
		})
	}
}

// Only the RDMA findings of opted-in processes stop blocking, and only
// when nixl-pause handles the pod: with ModelExpress too, or an engine that
// spans nodes, the pod is refused with the limit named.
func TestNIXLPauseFindings(t *testing.T) {
	in := []PreflightFinding{
		{Pod: "w", Check: "rdma", Severity: preflightBlock, PID: 7, Detail: "process 7 holds RDMA devices /dev/infiniband/uverbs0"},
		{Pod: "w", Check: "rdma", Severity: preflightBlock, PID: 9, Detail: "process 9 holds RDMA devices /dev/infiniband/uverbs0"},
		{Pod: "w", Check: "socket", Severity: preflightBlock, Detail: "mptcp"},
	}
	base := t.TempDir()
	fakeMXProc(t, base, 7, []string{"NVSNAP_NIXL_PAUSE=1"}, []string{"python3"})

	out := nixlPauseFindings(in, map[int]bool{7: true}, nixlPauseLimit(base, []int{7}, "", 1), "w", "engine")
	if out[0].Severity != preflightInfo || !strings.Contains(out[0].Detail, "nixl-pause") {
		t.Errorf("rdma finding of a pausable process = %+v, want info naming nixl-pause", out[0])
	}
	if out[1] != in[1] || out[2] != in[2] || len(out) != 3 {
		t.Errorf("other findings changed: %+v", out[1:])
	}
	if in[0].Severity != preflightBlock {
		t.Error("the input was changed")
	}
	if got := nixlPauseFindings(in, nil, "", "w", "engine"); !slices.Equal(got, in) {
		t.Errorf("no pausable process: findings changed: %+v", got)
	}

	for name, env := range map[string][]string{
		"ModelExpress": {"NVSNAP_NIXL_PAUSE=1", "MODEL_EXPRESS_URL=http://mx:8001"},
		"spans nodes":  {"NVSNAP_NIXL_PAUSE=1", "NVSNAP_GPUSHARE_FABRIC=1"},
	} {
		base := t.TempDir()
		fakeMXProc(t, base, 7, env, []string{"python3"})
		out := nixlPauseFindings(in, map[int]bool{7: true}, nixlPauseLimit(base, []int{7}, "", 1), "w", "engine")
		if out[0].Severity != preflightBlock {
			t.Errorf("%s: the rdma finding was downgraded: %+v", name, out[0])
		}
		last := out[len(out)-1]
		if len(out) != len(in)+1 || last.Check != "nixl-pause" || last.Severity != preflightBlock || !strings.Contains(last.Detail, "not supported") {
			t.Errorf("%s: findings = %+v, want one nixl-pause block naming the limit", name, out)
		}
	}
}

// The capture records that it paused the connector, for its restore.
func TestGPUShareInfo_NIXLRoundTrip(t *testing.T) {
	for _, gs := range []GPUShareInfo{{PIDs: []int{7}}, {PIDs: []int{7}, NIXL: true, NIXLPIDs: []int{7}}} {
		raw, err := json.Marshal(gs)
		if err != nil {
			t.Fatal(err)
		}
		var b GPUShareInfo
		if err := json.Unmarshal(raw, &b); err != nil {
			t.Fatal(err)
		}
		if !gs.NIXL && strings.Contains(string(raw), "nixl") {
			t.Errorf("%s: an unpaused capture's record names nixl", raw)
		}
		if b.NIXL != gs.NIXL || !slices.Equal(b.NIXLPIDs, gs.NIXLPIDs) {
			t.Errorf("NIXL %v %v came back %v %v", gs.NIXL, gs.NIXLPIDs, b.NIXL, b.NIXLPIDs)
		}
	}
}

// Any process of the container says the engine spans nodes: the fabric
// marker, a group size above one, a multi-node engine flag, or a capture
// that joins a multi-node session. A single-pod group, or an engine
// started without such a flag, does not.
func TestNIXLSpansNodes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		env     []string
		cmdline []string
		session string
		nodes   int
		spans   bool
	}{
		{"single node", []string{"PATH=/bin"}, []string{"vllm", "serve", "m"}, "", 0, false},
		{"fabric marker", []string{"NVSNAP_GPUSHARE_FABRIC=1"}, nil, "", 0, true},
		{"leader-worker group", []string{"LWS_GROUP_SIZE=2"}, nil, "", 0, true},
		{"group of one", []string{"LWS_GROUP_SIZE=1", "GROVE_PCSG_TEMPLATE_NUM_PODS=1"}, nil, "", 0, false},
		{"scaling group", []string{"GROVE_PCSG_TEMPLATE_NUM_PODS=3"}, nil, "", 0, true},
		{"headless engine", nil, []string{"vllm", "serve", "m", "--headless"}, "", 0, true},
		{"flag in a shell line", nil, []string{"/bin/sh", "-c", "vllm serve m --nnodes=2"}, "", 0, true},
		{"single node with a data-parallel address", nil, []string{"vllm", "serve", "m", "--data-parallel-address", "127.0.0.1"}, "", 0, false},
		{"--num-nodes 2", nil, []string{"vllm", "serve", "m", "--num-nodes", "2"}, "", 0, true},
		{"data-parallel leader of an engine spanning nodes", nil, []string{"vllm", "serve", "m", "--data-parallel-size=4", "--data-parallel-size-local", "2"}, "", 0, true},
		{"single node data-parallel engine", nil, []string{"vllm", "serve", "m", "--data-parallel-size", "2"}, "", 0, false},
		{"session, ranks on two nodes", nil, nil, "s1", 2, true},
		{"session, ranks on one node (with companions)", nil, nil, "s1", 1, false},
		{"session, driver does not say", nil, nil, "s1", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := t.TempDir()
			fakeGPUProc(t, base, 5000, 1, 5000, []string{testShim}, nil)
			fakeGPUProc(t, base, 5100, 2, 5000, []string{testShim, "/dev/nvidia0"}, nil)
			fakeMXProc(t, base, 5000, tc.env, tc.cmdline)
			fakeMXProc(t, base, 5100, nil, nil)
			fakeRetitledProc(t, base, 5100, "VLLM::EngineCore")
			why := nixlSpansNodes(base, sessionPIDs(base, 5000), tc.session, tc.nodes)
			if (why != "") != tc.spans {
				t.Errorf("nixlSpansNodes = %q, want spans %v", why, tc.spans)
			}
		})
	}
}

// A rank opted in to nixl-pause whose engine spans nodes by its spec is
// refused by the preflight even when its processes do not show it: a
// LeaderWorkerSet pod whose gpushare fabric is annotated "false" has no
// fabric marker in any process.
func TestPodPreflight_NIXLPauseRefusesAnEngineSpanningNodes(t *testing.T) {
	pod := func(ann map[string]string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "w", Namespace: "ns", Annotations: ann},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "engine", Env: []corev1.EnvVar{{Name: nixlPauseEnv, Value: "1"}}}}}}
	}
	spans := map[string]string{webhook.NIXLPauseAnnotation: "true", webhook.GPUShareFabricAnnotation: "false",
		"leaderworkerset.sigs.k8s.io/size": "2"}
	for _, tc := range []struct {
		name string
		on   bool
		ann  map[string]string
		want bool
	}{
		{"opted in, spans nodes", true, spans, true},
		{"switch off", false, spans, false},
		{"opted in, one node", true, map[string]string{webhook.NIXLPauseAnnotation: "true"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &Agent{config: Config{Webhook: WebhookConfig{NIXLPause: tc.on}}}
			if got := preflightBlocked(a.podPreflight(pod(tc.ann), "engine", true), "nixl-pause"); got != tc.want {
				t.Errorf("nixl-pause block = %v, want %v", got, tc.want)
			}
		})
	}
}

// The pause's outcome fails closed: a pod is taken as paused unless every
// pid answered "ok no ..." (no connector, no Python). Output the agent
// cannot read, or a pid that did not answer (the tool prints nothing for
// a pid without a control socket), counts as paused, so the source is
// resumed when it runs on and the checkpoint records the pause.
func TestPauseNIXL_UnclearOutputCountsAsPaused(t *testing.T) {
	for _, tc := range []struct {
		name string
		out  string
		want bool
	}{
		{"every pid without a connector", "pid=7 nixl pause: ok no NIXL KV connector\npid=8 nixl pause: ok no Python\n", false},
		{"one pid paused", "pid=7 nixl pause: ok no NIXL KV connector\npid=8 nixl pause: ok TP worker: NIXL connector released\n", true},
		{"no output", "", true},
		{"a pid did not answer", "pid=7 nixl pause: ok no NIXL KV connector\n", true},
		{"another format", "7: paused\n8: paused\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeNIXL(t, map[string]string{"nixl-pause": tc.out}, nil)
			paused, err := pauseNIXL(context.Background(), true, nixlToolText, 42, []int{7, 8}, testLog())
			if err != nil || (len(paused) > 0) != tc.want {
				t.Errorf("pauseNIXL = %v, %v; want %v, nil", paused, err, tc.want)
			}
		})
	}
}

// A resume succeeds only when every pid with a control socket answered ok:
// such a pid with no answer was not resumed, and an engine that is not
// resumed serves nothing. (A pid without a socket is not asked:
// TestNIXL_ProcessWithoutAControlSocket.)
func TestResumeNIXL_EveryPidAnswersOK(t *testing.T) {
	for _, tc := range []struct {
		name    string
		out     string
		wantErr bool
	}{
		{"every pid resumed", "pid=7 nixl resume: ok resumed as engine e2\npid=8 nixl resume: ok TP worker: NIXL connector up\n", false},
		{"not paused", "pid=7 nixl resume: ok not paused\npid=8 nixl resume 10.0.0.9: ok TP worker: NIXL connector up\n", false},
		{"a pid with a control socket did not answer", "pid=7 nixl resume: ok resumed as engine e2\n", true},
		{"no output", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeNIXL(t, map[string]string{"nixl-resume": tc.out}, nil)
			err := resumeNIXL(context.Background(), 42, "", []int{7, 8}, nil, testLog())
			if (err != nil) != tc.wantErr {
				t.Errorf("resumeNIXL = %v, want err %v", err, tc.wantErr)
			}
		})
	}
}

// fakeNIXLTool answers like the GPU tool: one line per pid it reached,
// nothing (and exit 0) for a pid without a control socket (socketless).
// It records the pids it was given.
func fakeNIXLTool(t *testing.T, socketless map[int]bool) *[][]int {
	t.Helper()
	var given [][]int
	prev := gpushareNIXL
	gpushareNIXL = func(_ context.Context, _ int, verb, _ string, pids []int) (string, error) {
		given = append(given, append([]int(nil), pids...))
		var b strings.Builder
		for _, p := range pids {
			if socketless[p] {
				continue
			}
			if verb == "nixl-pause" {
				fmt.Fprintf(&b, "pid=%d nixl pause: ok engine paused, NIXL connector released on 1 worker(s)\n", p)
			} else {
				fmt.Fprintf(&b, "pid=%d nixl resume: ok resumed as engine e2\n", p)
			}
		}
		return b.String(), nil
	}
	t.Cleanup(func() { gpushareNIXL = prev })
	return &given
}

// fakeControlSockets makes pids in socketless have no control socket.
func fakeControlSockets(t *testing.T, socketless map[int]bool) {
	t.Helper()
	prev := controlSocketPIDs
	controlSocketPIDs = func(_ int, pids []int) []int {
		var out []int
		for _, p := range pids {
			if !socketless[p] {
				out = append(out, p)
			}
		}
		return out
	}
	t.Cleanup(func() { controlSocketPIDs = prev })
}

// A process under the library that never used CUDA (a resource tracker,
// a shell wrapper, an API server) has no control socket, and the tool
// prints nothing for it. The pause and the resume address only the
// processes with one: such a process neither fails the resume nor makes a
// pod without a connector count as paused.
func TestNIXL_ProcessWithoutAControlSocket(t *testing.T) {
	socketless := map[int]bool{9: true}
	fakeControlSockets(t, socketless)
	given := fakeNIXLTool(t, socketless)
	if err := resumeNIXL(context.Background(), 42, "", []int{7, 8, 9}, nil, testLog()); err != nil {
		t.Errorf("resume with a socketless pid: %v", err)
	}
	paused, err := pauseNIXL(context.Background(), true, nixlToolText, 42, []int{7, 8, 9}, testLog())
	if err != nil || len(paused) == 0 {
		t.Errorf("pause = %v, %v; want paused", paused, err)
	}
	for _, g := range *given {
		if slices.Contains(g, 9) {
			t.Errorf("the tool was given the socketless pid: %v", g)
		}
	}
	// No process with a socket: nothing to pause; a restore that must
	// resume one cannot.
	fakeControlSockets(t, map[int]bool{7: true, 8: true, 9: true})
	if paused, err := pauseNIXL(context.Background(), true, nixlToolText, 42, []int{7, 8, 9}, testLog()); err != nil || len(paused) > 0 {
		t.Errorf("pause with no control socket = %v, %v; want not paused", paused, err)
	}
	if err := resumeNIXL(context.Background(), 42, "", []int{7, 8, 9}, nil, testLog()); err == nil {
		t.Error("resume with no control socket succeeded")
	}
}

// A pause that fails partway resumes every pid it addressed, each of
// which must answer: a pid that released its connector and then lost its
// control socket gets no answer, so the source is replaced rather than
// left with one process paused.
func TestPauseNIXL_FailedPauseRequiresEveryAddressedPID(t *testing.T) {
	fakeControlSockets(t, nil)
	var calls [][]int
	prev := gpushareNIXL
	gpushareNIXL = func(_ context.Context, _ int, verb, _ string, pids []int) (string, error) {
		calls = append(calls, append([]int(nil), pids...))
		if verb == "nixl-pause" {
			// 8 released its connector, then its socket went away.
			fakeControlSockets(t, map[int]bool{8: true})
			return "pid=7 nixl pause: err engine did not go idle\n", errors.New("exit status 1")
		}
		var b strings.Builder
		for _, p := range pids {
			if p != 8 {
				fmt.Fprintf(&b, "pid=%d nixl resume: ok not paused\n", p)
			}
		}
		return b.String(), nil
	}
	t.Cleanup(func() { gpushareNIXL = prev })
	_, err := pauseNIXL(context.Background(), true, nixlToolText, 42, []int{7, 8}, testLog())
	if !errors.Is(err, errNIXLPauseRefused) || !sourceMustBeReplaced(err) {
		t.Errorf("err = %v, want the refusal and the source replaced", err)
	}
	if len(calls) != 2 || !slices.Contains(calls[1], 8) {
		t.Errorf("calls = %v, want the resume to address pid 8", calls)
	}
}

// The pause records the pids whose connector it released, and the
// resume requires each of them to answer ok, whatever the socket scan
// says then: a paused pid without a control socket at the resume is
// still addressed, gets no answer, and the resume fails rather than
// report an engine that stays paused as resumed. A checkpoint from before
// the pids were recorded resumes the pids with a socket, as before.
func TestNIXL_ResumeRequiresEveryPausedPID(t *testing.T) {
	fakeNIXL(t, map[string]string{"nixl-pause": "pid=7 nixl pause: ok no NIXL KV connector\n" +
		"pid=8 nixl pause: ok engine paused, NIXL connector released on 1 worker(s)\n" +
		"pid=9 nixl pause: ok TP worker: NIXL connector released\n"}, nil)
	paused, err := pauseNIXL(context.Background(), true, nixlToolText, 42, []int{7, 8, 9}, testLog())
	if err != nil || !slices.Equal(paused, []int{8, 9}) {
		t.Fatalf("pause = %v, %v; want [8 9] paused", paused, err)
	}

	// At the resume, 9 has lost its control socket.
	socketless := map[int]bool{9: true}
	fakeControlSockets(t, socketless)
	given := fakeNIXLTool(t, socketless)
	if err := resumeNIXL(context.Background(), 42, "", []int{7, 8, 9}, paused, testLog()); err == nil {
		t.Error("resume succeeded with a paused pid that did not answer: the engine would stay paused while its pod reads Ready")
	}
	if len(*given) != 1 || !slices.Contains((*given)[0], 9) {
		t.Errorf("the tool was given %v, want the paused pid 9 addressed", *given)
	}
	gs := &GPUShareInfo{PIDs: []int{7, 8, 9}, NIXL: true, NIXLPIDs: paused}
	if err := resumeRestoredNIXL(context.Background(), gs, nixlToolText, 42, "", testLog()); err == nil {
		t.Error("restore resume succeeded with a paused pid that did not answer")
	}
	if err := resumeNIXLSource(context.Background(), 42, []int{7, 8, 9}, paused, nil, testLog()); !errors.Is(err, errSourceLeftSuspended) {
		t.Errorf("source resume = %v, want errSourceLeftSuspended", err)
	}

	// Every paused pid answers: the resume succeeds.
	fakeControlSockets(t, nil)
	fakeNIXLTool(t, nil)
	if err := resumeNIXL(context.Background(), 42, "", []int{7, 8, 9}, paused, testLog()); err != nil {
		t.Errorf("resume with every paused pid answering: %v", err)
	}

	// A checkpoint that did not record them: the pids with a socket.
	fakeControlSockets(t, socketless)
	fakeNIXLTool(t, socketless)
	if err := resumeRestoredNIXL(context.Background(), &GPUShareInfo{PIDs: []int{7, 8, 9}, NIXL: true}, nixlToolText, 42, "", testLog()); err != nil {
		t.Errorf("resume of a checkpoint without the paused pids: %v", err)
	}
}

// controlSocketPIDsIn reads the network namespace's unix sockets: a pid
// has a control socket when its library listens on @nvsnap-gpushare.<pid>.
func TestControlSocketPIDsIn(t *testing.T) {
	base := t.TempDir()
	if err := os.MkdirAll(filepath.Join(base, "42", "net"), 0o755); err != nil {
		t.Fatal(err)
	}
	unix := "Num       RefCount Protocol Flags    Type St Inode Path\n" +
		"0000000000000000: 00000002 00000000 00010000 0005 01 11111 @nvsnap-gpushare.7\n" +
		"0000000000000000: 00000002 00000000 00010000 0005 01 11112 @nvsnap-gpushare.80\n" +
		"0000000000000000: 00000002 00000000 00010000 0001 01 11113 /tmp/nvsnap-gpushare.8\n"
	if err := os.WriteFile(filepath.Join(base, "42", "net", "unix"), []byte(unix), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := controlSocketPIDsIn(base, 42, []int{7, 8, 9}); !slices.Equal(got, []int{7}) {
		t.Errorf("controlSocketPIDsIn = %v, want [7]", got)
	}
	// Unreadable: every pid, so a missing answer fails closed.
	if got := controlSocketPIDsIn(base, 43, []int{7, 8}); !slices.Equal(got, []int{7, 8}) {
		t.Errorf("unreadable namespace: %v, want every pid", got)
	}
}

// The shape a disaggregated vLLM prefill pod showed on GPUs: its API
// server (pid 1) maps the library and holds GPU device files but has no
// control socket; only the engine (pid 460) listens. The restore's
// nixl-resume reaches the engine alone, the tool prints its reply only,
// and the resume succeeds.
func TestNIXL_ResumeWithAnAPIServerWithoutASocket(t *testing.T) {
	base := t.TempDir()
	if err := os.MkdirAll(filepath.Join(base, "4242", "net"), 0o755); err != nil {
		t.Fatal(err)
	}
	unix := "Num       RefCount Protocol Flags    Type St Inode Path\n" +
		"0000000000000000: 00000002 00000000 00010000 0005 01 2201 @nvsnap-gpushare.460\n"
	if err := os.WriteFile(filepath.Join(base, "4242", "net", "unix"), []byte(unix), 0o644); err != nil {
		t.Fatal(err)
	}
	prev := controlSocketPIDs
	controlSocketPIDs = func(hostPID int, pids []int) []int { return controlSocketPIDsIn(base, hostPID, pids) }
	t.Cleanup(func() { controlSocketPIDs = prev })
	given := fakeNIXLTool(t, map[int]bool{1: true})
	gs := &GPUShareInfo{PIDs: []int{1, 460}, NIXL: true}
	if err := resumeRestoredNIXL(context.Background(), gs, nixlToolText, 4242, "10.245.18.115", testLog()); err != nil {
		t.Errorf("restore's nixl-resume: %v", err)
	}
	if len(*given) != 1 || !slices.Equal((*given)[0], []int{460}) {
		t.Errorf("the tool was given %v, want [[460]]", *given)
	}
}

// nixlPod is pod ns/w whose GPU container "engine" carries the
// annotation (annotated) and the webhook's marker in its spec (marked).
func nixlPod(annotated, marked bool) *corev1.Pod {
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "w", Namespace: "ns", Annotations: map[string]string{}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "sidecar"}, {Name: "engine"}}}}
	if annotated {
		p.Annotations[webhook.NIXLPauseAnnotation] = "true"
	}
	if marked {
		p.Spec.Containers[1].Env = []corev1.EnvVar{{Name: nixlPauseEnv, Value: "1"}}
	}
	return p
}

// The opt-in is what the cluster decided at admission: the switch, the
// annotation and the marker the webhook set in the container's spec. Not
// a process's environment: an image's own ENV does not opt in, and a
// process renamed with setproctitle does not opt out.
func TestNIXLOptInFromPod(t *testing.T) {
	for _, tc := range []struct {
		name          string
		on, ann, mark bool
		container     string
		want          bool
		whyHas        string
	}{
		{"opted in", true, true, true, "engine", true, ""},
		{"did not ask", true, false, false, "engine", false, ""},
		{"switch off", false, true, true, "engine", false, "does not enable"},
		{"admitted without the env", true, true, false, "engine", false, "admitted without"},
		{"env in the spec, no annotation", true, false, true, "engine", false, "not annotated"},
		{"another container", true, true, true, "sidecar", false, "admitted without"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, why := nixlOptInFromPod(tc.on, nixlPod(tc.ann, tc.mark), tc.container)
			if got != tc.want || (tc.whyHas == "") != (why == "") || !strings.Contains(why, tc.whyHas) {
				t.Errorf("nixlOptInFromPod = %v, %q; want %v, %q", got, why, tc.want, tc.whyHas)
			}
		})
	}
	a := &Agent{config: Config{Webhook: WebhookConfig{NIXLPause: true}}, kubeClient: fake.NewSimpleClientset()}
	if got, why := a.nixlOptIn(context.Background(), "ns", "w", "engine"); got || !strings.Contains(why, "cannot read") {
		t.Errorf("pod not found: %v, %q; want not opted in, saying why", got, why)
	}
}

// Not opted in, the capture runs no nixl-pause, whatever the processes'
// environment says.
func TestPauseNIXLFor_NotOptedInPausesNothing(t *testing.T) {
	base, _ := retitledSession(t, []string{"NVSNAP_NIXL_PAUSE=1"})
	calls := fakeNIXL(t, nil, nil)
	paused, err := (&Agent{}).pauseNIXLFor(context.Background(), false, "", base, 5000, "", 1, nixlToolText, 42, []int{2}, testLog())
	if err != nil || len(paused) > 0 || len(*calls) != 0 {
		t.Errorf("pauseNIXLFor = %v, %v, calls %v; want nothing paused", paused, err, *calls)
	}
}

// nixl-pause addresses the session's GPU processes under the library.
func TestNIXLGPUProcesses(t *testing.T) {
	base := t.TempDir()
	fakeGPUProc(t, base, 5000, 1, 5000, []string{testShim}, nil)                 // API server: no GPU state
	fakeGPUProc(t, base, 5100, 2, 5000, []string{testShim, "/dev/nvidia0"}, nil) // engine
	fakeGPUProc(t, base, 5200, 3, 5000, []string{"/dev/nvidia1"}, nil)           // GPU helper outside the library
	fakeGPUProc(t, base, 7100, 2, 7000, []string{testShim, "/dev/nvidia3"}, nil) // another session
	if got := nixlGPUProcesses(base, 5000); len(got) != 1 || !got[5100] {
		t.Errorf("nixlGPUProcesses = %v, want only 5100", got)
	}
}

// fakeRetitledProc gives process pid of base the environ a process has
// after setproctitle (vLLM names its engine and workers that way): the
// area is overwritten in place, mostly NULs with what is left of a few
// variables, and the webhook's markers are gone. Its cmdline is the title.
func fakeRetitledProc(t *testing.T, base string, pid int, title string) {
	t.Helper()
	dir := filepath.Join(base, fmt.Sprint(pid))
	environ := strings.Repeat("\x00", 4755) + "PATH=/usr/bin\x00UCX_TLS=rc\x00LD_LIBRARY_PATH=/usr/lib\x00"
	if err := os.WriteFile(filepath.Join(dir, "environ"), []byte(environ), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cmdline"), []byte(title+strings.Repeat(" ", 100)), 0o600); err != nil {
		t.Fatal(err)
	}
}

// retitledSession is a container (init 5000, the API server, no GPU state,
// its environment env) whose engine (5100, under the library, holding
// RDMA) was renamed with setproctitle, which wiped its environ.
func retitledSession(t *testing.T, env []string) (base string, rdma []PreflightFinding) {
	t.Helper()
	base = t.TempDir()
	fakeGPUProc(t, base, 5000, 1, 5000, []string{testShim}, nil) // the API server: no GPU state
	fakeGPUProc(t, base, 5100, 2, 5000, []string{testShim, "/dev/nvidia1"}, nil)
	fakeMXProc(t, base, 5000, env, []string{"/usr/bin/python3", "/usr/local/bin/vllm", "serve", "m"})
	fakeMXProc(t, base, 5100, nil, nil)
	fakeRetitledProc(t, base, 5100, "VLLM::EngineCore")
	return base, []PreflightFinding{{Pod: "w", Check: "rdma", Severity: preflightBlock, PID: 5100,
		Detail: "process 5100 (VLLM::EngineCor) holds RDMA devices /dev/infiniband/uverbs4"}}
}

// The preflight takes the opt-in from the pod, and the limits from the
// container: a renamed engine (no marker left in any process) is released
// when its pod opted in; an image's own ENV marker without the annotation
// is not an opt-in; a pod that asked but is not opted in is told why; an
// engine spanning nodes is refused even when only the init shows it.
func TestReleaseFindings_OptInFromThePod(t *testing.T) {
	for _, tc := range []struct {
		name    string
		on      bool
		pod     *corev1.Pod
		env     []string // the init's environment
		wantSev string
		nixlWhy string // a nixl-pause block naming this; "" none
	}{
		{"opted in, renamed engine", true, nixlPod(true, true), []string{"PATH=/bin"}, preflightInfo, ""},
		{"image ENV only", true, nixlPod(false, false), []string{"NVSNAP_NIXL_PAUSE=1"}, preflightBlock, ""},
		{"admitted without the env", true, nixlPod(true, false), nil, preflightBlock, "admitted without"},
		{"switch off", false, nixlPod(true, true), nil, preflightBlock, "does not enable"},
		{"spans nodes", true, nixlPod(true, true), []string{"NVSNAP_GPUSHARE_FABRIC=1"}, preflightBlock, "spans nodes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, rdma := retitledSession(t, tc.env)
			a := &Agent{config: Config{Webhook: WebhookConfig{NIXLPause: tc.on}}, kubeClient: fake.NewSimpleClientset(tc.pod)}
			fs := a.releaseFindings(context.Background(), rdma, nixlToolText, base, 5000, "ns", "w", "engine")
			if fs[0].Severity != tc.wantSev {
				t.Errorf("rdma finding %+v, want %s", fs[0], tc.wantSev)
			}
			got := ""
			for _, f := range fs {
				if f.Check == "nixl-pause" && f.Severity == preflightBlock {
					got = f.Detail
				}
			}
			if (tc.nixlWhy == "") != (got == "") || !strings.Contains(got, tc.nixlWhy) {
				t.Errorf("nixl-pause finding %q, want one naming %q", got, tc.nixlWhy)
			}
		})
	}
}

// preflightBlocked reports whether fs has a blocking finding of check.
func preflightBlocked(fs []PreflightFinding, check string) bool {
	for _, f := range fs {
		if f.Check == check && f.Severity == preflightBlock {
			return true
		}
	}
	return false
}

// bundledSession is retitledSession whose init (5000) also has the node
// bundle mounted, read-only, from the node's version "criu-bundle", whose
// GPU tool has nixl-pause; its engine (5100) holds an RDMA device.
func bundledSession(t *testing.T, env []string) (a *Agent, base string) {
	t.Helper()
	base, _ = retitledSession(t, env)
	root := t.TempDir()
	version := filepath.Join(root, webhook.BundleVersionsDir, "criu-bundle")
	if err := os.MkdirAll(version, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(version, gpushareToolName), []byte(nixlToolText), 0o755); err != nil {
		t.Fatal(err)
	}
	gpuToolTextCache.Delete("criu-bundle")
	t.Cleanup(func() { gpuToolTextCache.Delete("criu-bundle") })
	init := filepath.Join(base, "5000")
	if err := os.Symlink(filepath.Join(root, webhook.BundleVersionsDir), filepath.Join(init, "root")); err != nil {
		t.Fatal(err)
	}
	status := "Name:\tpython3\nNSpid:\t5000\t1\nCapEff:\t" + capsWorker + "\n"
	for f, c := range map[string]string{"status": status, "mountinfo": bundleRO} {
		if err := os.WriteFile(filepath.Join(init, f), []byte(c), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("/dev/infiniband/uverbs4", filepath.Join(base, "5100", "fd", "40")); err != nil {
		t.Fatal(err)
	}
	return &Agent{config: Config{Webhook: WebhookConfig{NIXLPause: true, HostBundleRoot: root}}, log: logrus.New()}, base
}

// The host preflight's container checks apply the release: an opted-in
// pod's engine RDMA is information, a pod that did not opt in is blocked.
// Drives containerFindings, which hostPreflight returns.
func TestContainerFindings_ReleasesAnOptedInEngine(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pod     *corev1.Pod
		wantSev string
	}{
		{"opted in", nixlPod(true, true), preflightInfo},
		{"not opted in", nixlPod(false, false), preflightBlock},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, base := bundledSession(t, nil)
			a.kubeClient = fake.NewSimpleClientset(tc.pod)
			fs := a.containerFindings(context.Background(), base, 5000, []int{5000, 5100}, "ns", "w", "engine")
			sev := ""
			for _, f := range fs {
				if f.Check == "rdma" && f.PID == 5100 {
					sev = f.Severity
				}
				if f.Check == "bundle" {
					t.Fatalf("bundle finding: %+v", f)
				}
			}
			if sev != tc.wantSev {
				t.Errorf("engine RDMA finding %q, want %q (findings %+v)", sev, tc.wantSev, fs)
			}
		})
	}
}

// The instance's preflight applies the capture's rule: opted-in ranks on
// two nodes are refused nixl-pause; one rank with its companions is not.
func TestCapturePreflight_NIXLRankNodes(t *testing.T) {
	rank := func(name, node string) *corev1.Pod {
		p := nixlPod(true, true)
		p.Name, p.Spec.NodeName = name, node
		p.Spec.Containers[1].Resources.Limits = corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}
		return p
	}
	companion := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "ns"}, Spec: corev1.PodSpec{NodeName: "n1",
		Containers: []corev1.Container{{Name: "etcd"}}}}
	a := &Agent{config: Config{Webhook: WebhookConfig{NIXLPause: true}}, kubeClient: fake.NewSimpleClientset(), log: logrus.New()}
	one := a.capturePreflight(context.Background(), []*corev1.Pod{rank("w0", "n1")}, []*corev1.Pod{companion},
		map[string]string{"etcd": "etcd"}, testLog())
	if preflightBlocked(one, "nixl-pause") {
		t.Errorf("one rank with a companion: %+v, want no nixl-pause block", one)
	}
	two := a.capturePreflight(context.Background(), []*corev1.Pod{rank("w0", "n1"), rank("w1", "n2")}, nil, nil, testLog())
	if !preflightBlocked(two, "nixl-pause") {
		t.Errorf("ranks on two nodes: %+v, want a nixl-pause block", two)
	}
	// Annotated but admitted without the marker (before the switch was
	// on): not opted in, so the capture does not pause them, and the
	// rule does not block them.
	unmarked := func(name, node string) *corev1.Pod {
		p := rank(name, node)
		p.Spec.Containers[1].Env = nil
		return p
	}
	notOpted := a.capturePreflight(context.Background(), []*corev1.Pod{unmarked("w0", "n1"), unmarked("w1", "n2")}, nil, nil, testLog())
	if preflightBlocked(notOpted, "nixl-pause") {
		t.Errorf("annotated ranks not opted in, on two nodes: %+v, want no nixl-pause block", notOpted)
	}
}

// A refused step builder still hands back a step, which fails with the
// refusal: a restore that ignored the builder's error and ran would fail
// its tail closed (marked not serving, for disposal), not call a nil
// step, which would crash the agent from the auto-restore goroutine.
func TestNIXLResumeFor_RefusedStepFailsClosed(t *testing.T) {
	toolPID := 0
	resume, err := (&Agent{}).nixlResumeFor(context.Background(), &GPUShareInfo{PIDs: []int{7}, NIXL: true}, mxToolText, "10.1.245.116",
		restoreV2Group{InetAddrMap: "10.1.245.116=10.1.253.45"}, &toolPID, testLog())
	if !errors.Is(err, errNIXLResumeUnavailable) {
		t.Fatalf("err = %v, want refused before CRIU", err)
	}
	tail, _ := recordingTail(false, true, false, nil)
	tail.resumeNIXL = resume
	err = tail.run()
	if !errors.Is(err, errNIXLResumeUnavailable) || !errors.As(err, new(*restoredNotServingError)) {
		t.Errorf("tail = %v, want the refusal, marked not serving", err)
	}
}

// restoreV2's NIXL step, as nixlResumeFor builds it: refused before CRIU
// when the node cannot resume it; otherwise a resume, run later, through
// the restored tree's tool pid (set after the build, by the GPU resume),
// at the address read up front.
func TestNIXLResumeFor_BuildsTheTailsStep(t *testing.T) {
	a := &Agent{}
	gs := &GPUShareInfo{PIDs: []int{7}, NIXL: true}
	moved := restoreV2Group{InetAddrMap: "10.1.245.116=10.1.253.45"}
	toolPID := 0
	if _, err := a.nixlResumeFor(context.Background(), gs, mxToolText, "10.1.245.116", moved, &toolPID, testLog()); !errors.Is(err, errNIXLResumeUnavailable) {
		t.Errorf("tool without nixl-resume: err = %v, want refused before CRIU", err)
	}
	calls := fakeNIXL(t, nil, nil)
	resume, err := a.nixlResumeFor(context.Background(), gs, nixlToolText, "10.1.245.116", moved, &toolPID, testLog())
	if err != nil {
		t.Fatal(err)
	}
	if len(*calls) != 0 {
		t.Fatalf("resumed at build time: %v", *calls)
	}
	toolPID = 99
	if err := resume(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(*calls, []string{"--host 10.1.253.45 nixl-resume@99 [7]"}) {
		t.Errorf("calls = %v, want the restored tree's resume at the new address", *calls)
	}
}
