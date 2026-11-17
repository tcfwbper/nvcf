// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// ModelExpress around a capture. A vLLM worker that loads its weights
// through ModelExpress keeps serving them to peers over NIXL/UCX; over
// RDMA it holds device files and mappings a checkpoint cannot save
// (checkpoint-restore/criu#267), and registrations that would not survive
// a restore anyway. The GPU tool's mx-pause has ModelExpress in each
// process drop that state, and fails while any RDMA device is still held
// (NCCL or NIXL KV over RDMA outside ModelExpress); mx-resume registers
// the weights again and publishes them under a new worker id. A capture
// pauses a ModelExpress worker right before its suspend, after the
// instance is held and the capture claimed, so a skipped or retried
// capture never pauses a serving source; every way the source runs on
// (a failed capture, a leave-running dump) resumes it. Its restore resumes
// it once its GPU launches run: mx refuses while they are held.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	// modelExpressEnv names the ModelExpress server a worker loads from.
	modelExpressEnv = "MODEL_EXPRESS_URL"
	// gpushareMXTimeout bounds an mx-pause or mx-resume: the tool waits
	// up to 120 s for each process's reply, the processes in parallel.
	gpushareMXTimeout = 3 * time.Minute
)

// errMXPauseRefused is an mx-pause that failed: a process still holds
// RDMA outside ModelExpress, or its ModelExpress is too old to pause.
var errMXPauseRefused = errors.New("ModelExpress could not release the worker's RDMA state")

// gpushareMXSupported reports whether the GPU tool (its binary's text,
// gpuToolText) has mx-pause and mx-resume. "" is false.
func gpushareMXSupported(toolText string) bool {
	return strings.Contains(toolText, "mx-pause") && strings.Contains(toolText, "mx-resume")
}

// usesModelExpress reports whether any of the processes pids (host
// numbering under procBase) loads its weights through ModelExpress: it has
// MODEL_EXPRESS_URL in its environment, or --load-format mx (or
// modelexpress) on its command line. A process that cannot be read counts as not.
func usesModelExpress(procBase string, pids []int) bool {
	for _, pid := range pids {
		base := filepath.Join(procBase, strconv.Itoa(pid))
		if env, err := os.ReadFile(filepath.Join(base, "environ")); err == nil {
			for _, kv := range bytes.Split(env, []byte{0}) {
				if k, v, ok := bytes.Cut(kv, []byte("=")); ok && string(k) == modelExpressEnv && len(v) > 0 {
					return true
				}
			}
		}
		if cmd, err := os.ReadFile(filepath.Join(base, "cmdline")); err == nil && loadFormatMX(strings.Split(string(cmd), "\x00")) {
			return true
		}
	}
	return false
}

// modelExpressLoadFormats are the --load-format values of the ModelExpress
// loader: "mx", and "modelexpress" as Dynamo workers name it.
var modelExpressLoadFormats = map[string]bool{"mx": true, "modelexpress": true}

// loadFormatMX reports whether args set vLLM's --load-format to the
// ModelExpress loader.
func loadFormatMX(args []string) bool {
	for i, a := range args {
		if a == "--load-format" && i+1 < len(args) && modelExpressLoadFormats[args[i+1]] {
			return true
		}
		if v, ok := strings.CutPrefix(a, "--load-format="); ok && modelExpressLoadFormats[v] {
			return true
		}
	}
	return false
}

// sessionPIDs are the processes of the session led by sessionHostPID
// (host numbering under procBase).
func sessionPIDs(procBase string, sessionHostPID int) []int {
	entries, err := os.ReadDir(procBase)
	if err != nil {
		return nil
	}
	var pids []int
	for _, e := range entries {
		pid, perr := strconv.Atoi(e.Name())
		if perr != nil {
			continue
		}
		if sid, serr := sessionID(procBase, pid); serr == nil && sid == sessionHostPID {
			pids = append(pids, pid)
		}
	}
	return pids
}

// gpushareMX runs mx-pause or mx-resume on pids (namespace pids) through
// the tool in the container whose init is hostPID; mx-resume with host
// passes it as the address ModelExpress advertises. A variable for tests.
var gpushareMX = func(ctx context.Context, hostPID int, verb, host string, pids []int) (string, error) {
	args := make([]string, 0, len(pids)+3)
	if host != "" {
		args = append(args, "--host", host)
	}
	args = append(append(args, verb), intsToStrings(pids)...)
	return gpushareTool(ctx, hostPID, gpushareMXTimeout, args...)
}

// gpushareMXHostSupported reports whether the GPU tool (its text) takes
// --host for mx-resume. A tool from before it answers "unknown command".
func gpushareMXHostSupported(toolText string) bool {
	return strings.Contains(toolText, "--host") && gpushareMXSupported(toolText)
}

// restoredMXHost is the address a restored ModelExpress worker advertises:
// its pod's own IP, podIP, when the restore moved it off the captured
// address and the tool can pass it. A restored pod keeps its captured
// address as a local one (RESTORE-POD-ADDRESS.md), so the process, left to
// itself, republishes that one, which a peer outside the instance cannot
// reach. "" leaves ModelExpress as it was.
func restoredMXHost(podIP, capturedIP, toolText string) string {
	if podIP == "" || podIP == capturedIP || !gpushareMXHostSupported(toolText) {
		return ""
	}
	return podIP
}

// pauseModelExpress has ModelExpress in pids release its RDMA state
// before a capture's suspend, when the workload (modelExpress) uses it and
// the GPU tool can (toolText). paused: it did, and the source must get
// resumeModelExpress whenever it runs on. A workload that does not use
// ModelExpress, or a tool without mx-pause, is not paused and captures as
// before (the preflight refuses RDMA it cannot release). A failed pause
// resumes what it paused and returns errMXPauseRefused with the tool's
// reason; the source has not been stopped.
func pauseModelExpress(ctx context.Context, modelExpress bool, toolText string, hostPID int, pids []int, log *logrus.Entry) (paused bool, err error) {
	if !modelExpress || len(pids) == 0 {
		return false, nil
	}
	if !gpushareMXSupported(toolText) {
		log.Info("gpushare: the workload loads its weights through ModelExpress, but the GPU tool has no mx-pause; capturing it as it is")
		return false, nil
	}
	t0 := time.Now()
	out, err := gpushareMX(ctx, hostPID, "mx-pause", "", pids)
	if err != nil {
		// Some processes may have paused before one refused: put them back.
		resumeModelExpress(ctx, hostPID, "", pids, log)
		return false, fmt.Errorf("%w: %w", errMXPauseRefused, err)
	}
	log.WithFields(logrus.Fields{"pids": pids, "duration": time.Since(t0).Round(time.Millisecond).String()}).
		Info("gpushare: ModelExpress released its RDMA state for the capture: " + lastLines(out, 1))
	return true, nil
}

// resumeModelExpress has ModelExpress in pids register its weights again
// and publish them. It runs to its end under its own deadline. A failure
// is logged, not returned: the worker serves either way, only no longer
// shares its weights with peers.
func resumeModelExpress(ctx context.Context, hostPID int, host string, pids []int, log *logrus.Entry) {
	t0 := time.Now()
	out, err := gpushareMX(context.WithoutCancel(ctx), hostPID, "mx-resume", host, pids)
	l := log.WithFields(logrus.Fields{"pids": pids, "host": host, "duration": time.Since(t0).Round(time.Millisecond).String()})
	if err != nil {
		l.WithError(err).Warn("gpushare: ModelExpress did not resume; the worker serves, but no longer shares its weights with peers")
		return
	}
	l.Info("gpushare: ModelExpress resumed: " + lastLines(out, 1))
}

// sourceRunsOn reports whether a capture's source runs on after it: its
// dump did not complete, or left it running.
func sourceRunsOn(dumped, leaveRunning bool) bool {
	return !dumped || leaveRunning
}

// resumeRestoredModelExpress resumes ModelExpress in a restore whose
// capture paused it (gs.ModelExpress), once the restored processes' GPU
// launches run: after open, or after a resume without the held gate. A
// node bundle whose tool has no mx-resume leaves it paused, said in the
// log: the worker serves, but does not share its weights with peers.
func resumeRestoredModelExpress(ctx context.Context, gs *GPUShareInfo, toolText string, toolPID int, host string, log *logrus.Entry) {
	if gs == nil || !gs.ModelExpress {
		return
	}
	if !gpushareMXSupported(toolText) {
		log.Warn("gpushare: the capture paused ModelExpress, but this node's GPU tool has no mx-resume; the worker serves, but does not share its weights with peers")
		return
	}
	if host == "" {
		log.Info("gpushare: ModelExpress resumes with the address it had; peers outside the instance may not reach it if the pod moved")
	}
	resumeModelExpress(ctx, toolPID, host, gs.PIDs, log)
}

// mxPausable are the processes of the container whose init is
// sessionHostPID (host numbering) that a capture's mx-pause addresses and
// that use ModelExpress: the GPU processes under the library, the ones
// the capture suspends (gpushareTargets). mx-pause checks the RDMA of
// these only.
func mxPausable(procBase string, sessionHostPID int) map[int]bool {
	out := map[int]bool{}
	for _, pid := range sessionPIDs(procBase, sessionHostPID) {
		dir := filepath.Join(procBase, strconv.Itoa(pid))
		shim, gpu, _ := scanMaps(filepath.Join(dir, "maps"))
		if !gpu {
			gpu = holdsNvidiaFD(filepath.Join(dir, "fd"))
		}
		if shim && gpu && usesModelExpress(procBase, []int{pid}) {
			out[pid] = true
		}
	}
	return out
}

// releasedByMXPause turns the RDMA findings of the processes in pausable
// (mxPausable) from blocking into information: the capture has
// ModelExpress in them release their RDMA right before its suspend, and
// fails there, before the instance is stopped, if any is still held (NCCL
// or NIXL KV outside ModelExpress). RDMA of any other process still
// blocks: mx-pause does not check it, and the dump would fail on it.
func releasedByMXPause(fs []PreflightFinding, pausable map[int]bool) []PreflightFinding {
	out := make([]PreflightFinding, len(fs))
	for i, f := range fs {
		if f.Check == "rdma" && f.Severity == preflightBlock && pausable[f.PID] {
			f.Severity = preflightInfo
			f.Detail += "; the process loads its weights through ModelExpress, which releases them (mx-pause) right before the suspend, " +
				"and the capture fails there, before the instance is stopped, if any RDMA device is still held"
		}
		out[i] = f
	}
	return out
}

// goResumeRestoredModelExpress runs resumeRestoredModelExpress off the
// restore's path: the worker serves without it, and a scale-up does not
// wait for its weights to be published again.
func goResumeRestoredModelExpress(ctx context.Context, gs *GPUShareInfo, toolText string, toolPID int, host string, log *logrus.Entry) {
	if gs == nil || !gs.ModelExpress {
		return
	}
	go resumeRestoredModelExpress(context.WithoutCancel(ctx), gs, toolText, toolPID, host, log)
}

// restoredMXHostFor is restoredMXHost for the placeholder of group, read
// only when the capture paused ModelExpress.
func (a *Agent) restoredMXHostFor(ctx context.Context, gs *GPUShareInfo, capturedIP string, group restoreV2Group, toolText string) string {
	if gs == nil || !gs.ModelExpress {
		return ""
	}
	return restoredMXHost(a.placeholderPodIP(ctx, group.Namespace, group.Pod, "", ""), capturedIP, toolText)
}
