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

// criu-v2 capture path: in-namespace CRIU dump.
//
// Instead of running "criu swrk" from the agent's own mount namespace
// (which requires Root/ExtMnt reconstruction, skip-mnt lists, and
// LD_LIBRARY_PATH gymnastics — the failure classes of the legacy path),
// criu-v2 runs the node bundle (criu + cuda_plugin.so + cuda-checkpoint),
// which the webhook mounts read-only in the target container
// (bundle_tools.go), and executes CRIU dump INSIDE the
// container's mnt/pid/net/ipc/uts namespaces via nsenter (full namespace
// fidelity — a partial join makes CRIU serialize the container's ipc/uts
// as foreign namespaces and recreate them isolated at restore, which
// breaks the GPU driver's resume). CRIU then sees the container's mount
// tree natively (no ExtMnt) and binds unix sockets in its own namespace
// on restore (no setns). No userspace interception is injected into the
// workload; the retired legacy engine needed a preload library and
// patched event-loop libraries for that.
//
// io_uring rings are dumped and restored by the bundled CRIU (quiesced
// ring state, SQPOLL, worker threads and the SQ-array identity map), so
// engines run with stock libuv and uvloop and no preload or event-loop
// environment override.
//
// The images land in <container>/opt/nvsnap-imgs (overlay upperdir) and are
// moved host-side into the standard checkpoint directory afterwards, so
// the artifact layout, metadata, rootfs-diff mirror, and upload contract
// are identical to the legacy path — NVCA and the server never know which
// engine ran. The staged /criu-bundle intentionally stays in the upperdir:
// the rootfs-diff replay delivers it into the restore container for free.

package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/containerd"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

// CapturePathCRIUV2 is the request value selecting the in-namespace CRIU
// engine. Kept as a plain string to match CheckpointRequest.CapturePath.
const CapturePathCRIUV2 = "criu-v2"

func isCRIUV2(capturePath string) bool {
	// criu-v2 is the only CRIU engine now: the legacy LD_PRELOAD injection
	// path is gone, so a plain "criu" request uses the in-namespace engine
	// too. "rootfs" (multi-GPU / arm) stays on the rootfs path — never route
	// it through dumpV2, which would stamp CapturePath=criu-v2 and make
	// restore call restoreV2 (needs a placeholder + /checkpoints mount that a
	// rootfs capture never has).
	if capturePath != "" {
		return capturePath == CapturePathCRIUV2 || capturePath == "criu"
	}
	// Unspecified capture on a CRIU-default node uses v2; a rootfs-default
	// node redirects to rootfs upstream (RootfsIsDefault) before reaching here.
	return os.Getenv("NVSNAP_CRIU_V2") != "0" && !RootfsIsDefault()
}

// v2ImagesDirInContainer is where CRIU writes images inside the container
// (lands in the overlay upperdir; moved to the checkpoint dir afterwards).
const v2ImagesDirInContainer = "/opt/nvsnap-imgs"

// v2BinDirInContainer is where the criu bundle is staged inside the
// container. It MUST be /criu-bundle: the bundle's criu is built with
// INTERP=/criu-bundle/lib/ld-linux-x86-64.so.2 and RUNPATH=/criu-bundle/lib
// (self-contained loader), so staging anywhere else makes execve fail with
// ENOENT inside the container (first criu-v2 e2e, 2026-07-12).
const v2BinDirInContainer = webhook.BundleToolsMountPath

// dumpFinal is what a dump leaves of the dumped tree.
type dumpFinal int

const (
	// dumpKill: the tree is killed (criu's default).
	dumpKill dumpFinal = iota
	// dumpLeaveRunning: the tree runs on.
	dumpLeaveRunning
)

func (f dumpFinal) flag() string {
	switch f {
	case dumpLeaveRunning:
		return "--leave-running"
	}
	return ""
}

func finalOf(leaveRunning bool) dumpFinal {
	if leaveRunning {
		return dumpLeaveRunning
	}
	return dumpKill
}

// dumpV2 runs CRIU dump inside the container's namespaces, with the
// node bundle mounted there. On success the image files have been moved into checkpointDir.
// resumeSource brings a source back after its dump, under the resume's own
// deadline (gpushareResumeDeadline), not the capture's context: that may
// have run out, which is often why the dump failed.
func resumeSource(ctx context.Context, hostPID int, pids []int, fabricDir string, syncOpen time.Duration, log *logrus.Entry) error {
	return gpushareResume(context.WithoutCancel(ctx), hostPID, pids, "", fabricDir, syncOpen, log)
}

// ownGPUShareStore makes the workload's chunk store (and the fabric
// session directory in it, when there is one) owned by the user the
// workload's process runs as, so it can write its GPU memory there. The
// store is the workload's mount point, which it cannot replace; the
// session directory is reached through its parent (fabricSessionDir).
func ownGPUShareStore(procBase string, hostPID int, root string, fabric *fabricSessionDir) error {
	uid, gid, err := procOwner(procBase, hostPID)
	if err != nil {
		return err
	}
	if err := os.Lchown(filepath.Join(root, strings.TrimPrefix(GPUShareStoreInContainer, "/")), uid, gid); err != nil {
		return err
	}
	if fabric != nil {
		return fabric.parent.root.Lchown(fabric.name, uid, gid)
	}
	return nil
}

// procOwner returns the real uid and gid of process pid.
func procOwner(procBase string, pid int) (uid, gid int, err error) {
	b, err := os.ReadFile(filepath.Join(procBase, strconv.Itoa(pid), "status"))
	if err != nil {
		return 0, 0, err
	}
	uid, gid = -1, -1
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		switch f[0] {
		case "Uid:":
			uid, err = strconv.Atoi(f[1])
		case "Gid:":
			gid, err = strconv.Atoi(f[1])
		}
		if err != nil {
			return 0, 0, err
		}
	}
	if uid < 0 || gid < 0 {
		return 0, 0, fmt.Errorf("no Uid/Gid in %d/status", pid)
	}
	return uid, gid, nil
}

// errSourceLeftSuspended marks a capture whose source workload could not
// be resumed: it holds no GPU memory and serves nothing, while its pod
// still reads Ready.
var errSourceLeftSuspended = errors.New("gpushare: the source workload could not be resumed and stays suspended")

// errSourceLeftLocked marks a capture whose dump was killed before CRIU
// released the workload's network lock (an nftables table in the pod's
// network namespace, named by a run id only CRIU knew): the workload no
// longer reaches its peers, and the caller replaces it.
var errSourceLeftLocked = errors.New("criu: the dump was killed before it released the workload's network lock")

// sourceMustBeReplaced reports whether a capture left its source unusable
// (suspended, or with its network locked): its pods are replaced.
func sourceMustBeReplaced(err error) bool {
	return errors.Is(err, errSourceLeftSuspended) || errors.Is(err, errSourceLeftLocked)
}

// sourceMustBeReplacedText is sourceMustBeReplaced on an error's text, as
// it comes back from another node's agent; a source that exited while
// suspended (markWorkloadExited) is replaced too.
func sourceMustBeReplacedText(s string) bool {
	return strings.Contains(s, errSourceLeftSuspended.Error()) || strings.Contains(s, errSourceLeftLocked.Error()) ||
		strings.Contains(s, errWorkloadExited.Error()) && strings.Contains(s, workloadExitedReplaced)
}

// runDump runs a CRIU dump (name args...) to its end, under its own
// deadline rather than its caller's context: killed half-way, CRIU leaves
// the workload stopped behind its network lock. A dump the deadline kills
// is reported as errSourceLeftLocked.
func runDump(ctx context.Context, deadline time.Duration, env []string, name string, args ...string) ([]byte, error) {
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deadline)
	defer cancel()
	cmd := exec.CommandContext(dctx, name, args...) //nolint:gosec // nsenter and CRIU, with arguments the agent builds
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil && dctx.Err() != nil {
		return out, fmt.Errorf("%w (after %s): %w", errSourceLeftLocked, deadline, err)
	}
	return out, err
}

// dumpV2Opts are the choices of one dumpV2.
type dumpV2Opts struct {
	LeaveRunning  bool
	FabricSession string
	// RankNodes: on how many nodes the instance's GPU ranks run
	// (CheckpointRequest.GPUShareRankNodes).
	RankNodes int
	// SyncOpen: the session's driver runs the open phase
	// (CheckpointRequest.GPUShareSyncOpen).
	SyncOpen bool
	// Freeze, for a member of a group capture, returns once every member
	// of the group is ready to be dumped (group_freeze.go); it runs right
	// before criu starts, so the members stop together.
	Freeze func(context.Context) error
	// HeldCgroup, for a held member, is its frozen container cgroup (or "")
	// once it is past the freeze point; ThawTarget thaws that container.
	HeldCgroup func() string
	ThawTarget func()
	// PostDumpDir, for a member of a group capture, is its post-dump
	// barrier: criu's post-dump hook waits there, the tree still stopped,
	// until every member is dumped (group_freeze.go).
	PostDumpDir string
	// Snapshot copies what a restore puts back beside the dump (the replay
	// mounts). dumpV2 runs it from criu's post-dump hook, the tree stopped,
	// so the copy matches the dumped memory (dump_snapshot.go); a dump whose
	// snapshot was not taken fails. criuSaves names the mountpoints the
	// dump saves itself, contents included (nil when it saves none).
	Snapshot func(criuSaves map[string]bool) error
	// nixlFence: the capture paused the NIXL KV connector, so its marker
	// fences the checkpoint from agents that cannot resume it
	// (pidNSMarkerName). dumpAroundSuspend sets it, through
	// sourceDump.nixlPaused, before the dump.
	nixlFence bool
}

func (a *Agent) dumpV2(ctx context.Context, containerInfo *containerd.ContainerInfo, checkpointDir string, o dumpV2Opts, log *logrus.Entry) (_ *GPUShareInfo, retErr error) {
	leaveRunning, fabricSession := o.LeaveRunning, o.FabricSession
	hostPID := int(containerInfo.PID)
	procBase := "/proc"
	if _, err := os.Stat("/host/proc"); err == nil {
		procBase = "/host/proc"
	}
	root := filepath.Join(procBase, strconv.Itoa(hostPID), "root")

	// A dump that fails before CRIU has dumped the container, with the
	// container's process ended, failed because the workload exited
	// (workload_exited.go). Once CRIU has dumped it, the dump itself
	// stopped the process, and a later failure is nvsnap's; unless the
	// dump left it running, when an end at any point is the workload's.
	// A process already gone here is the workload's exit too.
	initStart, initGone := initialStart(procBase, hostPID)
	dumped := false
	defer func() {
		retErr = dumpFailure(retErr, stoppedByDump(dumped, leaveRunning), procBase, hostPID, initStart, initGone)
	}()

	// 1. The tools run from the node bundle mounted read-only in the
	// container (bundle_tools.go), never from a copy in its filesystem. A
	// companion lacking them has them mounted into it (companion_tools.go).
	if err := a.ensureCompanionToolsIn(procBase, hostPID); err != nil {
		log.WithError(err).Warn("node bundle: cannot mount the tools into a pod without GPUs")
	}
	tools, err := bundleToolsOf(procBase, hostPID, a.bundleVersionsDir())
	if err != nil {
		return nil, fmt.Errorf("criu-v2: %w", err)
	}

	// From here the dump stops the source; a marker on the node names it
	// until the dump ends, for the next run of the agent should this one
	// stop mid-dump (capture_marker.go).
	if uid := containerInfo.Labels["io.kubernetes.pod.uid"]; uid != "" {
		clearMarker, merr := a.markCapturing(captureMarker{Namespace: containerInfo.Labels["io.kubernetes.pod.namespace"],
			Pod: containerInfo.Labels["io.kubernetes.pod.name"], UID: uid})
		if merr != nil {
			return nil, merr
		}
		defer clearMarker()
	} else {
		log.Warn("criu-v2: the container belongs to no pod; an agent restart mid-dump would not recover it")
	}

	// 3. NVIDIA device externals from the container's /dev view.
	externals, err := nvidiaDevExternals(filepath.Join(root, "dev"))
	if err != nil {
		return nil, fmt.Errorf("device externals: %w", err)
	}

	// 4. Dump target: the container's init, so CRIU dumps the whole pid
	// namespace and the restore recreates it fresh, every pid free by
	// construction; or, in a pod that shares one pid namespace, the
	// container's own process tree (pidns_subtree.go). CRIU runs from the
	// agent's own filesystem, never inside the workload's (pidns_capture.go,
	// docs/design/TOOLS-OUTSIDE-THE-WORKLOAD.md).
	targetHostPID := hostPID
	nsPID, err := nsPidOf(procBase, targetHostPID)
	if err != nil {
		return nil, fmt.Errorf("resolve ns pid of %d: %w", targetHostPID, err)
	}
	log.WithFields(logrus.Fields{
		"targetHostPID": targetHostPID,
		"nsPID":         nsPID,
		"externals":     len(externals),
	}).Info("criu-v2: dumping the container")

	// gpushare: when the workload runs under libnvsnap_gpushare.so, its GPU
	// state is saved by nvsnap-gpu-suspend before the dump and CRIU dumps the
	// CPU side only, without the CUDA plugin.
	gsPIDs, gsLib, gsOn, gsErr := gpushareTargets(procBase, targetHostPID)
	if gsErr != nil {
		return nil, gsErr
	}
	if fabricSession != "" && !gsOn {
		return nil, errNoGPUProcess(fabricSession)
	}
	if !gsOn {
		// A workload under the library is captured through it or not at
		// all: its configuration then falls back to the cachedir, never to
		// the CUDA plugin (gpushare_outside.go).
		if oerr := gpushareOutsideSession(procBase, targetHostPID); oerr != nil {
			return nil, oerr
		}
	}
	// A multi-node session's directory lives in the pod's store and must be
	// gone before the store is collected into the checkpoint.
	fabricDir, closeFabric := "", func() {}
	var fabric *fabricSessionDir
	if fabricSession != "" {
		s, closeFn, ferr := a.openFabricSession(fabricSession, containerInfo.Labels["io.kubernetes.pod.namespace"],
			containerInfo.Labels["io.kubernetes.pod.name"], containerInfo.Labels["io.kubernetes.pod.uid"])
		if ferr != nil {
			return nil, ferr
		}
		var once sync.Once
		fabric, fabricDir, closeFabric = s, s.container, func() { once.Do(closeFn) }
		defer closeFabric()
		log = log.WithField("fabricSession", fabricSession)
	}
	if gsOn {
		if !fileExists(filepath.Join(root, strings.TrimPrefix(GPUShareStoreInContainer, "/"))) {
			return nil, fmt.Errorf("gpushare: the workload loads %s but has no chunk store at %s "+
				"(the webhook mounts one into pods annotated %s=true)", gpushareLibName, GPUShareStoreInContainer, "nvsnap.io/gpushare")
		}
		if !tools.has(procBase, gpushareToolName) {
			return nil, fmt.Errorf("gpushare: %s is not in the pod's node bundle (version %s predates gpushare)", gpushareToolName, tools.version)
		}
		// The store is created by the kubelet, as root: an engine that runs
		// as another user cannot write its GPU memory there.
		if oerr := ownGPUShareStore(procBase, targetHostPID, root, fabric); oerr != nil {
			return nil, fmt.Errorf("gpushare: give the workload its chunk store: %w", oerr)
		}
	}
	nixlPaused, dump := fencedDumpSteps(o, func(o dumpV2Opts) (ran, done bool, cleanup func(), err error) {
		ran, done, cleanup, err = a.dumpPrepared(ctx, procBase, targetHostPID, checkpointDir, externals, leaveRunning, gsOn, o, log)
		dumped = done
		return ran, done, cleanup, err
	})
	return a.dumpAroundSuspend(ctx, a.sourceDumpFor(ctx, containerInfo, checkpointDir, o, sourceDump{
		on: gsOn, procBase: procBase, hostPID: hostPID, targetHostPID: targetHostPID, pids: gsPIDs, lib: gsLib,
		toolText: tools.gpuToolText(), bundle: tools.version, fabric: fabric, fabricDir: fabricDir,
		closeFabric: closeFabric, nixlPaused: nixlPaused, dump: dump,
	}, log), log)
}

// fencedDumpSteps returns a capture's sourceDump.nixlPaused and
// sourceDump.dump for options o and the dump itself (dump, given the
// options it runs with): once nixlPaused is called (the NIXL KV connector
// paused), the dump runs with o.nixlFence set, so its marker fences the
// checkpoint (pidNSMarkerName).
func fencedDumpSteps(o dumpV2Opts, dump func(dumpV2Opts) (ran, done bool, cleanup func(), err error)) (nixlPaused func(), run func() (ran, done bool, cleanup func(), err error)) {
	return func() { o.nixlFence = true }, func() (bool, bool, func(), error) { return dump(o) }
}

// sourceDumpFor completes d, the dump of containerInfo's container, with
// the capture's choices (o) and the production steps: the nixl-pause
// opt-in, read from the pod as admitted (nixlOptIn), the session and the
// nodes its GPU ranks run on, and the suspend, resume, fault watch and
// harvest. d carries the container's state and its dump.
func (a *Agent) sourceDumpFor(ctx context.Context, containerInfo *containerd.ContainerInfo, checkpointDir string, o dumpV2Opts, d sourceDump, log *logrus.Entry) sourceDump {
	d.leaveRunning, d.syncOpenAsked, d.postDumpDir = o.LeaveRunning, o.SyncOpen, o.PostDumpDir
	d.fabricSession, d.rankNodes, d.checkpointDir = o.FabricSession, o.RankNodes, checkpointDir
	if d.on {
		opted, why, pod, err := a.nixlOptInPod(ctx, containerInfo.Labels["io.kubernetes.pod.namespace"],
			containerInfo.Labels["io.kubernetes.pod.name"], containerInfo.Labels["io.kubernetes.container.name"])
		d.nixlOptIn, d.nixlRefusal = opted, nixlCaptureRefusal(a.nixlPauseOn(), opted, pod, err)
		if why != "" && d.nixlRefusal == "" {
			log.Warn("gpushare: not pausing the NIXL KV connector: " + why)
		}
	}
	d.suspend, d.resume, d.watchFaults = gpushareSuspend, resumeSource, startGPUFaultWatch
	d.collect = func(gpuMap string) error { return a.gpushareCollectStore(containerInfo, checkpointDir, gpuMap) }
	return d
}

// sourceDump is one capture's dump of its source, around the gpushare
// library's suspend when the workload runs under it (on): dumpV2 gathers
// it, dumpAroundSuspend runs it. Its function fields are the steps that act
// on the workload, which tests replace.
type sourceDump struct {
	on, leaveRunning bool
	// nixlOptIn: the pod opted in to nixl-pause (nixlOptIn).
	nixlOptIn bool
	// nixlRefusal: why the capture is refused before nixl-pause, from
	// the pod (nixlCaptureRefusal); "" when it is not.
	nixlRefusal string
	// nixlPaused, when set, is called once the NIXL KV connector is
	// paused, before the suspend and the dump: the dump then fences the
	// checkpoint (dumpV2Opts.nixlFence).
	nixlPaused func()
	// syncOpenAsked: the session's driver runs the open phase
	// (dumpV2Opts.SyncOpen).
	syncOpenAsked bool
	postDumpDir   string
	procBase      string
	// hostPID is the container's init; targetHostPID the dump's target.
	hostPID, targetHostPID int
	// pids are the GPU processes under the library (namespace pids), lib
	// the library's path.
	pids          []int
	lib           string
	toolText      string
	bundle        string
	fabric        *fabricSessionDir
	fabricDir     string
	fabricSession string
	// rankNodes: on how many nodes the instance's GPU ranks run (0: not
	// said).
	rankNodes     int
	checkpointDir string

	suspend     func(ctx context.Context, hostPID int, pids []int, fabricDir string, log *logrus.Entry) (string, bool, error)
	resume      func(ctx context.Context, hostPID int, pids []int, fabricDir string, syncOpen time.Duration, log *logrus.Entry) error
	watchFaults func(log *logrus.Entry) *gpuFaultWatch
	closeFabric func()
	collect     func(gpuMap string) error
	// dump prepares and runs the CRIU dump (dumpPrepared): ran is false
	// when it failed before CRIU ran, done is CRIU's completion. cleanup,
	// never nil, runs when dumpAroundSuspend returns, before the source is
	// settled by the suspend's guard.
	dump func() (ran, done bool, cleanup func(), err error)
}

// dumpAroundSuspend runs d. Under the library, in order: ModelExpress and
// the NIXL KV connector of a pod that opted in release their RDMA state
// (mx-pause, nixl-pause), the GPU processes are suspended, the tree is
// dumped, and the source, if it runs on, is resumed: its GPU first, then
// its NIXL KV connector (checked: a paused engine serves nothing), then
// ModelExpress. A source whose GPU resume failed is replaced
// (errSourceLeftSuspended) and gets neither.
func (a *Agent) dumpAroundSuspend(ctx context.Context, d sourceDump, log *logrus.Entry) (_ *GPUShareInfo, retErr error) {
	var gsInfo *GPUShareInfo
	var gpuMap string
	var guard suspendGuard
	// syncOpen: the source's in-place resume holds its GPU launches until
	// every node has resumed (syncOpenFor).
	var syncOpen time.Duration
	// A leave-running capture watches the node's kernel log from before
	// its suspend to after its resume (kmsg.go, resume_guard.go).
	var faults *gpuFaultWatch
	dumped := false
	// nixlResumed: the source's connector came back before the store's
	// harvest; the deferred rollback has nothing left to do.
	var nixlPaused []int
	nixlResumed := false
	if d.on {
		// ModelExpress drops its RDMA state, which the dump cannot save,
		// right before the suspend (gpushare_mx.go). Deferred before the
		// suspend's guard, its resume runs after the source's: mx refuses
		// while launches are held.
		modelExpress := usesModelExpress(d.procBase, sessionPIDs(d.procBase, d.targetHostPID))
		paused, mxErr := pauseModelExpress(ctx, modelExpress, d.toolText, d.hostPID, d.pids, log)
		if mxErr != nil {
			return nil, fmt.Errorf("gpushare: %w", mxErr)
		}
		if paused {
			defer func() {
				// Not on a source whose GPU resume failed: it is stopped,
				// and is replaced (errSourceLeftSuspended).
				if sourceRunsOn(dumped, d.leaveRunning) && !errors.Is(retErr, errSourceLeftSuspended) {
					resumeModelExpress(ctx, d.hostPID, "", d.pids, log)
				}
			}()
		}
		// The NIXL KV connector of a pod that opted in, likewise. Its
		// resume is checked: a paused engine takes no requests, so a
		// source that runs on and does not resume is replaced.
		var nixlErr error
		nixlPaused, nixlErr = a.pauseNIXLFor(ctx, d.nixlOptIn, d.nixlRefusal, d.procBase, d.targetHostPID, d.fabricSession, d.rankNodes, d.toolText, d.hostPID, d.pids, log)
		if nixlErr != nil {
			return nil, fmt.Errorf("gpushare: %w", nixlErr)
		}
		if len(nixlPaused) > 0 {
			if d.nixlPaused != nil {
				d.nixlPaused()
			}
			defer func() {
				if !nixlResumed && sourceRunsOn(dumped, d.leaveRunning) && !errors.Is(retErr, errSourceLeftSuspended) {
					retErr = resumeNIXLSource(ctx, d.hostPID, d.pids, nixlPaused, retErr, log)
				}
			}()
		}
		if d.leaveRunning {
			faults = d.watchFaults(log)
			defer faults.close()
		}
		syncOpen = syncOpenFor(d.leaveRunning, d.syncOpenAsked, d.fabric, d.toolText, d.bundle, log)
		gm, suspended, serr := d.suspend(ctx, d.hostPID, d.pids, d.fabricDir, log)
		if suspended {
			// The source is stopped with its GPU memory released from
			// here, even when the suspend's last step failed. The dump
			// settles it (settleSource); any return before the dump brings
			// it back through the guard. Deferred after the fabric
			// session's close, so it runs while the session is still open.
			guard.arm(func() error { return d.resume(ctx, d.hostPID, d.pids, d.fabricDir, syncOpen, log) }, func() {
				if d.postDumpDir != "" {
					markPostDumpFailed(d.postDumpDir, log)
				}
			})
			defer func() { retErr = guard.settle(retErr) }()
		}
		if serr != nil {
			return nil, fmt.Errorf("gpushare suspend: %w", serr)
		}
		gpuMap = gm
		gsInfo = &GPUShareInfo{PIDs: d.pids, StorePath: GPUShareStoreInContainer, LibPath: d.lib, ModelExpress: paused, NIXL: len(nixlPaused) > 0, NIXLPIDs: nixlPaused}
	}

	ran, done, cleanup, err := d.dump()
	defer cleanup()
	if !ran {
		return nil, err
	}
	dumped = done
	guard.handOff()
	resumeErr := settleSource(d.on, d.leaveRunning, err != nil, func() error {
		return d.resume(ctx, d.hostPID, d.pids, d.fabricDir, syncOpen, log)
	})
	// A workload left running is seen running again before the capture
	// succeeds: no GPU fault in the kernel log over the capture, and every
	// GPU process it suspended still there (resume_guard.go).
	var resumed error
	if d.leaveRunning && d.on {
		resumed = faults.check(driverGPUsDir, gpuMap, d.checkpointDir)
		if resumed == nil && err == nil && resumeErr == nil {
			resumed = membersAfterResume(d.procBase, d.targetHostPID, d.pids)
		}
	}
	if err != nil || resumeErr != nil || resumed != nil {
		return nil, errors.Join(resumed, err, resumeErr)
	}
	if d.on {
		// A source left running serves nothing until its connector is back:
		// resume it before the store's harvest, which may copy across
		// mounts.
		if len(nixlPaused) > 0 && d.leaveRunning {
			nixlResumed = true
			if rerr := resumeNIXLSource(ctx, d.hostPID, d.pids, nixlPaused, nil, log); rerr != nil {
				return nil, rerr
			}
		}
		d.closeFabric()
		if err := d.collect(gpuMap); err != nil {
			return nil, err
		}
	}
	log.Info("criu-v2: pid-namespace dump complete")
	return gsInfo, nil
}

// dumpPrepared prepares the dump's hooks (the snapshot, the post-dump
// barrier) and dumps the pid namespace of targetHostPID. ran is false when
// a preparation failed before CRIU ran; done is CRIU's completion. cleanup
// removes what it prepared.
func (a *Agent) dumpPrepared(ctx context.Context, procBase string, targetHostPID int, checkpointDir string, externals []string,
	leaveRunning, gsOn bool, o dumpV2Opts, log *logrus.Entry) (ran, done bool, cleanup func(), err error) {
	var cleanups []func()
	cleanup = func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}
	final := finalOf(leaveRunning)
	freeze := func() error {
		if o.Freeze == nil {
			return nil
		}
		return o.Freeze(ctx)
	}
	hooks := dumpHooksFor(o, freeze)
	// criu runs from the agent here: its action script sees the
	// agent's paths.
	var snapDir string
	if o.Snapshot != nil {
		snapDir = filepath.Join(checkpointDir, dumpHookDir)
		if merr := os.MkdirAll(snapDir, 0o755); merr != nil {
			return false, false, cleanup, fmt.Errorf("dump snapshot hook: %w", merr)
		}
		cleanups = append(cleanups, func() { _ = os.RemoveAll(snapDir) })
	}
	if o.PostDumpDir != "" {
		if merr := os.MkdirAll(o.PostDumpDir, 0o755); merr != nil {
			return false, false, cleanup, fmt.Errorf("post-dump barrier: %w", merr)
		}
		cleanups = append(cleanups, func() { _ = os.RemoveAll(o.PostDumpDir) })
	}
	if snapDir != "" || o.PostDumpDir != "" {
		dir := snapDir
		if dir == "" {
			dir = o.PostDumpDir
		}
		script := filepath.Join(dir, "action.sh")
		if werr := writeDumpActionScript(script, snapDir, o.PostDumpDir); werr != nil {
			return false, false, cleanup, fmt.Errorf("dump action script: %w", werr)
		}
		hooks.ActionScript = script
	}
	stopSnapshot := func() (bool, error) { return true, nil }
	if snapDir != "" {
		// The pid-namespace dump saves the tmpfs mounts it recreates
		// (pidns_capture.go); the snapshot need not copy them too.
		entries, external, gpu, merr := pidNSMounts(procBase, targetHostPID)
		if merr != nil {
			return false, false, cleanup, fmt.Errorf("dump snapshot: %w", merr)
		}
		saved := criuSavedTmpfs(entries, external, gpu)
		snap, oerr := openConfinedDir(snapDir)
		if oerr != nil {
			return false, false, cleanup, fmt.Errorf("dump snapshot hook: %w", oerr)
		}
		cleanups = append(cleanups, func() { _ = snap.Close() })
		stopSnapshot = serveSnapshot(ctx, snap, func() error { return o.Snapshot(saved) })
	}
	// The container's pid namespace, from its init, dumped from outside
	// with the agent's own CRIU (pidns_capture.go).
	// done is CRIU's completion, not the error: a marker that cannot be
	// written after CRIU dumped the container is nvsnap's failure.
	done, err = a.dumpPIDNamespace(ctx, procBase, targetHostPID, checkpointDir, externals, final, gsOn, hooks, log)
	if taken, serr := stopSnapshot(); err == nil && !taken {
		err = snapshotNotTaken(serr)
	}
	if err != nil && o.PostDumpDir != "" {
		// The others wait, stopped, for this member's dump; told it
		// failed, the group's driver aborts them now, and a fabric
		// resume, which needs them, can proceed.
		markPostDumpFailed(o.PostDumpDir, log)
	}
	return true, done, cleanup, err
}

// dumpHooksFor is the pid-namespace dump's hooks for a capture's options
// o, with freeze run before CRIU starts.
func dumpHooksFor(o dumpV2Opts, freeze func() error) pidnsDumpHooks {
	return pidnsDumpHooks{Before: freeze, FreezeCgroup: o.HeldCgroup, NIXLFence: o.nixlFence}
}

// settleSource brings back a source whose GPU state a dump suspended,
// whatever became of the dump: CRIU puts a failed dump's tasks back
// stopped, and a leave-running dump's tree holds no GPU memory until it is
// resumed, whether or not its images were then harvested. Only a whole
// dump of a tree CRIU went on to kill leaves nothing to resume. A resume
// that fails is reported as errSourceLeftSuspended, for the caller to
// replace the workload.
func settleSource(suspended, leaveRunning, dumpFailed bool, resume func() error) error {
	if !suspended || (!dumpFailed && !leaveRunning) {
		return nil
	}
	if err := resume(); err != nil {
		return resumeFailure("resume after the dump", err)
	}
	return nil
}

// syncOpenFor is the --sync-open of the source's resume: in place across
// nodes, the resume holds the GPU launches until every node has resumed
// (gpushare_fabric.go, fabricOpener). It declares that to the driver
// (holdsOpen) before the suspend, so that the driver waits for this member.
// Zero (no hold) on one node, for a dump that does not resume in place,
// when the driver did not ask for it (driverOpens: a driver from before the
// open phase never answers, and the hold would only delay the engine by
// gpushareSyncOpenWait and fail the capture), and with a tool from before
// --sync-open; the last two are logged.
func syncOpenFor(leaveRunning, driverOpens bool, fabric *fabricSessionDir, toolText, bundle string, log *logrus.Entry) time.Duration {
	if !leaveRunning || fabric == nil {
		return 0
	}
	if !driverOpens {
		log.Warn("gpushare: the fabric session's driver does not open the GPU launches once every node has resumed (an older agent); this node lets them through at its own resume")
		return 0
	}
	if !gpushareSyncOpenSupported(toolText) {
		log.Warnf("gpushare: the GPU tool of node bundle %s cannot hold the GPU launches until every node has resumed (no --sync-open); this node lets them through at its own resume", bundle)
		return 0
	}
	fabric.holdsOpen.Store(true)
	return gpushareSyncOpenWait
}

// resumeFailure is how a failed resume left the source: suspended, for the
// caller to replace, unless the resume ran it again and only found it
// stalled (its GPU launches let through before every node resumed).
func resumeFailure(what string, err error) error {
	if errors.Is(err, errResumeStalled) {
		return fmt.Errorf("%s: %w", what, err)
	}
	return fmt.Errorf("%w: %s: %w", errSourceLeftSuspended, what, err)
}

// suspendGuard settles a source the capture suspended when the capture
// returns before its dump could: a preparation step between the suspend
// and the dump that fails (no space for the snapshot hook, an action
// script that cannot be written) would otherwise leave the workload
// stopped with its GPU memory released, and its pod still Ready. Armed
// right after the suspend and handed off to the dump's own settlement
// (settleSource) once the dump has run.
type suspendGuard struct {
	resume func() error
	// abort tells a group capture's driver this member failed, so the
	// other members stop waiting for it (and a fabric resume can run).
	abort func()
}

func (g *suspendGuard) arm(resume func() error, abort func()) { g.resume, g.abort = resume, abort }

// handOff disarms the guard: the dump settles the source from here.
func (g *suspendGuard) handOff() { g.resume, g.abort = nil, nil }

// settle resumes a source no dump settled and returns the capture's err,
// joined with a failed resume as errSourceLeftSuspended for the caller to
// replace the workload.
func (g *suspendGuard) settle(err error) error {
	if g.resume == nil {
		return err
	}
	resume, abort := g.resume, g.abort
	g.handOff()
	if abort != nil {
		abort()
	}
	if rerr := resume(); rerr != nil {
		return errors.Join(err, resumeFailure("resume after the capture failed before its dump", rerr))
	}
	return err
}

// gpuDevPatterns are the character devices a GPU workload may hold open that
// CRIU cannot dump itself and must be told to treat as external.
//
// gdrdrv is the GPUDirect RDMA (gdrcopy) node. It does not match nvidia*, so
// globbing only that prefix left it undeclared and any workload holding an fd
// on it failed the dump with "Can't dump file N of that type (chr 506:0)" --
// observed on NIM, which uses gdrcopy where the other engines do not. Match on
// the device names rather than major numbers: the NVIDIA majors are
// dynamically allocated and differ per node (nvidia-uvm was 507 on one host
// and is documented as 511 elsewhere), while the names are stable.
var gpuDevPatterns = []string{"nvidia*", "gdrdrv"}

// nvidiaDevExternals builds CRIU --external dev[maj/min]:name entries for
// every GPU character device visible in the container, none for a
// container without GPUs.
func nvidiaDevExternals(devDir string) ([]string, error) {
	var matches []string
	for _, pat := range gpuDevPatterns {
		m, err := filepath.Glob(filepath.Join(devDir, pat))
		if err != nil {
			return nil, fmt.Errorf("glob GPU device pattern %q in %s: %w", pat, devDir, err)
		}
		matches = append(matches, m...)
	}
	var exts []string
	for _, m := range matches {
		// The device nodes themselves: /dev is the workload's to write, and
		// a link there names nothing the dump should declare.
		var st syscall.Stat_t
		if err := syscall.Lstat(m, &st); err != nil {
			continue
		}
		if st.Mode&syscall.S_IFMT != syscall.S_IFCHR {
			continue
		}
		maj := uint32(st.Rdev >> 8 & 0xfff) //nolint:gosec // masked device-number bits (same idiom as scanCharDeviceFDs)
		min := uint32(st.Rdev & 0xff)       //nolint:gosec // masked device-number bits
		exts = append(exts, fmt.Sprintf("dev[%d/%d]:%s", maj, min, filepath.Base(m)))
	}
	// A container without GPUs (an instance's companion) has none to
	// declare.
	return exts, nil
}

// sessionID returns the session id (host pid view) of pid.
func sessionID(procBase string, pid int) (int, error) {
	b, err := os.ReadFile(filepath.Join(procBase, strconv.Itoa(pid), "stat"))
	if err != nil {
		return 0, err
	}
	// field 6 (1-indexed) after the comm field; comm may contain spaces —
	// cut at the closing paren first.
	s := string(b)
	i := strings.LastIndex(s, ") ")
	if i < 0 {
		return 0, fmt.Errorf("malformed stat")
	}
	fields := strings.Fields(s[i+2:])
	if len(fields) < 4 {
		return 0, fmt.Errorf("short stat")
	}
	return strconv.Atoi(fields[3]) // sid
}

// nsPidOf returns pid as seen inside its innermost pid namespace (last
// entry of the NSpid line).
func nsPidOf(procBase string, pid int) (int, error) {
	b, err := os.ReadFile(filepath.Join(procBase, strconv.Itoa(pid), "status"))
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "NSpid:") {
			f := strings.Fields(line)
			if len(f) < 2 {
				break
			}
			return strconv.Atoi(f[len(f)-1])
		}
	}
	return 0, fmt.Errorf("no NSpid line for %d", pid)
}

func tailOfFile(path string, n int) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return "(no " + filepath.Base(path) + ")"
	}
	return tailOfText(string(b), n)
}

// tailOfText is tailOfFile on a log's content.
func tailOfText(content string, n int) string {
	lines := strings.Split(strings.TrimSpace(content), "\n")
	// CRIU's first errors name the cause; the tail is how it gave up
	// ("Unfreezing tasks", "Dumping FAILED"). Report both.
	var first []string
	for i, l := range lines {
		if i >= len(lines)-n {
			break
		}
		if strings.Contains(l, "Error (") {
			first = append(first, l)
			if len(first) == 3 {
				break
			}
		}
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	tail := strings.Join(lines, " | ")
	if len(first) > 0 {
		return "first errors: " + strings.Join(first, " | ") + " ... " + tail
	}
	return tail
}

func criuSupportsDirectImageIO(help string) bool {
	return strings.Contains(help, "--image-io-mode")
}

// gpushareCollectStore moves the workload's chunk store into the checkpoint
// (checkpointDir/gpushare). Entries are moved, not the directory: the
// workload's mount still points at that directory, and a later capture of
// the same pod must start empty rather than write into this checkpoint.
func (a *Agent) gpushareCollectStore(containerInfo *containerd.ContainerInfo, checkpointDir, gpuMap string) error {
	dst := filepath.Join(checkpointDir, GPUShareCheckpointSubdir)
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return fmt.Errorf("gpushare: %w", err)
	}
	// The pod's store as the agent sees it: same filesystem as the
	// checkpoint dir, so each entry moves by rename. Fall back to the
	// container's view (a copy across mounts) when the pod uid is unknown.
	// The workload writes the store: it is harvested through descriptors,
	// files only (confined_fs.go).
	var src *confinedDir
	if uid := containerInfo.Labels["io.kubernetes.pod.uid"]; uid != "" {
		if d, err := openConfinedDir(filepath.Join(a.config.CheckpointDir, GPUSharePodStoresSubdir, uid)); err == nil {
			src = d
		}
	}
	if src == nil {
		d, err := openWorkloadDir(hostProcBase(), int(containerInfo.PID), GPUShareStoreInContainer)
		if err != nil {
			return fmt.Errorf("gpushare: open the chunk store: %w", err)
		}
		src = d
	}
	defer func() { _ = src.Close() }()
	if err := harvestInto(src, dst); err != nil {
		return fmt.Errorf("gpushare: move chunk store %s to the checkpoint: %w", src.host, err)
	}
	// The GPU map goes in from the agent's side, into the checkpoint it owns:
	// the agent cannot create files inside a container's mounts through
	// /proc/<pid>/root. Restore finds it at <store>/gpus because the
	// placeholder mounts this directory at the store path.
	if err := os.WriteFile(filepath.Join(dst, gpushareGPUMapFile), []byte(gpuMap), 0o644); err != nil { //nolint:gosec // GPU UUIDs, read back by the restore tool
		return fmt.Errorf("gpushare: write GPU map: %w", err)
	}
	return nil
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// defaultGhostLimit is the largest deleted file a dump saves by default.
// Engines keep large buffers in deleted shared-memory files: a vLLM CPU
// KV-cache tier (--kv-offloading-size) is one such file, hundreds of GiB,
// part of the engine's state like its GPU memory.
const defaultGhostLimit int64 = 1 << 40

// ghostLimit is the agent's criu --ghost-limit.
func (a *Agent) ghostLimit() int64 {
	if a.config.CRIUGhostLimit > 0 {
		return a.config.CRIUGhostLimit
	}
	return defaultGhostLimit
}
