// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// A capture can fail because the workload ended under it: the container's
// process exits between the agent finding it and CRIU seizing it, and CRIU
// reports a missing task ("The root task ... does not exist") or cgroup.
// That is the workload's doing, not a fault of nvsnap's: the dump's error
// says so (errWorkloadExited), which its outcome's reason
// (metrics.Reason: workload_exited) and the configuration's record carry.
// It is told from the process itself, never from CRIU's words: the
// container's process is gone, or another process took its pid. Only a
// dump that failed before CRIU dumped the container is checked (dumpV2):
// a dump that completed stopped the process itself, so a failure after it
// is not the workload's. A leave-running dump does not stop it: there, an
// end at any point is the workload's.
//
// The GPU tool runs in the workload's pid namespace (gpushare.go): when
// the workload's init exits, the kernel kills the tool with it, or the
// tool finds the workload's processes gone ("no reply") and exits a moment
// before the init has ended. The capture then says the workload exited,
// not that the tool failed or that the workload could not be resumed.

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/metrics"
)

// errWorkloadExited marks a dump that failed because the workload's
// process had ended.
var errWorkloadExited = errors.New(metrics.WorkloadExitedText)

// errNoGPUProcess is the error of a multi-node instance's pod that has no
// process on the GPU under the gpushare library to capture.
func errNoGPUProcess(session string) error {
	return fmt.Errorf("gpushare: fabric session %s, but no process of this pod runs on the GPU under %s", session, gpushareLibName)
}

// procStat is what the agent reads of a process's /proc/<pid>/stat.
type procStat struct {
	state   byte   // field 3: R, S, D, Z (zombie), X (dead), ...
	threads uint64 // field 20
	start   uint64 // field 22: clock ticks since boot
}

// readProcStat reads the stat of process pid under procBase.
func readProcStat(procBase string, pid int) (procStat, error) {
	b, err := os.ReadFile(filepath.Join(procBase, strconv.Itoa(pid), "stat"))
	if err != nil {
		return procStat{}, err
	}
	// The command name, in parentheses, may hold spaces and parentheses:
	// the fields that follow start after its last ')'.
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return procStat{}, fmt.Errorf("%s/%d/stat: no command name", procBase, pid)
	}
	f := strings.Fields(s[i+1:])
	// Fields 1 and 2 are the pid and the name: field 3 is f[0].
	const stateField, threadsField, startTimeField = 0, 20 - 3, 22 - 3
	if len(f) <= startTimeField {
		return procStat{}, fmt.Errorf("%s/%d/stat: %d fields after the name", procBase, pid, len(f))
	}
	var st procStat
	st.state = f[stateField][0]
	if st.threads, err = strconv.ParseUint(f[threadsField], 10, 64); err != nil {
		return procStat{}, err
	}
	if st.start, err = strconv.ParseUint(f[startTimeField], 10, 64); err != nil {
		return procStat{}, err
	}
	return st, nil
}

// procStartTime returns the start time of process pid under procBase, in
// clock ticks since boot (field 22 of /proc/<pid>/stat), which tells the
// process from a later one given the same pid.
func procStartTime(procBase string, pid int) (uint64, error) {
	st, err := readProcStat(procBase, pid)
	return st.start, err
}

// processEnded reports whether process pid, which started at start, has
// ended: its pid is free, or held by a later process, or it is a zombie
// whose every thread has exited (a zombie leader of threads that still
// run is a process that runs). A start of 0 (not read) or a stat that
// cannot be read for another reason tells nothing.
func processEnded(procBase string, pid int, start uint64) bool {
	if start == 0 {
		return false
	}
	st, err := readProcStat(procBase, pid)
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	if err != nil {
		return false
	}
	return st.start != start || (st.state == 'Z' || st.state == 'X') && st.threads <= 1
}

// workloadExitedErr is err, marked errWorkloadExited when process pid
// (which started at start) ended by the time its dump failed.
func workloadExitedErr(err error, procBase string, pid int, start uint64) error {
	if err == nil || errors.Is(err, errWorkloadExited) || !processEnded(procBase, pid, start) {
		return err
	}
	return markWorkloadExited(err)
}

// workloadExitedReplaced is the text of a workload that exited during a
// capture that had suspended it: the capture's driver replaces its pods
// as it does a source it could not resume (sourceMustBeReplacedText).
const workloadExitedReplaced = "the instance is replaced"

// exitedError is a capture's err once the workload is known to have
// exited during it, when err said the workload could not be resumed: the
// workload's exit is the cause, and err's account of it is kept as what
// the capture saw. It still matches err (errors.Is), so that its pods are
// replaced as before.
type exitedError struct {
	msg   string
	cause error
}

func (e *exitedError) Error() string   { return e.msg }
func (e *exitedError) Unwrap() []error { return []error{errWorkloadExited, e.cause} }

// markWorkloadExited marks err as the workload's exit. An err that said
// the workload could not be resumed no longer says so: it exited.
func markWorkloadExited(err error) error {
	if !errors.Is(err, errSourceLeftSuspended) {
		return fmt.Errorf("%w: %w", errWorkloadExited, err)
	}
	saw := strings.ReplaceAll(err.Error(), errSourceLeftSuspended.Error()+": ", "")
	saw = strings.ReplaceAll(saw, errSourceLeftSuspended.Error(), "")
	return &exitedError{
		msg:   fmt.Sprintf("%s (its process ended; the GPU tool, in its pid namespace, ended with it); %s; what the capture saw: %s", errWorkloadExited, workloadExitedReplaced, saw),
		cause: err,
	}
}

// workloadExitGrace bounds the wait for the workload's process to end
// once the GPU tool lost it (toolLostWorkload).
var workloadExitGrace = 5 * time.Second

// toolLostWorkload reports whether err is the GPU tool's failure when the
// workload's processes went away under it: it was killed by a signal (the
// kernel kills a pid namespace's processes when its init exits), or a
// process stopped answering it.
func toolLostWorkload(err error) bool {
	var te *toolError
	if !errors.As(err, &te) {
		return false
	}
	var ee *exec.ExitError
	if errors.As(te.err, &ee) && !ee.Exited() {
		return true
	}
	return strings.Contains(te.out, "err no reply") || strings.Contains(te.out, "Connection reset by peer")
}

// awaitEnded waits up to grace for process pid (started at start) to end.
func awaitEnded(procBase string, pid int, start uint64, grace time.Duration) bool {
	deadline := time.Now().Add(grace)
	for {
		if processEnded(procBase, pid, start) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// stoppedByDump reports whether the dump stopped the workload's process
// itself: a completed dump that does not leave it running.
func stoppedByDump(dumped, leaveRunning bool) bool { return dumped && !leaveRunning }

// initialStart reads process pid's start time when its dump begins. gone
// reports the process already ended (its stat does not exist); any other
// read error gives start 0 and gone false, which tells nothing.
func initialStart(procBase string, pid int) (start uint64, gone bool) {
	start, err := procStartTime(procBase, pid)
	if err != nil {
		return 0, errors.Is(err, os.ErrNotExist)
	}
	return start, false
}

// dumpFailure is the error of a dump that failed with err: marked
// errWorkloadExited when CRIU had not stopped the container (dumped false,
// stoppedByDump)
// and its process, pid (started at start), had ended, or was already gone
// when the dump began (gone).
//
// When the GPU tool lost the workload (toolLostWorkload), the process may
// still be on its way out: it is given workloadExitGrace to end.
func dumpFailure(err error, dumped bool, procBase string, pid int, start uint64, gone bool) error {
	if dumped || err == nil || errors.Is(err, errWorkloadExited) {
		return err
	}
	if gone {
		return markWorkloadExited(err)
	}
	if toolLostWorkload(err) && start != 0 {
		awaitEnded(procBase, pid, start, workloadExitGrace)
	}
	return workloadExitedErr(err, procBase, pid, start)
}
