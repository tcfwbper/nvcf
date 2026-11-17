// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

// gpushareOutsideSession refuses the dump of a container whose GPU
// processes run under the gpushare library but outside its init's session
// (a process started with setsid), where gpushareTargets looks. Without
// it the dump would go through the CUDA plugin, which a workload under
// the library is never captured with: the capture fails instead, and the
// configuration falls back to the cachedir. nil when no process of the
// container runs on the GPU under the library.
func gpushareOutsideSession(procBase string, initHostPID int) error {
	cg, err := processCgroup(procBase, initHostPID)
	if err != nil {
		return nil // nothing to tell; the dump goes on as before
	}
	pids, err := cgroupPIDs(filepath.Join(cgroupRoot(), cg))
	if err != nil {
		return nil
	}
	if outside := shimGPUProcesses(procBase, pids); len(outside) > 0 {
		return fmt.Errorf("gpushare: process(es) %s run on the GPU under %s outside the container's session; "+
			"the capture saves GPU memory through the library only for the session of the container's first process "+
			"(start the engine without setsid), so it is refused rather than dumped another way", strings.Join(outside, ","), gpushareLibName)
	}
	return nil
}

// shimGPUProcesses are the processes of pids (host numbering) that map the
// gpushare library and use the GPU.
func shimGPUProcesses(procBase string, pids []int) []string {
	var out []string
	for _, pid := range pids {
		dir := filepath.Join(procBase, strconv.Itoa(pid))
		shim, gpu, _ := scanMaps(filepath.Join(dir, "maps"))
		if !gpu {
			gpu = holdsNvidiaFD(filepath.Join(dir, "fd"))
		}
		if shim && gpu {
			out = append(out, strconv.Itoa(pid))
		}
	}
	return out
}
