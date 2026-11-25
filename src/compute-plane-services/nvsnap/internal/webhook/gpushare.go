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

package webhook

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// gpushare: pods annotated nvsnap.io/gpushare=true run their GPU containers
// under libnvsnap_gpushare.so, so the agent can checkpoint processes that
// share GPU memory (NCCL P2P and NVLS, CUDA IPC). The library must be loaded
// from process start, which only admission can arrange. See docs/design/GPUSHARE.md
// and internal/agent/gpushare.go.
const (
	// GPUShareAnnotation opts a pod in.
	GPUShareAnnotation = "nvsnap.io/gpushare"
	// GPUShareFabricAnnotation marks a pod whose GPU memory is shared with
	// pods on other nodes over multi-node NVLink. The library then shares
	// that memory as fabric handles, which a multi-node suspend can
	// re-establish (see the agent's group checkpoint).
	GPUShareFabricAnnotation = "nvsnap.io/gpushare-fabric"
	// GPUShareIBVAnnotation ("true") sets NCCL_IBVERBS_LIB to
	// libnvsnap_ibv.so in each GPU container that gets the gpushare library
	// (nvsnap.io/gpushare, gpusharePatches) and does not set that variable
	// in its env, so that a pod whose NCCL uses RDMA (IB or RoCE) can be
	// captured: the library destroys NCCL's RDMA state before the checkpoint
	// and creates it again after the restore (docs/design/GPUSHARE.md).
	// "false" opts a pod out of Mutator.GPUShareIBVByDefault; unset follows
	// it (off unless the chart sets webhook.gpushareIBV), except in a
	// container whose envFrom may set the variable (admission does not read
	// ConfigMaps or Secrets): only "true" overrides that. A container that
	// already mounts something other than the selected node bundle at
	// /nvsnap gets no path, annotated or not: a library that is not there
	// would turn NCCL's RDMA off.
	GPUShareIBVAnnotation = "nvsnap.io/gpushare-ibv"
	// GPUShareIBVLibPath is that library, in the node bundle.
	GPUShareIBVLibPath = nvsnapToolsMountPath + "/libnvsnap_ibv.so"
	// GPUShareStorePath is where a GPU container sees its chunk store: a
	// per-pod directory on the node's local checkpoint disk. A restore
	// placeholder mounts the checkpoint's copy at the same path. It is a
	// top-level directory on purpose: the agent reaches it through
	// /proc/<pid>/root, where an absolute symlink on the way (/var/run is
	// one in many images) resolves against the agent's root instead.
	GPUShareStorePath = "/nvsnap-gpushare"
	// GPUShareLibPath is the preloaded library, from the node bundle the
	// agent DaemonSet stages and mounts at nvsnapToolsMountPath.
	GPUShareLibPath = nvsnapToolsMountPath + "/libnvsnap_gpushare.so"
	// DefaultGPUShareHostRoot is the node directory holding the per-pod
	// stores (the agent's checkpoint root plus gpushare-pods).
	DefaultGPUShareHostRoot = "/var/lib/containerd/nvsnap-checkpoints/gpushare-pods"

	gpushareLibVolume   = "nvsnap-gpushare-lib"
	gpushareStoreVolume = "nvsnap-gpushare-store"
	gpusharePodUIDEnv   = "NVSNAP_POD_UID"
	gpushareFabricEnv   = "NVSNAP_GPUSHARE_FABRIC"
	ncclIBVerbsLibEnv   = "NCCL_IBVERBS_LIB"

	// NIXLPauseAnnotation opts a gpushare pod in to nixl-pause when it is
	// "true" and webhook.nixlPause.enabled is on (default off; alone, the
	// annotation does nothing). Its NIXL KV connector then releases its
	// RDMA before the suspend and comes back after the restore
	// (docs/design/NIXL-PAUSE.md; Mutator.NIXLPause).
	NIXLPauseAnnotation = "nvsnap.io/nixl-pause"
	// nixlPauseEnv tells the agent the process's pod opted in.
	nixlPauseEnv = "NVSNAP_NIXL_PAUSE"
	// gpushareCtlOnCUDAUseEnv has the library open its control socket on
	// a process's first CUDA entry-point lookup: vLLM's engine-core
	// process at TP > 1 allocates no GPU memory, and nixl-pause reaches
	// the engine through it.
	gpushareCtlOnCUDAUseEnv = "NVSNAP_GPUSHARE_CTL_ON_CUDA_USE"
)

// nixlPauseEnvVars are the nixl-pause variables container c does not set
// itself: a value the pod sets wins.
func nixlPauseEnvVars(c *corev1.Container) []corev1.EnvVar {
	var vars []corev1.EnvVar
	for _, name := range []string{nixlPauseEnv, gpushareCtlOnCUDAUseEnv} {
		if !hasEnv(c, name) {
			vars = append(vars, corev1.EnvVar{Name: name, Value: "1"})
		}
	}
	return vars
}

// nixlPauseEnvOff replaces a NVSNAP_NIXL_PAUSE that container c (index
// i) sets itself with "0", in place, when the cluster's switch is off: the
// pod did not get the opt-in, so it does not keep the marker. The agent
// also checks its own switch, which covers a value that comes through
// envFrom, where admission cannot see it.
func (m *Mutator) nixlPauseEnvOff(i int, c *corev1.Container) []PatchOp {
	if m.NIXLPause {
		return nil
	}
	for j, e := range c.Env {
		if e.Name == nixlPauseEnv && (e.Value != "0" || e.ValueFrom != nil) {
			return []PatchOp{{Op: "replace", Path: fmt.Sprintf("/spec/containers/%d/env/%d", i, j), Value: corev1.EnvVar{Name: nixlPauseEnv, Value: "0"}}}
		}
	}
	return nil
}

// nixlPauseWanted reports whether a gpushare pod gets the nixl-pause env:
// annotated nvsnap.io/nixl-pause "true" with the cluster's switch on. The
// annotation with the switch off is logged and ignored.
func (m *Mutator) nixlPauseWanted(pod *corev1.Pod) bool {
	if pod.Annotations[NIXLPauseAnnotation] != "true" {
		return false
	}
	if !m.NIXLPause {
		m.logger().WithField("pod", pod.Namespace+"/"+pod.Name).
			Info("gpushare: the pod opts in to nixl-pause, which this cluster does not enable (webhook.nixlPause.enabled); ignoring it")
		return false
	}
	return true
}

// gpushareMode reports whether a pod runs under the gpushare library and
// whether the library shares its GPU memory over multi-node NVLink
// (fabric). The library runs when annotated, or by default where CRIU is
// the capture method (it keeps GPU memory out of the CRIU image), unless
// annotated "false". An engine that spans pods needs fabric: without it
// the library hides multi-node NVLink memory, and the engine never
// finishes its distributed setup (kimi-k3 TP=8 on two GB300 nodes hung 30
// minutes in its first collective, 2026-10-08). nvsnap.io/gpushare-fabric
// set either way wins.
func (m *Mutator) gpushareMode(ctx context.Context, pod *corev1.Pod) (wanted, fabric bool) {
	switch pod.Annotations[GPUShareAnnotation] {
	case "true":
		wanted = true
	case "false":
		return false, false
	default:
		// A pod that runs to completion (a Job's, an NVCF task's) is never
		// captured: the library would only add risk to it.
		wanted = m.GPUShareByDefault && !runsToCompletion(pod)
	}
	if !wanted {
		return false, false
	}
	switch pod.Annotations[GPUShareFabricAnnotation] {
	case "true":
		return true, true
	case "false":
		return true, false
	}
	if flag := multiNodeFlag(pod); flag != "" || m.groupSize(ctx, pod) > 1 {
		m.logger().WithField("pod", pod.Namespace+"/"+pod.Name).WithField("flag", flag).
			Info("gpushare: the engine spans pods; sharing its GPU memory over multi-node NVLink (fabric)")
		return true, true
	}
	return true, false
}

// gpushareIBV reports whether a pod under the library gets NCCL pointed at
// the bundled verbs library: its nvsnap.io/gpushare-ibv annotation
// (explicit), or GPUShareIBVByDefault when it has none.
func (m *Mutator) gpushareIBV(pod *corev1.Pod) (on, explicit bool) {
	switch pod.Annotations[GPUShareIBVAnnotation] {
	case "true":
		return true, true
	case "false":
		return false, true
	}
	return m.GPUShareIBVByDefault, false
}

// envFromMaySet reports whether one of c's envFrom sources could set name:
// admission does not read them, so any whose prefix starts name may.
func envFromMaySet(c *corev1.Container, name string) bool {
	for _, ef := range c.EnvFrom {
		if strings.HasPrefix(name, ef.Prefix) {
			return true
		}
	}
	return false
}

// mountsBundle reports whether c's mount at nvsnapToolsMountPath is the
// selected node bundle (a hostPath of bundleDir(BundleID), its root: no
// subPath), which has libnvsnap_ibv.so; an older bundle, another volume or
// a directory inside one may not.
func (m *Mutator) mountsBundle(pod *corev1.Pod, c *corev1.Container) bool {
	for _, vm := range c.VolumeMounts {
		if vm.MountPath != nvsnapToolsMountPath {
			continue
		}
		if vm.SubPath != "" || vm.SubPathExpr != "" {
			return false
		}
		for _, v := range pod.Spec.Volumes {
			if v.Name == vm.Name {
				return v.HostPath != nil && v.HostPath.Path == m.bundleDir(m.BundleID)
			}
		}
	}
	return false
}

// runsToCompletion reports a pod that is not restarted when it exits: a
// Job's or a task's, not a server's (every serving controller runs its
// pods with restartPolicy Always, the default).
func runsToCompletion(pod *corev1.Pod) bool {
	return pod.Spec.RestartPolicy == corev1.RestartPolicyNever || pod.Spec.RestartPolicy == corev1.RestartPolicyOnFailure
}

// multiNodeEngineFlags only appear on an engine that spans nodes.
var multiNodeEngineFlags = []string{"--nnodes", "--node-rank", "--node_rank", "--dist-init-addr", "--master-addr", "--master_addr",
	"--data-parallel-address", "--headless"}

// multiNodeFlag returns the first multi-node flag on a GPU container's
// command line, or "".
func multiNodeFlag(pod *corev1.Pod) string {
	for _, c := range pod.Spec.Containers {
		if _, ok := c.Resources.Limits["nvidia.com/gpu"]; !ok {
			continue
		}
		if f := multiNodeEngineArg(append(append([]string{}, c.Command...), c.Args...)); f != "" {
			return f
		}
	}
	return ""
}

// EngineSpansPods says why pod's engine spans pods (nodes) by its spec,
// or "": the gpushare fabric annotation "true"; a GPU container's command
// line that only an engine on more than one node has (EngineSpansArg); a
// Ray cluster's pod (KubeRay's ray.io/node-type label); or a group size
// above one from its controller (a LeaderWorkerSet's size annotation, a
// Grove scaling group's pod count, which a reference the webhook cannot
// read counts as). It reads nothing else, unlike groupSize: a
// StatefulSet's replicas are not an engine's ranks.
func EngineSpansPods(pod *corev1.Pod) string {
	if pod.Annotations[GPUShareFabricAnnotation] == "true" {
		return GPUShareFabricAnnotation + `: "true"`
	}
	if t := pod.Labels["ray.io/node-type"]; t != "" {
		return "it is a Ray cluster's " + t + " pod (ray.io/node-type)"
	}
	for _, c := range pod.Spec.Containers {
		if _, ok := c.Resources.Limits["nvidia.com/gpu"]; !ok {
			continue
		}
		if why := EngineSpansArg(append(append([]string{}, c.Command...), c.Args...)); why != "" {
			return why
		}
	}
	if n, err := strconv.Atoi(pod.Annotations[lwsSizeAnnotation]); err == nil && n > 1 {
		return fmt.Sprintf("%s=%d", lwsSizeAnnotation, n)
	}
	for _, c := range pod.Spec.Containers {
		for _, e := range c.Env {
			if e.Name != groveScalingGroupSizeEnv {
				continue
			}
			if e.ValueFrom != nil {
				return groveScalingGroupSizeEnv + " comes from a reference the webhook cannot read"
			}
			if n, err := strconv.Atoi(e.Value); err == nil && n > 1 {
				return fmt.Sprintf("%s=%d", groveScalingGroupSizeEnv, n)
			}
		}
	}
	return ""
}

// EngineSpansArg says why a command line (args, each split on spaces, as
// in a shell line) is one only an engine on more than one node is started
// with, or "": --headless (a node without the API server), a node count
// above one (--nnodes, --num-nodes), a node rank above zero (--node-rank),
// --dist-init-addr, a vLLM data-parallel engine's start rank above zero
// (--data-parallel-start-rank, a node after the first), or fewer local
// data-parallel ranks than in all (--data-parallel-size-local below
// --data-parallel-size: the leader of an engine spanning nodes). An
// address alone (--data-parallel-address, --master-addr) does not say:
// single-node engines set them too.
func EngineSpansArg(args []string) string {
	var words []string
	for _, a := range args {
		words = append(words, strings.Fields(a)...)
	}
	dpSize, dpLocal := -1, -1
	for i, w := range words {
		name, val, hasVal := strings.Cut(w, "=")
		if !hasVal && i+1 < len(words) {
			val = words[i+1]
		}
		n, nerr := strconv.Atoi(val)
		switch name {
		case "--headless", "--dist-init-addr":
			return "its engine is started with " + name
		case "--nnodes", "--num-nodes":
			if nerr != nil || n > 1 {
				return fmt.Sprintf("its engine is started with %s %s", name, val)
			}
		case "--node-rank", "--node_rank", "--data-parallel-start-rank":
			if nerr != nil || n > 0 {
				return fmt.Sprintf("its engine is started with %s %s", name, val)
			}
		case "--data-parallel-size":
			if nerr == nil {
				dpSize = n
			}
		case "--data-parallel-size-local":
			if nerr == nil {
				dpLocal = n
			}
		}
	}
	if dpLocal >= 0 && dpSize > dpLocal {
		return fmt.Sprintf("its engine is started with --data-parallel-size %d and --data-parallel-size-local %d", dpSize, dpLocal)
	}
	return ""
}

// multiNodeEngineArg returns the first flag in args (each split on
// spaces, as in a shell command line) that only an engine spanning nodes
// is started with, or "". A node count of one (--nnodes 1) and a node
// rank of zero (--node-rank 0) do not count on their own, as in
// EngineSpansArg: a single-node engine may be started with them.
func multiNodeEngineArg(args []string) string {
	var words []string
	for _, a := range args {
		words = append(words, strings.Fields(a)...)
	}
	for i, w := range words {
		for _, f := range multiNodeEngineFlags {
			if w != f && !strings.HasPrefix(w, f+"=") {
				continue
			}
			val, hasVal := strings.CutPrefix(w, f+"=")
			if !hasVal && i+1 < len(words) {
				val = words[i+1]
			}
			n, err := strconv.Atoi(val)
			switch {
			case f == "--nnodes" && err == nil && n <= 1:
			case (f == "--node-rank" || f == "--node_rank") && err == nil && n == 0:
			default:
				return f
			}
		}
	}
	return ""
}

// gpusharePatches places the library and the store into every container of
// an opted-in pod that requests GPUs.
func (m *Mutator) gpusharePatches(ctx context.Context, pod *corev1.Pod) []PatchOp {
	wanted, fabric := m.gpushareMode(ctx, pod)
	if !wanted {
		return nil
	}
	ibv, ibvExplicit := m.gpushareIBV(pod)
	gpuContainers := make([]int, 0, len(pod.Spec.Containers))
	preloads := map[int]corev1.EnvVar{}
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		if _, ok := c.Resources.Limits["nvidia.com/gpu"]; !ok {
			continue
		}
		preload, ok := ldPreloadWith(c.Env, GPUShareLibPath)
		if !ok {
			m.logger().WithField("pod", pod.Namespace+"/"+pod.Name).WithField("container", c.Name).
				Warn("gpushare: LD_PRELOAD is set from a reference the webhook cannot read; not placing the library, so this container cannot be checkpointed")
			continue
		}
		gpuContainers = append(gpuContainers, i)
		preloads[i] = preload
	}
	if len(gpuContainers) == 0 {
		return nil
	}
	storeRoot := m.GPUShareHostRoot
	if storeRoot == "" {
		storeRoot = DefaultGPUShareHostRoot
	}
	placed, unsafe := gpushareStoreState(pod, storeRoot, gpuContainers)
	if unsafe != "" {
		// The store volume is every pod's saved GPU memory on the node; the
		// per-pod subPathExpr confines only the mounts placed here. Placing
		// it beside a reference the pod brought would hand that container
		// the whole directory. The pod is left without the library (and a
		// mount of a volume it does not declare is refused by the API).
		m.logger().WithField("pod", pod.Namespace+"/"+pod.Name).WithField("reason", unsafe).
			Warn("gpushare: the pod references the reserved store volume itself; not placing the library or the store")
		return nil
	}
	nixlPause := m.nixlPauseWanted(pod)
	if placed {
		// Already placed (re-admission, or written into the manifest):
		// only the nixl-pause env, if the opt-in came after the placement,
		// or the pod's own marker turned off without the switch.
		var patches []PatchOp
		for _, i := range gpuContainers {
			c := &pod.Spec.Containers[i]
			patches = append(patches, m.nixlPauseEnvOff(i, c)...)
			if nixlPause {
				patches = append(patches, setContainerEnv(i, c, nixlPauseEnvVars(c))...)
			}
		}
		return patches
	}
	bundleRoot := m.HostBundleRoot
	if bundleRoot == "" {
		bundleRoot = DefaultHostBundleRoot
	}
	dir, dirOrCreate := corev1.HostPathDirectory, corev1.HostPathDirectoryOrCreate

	var patches []PatchOp
	vols := []corev1.Volume{
		{Name: gpushareStoreVolume, VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: storeRoot, Type: &dirOrCreate}}},
	}
	libMountedAt := map[int]bool{}
	for _, i := range gpuContainers {
		for _, vm := range pod.Spec.Containers[i].VolumeMounts {
			if vm.MountPath == nvsnapToolsMountPath {
				libMountedAt[i] = true // the node bundle is already there
			}
		}
	}
	if len(libMountedAt) < len(gpuContainers) {
		vols = append(vols, corev1.Volume{Name: gpushareLibVolume, VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: m.bundleDir(m.BundleID), Type: &dir}}})
	}
	// The restore's work volume, here empty: a restored tree keeps the
	// mounts it was dumped with, and its placeholder binds its own work
	// volume at this path into it, where the GPU resume of a group restore
	// and the restored processes meet in the session directory
	// (gpushare_fabric.go). A tree dumped without the mount has no such
	// path.
	work := !podDeclaresVolume(pod, restoreWorkVolume) && !mountAtPath(pod, RestoreWorkMountPath)
	if work {
		vols = append(vols, corev1.Volume{Name: restoreWorkVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
	}
	// The tools a capture runs in these containers (tools_mount.go).
	toolsVol, toolsMount, tools := m.bundleTools(pod, m.BundleID)
	if tools {
		vols = append(vols, toolsVol)
	}
	patches = append(patches, addVolumes(pod, vols)...)

	for _, i := range gpuContainers {
		c := &pod.Spec.Containers[i]
		mounts := []corev1.VolumeMount{
			// Per-pod directory, so one pod never sees another's saved memory.
			{Name: gpushareStoreVolume, MountPath: GPUShareStorePath, SubPathExpr: "$(" + gpusharePodUIDEnv + ")"},
		}
		if !libMountedAt[i] {
			mounts = append(mounts, corev1.VolumeMount{Name: gpushareLibVolume, MountPath: nvsnapToolsMountPath, ReadOnly: true})
		}
		if tools {
			mounts = append(mounts, toolsMount)
		}
		if work {
			mounts = append(mounts, corev1.VolumeMount{Name: restoreWorkVolume, MountPath: RestoreWorkMountPath})
		}
		patches = append(patches, addContainerMounts(i, c, mounts)...)
		uid := corev1.EnvVar{Name: gpusharePodUIDEnv, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}}
		vars := []corev1.EnvVar{uid, preloads[i]}
		// A value the pod sets itself wins: it may switch fabric sharing off.
		if fabric && !hasEnv(c, gpushareFabricEnv) {
			vars = append(vars, corev1.EnvVar{Name: gpushareFabricEnv, Value: "1"})
		}
		// NCCL's own choice of verbs library wins: then its RDMA state stays
		// as before, and a capture of the pod is refused. A choice an envFrom
		// source may make wins over the default, not over the annotation.
		// The path goes only where the bundle that has it is mounted.
		if ibv && !hasEnv(c, ncclIBVerbsLibEnv) && (ibvExplicit || !envFromMaySet(c, ncclIBVerbsLibEnv)) {
			if libMountedAt[i] && !m.mountsBundle(pod, c) {
				m.logger().WithField("pod", pod.Namespace+"/"+pod.Name).WithField("container", c.Name).
					Warn("gpushare: the container mounts something other than the node bundle at " + nvsnapToolsMountPath +
						"; not pointing NCCL at " + GPUShareIBVLibPath + ", so a capture of it is refused if NCCL uses RDMA")
			} else {
				vars = append(vars, corev1.EnvVar{Name: ncclIBVerbsLibEnv, Value: GPUShareIBVLibPath})
			}
		}
		if nixlPause {
			vars = append(vars, nixlPauseEnvVars(c)...)
		}
		patches = append(patches, m.nixlPauseEnvOff(i, c)...)
		patches = append(patches, setContainerEnv(i, c, vars)...)
	}
	return patches
}

// gpushareStoreState checks the pod's own references to the reserved store
// volume, which only gpusharePatches may make: a hostPath of storeRoot
// mounted at GPUShareStorePath through the per-pod subPathExpr, in a
// container whose pod-uid variable is the downward-API uid and nothing
// else. placed reports a pod that already carries exactly that placement
// (a manifest written from an admitted pod). unsafe names the first
// reference that is not it, or a GPU container (gpuContainers) whose
// pod-uid variable the placement could not make the pod's own.
func gpushareStoreState(pod *corev1.Pod, storeRoot string, gpuContainers []int) (placed bool, unsafe string) {
	declared := false
	for i := range pod.Spec.Volumes {
		v := &pod.Spec.Volumes[i]
		if v.Name != gpushareStoreVolume {
			continue
		}
		if v.HostPath == nil || v.HostPath.Path != storeRoot {
			return false, "the pod declares a volume under the reserved store volume name"
		}
		declared = true
	}
	for i := range pod.Spec.InitContainers {
		if c := &pod.Spec.InitContainers[i]; mountsVolume(c.VolumeMounts, gpushareStoreVolume) {
			return false, "init container " + c.Name + " mounts the store volume"
		}
	}
	for i := range pod.Spec.EphemeralContainers {
		if c := &pod.Spec.EphemeralContainers[i]; mountsVolume(c.VolumeMounts, gpushareStoreVolume) {
			return false, "ephemeral container " + c.Name + " mounts the store volume"
		}
	}
	mounted := false
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		for _, vm := range c.VolumeMounts {
			if vm.Name != gpushareStoreVolume {
				continue
			}
			if !declared {
				return false, "container " + c.Name + " mounts the store volume, which the pod does not declare"
			}
			if vm.MountPath != GPUShareStorePath || vm.SubPath != "" || vm.SubPathExpr != "$("+gpusharePodUIDEnv+")" || !podUIDFromDownwardAPI(c.Env) {
				return false, "container " + c.Name + " mounts the store volume outside its own per-pod directory"
			}
			mounted = true
		}
	}
	if declared {
		if !mounted {
			return false, "the pod declares the store volume without the per-pod mount"
		}
		return true, ""
	}
	for _, i := range gpuContainers {
		n := 0
		for _, e := range pod.Spec.Containers[i].Env {
			if e.Name == gpusharePodUIDEnv {
				n++
			}
		}
		if n > 1 {
			// The placement replaces one entry; the last of several is the
			// one Kubernetes resolves the subPathExpr against.
			return false, "container " + pod.Spec.Containers[i].Name + " sets " + gpusharePodUIDEnv + " more than once"
		}
	}
	return false, ""
}

// GPUShareMounted reports whether the named container (every container
// when name is empty) has the gpushare library or store mounted: the
// paths this webhook places in a pod admitted for gpushare. A CRIU dump
// saves the mounts the process had, so a restore must recreate these
// mountpoints whether or not the process ended up loading the library.
func GPUShareMounted(pod *corev1.Pod, name string) bool {
	if pod == nil {
		return false
	}
	for _, c := range pod.Spec.Containers {
		if name != "" && c.Name != name {
			continue
		}
		for _, vm := range c.VolumeMounts {
			if vm.MountPath == nvsnapToolsMountPath || vm.MountPath == GPUShareStorePath {
				return true
			}
		}
	}
	return false
}

func mountsVolume(mounts []corev1.VolumeMount, name string) bool {
	for _, vm := range mounts {
		if vm.Name == name {
			return true
		}
	}
	return false
}

// podUIDFromDownwardAPI reports env setting the pod-uid variable exactly
// once, to the pod's own uid.
func podUIDFromDownwardAPI(env []corev1.EnvVar) bool {
	n, own := 0, false
	for _, e := range env {
		if e.Name != gpusharePodUIDEnv {
			continue
		}
		n++
		own = e.Value == "" && e.ValueFrom != nil && e.ValueFrom.FieldRef != nil && e.ValueFrom.FieldRef.FieldPath == "metadata.uid"
	}
	return n == 1 && own
}

// ldPreloadWith returns the container's LD_PRELOAD with lib appended. ok is
// false when LD_PRELOAD comes from a ConfigMap or Secret reference: its
// value is not known here, and replacing it would drop the image's own
// preloads.
func ldPreloadWith(env []corev1.EnvVar, lib string) (corev1.EnvVar, bool) {
	for _, e := range env {
		if e.Name != "LD_PRELOAD" {
			continue
		}
		if e.ValueFrom != nil {
			return corev1.EnvVar{}, false
		}
		for _, p := range strings.FieldsFunc(e.Value, func(r rune) bool { return r == ':' || r == ' ' }) {
			if p == lib {
				return e, true
			}
		}
		v := lib
		if strings.TrimSpace(e.Value) != "" {
			v = e.Value + ":" + lib
		}
		return corev1.EnvVar{Name: "LD_PRELOAD", Value: v}, true
	}
	return corev1.EnvVar{Name: "LD_PRELOAD", Value: lib}, true
}

func addVolumes(pod *corev1.Pod, vols []corev1.Volume) []PatchOp {
	if len(vols) == 0 {
		return nil
	}
	if pod.Spec.Volumes == nil {
		return []PatchOp{{Op: "add", Path: "/spec/volumes", Value: vols}}
	}
	out := make([]PatchOp, 0, len(vols))
	for i := range vols {
		out = append(out, PatchOp{Op: "add", Path: "/spec/volumes/-", Value: vols[i]})
	}
	return out
}

func addContainerMounts(i int, c *corev1.Container, mounts []corev1.VolumeMount) []PatchOp {
	if c.VolumeMounts == nil {
		return []PatchOp{{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/volumeMounts", i), Value: mounts}}
	}
	out := make([]PatchOp, 0, len(mounts))
	for _, vm := range mounts {
		out = append(out, PatchOp{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/volumeMounts/-", i), Value: vm})
	}
	return out
}

// setContainerEnv adds env vars, replacing an existing entry of the same name
// in place. Kubernetes resolves a SubPathExpr against the container's whole
// env, so the pod-uid variable only has to exist, not come first.
func setContainerEnv(i int, c *corev1.Container, vars []corev1.EnvVar) []PatchOp {
	out := make([]PatchOp, 0, len(vars))
	add := make([]corev1.EnvVar, 0, len(vars))
	for _, v := range vars {
		replaced := false
		for j, e := range c.Env {
			if e.Name == v.Name {
				out = append(out, PatchOp{Op: "replace", Path: fmt.Sprintf("/spec/containers/%d/env/%d", i, j), Value: v})
				replaced = true
				break
			}
		}
		if !replaced {
			add = append(add, v)
		}
	}
	if len(add) == 0 {
		return out
	}
	if c.Env == nil {
		return append(out, PatchOp{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/env", i), Value: add})
	}
	for _, v := range add {
		out = append(out, appendEnv(i, v))
	}
	return out
}
