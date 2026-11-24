// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package webhook

// A group restore's placeholders run the init containers that write what
// the pod's running containers read (initsToKeep), which includes a model
// download into an emptyDir the engine container mounts: on a node with
// only local NVMe and no model volume, every restore downloads the whole
// model before it can start (1.56 TB for one Kimi worker), so a restore
// takes longer than a cold start. Yet a restored process keeps the files
// it had open and the mappings it had; it does not read the model again.
//
// So, with Mutator.SkipUnusedInits, a group capture records the emptyDir
// volumes that were too large to copy into the checkpoint and that no
// restored process of any member uses (CRIUGroup.UnusedVolumes, decided
// once for the whole group by GroupUnusedVolumes from each member's dump),
// and every placeholder of the group skips the inits that fill only those.
// Only those: a smaller emptyDir the engine writes is copied into the
// checkpoint and back, and one it only reads (merged CA certificates) is
// cheap to fill and may well be read again after the restore (a new TLS
// connection), so its setup runs as before. The decision is all or nothing: a member that would
// still fill such a volume would hold the group restore back past the
// leader's wait for its members.
//
// A failed restore deletes the placeholders; their controller's
// replacements are admitted again, and after the capture is blocked start
// cold, running every init. A pod nothing recreates, or a Job's (which
// stops recreating), would start cold in place instead, which needs what
// the inits fill: it never skips them. Neither does a pod annotated
// KeepInitsAnnotation.

import (
	"path"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/checkpointstore"
)

const (
	// SkippedInitsAnnotation names, on a group placeholder, the init
	// containers it skips because they fill only volumes no restored
	// process uses.
	SkippedInitsAnnotation = "nvsnap.io/skipped-inits"
	// KeepInitsAnnotation keeps every init a group placeholder would skip
	// for unused volumes, when set to any value on a workload's pods: for
	// a workload that reads its setup's output again after it starts.
	KeepInitsAnnotation = "nvsnap.io/keep-inits"

	// unusedVolumesMeta carries a group record's UnusedVolumes to the
	// placeholder patches, comma-separated.
	unusedVolumesMeta = "nvsnap.io/unused-volumes"
)

// RecreatingOwners are the controller kinds known to replace a pod of
// theirs that is deleted.
var RecreatingOwners = map[string]bool{
	"ReplicaSet":            true,
	"ReplicationController": true,
	"StatefulSet":           true,
	"DaemonSet":             true,
	"Job":                   true,
	"PodClique":             true, // grove.io: Dynamo multi-node graphs
}

// PodRecreatedOnDelete reports whether a controller replaces pod when it is
// deleted.
func PodRecreatedOnDelete(pod *corev1.Pod) bool {
	return controllerKind(pod) != ""
}

// controllerKind is the kind of pod's recreating controller, "" for none.
func controllerKind(pod *corev1.Pod) string {
	for _, o := range pod.OwnerReferences {
		if o.Controller != nil && *o.Controller && RecreatingOwners[o.Kind] {
			return o.Kind
		}
	}
	return ""
}

// mayDropSetup reports whether pod's placeholder may skip setup for
// unused volumes: a failed restore of it is replaced by a fresh pod that
// runs every init. A Job stops replacing its pods past its backoff limit.
func mayDropSetup(pod *corev1.Pod) bool {
	if _, keep := pod.Annotations[KeepInitsAnnotation]; keep {
		return false
	}
	k := controllerKind(pod)
	return k != "" && k != "Job"
}

// MemberVolumeUse is one member of a group capture: its pod, the
// container its checkpoint was taken from, the pod volumes the dumped
// processes used (VolumesAt; nil when that is not known), and the emptyDir
// volumes too large to copy into its checkpoint.
type MemberVolumeUse struct {
	Pod       *corev1.Pod
	Container string
	Used      []string
	Large     []string
}

// GroupUnusedVolumes decides, for a whole group capture, the emptyDir
// volumes its placeholders may skip filling: those a member's checkpointed
// container mounts, too large to copy into its checkpoint, and used by no
// member's dumped processes. It returns nil,
// and why, when the group skips nothing: a member's use is not known, a
// member may not drop setup, or some member would still run an init that
// fills one of them.
func GroupUnusedVolumes(members []MemberVolumeUse) (unused []string, why string) {
	used := map[string]bool{}
	for _, m := range members {
		if m.Used == nil {
			return nil, "the volumes member " + m.Pod.Namespace + "/" + m.Pod.Name + " used are not known"
		}
		if !mayDropSetup(m.Pod) {
			return nil, "member " + m.Pod.Namespace + "/" + m.Pod.Name + " keeps its setup (no recreating controller, a Job, or " + KeepInitsAnnotation + ")"
		}
		for _, v := range m.Used {
			used[v] = true
		}
	}
	cand := map[string]bool{}
	for _, m := range members {
		mounted := map[string]bool{}
		for _, v := range containerEmptyDirs(m.Pod, m.Container) {
			mounted[v] = true
		}
		for _, v := range m.Large {
			if mounted[v] && !used[v] {
				cand[v] = true
			}
		}
	}
	if len(cand) == 0 {
		return nil, "no emptyDir too large to copy into a checkpoint is unused"
	}
	skipsAny := false
	for _, m := range members {
		skips, blocked := unusedVolumeSkips(m.Pod, m.Container, cand)
		if blocked != "" {
			return nil, "member " + m.Pod.Namespace + "/" + m.Pod.Name + " would still run " + blocked
		}
		skipsAny = skipsAny || len(skips) > 0
	}
	if !skipsAny {
		return nil, "no member has setup that fills only unused volumes"
	}
	out := make([]string, 0, len(cand))
	for v := range cand {
		out = append(out, v)
	}
	sort.Strings(out)
	return out, ""
}

// unusedVolumeSkips returns the inits pod's group placeholder skips for
// unused that it would run without, and the name of an init it still runs
// that writes one of them ("" for none): one some other container's reads
// keep, which a restore would still wait for. A native sidecar that writes
// one does not hold the restore back (the pod's containers start once it
// has started), so it does not count, and runs as it did before.
func unusedVolumeSkips(pod *corev1.Pod, container string, unused map[string]bool) (skips []string, blocked string) {
	keep := initsToKeep(pod, container, unused)
	base := initsToKeep(pod, container, nil)
	for i := range pod.Spec.InitContainers {
		init := &pod.Spec.InitContainers[i]
		if base[i] && !keep[i] {
			skips = append(skips, init.Name)
		}
		if !keep[i] || NativeSidecar(init) {
			continue
		}
		for _, vm := range init.VolumeMounts {
			if !vm.ReadOnly && unused[vm.Name] {
				return nil, init.Name
			}
		}
	}
	return skips, ""
}

// containerEmptyDirs lists the emptyDir volumes container of pod mounts,
// leaving out any it mounts through a subPath (a file its setup put there).
func containerEmptyDirs(pod *corev1.Pod, container string) []string {
	empty := map[string]bool{}
	for i := range pod.Spec.Volumes {
		if pod.Spec.Volumes[i].EmptyDir != nil {
			empty[pod.Spec.Volumes[i].Name] = true
		}
	}
	var out []string
	sub := map[string]bool{}
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		if c.Name != container {
			continue
		}
		for _, vm := range c.VolumeMounts {
			if vm.SubPath != "" || vm.SubPathExpr != "" {
				sub[vm.Name] = true
			}
		}
		for _, vm := range c.VolumeMounts {
			if empty[vm.Name] && !sub[vm.Name] {
				out = append(out, vm.Name)
			}
		}
	}
	return out
}

// VolumesAt maps mountpoints, as a dump of container recorded the mounts
// its processes used, to the pod volumes container mounts there. A
// mountpoint no volume of container is mounted at (the root filesystem,
// /dev/shm, a mount the runtime adds) names none.
func VolumesAt(pod *corev1.Pod, container string, mountpoints []string) []string {
	at := map[string]string{}
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		if c.Name != container {
			continue
		}
		for _, vm := range c.VolumeMounts {
			at[path.Clean(vm.MountPath)] = vm.Name
		}
	}
	seen := map[string]bool{}
	out := []string{}
	for _, mp := range mountpoints {
		if v, ok := at[path.Clean(mp)]; ok && !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	sort.Strings(out)
	return out
}

// unusedForPlaceholder is the set of volumes pod's placeholder skips
// filling: the group record's UnusedVolumes (carried in man), when the
// Mutator skips setup for unused volumes, the placeholder is a group's,
// and the pod may drop its setup. Nil otherwise.
func (m *Mutator) unusedForPlaceholder(pod *corev1.Pod, group bool, man checkpointstore.Manifest) map[string]bool {
	if !group || !m.SkipUnusedInits || !mayDropSetup(pod) {
		return nil
	}
	out := map[string]bool{}
	for _, v := range strings.Split(man.SourcePodMeta[unusedVolumesMeta], ",") {
		if v != "" {
			out[v] = true
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
