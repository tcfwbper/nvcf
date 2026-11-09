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
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/election"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

// A pod without GPUs that a capture may dump (a companion) mounts the
// companion directory of a bundle version (webhook.BundleCompanionDir),
// which the kubelet creates empty where it is missing, so the pod starts on
// any node, a CPU node with no agent included. On a node an agent runs on,
// the agent mounts the staged version there, read-only; the pod's mount
// (HostToContainer) shows it, whether the pod started before or after.
// The capture checks the pod's /criu-bundle is a staged version, read-only
// (bundle_tools.go), as for a GPU pod.

// companionToolsDir is this node's companion directory of version id.
func (a *Agent) companionToolsDir(id string) string {
	return filepath.Join(a.bundleRoot(), webhook.BundleCompanionDir, id)
}

// attachReadOnly mounts directory src on dst, read-only from the moment it
// is attached: the mount is cloned detached (open_tree), made read-only
// (mount_setattr), then attached (move_mount). A bind remounted read-only
// afterwards would stay writable in the mount namespaces it had already
// propagated to, the pods' among them. Linux 5.12 or later; on an older
// kernel it fails, and companions there are not captured.
var attachReadOnly = func(src, dst string) error {
	fd, err := unix.OpenTree(unix.AT_FDCWD, src, unix.OPEN_TREE_CLONE|unix.OPEN_TREE_CLOEXEC)
	if err != nil {
		return fmt.Errorf("open_tree %s: %w", src, err)
	}
	defer func() { _ = unix.Close(fd) }()
	attr := &unix.MountAttr{Attr_set: unix.MOUNT_ATTR_RDONLY | unix.MOUNT_ATTR_NOSUID | unix.MOUNT_ATTR_NODEV}
	if err := unix.MountSetattr(fd, "", unix.AT_EMPTY_PATH, attr); err != nil {
		return fmt.Errorf("mount_setattr %s read-only: %w", src, err)
	}
	if err := unix.MoveMount(fd, "", unix.AT_FDCWD, dst, unix.MOVE_MOUNT_F_EMPTY_PATH); err != nil {
		return fmt.Errorf("move_mount onto %s: %w", dst, err)
	}
	return nil
}

// injectReadOnly mounts directory src, read-only, on dst inside the mount
// namespace at nsPath (a workload's /proc/<pid>/ns/mnt): a companion that
// started before its version was mounted on its companion directory, whose
// mount of it does not propagate. The mount is cloned detached and made
// read-only here, then attached from a thread that has entered the
// namespace (attachInNamespace). dst is resolved in the workload's root
// and a trailing symlink is not followed.
var injectReadOnly = func(src, nsPath, dst string) error {
	fd, err := unix.OpenTree(unix.AT_FDCWD, src, unix.OPEN_TREE_CLONE|unix.OPEN_TREE_CLOEXEC)
	if err != nil {
		return fmt.Errorf("open_tree %s: %w", src, err)
	}
	defer func() { _ = unix.Close(fd) }()
	attr := &unix.MountAttr{Attr_set: unix.MOUNT_ATTR_RDONLY | unix.MOUNT_ATTR_NOSUID | unix.MOUNT_ATTR_NODEV}
	if err = unix.MountSetattr(fd, "", unix.AT_EMPTY_PATH, attr); err != nil {
		return fmt.Errorf("mount_setattr %s read-only: %w", src, err)
	}
	ns, err := unix.Open(nsPath, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open %s: %w", nsPath, err)
	}
	defer func() { _ = unix.Close(ns) }()
	// The mount namespace must be owned by the initial user namespace: in
	// one of the workload's own it could hold CAP_SYS_ADMIN over the mount
	// and make it writable. Checked on the namespace held open, the one
	// entered below.
	owner, err := unix.IoctlRetInt(ns, unix.NS_GET_USERNS)
	if err != nil {
		return fmt.Errorf("the owner of %s: %w", nsPath, err)
	}
	ownerFile := os.NewFile(uintptr(owner), "userns")
	defer func() { _ = ownerFile.Close() }()
	if err = initialUserNS(ownerFile); err != nil {
		return fmt.Errorf("the workload's mount namespace %w", err)
	}
	errc := make(chan error, 1)
	go func() {
		// Never unlocked: the thread's filesystem context is its own from
		// here on, so it exits with this goroutine (or, the main thread,
		// is parked by the runtime).
		runtime.LockOSThread()
		errc <- attachInNamespace(ns, fd, dst)
	}()
	return <-errc
}

// attachInNamespace attaches the detached mount fd at dst inside the mount
// namespace ns, from the calling thread, which the caller has locked and
// never unlocks. The thread returns to its own mount namespace before this
// returns: the runtime cannot end the main thread, which it parks instead,
// and a main thread left in a workload's namespace would keep that
// namespace alive for the agent's life and show it at /proc/self.
func attachInNamespace(ns, fd int, dst string) error {
	// A thread shares its filesystem context with the others until it
	// unshares it, and setns into a mount namespace refuses a shared one.
	if err := unix.Unshare(unix.CLONE_FS); err != nil {
		return fmt.Errorf("unshare the filesystem context: %w", err)
	}
	own, err := unix.Open("/proc/thread-self/ns/mnt", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open the agent's mount namespace: %w", err)
	}
	defer func() { _ = unix.Close(own) }()
	if err = unix.Setns(ns, unix.CLONE_NEWNS); err != nil {
		return fmt.Errorf("enter the workload's mount namespace: %w", err)
	}
	moved := unix.MoveMount(fd, "", unix.AT_FDCWD, dst, unix.MOVE_MOUNT_F_EMPTY_PATH)
	if err = unix.Setns(own, unix.CLONE_NEWNS); err != nil {
		return fmt.Errorf("return to the agent's mount namespace: %w", err)
	}
	if moved != nil {
		return fmt.Errorf("move_mount onto %s in the workload: %w", dst, moved)
	}
	return nil
}

// unmountDir detaches the mount on dir.
var unmountDir = func(dir string) error { return unix.Unmount(dir, unix.MNT_DETACH) }

// sameDir reports whether a and b are the same directory: dst carries the
// version mounted on it.
func sameDir(a, b string) bool {
	var sa, sb syscall.Stat_t
	return syscall.Stat(a, &sa) == nil && syscall.Stat(b, &sb) == nil && sa.Dev == sb.Dev && sa.Ino == sb.Ino
}

// ensureCompanionTools mounts staged version id on its companion directory,
// unless it is mounted there already.
func (a *Agent) ensureCompanionTools(id string) error {
	if !validBundleID(id) {
		return fmt.Errorf("bundle version %q", id)
	}
	// Pod events, a capture and the node sweep all get here: checked and
	// mounted under the lock, a second caller sees the first one's mount.
	a.companionMu.Lock()
	defer a.companionMu.Unlock()
	src := filepath.Join(a.bundleVersionsDir(), id)
	//nolint:gosec // id passed validBundleID: one path element, no leading dot, no separator
	if fi, err := os.Stat(filepath.Join(src, "criu")); err != nil || fi.Mode()&0o111 == 0 {
		return fmt.Errorf("bundle version %s is not staged on this node", id)
	}
	dst := a.companionToolsDir(id)
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	if sameDir(src, dst) {
		return nil
	}
	return attachReadOnly(src, dst)
}

// maxCompanionUnmounts bounds the mounts releaseCompanionTools detaches
// from one companion directory. One is expected; more were stacked by an
// agent before companionMu.
const maxCompanionUnmounts = 16

// releaseCompanionTools unmounts version id from its companion directory,
// every mount of it stacked there, and removes the directory, before the
// version itself is removed. A pod that still mounts the directory keeps
// it. The caller holds companionMu.
func (a *Agent) releaseCompanionTools(id string) error {
	dst := a.companionToolsDir(id)
	src := filepath.Join(a.bundleVersionsDir(), id)
	for n := 0; sameDir(src, dst); n++ {
		if n == maxCompanionUnmounts {
			return fmt.Errorf("%s is still mounted after %d unmounts", dst, n)
		}
		if err := unmountDir(dst); err != nil {
			return fmt.Errorf("unmount %s: %w", dst, err)
		}
	}
	if err := os.Remove(dst); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// removeBundleVersion releases version id's companion directory and removes
// the version, under companionMu: a mount of it started in between would
// outlive it.
func (a *Agent) removeBundleVersion(id string) error {
	a.companionMu.Lock()
	defer a.companionMu.Unlock()
	// Unmounted from its companion directory first, else the version is
	// kept: a mount of it must not outlive it.
	if err := a.releaseCompanionTools(id); err != nil {
		return fmt.Errorf("release its companion directory: %w", err)
	}
	return os.RemoveAll(filepath.Join(a.bundleVersionsDir(), id))
}

// syncCompanionTools mounts every staged version on this node on its
// companion directory, so that a companion of any of them can be captured
// here. Versions that are gone have their directories released by
// reapBundleVersions.
func (a *Agent) syncCompanionTools() {
	ents, err := os.ReadDir(a.bundleVersionsDir())
	if err != nil {
		return
	}
	for _, e := range ents {
		if !e.IsDir() || !validBundleID(e.Name()) {
			continue
		}
		if _, err := os.Stat(filepath.Join(a.bundleVersionsDir(), e.Name(), "criu")); err != nil {
			continue // not staged (a version being fetched)
		}
		if err := a.ensureCompanionTools(e.Name()); err != nil {
			a.log.WithError(err).WithField("bundle", e.Name()).
				Warn("node bundle: cannot mount a version for the pods without GPUs here; they are not captured until it is")
		}
	}
}

// mountsCompanionTools reports whether pod mounts the companion directory
// of version id.
func mountsCompanionTools(pod *corev1.Pod, id string) bool {
	suffix := "/" + webhook.BundleCompanionDir + "/" + id
	for i := range pod.Spec.Volumes {
		if hp := pod.Spec.Volumes[i].HostPath; hp != nil && len(hp.Path) >= len(suffix) && hp.Path[len(hp.Path)-len(suffix):] == suffix {
			return true
		}
	}
	return false
}

// mountsAnyCompanionTools reports whether pod mounts a companion directory.
func mountsAnyCompanionTools(pod *corev1.Pod) bool {
	marker := "/" + webhook.BundleCompanionDir + "/"
	for i := range pod.Spec.Volumes {
		if hp := pod.Spec.Volumes[i].HostPath; hp != nil && strings.Contains(hp.Path, marker) {
			return true
		}
	}
	return false
}

// companionBundleWait bounds how long a capture waits for a companion's
// node bundle to be fetched and mounted (ensurePlaceholderBundles) before
// it decides the instance is not captured. Before companions started
// without their bundle, such a companion stayed Pending, and its instance
// unready, until the fetch succeeded.
const companionBundleWait = 5 * time.Minute

// companionBundlesPending reports whether blocking findings fs are all a
// companion's missing node bundle (check "bundle" on a pod of companions),
// and leader's capture has waited less than companionBundleWait for them:
// the capture is then tried again, its instance still held.
func (c *criuAutoRestorer) companionBundlesPending(leader *corev1.Pod, companions []*corev1.Pod, fs []PreflightFinding, now time.Time) bool {
	if len(fs) == 0 {
		return false
	}
	names := map[string]bool{}
	for _, p := range companions {
		names[p.Name] = true
	}
	for _, f := range fs {
		if f.Check != "bundle" || !names[f.Pod] {
			return false
		}
	}
	first, _ := c.bundleWaitSince.LoadOrStore(leader.UID, now)
	return now.Sub(first.(time.Time)) < companionBundleWait
}

// companionToolsVersion is the bundle version whose companion directory a
// workload mounts at /criu-bundle (mounts: its mountinfo), "" when it does
// not mount one: a companion's restore placeholder.
func companionToolsVersion(mounts []mountEntry) string {
	marker := "/" + webhook.BundleCompanionDir + "/"
	for _, m := range mounts {
		if m.point != v2BinDirInContainer {
			continue
		}
		if i := strings.LastIndex(m.root, marker); i >= 0 {
			if id := m.root[i+len(marker):]; validBundleID(id) {
				return id
			}
		}
	}
	return ""
}

// errAgentNotYet is a pod of the instance on a node the agent's placement
// covers but no agent pod is bound to yet (an agent rollout, a new node):
// the capture is tried again once it is.
var errAgentNotYet = errors.New("the nvsnap agent of a node of the instance is not running yet")

// nodeInAgentPlacement reports whether node is one the agent DaemonSet runs
// on (its placement, the chart's --webhook-agent-placement); without one,
// whether the node has GPUs.
func (a *Agent) nodeInAgentPlacement(ctx context.Context, name string) (bool, error) {
	node, err := a.kubeClient.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	if a.config.Webhook.AgentPlacement == "" {
		q, ok := node.Status.Allocatable["nvidia.com/gpu"]
		return ok && !q.IsZero() && agentTolerates(nil, node), nil
	}
	var placement webhook.AgentPlacement
	if err := json.Unmarshal([]byte(a.config.Webhook.AgentPlacement), &placement); err != nil {
		return false, fmt.Errorf("agent placement: %w", err)
	}
	agentPod := &corev1.Pod{Spec: corev1.PodSpec{NodeSelector: placement.NodeSelector}}
	if placement.NodeAffinity != nil {
		agentPod.Spec.Affinity = &corev1.Affinity{NodeAffinity: placement.NodeAffinity}
	}
	return election.Admits(agentPod, node) && agentTolerates(placement.Tolerations, node), nil
}

// daemonSetTolerations are the tolerations the DaemonSet controller adds to
// every pod of a DaemonSet.
var daemonSetTolerations = []corev1.Toleration{
	{Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute},
	{Key: "node.kubernetes.io/unreachable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute},
	{Key: "node.kubernetes.io/disk-pressure", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
	{Key: "node.kubernetes.io/memory-pressure", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
	{Key: "node.kubernetes.io/pid-pressure", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
	{Key: "node.kubernetes.io/unschedulable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
	{Key: "node.kubernetes.io/network-unavailable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
}

// agentTolerates reports whether an agent pod with tolerations (the
// chart's, and the DaemonSet's own) may run on node: every NoSchedule and
// NoExecute taint of it is tolerated.
func agentTolerates(tolerations []corev1.Toleration, node *corev1.Node) bool {
	all := append(append([]corev1.Toleration(nil), tolerations...), daemonSetTolerations...)
	for i := range node.Spec.Taints {
		taint := &node.Spec.Taints[i]
		if taint.Effect == corev1.TaintEffectPreferNoSchedule {
			continue
		}
		tolerated := false
		for j := range all {
			if all[j].ToleratesTaint(taint) {
				tolerated = true
				break
			}
		}
		if !tolerated {
			return false
		}
	}
	return true
}

// agentArrivalWait bounds how long a capture waits for the agent of a node
// the agent's placement covers (errAgentNotYet) before it decides none
// will come and the instance is not captured.
const agentArrivalWait = 5 * time.Minute

// ensureCompanionToolsIn makes a companion workload (host pid, under
// procBase) see its bundle version at v2BinDirInContainer when it does not
// already: its companion directory does not propagate mounts, so a
// companion that started before the version was mounted there has it
// mounted into its own mount namespace. A version not staged on this node
// is not fetched here; the capture tries again once the node's sweep has
// fetched it. A workload that is not a companion, already sees a staged
// version, holds CAP_SYS_ADMIN or runs in a user namespace of its own is
// left as it is; bundleToolsOf's callers decide about it.
func (a *Agent) ensureCompanionToolsIn(procBase string, pid int) error {
	base := filepath.Join(procBase, strconv.Itoa(pid))
	id := companionToolsVersion(readMountinfo(filepath.Join(base, "mountinfo")))
	if id == "" {
		return nil
	}
	if _, err := bundleToolsOf(procBase, pid, a.bundleVersionsDir()); err == nil {
		return nil
	}
	if admin, err := hasCapability(filepath.Join(base, "status"), capSysAdmin); err != nil || admin {
		return nil
	}
	// A workload in a user namespace of its own can change its mounts
	// (nsenter_fd.go); bundleToolsOf's callers refuse it, so its tools are
	// not mounted either.
	user, err := os.Open(filepath.Join(base, "ns", "user")) //nolint:gosec // a /proc namespace file of a host pid
	if err != nil {
		return nil
	}
	defer func() { _ = user.Close() }()
	if initialUserNS(user) != nil {
		return nil
	}
	if err := a.ensureCompanionTools(id); err != nil {
		return err
	}
	return injectReadOnly(filepath.Join(a.bundleVersionsDir(), id), filepath.Join(base, "ns", "mnt"), v2BinDirInContainer)
}
