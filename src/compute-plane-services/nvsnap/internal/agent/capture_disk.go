// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// Checkpoint disk. A capture writes its pods' memory to the node's
// checkpoint filesystem: about 570 GB per rank of a large model. On a
// node whose checkpoint directory shares the kubelet's image filesystem,
// a capture that fills it past the kubelet's eviction threshold makes the
// kubelet evict every pod on the node, the instance's ranks and the agent
// among them, and keep doing so: what fills the disk is not a pod's, so
// evicting pods frees nothing.
//
// So the capture preflight (capture_preflight.go), before any member
// suspends, estimates on each member's node everything the member's
// capture writes there (dumpBytes) and the room the node has above
// CaptureDiskKeepPercent free (checkpointRoom). A member that does not fit
// blocks the capture; so do the members of one node that do not fit
// together (nodeDiskFindings), and a member whose estimate cannot be made.
// The check keeps no state: nothing is reserved, nothing to release.
//
// It does not cover two instances captured on one node at the same time,
// nor other writers (peer fetches, L2 pulls) between the preflight and the
// dump. While the filesystem is low the node reaper retires the
// checkpoints no capture restores again early (pressureRetireAge).

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/criu/mountinfo"
)

// pressureRetireAge is how old a checkpoint no record names must be to
// be retired while the checkpoint filesystem is low.
const pressureRetireAge = 15 * time.Minute

// diskCheck is the preflight finding's Check for the checkpoint disk.
const diskCheck = "disk"

// Host lookups dumpBytes makes; variables for tests.
var (
	// gpuUsedBytes returns the GPU memory pids use, from nvidia-smi.
	gpuUsedBytes = func(pids []int) (int64, error) {
		out, err := hostNvidiaSMI("--query-compute-apps=pid,used_memory", "--format=csv,noheader,nounits")
		if err != nil {
			return 0, err
		}
		return sumGPUUsed(string(out), pids)
	}
	// containerMounts returns the mounts of the container whose init is pid.
	containerMounts = func(pid int) ([]mountinfo.MountInfo, error) {
		return mountinfo.ParseMountinfo(mountinfo.ProcMountinfoPath(pid))
	}
	// containerUpperdir returns the overlay upperdir of the container whose
	// init is pid, as the capture resolves it; "" and no error for none.
	containerUpperdir = mountinfo.ResolveOverlayUpperdir
	// tmpfsUsed returns the bytes in use on the tmpfs mounted at mp in the
	// container whose init is pid.
	tmpfsUsed = func(pid int, mp string) (int64, error) {
		var st syscall.Statfs_t
		if err := syscall.Statfs(filepath.Join("/proc", strconv.Itoa(pid), "root", mp), &st); err != nil {
			return 0, err
		}
		return int64(st.Blocks-st.Bfree) * st.Bsize, nil //nolint:gosec // block counts and size are small and positive
	}
	// replayArchiveBytes bounds the archive the capture writes of the mount
	// at mp (tarMount), leaving out its nested mounts; it stops past limit.
	replayArchiveBytes = func(pid int, mp string, nested []string, limit int64) (int64, error) {
		d, err := openWorkloadDir("/proc", pid, mp)
		if err != nil {
			return 0, err
		}
		defer func() { _ = d.Close() }()
		_, n, err := tarBytesOver(d.root.FS(), limit, tarMountPatterns(nested))
		return n, err
	}
	// upperdirArchiveBytes bounds the rootfs diff archive of upperdir
	// (archiveOverlayDir); it stops past limit.
	upperdirArchiveBytes = func(upperdir string, exclude []string, limit int64) (int64, error) {
		_, n, err := tarBytesOver(os.DirFS(upperdir), limit, exclude)
		return n, err
	}
)

// sumGPUUsed sums the used_memory (MiB) of nvidia-smi's "pid, used_memory"
// lines whose pid is in pids; an error for such a line without a number
// ("[N/A]" on a MIG slice), which would otherwise count as nothing.
func sumGPUUsed(out string, pids []int) (int64, error) {
	want := make(map[int]bool, len(pids))
	for _, p := range pids {
		want[p] = true
	}
	var mib int64
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		parts := strings.SplitN(line, ",", 2)
		if len(parts) != 2 {
			continue
		}
		pid, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil || !want[pid] {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("nvidia-smi gives no GPU memory for pid %d: %q", pid, strings.TrimSpace(parts[1]))
		}
		mib += n
	}
	return mib << 20, nil
}

// pidsUseGPU reports whether any of pids holds an NVIDIA device open; an
// error when a live process's descriptors cannot be read. A process, or a
// descriptor, gone since it was listed is skipped.
func pidsUseGPU(procBase string, pids []int) (bool, error) {
	for _, pid := range pids {
		fdDir := filepath.Join(procBase, strconv.Itoa(pid), "fd")
		ents, err := os.ReadDir(fdDir)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return false, fmt.Errorf("cannot tell whether pid %d uses a GPU: %w", pid, err)
		}
		for _, e := range ents {
			t, err := os.Readlink(filepath.Join(fdDir, e.Name()))
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return false, fmt.Errorf("cannot tell whether pid %d uses a GPU: %w", pid, err)
			}
			if strings.HasPrefix(t, "/dev/nvidia") {
				return true, nil
			}
		}
	}
	return false, nil
}

// anonymousBytesOf is the anonymous memory of process pid, an error when
// it cannot be read; a process gone since it was listed has nothing to
// dump.
func anonymousBytesOf(procBase string, pid int) (int64, error) {
	b, err := os.ReadFile(filepath.Join(procBase, strconv.Itoa(pid), "smaps_rollup"))
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("cannot read the memory of pid %d: %w", pid, err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		if rest, ok := strings.CutPrefix(line, "Anonymous:"); ok {
			kb, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimSpace(rest), " kB"), 10, 64)
			if err != nil {
				return 0, fmt.Errorf("pid %d: unreadable Anonymous line %q", pid, rest)
			}
			return kb << 10, nil
		}
	}
	return 0, fmt.Errorf("pid %d: no Anonymous line in smaps_rollup", pid)
}

// memfdBytes is the allocated size of the memfds pids hold or map, each
// once: the dump saves their contents, and they are neither anonymous
// memory nor deleted files. An error when a live process's descriptors or
// mappings, or a memfd of them, cannot be read; what is gone since it was
// listed is skipped.
func memfdBytes(procBase string, pids []int) (int64, error) {
	seen := map[string]bool{}
	var n int64
	for _, pid := range pids {
		for _, dir := range []string{"fd", "map_files"} {
			base := filepath.Join(procBase, strconv.Itoa(pid), dir)
			ents, err := os.ReadDir(base)
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return 0, fmt.Errorf("cannot read the %s of pid %d: %w", dir, pid, err)
			}
			for _, e := range ents {
				link := filepath.Join(base, e.Name())
				t, err := os.Readlink(link)
				if errors.Is(err, fs.ErrNotExist) {
					continue
				}
				if err != nil {
					return 0, fmt.Errorf("cannot read %s: %w", link, err)
				}
				if !strings.HasPrefix(t, "/memfd:") {
					continue
				}
				var st syscall.Stat_t
				if err := syscall.Stat(link, &st); errors.Is(err, syscall.ENOENT) {
					continue
				} else if err != nil {
					return 0, fmt.Errorf("cannot size the memfd %s of pid %d: %w", t, pid, err)
				}
				key := fmt.Sprintf("%d:%d", st.Dev, st.Ino)
				if !seen[key] {
					seen[key] = true
					n += st.Blocks * 512
				}
			}
		}
	}
	return n, nil
}

// dumpBytes estimates everything the capture of the container whose init
// is initPID and whose processes are pids writes to the checkpoint
// filesystem: its processes' anonymous memory, the memfds and the deleted
// files the dump saves (each once, up to the ghost limit; a larger one
// blocks on its own), its tmpfs mounts, the GPU memory it uses, the
// archive of its rootfs diff and those of its replay mounts. Archives are
// sized only up to limit: past it the capture is blocked anyway. An error
// when a part cannot be measured.
func (a *Agent) dumpBytes(procBase string, initPID int, pids []int, limit int64) (int64, error) {
	var n int64
	ghosts := map[string]ghostFile{}
	for _, pid := range pids {
		anon, err := anonymousBytesOf(procBase, pid)
		if err != nil {
			return 0, err
		}
		n += anon
		if err := deletedFiles(procBase, pid, ghosts); err != nil {
			return 0, err
		}
	}
	for _, g := range ghosts {
		if g.allocated <= a.ghostLimit() {
			n += g.allocated
		}
	}
	memfds, err := memfdBytes(procBase, pids)
	if err != nil {
		return 0, err
	}
	n += memfds
	gpuOpen, err := pidsUseGPU(procBase, pids)
	if err != nil {
		return 0, err
	}
	if gpuOpen {
		gpu, err := gpuUsedBytes(pids)
		if err != nil {
			return 0, fmt.Errorf("cannot read the GPU memory the dump saves: %w", err)
		}
		n += gpu
	}
	mounts, err := containerMounts(initPID)
	if err != nil {
		return 0, fmt.Errorf("cannot read the container's mounts: %w", err)
	}
	var mountPoints []string
	for _, m := range mounts {
		if m.MountPoint != "/" {
			mountPoints = append(mountPoints, m.MountPoint)
		}
	}
	allowlist := ReplayMountAllowlist()
	for _, m := range mounts {
		switch {
		case m.MountPoint == "/":
		case m.FsType == "tmpfs":
			// The dump saves a tmpfs's contents; counted even where it is
			// external, which overcounts.
			used, err := tmpfsUsed(initPID, m.MountPoint)
			if err != nil {
				return 0, fmt.Errorf("cannot size the tmpfs at %s: %w", m.MountPoint, err)
			}
			n += used
		case classifyMount(m.Root, m.MountPoint, m.FsType, m.Opts, allowlist) == MountClassReplay:
			if isEmptyDirMount(m.Root) && !onAllowlist(m.MountPoint, allowlist) {
				if over, _, err := emptyDirOver(initPID, m.MountPoint, ReplayEmptyDirMaxBytes()); err != nil || over {
					continue // not copied (captureReplayMounts)
				}
			}
			b, err := replayArchiveBytes(initPID, m.MountPoint, nestedMountPoints(mountPoints, m.MountPoint), limit-n)
			if err != nil {
				return 0, fmt.Errorf("cannot size the mount at %s: %w", m.MountPoint, err)
			}
			n += b
		}
		if n > limit {
			return n, nil
		}
	}
	upper, err := containerUpperdir(initPID)
	if err != nil {
		return 0, fmt.Errorf("cannot find the container's rootfs diff: %w", err)
	}
	if upper != "" {
		exclude := append(tarPatternsForMountpoints(append(mountPoints, v2BinDirInContainer, v2ImagesDirInContainer)), rootfsDiffExcludeGlobs()...)
		b, err := upperdirArchiveBytes(upper, exclude, limit-n)
		if err != nil {
			return 0, fmt.Errorf("cannot size the rootfs diff: %w", err)
		}
		n += b
	}
	return n, nil
}

// checkpointRoom is how many bytes the checkpoint filesystem takes before
// it has less than CaptureDiskKeepPercent free; on is false with the
// check off.
func (a *Agent) checkpointRoom() (room int64, on bool, err error) {
	keep := a.config.CaptureDiskKeepPercent
	if keep <= 0 || keep >= 100 {
		return 0, false, nil
	}
	avail, total, err := diskSpace(a.config.CheckpointDir)
	if err != nil {
		return 0, true, err
	}
	reserve := total / 100 * uint64(keep) //nolint:gosec // keep checked to be 1..99
	if avail <= reserve {
		return 0, true, nil
	}
	return int64(avail - reserve), true, nil //nolint:gosec // bounded by the filesystem size
}

// diskFindings is the checkpoint-disk check of a container on this node:
// an info finding carrying its estimate and the node's room
// (nodeDiskFindings adds up a node's members), or a block when it does not
// fit alone or cannot be measured. It reads and changes nothing else.
func (a *Agent) diskFindings(procBase string, initPID int, pids []int, pod, container string) []PreflightFinding {
	f := PreflightFinding{Pod: pod, Container: container, Check: diskCheck, Node: a.config.NodeName}
	room, on, err := a.checkpointRoom()
	if !on {
		return nil
	}
	if err != nil {
		f.Severity, f.Detail = preflightBlock, "cannot read the checkpoint filesystem's free space: "+err.Error()
		return []PreflightFinding{f}
	}
	size, err := a.dumpBytes(procBase, initPID, pids, room)
	if err != nil {
		f.Severity, f.Detail = preflightBlock, err.Error()+"; the capture is skipped rather than risk filling the checkpoint disk"
		return []PreflightFinding{f}
	}
	f.DiskBytes, f.DiskRoom = size, room
	if size > room {
		f.Severity = preflightBlock
		f.Detail = fmt.Sprintf("the capture writes about %d GiB but the checkpoint filesystem has %d GiB above its %d%% kept free; the kubelet evicts every pod on a node whose disk runs this low",
			size>>30, room>>30, a.config.CaptureDiskKeepPercent)
		return []PreflightFinding{f}
	}
	f.Severity = preflightInfo
	f.Detail = fmt.Sprintf("the capture writes about %d GiB; the checkpoint filesystem has %d GiB above its kept free space", size>>30, room>>30)
	return []PreflightFinding{f}
}

// nodeDiskFindings blocks the capture where the members on one node fit
// alone but not together, from the members' disk findings in fs.
func nodeDiskFindings(fs []PreflightFinding) []PreflightFinding {
	type node struct {
		bytes, room int64
		members     int
		pod         string
	}
	nodes := map[string]*node{}
	for _, f := range fs {
		if f.Check != diskCheck || f.Severity != preflightInfo || f.Node == "" {
			continue
		}
		n := nodes[f.Node]
		if n == nil {
			n = &node{room: f.DiskRoom, pod: f.Pod}
			nodes[f.Node] = n
		}
		n.bytes += f.DiskBytes
		n.members++
		n.room = min(n.room, f.DiskRoom)
	}
	var out []PreflightFinding
	for name, n := range nodes {
		if n.members > 1 && n.bytes > n.room {
			out = append(out, PreflightFinding{Pod: n.pod, Severity: preflightBlock, Check: diskCheck, Node: name,
				Detail: fmt.Sprintf("the %d members on node %s write about %d GiB together but its checkpoint filesystem has %d GiB above the kept free space",
					n.members, name, n.bytes>>30, n.room>>30)})
		}
	}
	return out
}

// checkpointDiskLow reports whether the checkpoint filesystem has less
// than CaptureDiskKeepPercent plus 10 points free: the reaper then retires
// early (pressureRetireAge).
func (a *Agent) checkpointDiskLow() bool {
	keep := a.config.CaptureDiskKeepPercent
	if keep <= 0 || keep >= 100 {
		return false
	}
	avail, total, err := diskSpace(a.config.CheckpointDir)
	if err != nil || total == 0 {
		return false
	}
	return avail < total/100*uint64(min(keep+10, 99)) //nolint:gosec // keep checked to be 1..99
}

// tarBytesOver bounds the tar archive of fsys: per entry a header block
// and one more for a long name or extended attributes, file data padded
// to 512-byte blocks, and the two end blocks. It leaves out what the
// writer's tar --exclude patterns exclude, and stops once over limit.
// Sparse files and hard links count at full size: an overcount, which
// only blocks before the capture.
func tarBytesOver(fsys fs.FS, limit int64, exclude []string) (over bool, total int64, err error) {
	excluded, err := tarExcluder(exclude)
	if err != nil {
		return false, 0, err
	}
	total = 1024
	err = fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == "." {
			return nil
		}
		if excluded("./" + p) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		total += 1024
		if d.Type().IsRegular() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			total += (info.Size() + 511) / 512 * 512
		}
		if total > limit {
			return fs.SkipAll
		}
		return nil
	})
	return total > limit, total, err
}

// tarExcluded reports whether GNU tar's --exclude patterns leave out the
// member name (as "./path").
func tarExcluded(name string, patterns []string) bool {
	excluded, err := tarExcluder(patterns)
	return err == nil && excluded(name)
}

// tarExcluder compiles patterns once into tarExcluded's test. GNU tar's
// exclusion defaults apply: "*" and "?" match "/" too, and a pattern
// matches the whole name or any part of it that starts after a "/".
func tarExcluder(patterns []string) (func(name string) bool, error) {
	res := make([]*regexp.Regexp, 0, len(patterns))
	for _, pat := range patterns {
		if _, err := path.Match(pat, ""); err != nil {
			return nil, fmt.Errorf("tar exclude pattern %q: %w", pat, err)
		}
		re, err := regexp.Compile("^" + tarGlobRegexp(pat) + "$")
		if err != nil {
			return nil, fmt.Errorf("tar exclude pattern %q: %w", pat, err)
		}
		res = append(res, re)
	}
	return func(name string) bool {
		for _, re := range res {
			if matchesTarName(re, name) {
				return true
			}
		}
		return false
	}, nil
}

// matchesTarName reports whether re matches name or a part of it that
// starts after a "/" (GNU tar's unanchored exclusion).
func matchesTarName(re *regexp.Regexp, name string) bool {
	for rest := name; ; {
		if re.MatchString(rest) {
			return true
		}
		i := strings.IndexByte(rest, '/')
		if i < 0 {
			return false
		}
		rest = rest[i+1:]
	}
}

// tarGlobRegexp turns a tar wildcard pattern into a regular expression:
// "*" and "?" match any characters, "/" included; a bracket expression
// stays a character class ("!" negates); "\" escapes the next character.
func tarGlobRegexp(pat string) string {
	var b strings.Builder
	for i := 0; i < len(pat); i++ {
		switch c := pat[i]; c {
		case '*':
			b.WriteString(".*")
		case '?':
			b.WriteString(".")
		case '\\':
			if i+1 < len(pat) {
				i++
				b.WriteString(regexp.QuoteMeta(pat[i : i+1]))
			} else {
				b.WriteString(`\\`)
			}
		case '[':
			if j := strings.IndexByte(pat[i+1:], ']'); j >= 0 {
				class := pat[i+1 : i+1+j]
				if strings.HasPrefix(class, "!") {
					class = "^" + class[1:]
				}
				b.WriteString("[" + strings.ReplaceAll(class, `\`, `\\`) + "]")
				i += j + 1
			} else {
				b.WriteString(`\[`)
			}
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	return b.String()
}

// hasDiskFinding reports whether fs, a member's host findings, has its
// disk check: a block, or an estimate from the member's node.
func hasDiskFinding(fs []PreflightFinding, pod, node string) bool {
	for _, f := range fs {
		if f.Check != diskCheck || f.Pod != pod {
			continue
		}
		if f.Severity == preflightBlock || (f.Severity == preflightInfo && f.Node == node) {
			return true
		}
	}
	return false
}
