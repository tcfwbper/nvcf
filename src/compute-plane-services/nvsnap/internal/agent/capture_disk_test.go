// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/criu/mountinfo"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

const tib = int64(1) << 40

// diskHost is a fake node for dumpBytes: free space, GPU memory, mounts
// and archive sizes. install swaps it in for the test.
type diskHost struct {
	avail, total  uint64
	gpu           int64
	gpuErr        error
	mounts        []mountinfo.MountInfo
	mountsErr     error
	tmpfs         map[string]int64
	replay        map[string]int64
	upperdir      string
	upperdirErr   error
	upperdirBytes int64
}

func (h *diskHost) install(t *testing.T) {
	t.Helper()
	ds, gu, cm, cu, tu, rb, ub := diskSpace, gpuUsedBytes, containerMounts, containerUpperdir, tmpfsUsed, replayArchiveBytes, upperdirArchiveBytes
	t.Cleanup(func() {
		diskSpace, gpuUsedBytes, containerMounts, containerUpperdir, tmpfsUsed, replayArchiveBytes, upperdirArchiveBytes = ds, gu, cm, cu, tu, rb, ub
	})
	diskSpace = func(string) (uint64, uint64, error) { return h.avail, h.total, nil }
	gpuUsedBytes = func([]int) (int64, error) { return h.gpu, h.gpuErr }
	containerMounts = func(int) ([]mountinfo.MountInfo, error) { return h.mounts, h.mountsErr }
	containerUpperdir = func(int) (string, error) { return h.upperdir, h.upperdirErr }
	tmpfsUsed = func(_ int, mp string) (int64, error) {
		if n, ok := h.tmpfs[mp]; ok {
			return n, nil
		}
		return 0, errors.New("statfs failed")
	}
	replayArchiveBytes = func(_ int, mp string, _ []string, _ int64) (int64, error) { return h.replay[mp], nil }
	upperdirArchiveBytes = func(string, []string, int64) (int64, error) { return h.upperdirBytes, nil }
}

// diskProc is a /proc with one process pid of anon bytes, holding a GPU
// device open when gpu.
func diskProc(t *testing.T, pid int, anon int64, gpu bool) string {
	t.Helper()
	proc := t.TempDir()
	dir := filepath.Join(proc, strconv.Itoa(pid))
	if err := os.MkdirAll(filepath.Join(dir, "fd"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "smaps_rollup"), []byte("Rss: 1 kB\nAnonymous: "+strconv.FormatInt(anon>>10, 10)+" kB\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if gpu {
		if err := os.Symlink("/dev/nvidia0", filepath.Join(dir, "fd", "5")); err != nil {
			t.Fatal(err)
		}
	}
	return proc
}

func diskAgent(t *testing.T, keep int) *Agent {
	return &Agent{log: logrus.New(), config: Config{NodeName: "n1", CheckpointDir: t.TempDir(), CaptureDiskKeepPercent: keep}}
}

func TestSumGPUUsed(t *testing.T) {
	if got, err := sumGPUUsed("112, 140000\n519, 139000\n777, 5000\n", []int{112, 519}); err != nil || got != int64(279000)<<20 {
		t.Fatalf("sumGPUUsed = %d %v, want %d", got, err, int64(279000)<<20)
	}
	// "[N/A]" (MIG) for a wanted pid is an error, not zero; another pid's
	// line doesn't matter.
	if _, err := sumGPUUsed("112, [N/A]\n", []int{112}); err == nil {
		t.Fatal("an unparsable used_memory for a wanted pid: want an error")
	}
	if _, err := sumGPUUsed("112, [N/A]\n519, 10\n", []int{519}); err != nil {
		t.Fatalf("an unparsable line of another pid: %v", err)
	}
}

// Everything the capture writes is counted: anonymous memory, GPU memory,
// tmpfs contents, replay-mount archives and the rootfs diff.
func TestDumpBytesCountsEverythingTheCaptureWrites(t *testing.T) {
	h := &diskHost{avail: uint64(3 * tib), total: uint64(4 * tib), gpu: 500 << 30,
		mounts: []mountinfo.MountInfo{
			{MountPoint: "/", FsType: "overlay"},
			{MountPoint: "/dev/shm", FsType: "tmpfs"},
			{MountPoint: "/opt/nvsnap", Root: "/var/lib/kubelet/pods/u/volumes/kubernetes.io~empty-dir/c", FsType: "ext4", Opts: "rw"},
		},
		tmpfs: map[string]int64{"/dev/shm": 64 << 30}, replay: map[string]int64{"/opt/nvsnap": 3 << 30},
		upperdir: "/var/lib/up", upperdirBytes: 2 << 30}
	h.install(t)
	t.Setenv("NVSNAP_REPLAY_MOUNTS", "/opt/nvsnap")
	a := diskAgent(t, 20)
	got, err := a.dumpBytes(diskProc(t, 7, 10<<30, true), 7, []int{7}, 2*tib)
	if err != nil {
		t.Fatal(err)
	}
	want := int64(10<<30 + 500<<30 + 64<<30 + 3<<30 + 2<<30)
	if got != want {
		t.Fatalf("dumpBytes = %d GiB, want %d GiB", got>>30, want>>30)
	}
}

// Every part that cannot be measured fails the estimate: the capture is
// then blocked, not let through.
func TestDumpBytesFailsClosed(t *testing.T) {
	for name, tc := range map[string]struct {
		host    diskHost
		gpuOpen bool
		noAnon  bool
		// breakDir replaces /proc/<pid>/<breakDir> with a file, so it
		// cannot be read as a directory.
		breakDir string
	}{
		"upperdir lookup fails":                 {host: diskHost{upperdirErr: errors.New("mountinfo unreadable")}},
		"descriptors unreadable":                {breakDir: "fd"},
		"memory mappings unreadable":            {breakDir: "map_files"},
		"GPU open, nvidia-smi fails":            {host: diskHost{gpuErr: errors.New("driver not loaded")}, gpuOpen: true},
		"GPU open, no nvidia-smi":               {host: diskHost{gpuErr: errNoNvidiaSMI}, gpuOpen: true},
		"mounts unreadable":                     {host: diskHost{mountsErr: errors.New("no mountinfo")}},
		"tmpfs not measurable":                  {host: diskHost{mounts: []mountinfo.MountInfo{{MountPoint: "/dev/shm", FsType: "tmpfs"}}}},
		"memory unreadable (no Anonymous line)": {noAnon: true},
	} {
		t.Run(name, func(t *testing.T) {
			h := tc.host
			h.avail, h.total = uint64(3*tib), uint64(4*tib)
			h.install(t)
			proc := diskProc(t, 7, 1<<30, tc.gpuOpen)
			if tc.breakDir != "" {
				d := filepath.Join(proc, "7", tc.breakDir)
				if err := os.RemoveAll(d); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(d, nil, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if tc.noAnon {
				if err := os.WriteFile(filepath.Join(proc, "7", "smaps_rollup"), []byte("Rss: 1 kB\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := diskAgent(t, 20).dumpBytes(proc, 7, []int{7}, 2*tib); err == nil {
				t.Fatal("want an error")
			}
			fs := diskAgent(t, 20).diskFindings(proc, 7, []int{7}, "rank-0", "main")
			if len(fs) != 1 || fs[0].Severity != preflightBlock {
				t.Fatalf("findings = %+v, want one block", fs)
			}
		})
	}
}

// A process with no GPU open never asks nvidia-smi: a companion on a node
// whose nvidia-smi fails is not blocked.
func TestDumpBytesSkipsTheGPUQueryWithoutAGPUOpen(t *testing.T) {
	h := &diskHost{avail: uint64(3 * tib), total: uint64(4 * tib), gpuErr: errors.New("driver not loaded")}
	h.install(t)
	got, err := diskAgent(t, 20).dumpBytes(diskProc(t, 7, 1<<30, false), 7, []int{7}, 2*tib)
	if err != nil || got != 1<<30 {
		t.Fatalf("dumpBytes = %d %v, want 1 GiB and no error", got, err)
	}
}

// The incident: a ~570 GB rank on a node with 584 GB of 4 TB free.
func TestDiskFindingsBlockACaptureThatDoesNotFit(t *testing.T) {
	h := &diskHost{avail: 584 << 30, total: uint64(4 * tib), gpu: 560 << 30}
	h.install(t)
	fs := diskAgent(t, 20).diskFindings(diskProc(t, 7, 10<<30, true), 7, []int{7}, "rank-0", "main")
	if len(fs) != 1 || fs[0].Severity != preflightBlock || fs[0].Check != diskCheck {
		t.Fatalf("findings = %+v, want one disk block", fs)
	}
}

func TestDiskFindingsReportTheEstimateWhenItFits(t *testing.T) {
	h := &diskHost{avail: uint64(3 * tib), total: uint64(4 * tib), gpu: 560 << 30}
	h.install(t)
	fs := diskAgent(t, 20).diskFindings(diskProc(t, 7, 10<<30, true), 7, []int{7}, "rank-0", "main")
	room := 3*tib - 4*tib/100*20
	if len(fs) != 1 || fs[0].Severity != preflightInfo || fs[0].DiskBytes != 570<<30 || fs[0].DiskRoom != room || fs[0].Node != "n1" {
		t.Fatalf("findings = %+v, want one info with 570 GiB, the room and the node", fs)
	}
}

func TestDiskFindingsOffAtZero(t *testing.T) {
	h := &diskHost{avail: 1 << 30, total: uint64(4 * tib), gpu: 560 << 30}
	h.install(t)
	if fs := diskAgent(t, 0).diskFindings(diskProc(t, 7, 10<<30, true), 7, []int{7}, "rank-0", "main"); len(fs) != 0 {
		t.Fatalf("findings = %+v, want none with the check off", fs)
	}
}

// Members of one node that fit alone but not together block; members on
// different nodes are counted apart.
func TestNodeDiskFindingsAddUpANodesMembers(t *testing.T) {
	member := func(pod, node string, bytes int64) PreflightFinding {
		return PreflightFinding{Pod: pod, Check: diskCheck, Severity: preflightInfo, Node: node, DiskBytes: bytes, DiskRoom: 1000 << 30}
	}
	if got := nodeDiskFindings([]PreflightFinding{member("a", "n1", 600<<30), member("b", "n2", 600<<30)}); len(got) != 0 {
		t.Fatalf("different nodes: %+v, want none", got)
	}
	got := nodeDiskFindings([]PreflightFinding{member("a", "n1", 600<<30), member("b", "n1", 600<<30)})
	if len(got) != 1 || got[0].Severity != preflightBlock || got[0].Node != "n1" {
		t.Fatalf("one node: %+v, want one block for n1", got)
	}
}

// A member whose host checks did not run blocks the capture: its node's
// disk was not checked.
func TestCapturePreflightBlocksWhenAMembersDiskWasNotChecked(t *testing.T) {
	a := &Agent{kubeClient: fake.NewSimpleClientset(), log: logrus.New(), config: Config{CaptureDiskKeepPercent: 20}}
	rank := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "w0", Namespace: "ns"}, Spec: corev1.PodSpec{NodeName: "n-gone",
		Containers: []corev1.Container{{Name: "main"}}}}
	fs := a.capturePreflight(context.Background(), []*corev1.Pod{rank}, nil, nil, logrus.NewEntry(logrus.New()))
	for _, f := range fs {
		if f.Check == diskCheck && f.Severity == preflightBlock {
			return
		}
	}
	t.Fatalf("findings = %+v, want a disk block", fs)
}

// The disk finding's estimate reaches the capture's driver intact.
func TestDiskFindingSurvivesTheWire(t *testing.T) {
	in := PreflightFinding{Pod: "p", Check: diskCheck, Severity: preflightInfo, Node: "n1", DiskBytes: 570 << 30, DiskRoom: 2 << 40}
	b, _ := json.Marshal([]PreflightFinding{in})
	var out []PreflightFinding
	if err := json.Unmarshal(b, &out); err != nil || len(out) != 1 || out[0] != in {
		t.Fatalf("round trip = %+v %v, want %+v", out, err, in)
	}
}

func writeCheckpoint(t *testing.T, dir, id string, created time.Time, instance bool) {
	t.Helper()
	md := CheckpointMetadata{ID: id, CreatedAt: created}
	if instance {
		md.CatalogInfo = &CatalogInfo{InstanceCapture: true}
	}
	b, _ := json.Marshal(md)
	if err := os.MkdirAll(filepath.Join(dir, id), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, id, "metadata.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func diskTestRecord(key string, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: criuGroupConfigMapName(key), Namespace: "nvsnap-system",
		Labels: map[string]string{"app.kubernetes.io/managed-by": "nvsnap"}}, Data: data}
}

func exists(dir, id string) bool {
	_, err := os.Stat(filepath.Join(dir, id))
	return err == nil
}

// A low filesystem makes the reaper retire unnamed checkpoints at
// pressureRetireAge; with room it waits checkpointRetireAge.
func TestReaperRetiresEarlyWhileTheDiskIsLow(t *testing.T) {
	for _, low := range []bool{false, true} {
		dir := t.TempDir()
		now := time.Now()
		writeCheckpoint(t, dir, "orphan", now.Add(-time.Hour), true)
		h := &diskHost{avail: uint64(3 * tib), total: uint64(4 * tib)}
		if low {
			h.avail = 600 << 30
		}
		h.install(t)
		a := &Agent{kubeClient: fake.NewSimpleClientset(), log: logrus.New(), config: Config{NodeName: "n1", CheckpointDir: dir, CaptureDiskKeepPercent: 20}}
		a.reapCheckpoints(context.Background(), nil, now)
		if exists(dir, "orphan") == low {
			t.Errorf("low=%v: orphan kept=%v", low, exists(dir, "orphan"))
		}
	}
}

// Records left "verifying" after their namespaces went, marked abandoned,
// kept 2.5 TB of checkpoints per node for 11 days. An abandoned record
// names nothing; one still being verified, or still capturing, does.
func TestRetirableCheckpointsByRecordState(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	for _, id := range []string{"abandoned", "verifying", "capturing"} {
		writeCheckpoint(t, dir, id, now.Add(-5*time.Hour), true)
	}
	kc := fake.NewSimpleClientset(
		diskTestRecord("a", map[string]string{"state": criuGroupStateVerifying, "sourceNamespace": "gone", groupAbandonedKey: now.Format(time.RFC3339), "size": "1", "checkpoint.0": "abandoned"}),
		diskTestRecord("v", map[string]string{"state": criuGroupStateVerifying, "sourceNamespace": "live", "size": "1", "checkpoint.0": "verifying"}),
		diskTestRecord("c", map[string]string{"state": criuGroupStateCapturing, "size": "1", "checkpoint.0": "capturing"}),
	)
	a := &Agent{kubeClient: kc, log: logrus.New(), config: Config{NodeName: "n1", CheckpointDir: dir}}
	dead, err := a.retirableCheckpoints(context.Background(), nil, now, pressureRetireAge)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := dead["abandoned"]; !ok || len(dead) != 1 {
		t.Fatalf("retirable = %v, want only the abandoned record's checkpoint", dead)
	}
}

// catalogServer answers each DELETE with status() and records the paths.
func catalogServer(t *testing.T, status func() int) (*httptest.Server, func() []string) {
	var mu sync.Mutex
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.WriteHeader(status())
	}))
	t.Cleanup(srv.Close)
	return srv, func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), paths...) }
}

// A retired instance capture's catalog row is deleted; a lone capture's
// is not: nvsnap-server deletes by hash, and a lone capture's hash is its
// configuration's, which a newer capture may share.
func TestReaperDropsOnlyInstanceCapturesCatalogRows(t *testing.T) {
	srv, paths := catalogServer(t, func() int { return http.StatusNoContent })
	dir := t.TempDir()
	now := time.Now()
	writeCheckpoint(t, dir, "instance", now.Add(-5*time.Hour), true)
	writeCheckpoint(t, dir, "lone", now.Add(-5*time.Hour), false)
	kc := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: criuRestoreFailedConfigMap, Namespace: "nvsnap-system"},
		Data: map[string]string{"lone": "x"}})
	a := &Agent{kubeClient: kc, log: logrus.New(), config: Config{NodeName: "n1", CheckpointDir: dir, CatalogURL: srv.URL}}
	a.reapCheckpoints(context.Background(), nil, now)
	if exists(dir, "instance") || exists(dir, "lone") {
		t.Fatal("want both retired locally")
	}
	if got := paths(); len(got) != 1 || got[0] != "/api/v1/checkpoints/instance" {
		t.Fatalf("catalog deletes = %v, want only the instance capture's", got)
	}
}

// A catalog delete that fails is retried on later sweeps until it
// succeeds.
func TestCatalogDeleteIsRetriedFromItsTombstone(t *testing.T) {
	var mu sync.Mutex
	status := http.StatusInternalServerError
	srv, paths := catalogServer(t, func() int { mu.Lock(); defer mu.Unlock(); return status })
	dir := t.TempDir()
	now := time.Now()
	writeCheckpoint(t, dir, "orphan", now.Add(-5*time.Hour), true)
	a := &Agent{kubeClient: fake.NewSimpleClientset(), log: logrus.New(), config: Config{NodeName: "n1", CheckpointDir: dir, CatalogURL: srv.URL}}
	a.reapCheckpoints(context.Background(), nil, now)
	tombs := filepath.Join(dir, catalogTombstoneDir)
	if exists(dir, "orphan") || !exists(tombs, "orphan") {
		t.Fatal("want the checkpoint gone and its tombstone kept after a failed catalog delete")
	}
	mu.Lock()
	status = http.StatusNoContent
	mu.Unlock()
	a.reapCheckpoints(context.Background(), nil, now)
	if exists(tombs, "orphan") {
		t.Fatal("tombstone kept after the catalog delete succeeded")
	}
	if got := paths(); len(got) != 2 {
		t.Fatalf("catalog deletes = %v, want 2", got)
	}
}

// An unreachable nvsnap-server does not hold up local reclamation: the
// sweep retires every local checkpoint and bounds its catalog retries.
func TestCatalogRetriesDoNotDelayReclamation(t *testing.T) {
	stop := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-stop:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(stop)
	b, rt := catalogRetryBudget, catalogRequestTimeout
	catalogRetryBudget, catalogRequestTimeout = 300*time.Millisecond, 200*time.Millisecond
	defer func() { catalogRetryBudget, catalogRequestTimeout = b, rt }()
	dir := t.TempDir()
	now := time.Now()
	for i := 0; i < 20; i++ {
		writeCheckpoint(t, dir, "orphan"+strconv.Itoa(i), now.Add(-5*time.Hour), true)
	}
	a := &Agent{kubeClient: fake.NewSimpleClientset(), log: logrus.New(), config: Config{NodeName: "n1", CheckpointDir: dir, CatalogURL: srv.URL}}
	start := time.Now()
	a.reapCheckpoints(context.Background(), nil, now)
	for i := 0; i < 20; i++ {
		if exists(dir, "orphan"+strconv.Itoa(i)) {
			t.Fatalf("orphan%d not retired", i)
		}
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("sweep took %v with an unreachable server, want it bounded", d)
	}
	if ents, _ := os.ReadDir(filepath.Join(dir, catalogTombstoneDir)); len(ents) != 20 {
		t.Fatalf("tombstones = %d, want all 20 kept for later sweeps", len(ents))
	}
}

// A delete cascaded from another node's reaper keeps a copy a pod on this
// node restores from.
func TestDeleteKeepsACheckpointAPodRestoresFrom(t *testing.T) {
	dir := t.TempDir()
	writeCheckpoint(t, dir, "busy", time.Now(), true)
	writeCheckpoint(t, dir, "idle", time.Now(), true)
	kc := fake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns",
		Annotations: map[string]string{webhook.CRIURestoreAnnotation: "busy"}}, Spec: corev1.PodSpec{NodeName: "n1"}})
	a := &Agent{kubeClient: kc, log: logrus.New(), config: Config{NodeName: "n1", CheckpointDir: dir}}
	for id, want := range map[string]int{"busy": http.StatusConflict, "idle": http.StatusNoContent} {
		req := mux.SetURLVars(httptest.NewRequest(http.MethodDelete, "/v1/checkpoints/"+id, nil), map[string]string{"id": id})
		rec := httptest.NewRecorder()
		a.deleteCheckpointHandler(rec, req)
		if rec.Code != want || exists(dir, id) != (want == http.StatusConflict) {
			t.Errorf("%s: status %d (want %d), still there %v", id, rec.Code, want, exists(dir, id))
		}
	}
}

// Tar headers and padding count: many small files are far larger as an
// archive than their payload.
func TestTarBytesCountHeadersAndPadding(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 2000; i++ {
		if err := os.WriteFile(filepath.Join(dir, "f"+strconv.Itoa(i)), []byte{1}, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if over, n, err := tarBytesOver(os.DirFS(dir), 1<<20, nil); err != nil || !over {
		t.Fatalf("2000 one-byte files: over=%v n=%d err=%v, want over 1 MiB", over, n, err)
	}
}

// Sizing leaves out what the archive leaves out.
func TestTarBytesLeaveOutTheWritersExclusions(t *testing.T) {
	dir := t.TempDir()
	hub := filepath.Join(dir, "root", ".cache", "huggingface", "hub")
	if err := os.MkdirAll(hub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(hub, "weights"), make([]byte, 4<<20), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "proc-mount"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "proc-mount", "x"), make([]byte, 4<<20), 0o644); err != nil {
		t.Fatal(err)
	}
	exclude := append(tarPatternsForMountpoints([]string{"/proc-mount"}), rootfsDiffExcludeGlobs()...)
	if over, n, err := tarBytesOver(os.DirFS(dir), 1<<20, exclude); err != nil || over {
		t.Fatalf("with the exclusions: over=%v n=%d err=%v, want it to fit in 1 MiB", over, n, err)
	}
	if over, _, err := tarBytesOver(os.DirFS(dir), 1<<20, nil); err != nil || !over {
		t.Fatalf("without the exclusions: over=%v err=%v, want over 1 MiB", over, err)
	}
}

func TestTarExcluded(t *testing.T) {
	for _, tc := range []struct {
		name string
		pats []string
		want bool
	}{
		{"./root/.cache/huggingface/hub", []string{"*/huggingface/hub"}, true},
		{"./root/.cache/huggingface/hub/x/y", []string{"*/huggingface/hub/*"}, true},
		{"./root/.cache/huggingface", []string{"*/huggingface/hub"}, false},
		{"./usr/bin/nvidia-smi", tarPatternsForMountpoints([]string{"/usr/bin/nvidia-smi"}), true},
		{"./usr/bin/nvidia-smi-x", tarPatternsForMountpoints([]string{"/usr/bin/nvidia-smi"}), false},
		{"./sub/mnt", tarMountPatterns([]string{"sub/mnt"}), true},
		{"./data/a.bin", []string{"*.[ab]in"}, true},
		{"./data/a.cin", []string{"*.[!c]in"}, false},
		{"./data/a.bin", []string{"*.[!c]in"}, true},
		{"./data/x*y", []string{`*x\*y`}, true},
		{"./data/xzy", []string{`*x\*y`}, false},
	} {
		if got := tarExcluded(tc.name, tc.pats); got != tc.want {
			t.Errorf("tarExcluded(%q, %q) = %v, want %v", tc.name, tc.pats, got, tc.want)
		}
	}
}

// The capture's driver adds up the disk findings its members' nodes send
// back: two ranks on one node that fit alone but not together block.
func TestCapturePreflightAddsUpEachNodesMembers(t *testing.T) {
	// Each member's node answers its host checks: 600 GiB to write, 1000
	// GiB of room.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]PreflightFinding{{Pod: filepath.Base(r.URL.Path), Check: diskCheck, Severity: preflightInfo, Node: "n1", DiskBytes: 600 << 30, DiskRoom: 1000 << 30}})
	}))
	defer srv.Close()
	port := srv.URL[strings.LastIndexByte(srv.URL, ':'):]
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}, Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "127.0.0.1"}}}}
	a := &Agent{kubeClient: fake.NewSimpleClientset(node), log: logrus.New(), config: Config{ListenAddr: port, CaptureDiskKeepPercent: 20}}
	var ranks []*corev1.Pod
	for _, name := range []string{"w0", "w1"} {
		ranks = append(ranks, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"}, Spec: corev1.PodSpec{NodeName: "n1",
			Containers: []corev1.Container{{Name: "main"}}}})
	}
	fs := a.capturePreflight(context.Background(), ranks, nil, nil, logrus.NewEntry(logrus.New()))
	for _, f := range fs {
		if f.Check == diskCheck && f.Severity == preflightBlock && f.Node == "n1" {
			return
		}
	}
	t.Fatalf("findings = %+v, want a disk block for node n1", fs)
}

// A member whose node sends no disk check (its agent has it off, or
// predates it) blocks: its writes would go uncounted.
func TestCapturePreflightRequiresEachMembersDiskCheck(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]PreflightFinding{{Pod: filepath.Base(r.URL.Path), Check: "rdma", Severity: preflightInfo}})
	}))
	defer srv.Close()
	port := srv.URL[strings.LastIndexByte(srv.URL, ':'):]
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}, Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "127.0.0.1"}}}}
	rank := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "w0", Namespace: "ns"}, Spec: corev1.PodSpec{NodeName: "n1", Containers: []corev1.Container{{Name: "main"}}}}
	for _, keep := range []int{20, 0} {
		a := &Agent{kubeClient: fake.NewSimpleClientset(node), log: logrus.New(), config: Config{ListenAddr: port, CaptureDiskKeepPercent: keep}}
		blocked := false
		for _, f := range a.capturePreflight(context.Background(), []*corev1.Pod{rank}, nil, nil, logrus.NewEntry(logrus.New())) {
			blocked = blocked || (f.Check == diskCheck && f.Severity == preflightBlock)
		}
		if blocked != (keep > 0) {
			t.Errorf("keep %d: blocked=%v, want %v", keep, blocked, keep > 0)
		}
	}
}

// Retries go least recently tried first: tombstones that keep failing do
// not take every sweep's budget from the others.
func TestCatalogRetriesRotate(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, filepath.Base(r.URL.Path))
		mu.Unlock()
		<-r.Context().Done() // never answers: each attempt uses its whole timeout
	}))
	defer srv.Close()
	b, rt := catalogRetryBudget, catalogRequestTimeout
	catalogRetryBudget, catalogRequestTimeout = 150*time.Millisecond, 100*time.Millisecond
	defer func() { catalogRetryBudget, catalogRequestTimeout = b, rt }()
	dir := t.TempDir()
	tombs := filepath.Join(dir, catalogTombstoneDir)
	if err := os.MkdirAll(tombs, 0o755); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for i, id := range []string{"a", "b", "c", "d"} {
		p := filepath.Join(tombs, id)
		if err := os.WriteFile(p, []byte(now.UTC().Format(time.RFC3339)), 0o644); err != nil {
			t.Fatal(err)
		}
		old := now.Add(-time.Duration(10-i) * time.Minute)
		if err := os.Chtimes(p, old, old); err != nil {
			t.Fatal(err)
		}
	}
	a := &Agent{kubeClient: fake.NewSimpleClientset(), log: logrus.New(), config: Config{NodeName: "n1", CheckpointDir: dir, CatalogURL: srv.URL}}
	a.reapCheckpoints(context.Background(), nil, now)
	mu.Lock()
	first := len(paths)
	mu.Unlock()
	a.reapCheckpoints(context.Background(), nil, now)
	mu.Lock()
	defer mu.Unlock()
	if first == 0 || first >= 4 || len(paths) <= first {
		t.Fatalf("attempts per sweep: %v (first sweep %d), want a budget that fits some, not all", paths, first)
	}
	if paths[first] != []string{"a", "b", "c", "d"}[first] {
		t.Fatalf("second sweep started with %q, want the least recently tried %q: %v", paths[first], []string{"a", "b", "c", "d"}[first], paths)
	}
}

// pidsUseGPU fails when a live process's descriptors can't be read,
// skips a process that is gone, and finds a GPU device held open.
func TestPidsUseGPU(t *testing.T) {
	proc := diskProc(t, 7, 1<<30, true)
	if got, err := pidsUseGPU(proc, []int{7, 99}); err != nil || !got {
		t.Fatalf("GPU open, pid 99 gone: %v %v, want true and no error", got, err)
	}
	cpu := diskProc(t, 8, 1<<30, false)
	if got, err := pidsUseGPU(cpu, []int{8}); err != nil || got {
		t.Fatalf("no GPU open: %v %v, want false and no error", got, err)
	}
	fd := filepath.Join(cpu, "8", "fd")
	if err := os.RemoveAll(fd); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fd, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := pidsUseGPU(cpu, []int{8}); err == nil {
		t.Fatal("descriptors unreadable: want an error")
	}
}

// A deleted file that cannot be sized fails the estimate; one gone since
// it was listed is skipped.
func TestDumpBytesFailsOnAnUnsizableDeletedFile(t *testing.T) {
	h := &diskHost{avail: uint64(3 * tib), total: uint64(4 * tib)}
	h.install(t)
	proc := diskProc(t, 7, 1<<30, false)
	files := t.TempDir()
	gone := filepath.Join(files, "gone (deleted)")
	if err := os.Symlink(gone, filepath.Join(proc, "7", "fd", "3")); err != nil {
		t.Fatal(err)
	}
	if _, err := diskAgent(t, 20).dumpBytes(proc, 7, []int{7}, 2*tib); err != nil {
		t.Fatalf("a deleted file gone since: %v, want no error", err)
	}
	// A link that loops: stat fails with ELOOP, not ENOENT.
	loop := filepath.Join(files, "loop (deleted)")
	if err := os.Symlink(loop, loop); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(loop, filepath.Join(proc, "7", "fd", "4")); err != nil {
		t.Fatal(err)
	}
	if _, err := diskAgent(t, 20).dumpBytes(proc, 7, []int{7}, 2*tib); err == nil {
		t.Fatal("an unsizable deleted file: want an error")
	}
}
