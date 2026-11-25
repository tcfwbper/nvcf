// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// Multi-node gpushare. A workload whose ranks share GPU memory across nodes
// (multi-node NVLink, fabric handles) is suspended and resumed by one
// nvsnap-gpu-suspend per pod, and those tools must agree twice:
//
//   - Quiesce vote. No node may release memory while another node's kernels
//     still read it. Each attempt, every tool writes DIR/q
//     ("<attempt> ok|busy") and waits for DIR/d ("<attempt> go|retry").
//   - Handle exchange. Fabric handles change across a restore. After load,
//     every tool writes its new handles to DIR/out and waits for DIR/in,
//     the out files of every pod concatenated.
//
// An in-place resume (leave-running) adds a third: no node's GPU launches
// may run until every node has resumed. Each node binds its share of the
// memory multicast across nodes (NCCL NVLS) at its resume; a node that
// launched before another had bound its share would have its NVLink
// accesses to that node refused (Xid 137) and its kernels would spin for
// good. So each tool resumes with its launches held, writes DIR/resumed
// ("<token>"), and waits for DIR/o ("<token> open|abort") before it lets
// them through (--sync-open, see driveFabric's open phase).
//
// DIR is a fabric session directory the agent creates host-side inside the
// pod's gpushare store (the workload sees it under GPUShareStoreInContainer),
// fresh for every session, so a list from an earlier cycle can never be
// merged into a later one. Each agent serves the directories of its pods
// (fabricStateHandler and friends); the agent that receives a group
// checkpoint drives the session: it starts every member's checkpoint and
// answers the votes and the exchange through those endpoints.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

const (
	// fabricDirPrefix names a session's directory inside a pod's store.
	// The dot keeps it apart from the tool's own chunk lists.
	fabricDirPrefix = ".fabric-"

	// fabricEnv switches libnvsnap_gpushare to fabric handles for memory
	// shared across nodes; the webhook sets it on pods annotated
	// GPUShareFabricAnnotation.
	fabricEnv = "NVSNAP_GPUSHARE_FABRIC"

	// fabricVerdictAbort answers a vote once the session cannot succeed.
	// The tool rolls the workload back as soon as it reads it.
	fabricVerdictAbort = "abort"
	// fabricVerdictUnlock tells a restored rank that every pod of its
	// instance has resumed.
	fabricVerdictUnlock = "unlock"
	// fabricVerdictOpen tells a node resumed in place, its GPU launches
	// held, that every node of the instance has resumed.
	fabricVerdictOpen = "open"

	fabricPollInterval = 200 * time.Millisecond
)

// fabricMemberTimeout bounds one request to a member's agent. A reply is a
// local file read or write on the member's node (milliseconds); the bound
// keeps a member that accepts a connection and never answers from holding
// the driver past the vote deadline (2 min) by more than this. A variable
// for tests.
var fabricMemberTimeout = 10 * time.Second

var fabricSessionRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

// fabricOpenTokenRe is the token a tool writes to DIR/resumed: its own
// pid and its resume's round in it, so that a verdict for another tool or
// round is never taken for its own.
var fabricOpenTokenRe = regexp.MustCompile(`^\d{1,10}-\d{1,10}$`)

// fabricOpenDeadline bounds the driver's wait for every node's resumed
// marker, from the first one it sees: the handle exchange has every node
// loaded before any remaps, so the markers normally come within a second of
// each other. At the deadline the nodes that resumed get "abort", and open
// anyway. The tool's own wait (gpushareSyncOpenWait) is a minute longer,
// for a driver that never answers.
const fabricOpenDeadline = 2 * time.Minute

// validFabricSession reports whether s can name a session directory.
func validFabricSession(s string) bool { return fabricSessionRe.MatchString(s) }

// newFabricSession returns a fresh session id.
func newFabricSession() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "s" + hex.EncodeToString(b)
}

// fabricKey identifies one pod's directory in a session.
func fabricKey(session, namespace, pod string) string {
	return session + "/" + namespace + "/" + pod
}

// fabricContainerDir is the session directory as the workload (and the tool
// running in its namespaces) sees it.
func fabricContainerDir(session string) string {
	return GPUShareStoreInContainer + "/" + fabricDirPrefix + session
}

// fabricHostDir is the session directory as the agent sees it, in the pod's
// store under the checkpoint dir.
func (a *Agent) fabricHostDir(podUID, session string) string {
	return filepath.Join(a.config.CheckpointDir, GPUSharePodStoresSubdir, podUID, fabricDirPrefix+session)
}

// openFabricSession creates a pod's session directory and registers it, so
// the driver can reach it. The returned func removes both.
func (a *Agent) openFabricSession(session, namespace, pod, podUID string) (*fabricSessionDir, func(), error) {
	if !validFabricSession(session) {
		return nil, nil, fmt.Errorf("gpushare: invalid fabric session %q", session)
	}
	if podUID == "" {
		return nil, nil, errors.New("gpushare: a fabric session needs the pod uid to find its store")
	}
	s, closeFn, err := a.openFabricSessionAt(session, namespace, pod, a.fabricHostDir(podUID, session))
	if err != nil {
		return nil, nil, err
	}
	s.container = fabricContainerDir(session)
	return s, closeFn, nil
}

// fabricSessionDir is a session directory, worked in through its parent: the
// pod's chunk store, or a checkpoint's, which the workload can write
// (confined_fs.go).
type fabricSessionDir struct {
	parent *confinedDir
	name   string
	// container is the directory as the workload and the tool see it.
	container string
	// holdsOpen: this node's tool holds the GPU launches at its in-place
	// resume until the driver's open verdict (--sync-open). The driver
	// waits only for the members that do.
	holdsOpen atomic.Bool
	// warnedMarker: a malformed DIR/resumed was logged. The driver reads
	// the state every fabricPollInterval, and the workload writes the
	// marker: it is logged once per session, not at every read.
	warnedMarker atomic.Bool
}

// own gives the directory to the user process pid runs as: the tool and
// the restored processes write their handle lists there as that user.
func (s *fabricSessionDir) own(procBase string, pid int) error {
	uid, gid, err := procOwner(procBase, pid)
	if err != nil {
		return err
	}
	return s.parent.root.Lchown(s.name, uid, gid)
}

func (s *fabricSessionDir) read(name string, limit int64) ([]byte, error) {
	return s.parent.readFile(s.name+"/"+name, limit)
}

func (s *fabricSessionDir) exists(name string) bool { return s.parent.exists(s.name + "/" + name) }

// write writes name by rename, so the tool never reads a partial file.
func (s *fabricSessionDir) write(name string, data []byte) error {
	return s.parent.writeFileAtomic(s.name+"/"+name, data) // read by the tool inside the workload
}

// fabricSmallLimit bounds the files of a session other than the handle
// lists: a vote, a verdict.
const fabricSmallLimit = 4 << 10

// openFabricSessionAt creates host (fresh) and registers it for the pod.
// host's parent is created when missing; host itself is worked in only
// through its parent (fabricSessionDir).
func (a *Agent) openFabricSessionAt(session, namespace, pod, host string) (*fabricSessionDir, func(), error) {
	if !validFabricSession(session) {
		return nil, nil, fmt.Errorf("gpushare: invalid fabric session %q", session)
	}
	parentDir, name := filepath.Dir(host), filepath.Base(host)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("gpushare: fabric dir parent: %w", err)
	}
	parent, err := openConfinedDir(parentDir)
	if err != nil {
		return nil, nil, fmt.Errorf("gpushare: fabric dir parent: %w", err)
	}
	s, closeFn, err := a.openFabricSessionIn(session, namespace, pod, parent, name)
	if err != nil {
		_ = parent.Close()
		return nil, nil, err
	}
	return s, func() { closeFn(); _ = parent.Close() }, nil
}

// openFabricSessionIn creates name (fresh) in parent and registers it for
// the pod. The returned func removes both; parent stays open.
func (a *Agent) openFabricSessionIn(session, namespace, pod string, parent *confinedDir, name string) (*fabricSessionDir, func(), error) {
	if err := parent.root.RemoveAll(name); err != nil {
		return nil, nil, fmt.Errorf("gpushare: clear fabric dir: %w", err)
	}
	if err := parent.root.Mkdir(name, 0o755); err != nil {
		return nil, nil, fmt.Errorf("gpushare: create fabric dir: %w", err)
	}
	s := &fabricSessionDir{parent: parent, name: name}
	key := fabricKey(session, namespace, pod)
	a.fabricSessions.Store(key, s)
	return s, func() {
		a.fabricSessions.Delete(key)
		_ = parent.root.RemoveAll(name)
	}, nil
}

// GPUShareGroupInfo places a checkpoint in its instance.
type GPUShareGroupInfo struct {
	Session string `json:"session"`
	Index   int    `json:"index"`
	Size    int    `json:"size"`
	// Companion marks a member without GPUs: restored stopped and
	// continued once the GPU ranks resume (criu_companion.go).
	Companion bool `json:"companion,omitempty"`
}

func gpushareGroupInfo(req CheckpointRequest) *GPUShareGroupInfo {
	if req.Companion && req.GroupSession != "" {
		return &GPUShareGroupInfo{Session: req.GroupSession, Index: req.GPUShareGroupIndex, Size: req.GPUShareGroupSize, Companion: true}
	}
	if req.GPUShareFabricSession == "" {
		return nil
	}
	return &GPUShareGroupInfo{Session: req.GPUShareFabricSession, Index: req.GPUShareGroupIndex, Size: req.GPUShareGroupSize}
}

// restoreV2Group is a restore's part in a group restore.
type restoreV2Group struct {
	Session, InetAddrMap string
	Namespace, Pod       string
	// SpanMS is the restore span every GPU rank of the group budgets its
	// clocks with (groupSpanBudget); 0 outside a group restore.
	SpanMS int64
	// SpanLearned is whether SpanMS is the cluster's learned span, another
	// checkpoint's, which the rank's estimate bounds (groupSpanBudget).
	SpanLearned bool
}

// restoreSession is one pod's directory in a group restore.
type restoreSession struct {
	dir *fabricSessionDir
	// containerDir is where the gpushare tool sees it ("" without gpushare).
	containerDir string
	close        func()
}

// openRestoreSession creates the pod's session directory in the
// placeholder's work volume (webhook.RestoreWorkMountPath), which the
// placeholder mounts writable: the tool's resume uses it as --fabric-map,
// and the checkpoint itself is read-only there.
func (a *Agent) openRestoreSession(g restoreV2Group, work *confinedDir, gpushare bool) (*restoreSession, error) {
	if !validFabricSession(g.Session) {
		return nil, fmt.Errorf("gpushare: invalid fabric session %q", g.Session)
	}
	name := fabricDirPrefix + g.Session
	cdir := ""
	if gpushare {
		cdir = webhook.RestoreWorkMountPath + "/" + name
	}
	dir, closeFn, err := a.openFabricSessionIn(g.Session, g.Namespace, g.Pod, work, name)
	if err != nil {
		return nil, err
	}
	var once sync.Once
	return &restoreSession{dir: dir, containerDir: cdir, close: func() { once.Do(closeFn) }}, nil
}

// fabricUnlockWait bounds the hold: TCP gives up on a connection held
// longer than about 13 minutes.
const fabricUnlockWait = 10 * time.Minute

// awaitUnlock reports this pod restored and waits for the driver's
// verdict, "unlock" or "abort".
func (s *restoreSession) awaitUnlock(ctx context.Context, log *logrus.Entry) (string, error) {
	if err := s.dir.write("restored", []byte("1\n")); err != nil {
		return "", fmt.Errorf("group restore: report restored: %w", err)
	}
	log.Info("group restore: CRIU restore done, holding the network lock until every pod is restored")
	return waitFabricVerdictFile(ctx, s.dir, "u", fabricUnlockWait)
}

// waitFabricVerdictFile polls name in the session for "unlock" or "abort".
func waitFabricVerdictFile(ctx context.Context, s *fabricSessionDir, name string, limit time.Duration) (string, error) {
	t0 := time.Now()
	for {
		if b, err := s.read(name, fabricSmallLimit); err == nil {
			v := strings.TrimSpace(string(b))
			if v == fabricVerdictUnlock || v == fabricVerdictAbort {
				return v, nil
			}
			return "", fmt.Errorf("group restore: unexpected verdict %q", v)
		}
		if time.Since(t0) > limit {
			return "", fmt.Errorf("group restore: no unlock after %s; the other pods did not finish restoring", limit)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(fabricPollInterval):
		}
	}
}

// netUnlockArgs releases a group restore's network lock: CRIU reads the
// lock's name from the images and removes it in the pod's network
// namespace (through libnftables; no nft binary needed). The agent's own
// CRIU runs it, with only the network namespace entered, so the binary and
// the images are the agent's paths.
func netUnlockArgs(hostPID int, imagesDir string) []string {
	return []string{"-t", strconv.Itoa(hostPID), "-n", "--",
		v2BinDirInContainer + "/criu", "net-unlock", "-D", imagesDir, "-o", "net-unlock.log"}
}

func netUnlock(ctx context.Context, hostPID int, imagesDir string, log *logrus.Entry) error {
	cctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	t0 := time.Now()
	out, err := exec.CommandContext(cctx, "nsenter", netUnlockArgs(hostPID, imagesDir)...).CombinedOutput() //nolint:gosec // fixed binary; args are a pid and a path built here
	if err != nil {
		return fmt.Errorf("criu net-unlock: %w (output: %s; net-unlock.log tail: %s)", err,
			strings.TrimSpace(string(out)), tailOfFile(filepath.Join(imagesDir, "net-unlock.log"), 6))
	}
	log.WithField("duration", time.Since(t0).Round(time.Millisecond).String()).Info("group restore: network unlocked")
	return nil
}

func criuSupportsNetMigration(help string) bool {
	return strings.Contains(help, "--inet-addr-map") && strings.Contains(help, "net-unlock")
}

// fabricVote is one tool's vote for one attempt.
type fabricVote struct {
	Attempt int  `json:"attempt"`
	OK      bool `json:"ok"`
}

// fabricState is what a driver needs to know about one pod's session.
type fabricState struct {
	Vote *fabricVote `json:"vote,omitempty"`
	// Out is the pod's handle list, once its tool has written it.
	Out *string `json:"out,omitempty"`
	// HaveIn reports whether the merged list has been delivered.
	HaveIn bool `json:"haveIn"`
	// Restored reports a group restore member's CRIU restore done (it then
	// holds its network lock until the driver unlocks it).
	Restored bool `json:"restored,omitempty"`
	// HoldsOpen: the member's tool holds its GPU launches at an in-place
	// resume until the open verdict.
	HoldsOpen bool `json:"holdsOpen,omitempty"`
	// Resumed is the token of the member's resume, once its tool has
	// resumed with the launches held (DIR/resumed).
	Resumed string `json:"resumed,omitempty"`
}

// parseFabricVote reads "<attempt> ok|busy".
func parseFabricVote(s string) (*fabricVote, error) {
	f := strings.Fields(s)
	if len(f) != 2 || (f[1] != "ok" && f[1] != "busy") {
		return nil, fmt.Errorf("malformed vote %q", strings.TrimSpace(s))
	}
	n, err := strconv.Atoi(f[0])
	if err != nil || n < 0 {
		return nil, fmt.Errorf("malformed vote %q", strings.TrimSpace(s))
	}
	return &fabricVote{Attempt: n, OK: f[1] == "ok"}, nil
}

// readFabricState reads a session directory. The tool writes q and out by
// rename, so a file that exists is complete.
func readFabricState(s *fabricSessionDir) (fabricState, error) {
	var st fabricState
	if b, err := s.read("q", fabricSmallLimit); err == nil {
		v, perr := parseFabricVote(string(b))
		if perr != nil {
			return st, perr
		}
		st.Vote = v
	} else if !errors.Is(err, os.ErrNotExist) {
		return st, err
	}
	if b, err := s.read("out", fabricInLimit); err == nil {
		s := string(b)
		st.Out = &s
	} else if !errors.Is(err, os.ErrNotExist) {
		return st, err
	}
	if b, err := s.read("resumed", fabricSmallLimit); err == nil {
		// The directory is the workload's to write: a marker that is not
		// a token is no report (the tool writes one by rename), and the
		// rest of the state is still served.
		if tok := strings.TrimSpace(string(b)); fabricOpenTokenRe.MatchString(tok) {
			st.Resumed = tok
		} else {
			if s.warnedMarker.CompareAndSwap(false, true) {
				logrus.WithFields(logrus.Fields{"session": s.name, "marker": truncate(tok, 64)}).
					Warn("gpushare fabric: malformed resumed marker; not counted as resumed")
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return st, err
	}
	st.HaveIn, st.Restored = s.exists("in"), s.exists("restored")
	st.HoldsOpen = s.holdsOpen.Load()
	return st, nil
}

// Member endpoints.

func (a *Agent) fabricDirFor(w http.ResponseWriter, r *http.Request) (*fabricSessionDir, bool) {
	v := mux.Vars(r)
	if !validFabricSession(v["session"]) {
		http.Error(w, "invalid session", http.StatusBadRequest)
		return nil, false
	}
	d, ok := a.fabricSessions.Load(fabricKey(v["session"], v["namespace"], v["pod"]))
	if !ok {
		// Not yet registered (the member's checkpoint has not reached the
		// suspend) or already closed: the driver polls again.
		http.Error(w, "no such fabric session on this node", http.StatusNotFound)
		return nil, false
	}
	return d.(*fabricSessionDir), true
}

func (a *Agent) fabricStateHandler(w http.ResponseWriter, r *http.Request) {
	dir, ok := a.fabricDirFor(w, r)
	if !ok {
		return
	}
	st, err := readFabricState(dir)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(st)
}

func (a *Agent) fabricVerdictHandler(w http.ResponseWriter, r *http.Request) {
	dir, ok := a.fabricDirFor(w, r)
	if !ok {
		return
	}
	attempt, err := strconv.Atoi(r.URL.Query().Get("attempt"))
	verdict := r.URL.Query().Get("verdict")
	if err != nil || attempt < 0 || (verdict != "go" && verdict != "retry" && verdict != fabricVerdictAbort) {
		http.Error(w, "attempt (>= 0) and verdict (go, retry, abort) required", http.StatusBadRequest)
		return
	}
	if err := dir.write("d", []byte(fmt.Sprintf("%d %s\n", attempt, verdict))); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *Agent) fabricUnlockHandler(w http.ResponseWriter, r *http.Request) {
	dir, ok := a.fabricDirFor(w, r)
	if !ok {
		return
	}
	verdict := r.URL.Query().Get("verdict")
	if verdict != "unlock" && verdict != fabricVerdictAbort {
		http.Error(w, "verdict (unlock, abort) required", http.StatusBadRequest)
		return
	}
	if err := dir.write("u", []byte(verdict+"\n")); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// fabricOpenHandler writes a node's open verdict, "<token> open|abort",
// for its tool to read in DIR/o.
func (a *Agent) fabricOpenHandler(w http.ResponseWriter, r *http.Request) {
	dir, ok := a.fabricDirFor(w, r)
	if !ok {
		return
	}
	token, verdict := r.URL.Query().Get("token"), r.URL.Query().Get("verdict")
	if !fabricOpenTokenRe.MatchString(token) || (verdict != fabricVerdictOpen && verdict != fabricVerdictAbort) {
		http.Error(w, "token (<pid>-<round>) and verdict (open, abort) required", http.StatusBadRequest)
		return
	}
	if err := dir.write("o", []byte(token+" "+verdict+"\n")); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// fabricInLimit bounds a merged handle list: one line per shared allocation
// per process, around a hundred bytes each.
const fabricInLimit = 64 << 20

func (a *Agent) fabricInHandler(w http.ResponseWriter, r *http.Request) {
	dir, ok := a.fabricDirFor(w, r)
	if !ok {
		return
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, fabricInLimit+1))
	if err != nil || len(data) > fabricInLimit {
		http.Error(w, "unreadable or oversized handle list", http.StatusBadRequest)
		return
	}
	if err := dir.write("in", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// Driver.

// fabricMember is one pod of a session as the driver reaches it.
type fabricMember interface {
	Name() string
	// State returns ok=false while the member's session is not registered.
	State(ctx context.Context) (st fabricState, ok bool, err error)
	Verdict(ctx context.Context, attempt int, verdict string) error
	PutIn(ctx context.Context, data []byte) error
	Unlock(ctx context.Context, verdict string) error
	// Open answers the member's resume token with "open" or "abort".
	Open(ctx context.Context, token, verdict string) error
}

// fabricOutcome records how the driver ended a session, for the result.
type fabricOutcome struct {
	Attempts  int  `json:"attempts,omitempty"`
	Went      bool `json:"went,omitempty"`
	Unlocked  bool `json:"unlocked,omitempty"`
	Exchanged bool `json:"exchanged"`
	Handles   int  `json:"handles"`
	// Opened: every member that held its GPU launches at an in-place
	// resume got "open".
	Opened bool `json:"opened,omitempty"`
	// OpenAborted says why the members that resumed got "abort" instead,
	// naming the members that did not report resumed.
	OpenAborted string `json:"openAborted,omitempty"`
}

// fabricDriveOpts picks the phases a session runs.
type fabricDriveOpts struct {
	// Vote: a suspend's quiesce vote (group checkpoint).
	Vote bool
	// LostOther reports a member of the group outside the session (a
	// companion) that ended before the vote passed: the capture is lost,
	// so the vote is aborted too. Nil: none.
	LostOther func() bool
	// OnGo runs once, when the vote goes: the GPU ranks stop now.
	OnGo func()
	// VoteDeadline ends a vote that has not gone this long after the
	// first vote: every member is answered "abort" at its next vote, so
	// all stop at the same attempt and roll back. The members' tools keep
	// a later deadline of their own, for a driver that never answers.
	// Zero: none.
	VoteDeadline time.Duration
	// Unlock: hold every member until all are restored, then unlock them
	// together (group restore). The exchange waits for the unlock.
	Unlock bool
	// NoExchange: this drive does not exchange handle lists (another drive
	// of the session does, over the members that have them).
	NoExchange bool
	// Open: an in-place resume. The members whose tools hold their GPU
	// launches at the resume (HoldsOpen) get "open" once every one of
	// them has reported resumed. OpenDeadline, from the first report,
	// bounds the wait (zero: none).
	Open         bool
	OpenDeadline time.Duration
}

// driveFabric runs one session until every member's operation has returned
// (done reports that per member) or ctx ends.
//
// Vote: once every member still running has voted the same attempt, the
// answer is "go" if all are quiesced, else "retry". A member whose
// checkpoint returned before the go cannot release: every later vote is
// answered "abort", and the others roll back as soon as they read it. So
// is every vote once VoteDeadline has passed. A verdict that some members
// already have reaches the rest first, so no member gets "go" while another
// gets "abort" for the same attempt.
//
// Unlock: once every member has reported its CRIU restore done, all get
// "unlock". A member that returns first has failed; the rest get "abort".
//
// Exchange: once every member still running has written its out, the
// lists are concatenated in member order and delivered once to all of
// them. A member whose operation returned without an out (it failed, or
// does not resume) takes no part.
//
// Open: see fabricOpener.
func driveFabric(ctx context.Context, members []fabricMember, done func(i int) bool, opts fabricDriveOpts, log *logrus.Entry) fabricOutcome {
	var out fabricOutcome
	vote := &fabricVoter{decided: -1, abortSent: map[int]int{}}
	aborted := false
	var firstVote time.Time
	// Each phase is sent to every running member and retried, each poll, to
	// the ones it did not reach; it is complete only once all have it. A
	// member waits for its message, and the receivers' writes are atomic
	// and idempotent, so a repeat is harmless and a loss is not.
	var unlock, exchange *fabricDelivery
	unlockDone, exchanged := false, false
	var opener *fabricOpener
	if opts.Open {
		opener = &fabricOpener{deadline: opts.OpenDeadline, holds: map[int]bool{}, tokens: map[int]string{}, sent: map[int]string{}}
	}
	tick := time.NewTicker(fabricPollInterval)
	defer tick.Stop()
	for {
		running := make([]int, 0, len(members))
		for i := range members {
			if !done(i) {
				running = append(running, i)
			}
		}
		if len(running) == 0 {
			return out
		}
		lostOne := len(running) < len(members)

		states := memberStates(ctx, members, running, log)

		if opts.Vote && !out.Went {
			// A go being delivered is never replaced by an abort: a member
			// that has it may already be suspending (fabricVoter.drive).
			if !vote.goPending() && (lostOne || (opts.LostOther != nil && opts.LostOther())) && !aborted {
				aborted = true
				log.Warn("gpushare fabric: a member ended before the quiesce vote passed; aborting the vote")
			}
			if firstVote.IsZero() {
				for _, st := range states {
					if st.Vote != nil {
						firstVote = time.Now()
						break
					}
				}
			}
			if !aborted && !vote.goPending() && opts.VoteDeadline > 0 && !firstVote.IsZero() && time.Since(firstVote) > opts.VoteDeadline {
				aborted = true
				log.WithFields(logrus.Fields{"deadline": opts.VoteDeadline.String(), "attempts": vote.decided + 1}).
					Warn("gpushare fabric: the GPU work did not quiesce on every node in time; aborting the vote, the members roll back")
			}
			vote.drive(ctx, members, running, states, aborted, &out, log)
			if out.Went && opts.OnGo != nil {
				opts.OnGo()
			}
		}

		if opts.Unlock && !unlockDone {
			if unlock == nil {
				verdict := ""
				if lostOne {
					verdict = fabricVerdictAbort
				} else {
					all := true
					for _, i := range running {
						if st, ok := states[i]; !ok || !st.Restored {
							all = false
							break
						}
					}
					if all {
						verdict = "unlock"
					}
				}
				if verdict != "" {
					unlock = &fabricDelivery{what: "unlock verdict", value: verdict, sent: map[int]bool{}}
				}
			}
			if unlock != nil && unlock.deliver(running, func(i int) error { return members[i].Unlock(ctx, unlock.value) }, members, log) {
				unlockDone = true
				out.Unlocked = unlock.value == "unlock"
				log.WithFields(logrus.Fields{"verdict": unlock.value, "members": len(running)}).Info("gpushare fabric: group restore network lock")
			}
		}

		if opener != nil {
			opener.drive(ctx, members, running, states, &out, log)
		}

		if !exchanged && !opts.NoExchange && (!opts.Unlock || out.Unlocked) {
			if exchange == nil {
				var merged bytes.Buffer
				ready := true
				for _, i := range running {
					st, ok := states[i]
					if !ok || st.Out == nil {
						ready = false
						break
					}
					merged.WriteString(*st.Out)
					if n := merged.Len(); n > 0 && merged.Bytes()[n-1] != '\n' {
						merged.WriteByte('\n')
					}
				}
				if ready {
					exchange = &fabricDelivery{what: "handle list", value: merged.String(), sent: map[int]bool{}}
				}
			}
			if exchange != nil && exchange.deliver(running, func(i int) error { return members[i].PutIn(ctx, []byte(exchange.value)) }, members, log) {
				exchanged = true
				out.Exchanged = true
				out.Handles = strings.Count(exchange.value, "\n")
				log.WithFields(logrus.Fields{"members": len(running), "handles": out.Handles}).Info("gpushare fabric: handle lists exchanged")
			}
		}

		select {
		case <-ctx.Done():
			return out
		case <-tick.C:
		}
	}
}

// fabricOpener runs the open phase of an in-place group resume. Each
// member whose tool holds its GPU launches at the resume (HoldsOpen, seen
// from the vote on) writes DIR/resumed with a token once resumed, and
// waits for "<token> open" or "<token> abort" in DIR/o.
//
//   - Once every member that holds and still runs has reported resumed,
//     each gets "open" for its own token. A member that reported resumed
//     and then ended counts as resumed: its share of the multicast memory
//     was bound.
//   - A member that holds and ends without reporting resumed (its resume
//     failed), or one that has not reported by deadline after the first
//     report, ends the wait: every member that resumed gets "abort", now
//     or when it reports. Its tool then lets the launches through anyway
//     and its capture fails as stalled: a launch may hit memory a node
//     has not bound, but gates left closed would wedge the workload for
//     good, and the Xid check (kmsg.go) reports a launch that did.
//
// The verdict is decided once and never replaced: an "open" that some
// members already have is delivered to the rest even if one of them ends
// meanwhile. A member that does not hold (a tool from before --sync-open)
// is not waited for. Tokens scope the verdicts: a tool reads only a verdict
// for the token it wrote, in a session directory made fresh for the
// session. Durations are the driver's own; no clock is compared across
// nodes.
type fabricOpener struct {
	deadline time.Duration
	holds    map[int]bool   // members seen holding their launches
	tokens   map[int]string // each member's latest resumed token
	sent     map[int]string // the token each member has the verdict for
	first    time.Time      // when the first resumed marker was seen
	verdict  string         // "", fabricVerdictOpen or fabricVerdictAbort
	reason   string
	done     bool
}

func (o *fabricOpener) drive(ctx context.Context, members []fabricMember, running []int, states map[int]fabricState, out *fabricOutcome, log *logrus.Entry) {
	if o.done {
		return
	}
	isRunning := make(map[int]bool, len(running))
	for _, i := range running {
		isRunning[i] = true
		st, ok := states[i]
		if !ok {
			continue
		}
		if st.HoldsOpen {
			o.holds[i] = true
		}
		if st.Resumed != "" {
			o.tokens[i] = st.Resumed
			if o.first.IsZero() {
				o.first = time.Now()
			}
		}
	}
	if o.verdict == "" {
		var ended, missing []string
		waiting := 0
		for i := range members {
			switch {
			case !o.holds[i]:
			case !isRunning[i]:
				if o.tokens[i] == "" {
					ended = append(ended, members[i].Name())
				}
			default:
				waiting++
				if o.tokens[i] == "" {
					missing = append(missing, members[i].Name())
				}
			}
		}
		switch {
		case len(ended) > 0:
			o.verdict, o.reason = fabricVerdictAbort, "member(s) ended before reporting resumed: "+strings.Join(ended, ", ")
		case waiting > 0 && len(missing) == 0:
			o.verdict = fabricVerdictOpen
		case o.deadline > 0 && !o.first.IsZero() && time.Since(o.first) > o.deadline:
			o.verdict, o.reason = fabricVerdictAbort, fmt.Sprintf("member(s) did not report resumed within %s of the first: %s",
				o.deadline, strings.Join(missing, ", "))
		}
		switch o.verdict {
		case fabricVerdictOpen:
			log.WithFields(logrus.Fields{"members": waiting, "spread": time.Since(o.first).Round(time.Millisecond).String()}).
				Info("gpushare fabric: every node resumed; opening the GPU launch gates")
		case fabricVerdictAbort:
			log.Warn("gpushare fabric: not every node resumed (" + o.reason + "); the nodes that did let their GPU launches through anyway")
		}
	}
	if o.verdict == "" {
		return
	}
	all := true
	for _, i := range running {
		if !o.holds[i] {
			continue
		}
		tok := o.tokens[i]
		if tok == "" {
			all = false // answered once it reports
			continue
		}
		if o.sent[i] == tok {
			continue
		}
		if err := members[i].Open(ctx, tok, o.verdict); err != nil {
			log.WithError(err).WithField("member", members[i].Name()).Warn("gpushare fabric: deliver open verdict; retrying")
			all = false
			continue
		}
		o.sent[i] = tok
		if o.verdict == fabricVerdictAbort && out.OpenAborted == "" {
			out.OpenAborted = o.reason
		}
	}
	if o.verdict == fabricVerdictOpen && all {
		o.done = true
		out.Opened = true
	}
}

// fabricDelivery is one message to every member, tracked per member.
type fabricDelivery struct {
	what, value string
	sent        map[int]bool
}

// deliver sends to the running members not yet reached and reports whether
// every running member now has the message.
func (d *fabricDelivery) deliver(running []int, send func(i int) error, members []fabricMember, log *logrus.Entry) bool {
	all := true
	for _, i := range running {
		if d.sent[i] {
			continue
		}
		if err := send(i); err != nil {
			log.WithError(err).WithField("member", members[i].Name()).Warn("gpushare fabric: deliver " + d.what + "; retrying")
			all = false
			continue
		}
		d.sent[i] = true
	}
	return all
}

// fabricVoter answers the quiesce vote round by round. A verdict is
// decided only once every running member has received it.
type fabricVoter struct {
	decided   int // highest attempt every member has the verdict for
	pending   *fabricDelivery
	attempt   int
	abortSent map[int]int // member -> highest attempt it has the abort for
}

// goPending reports whether a go verdict is being delivered: some members
// may have it already.
func (v *fabricVoter) goPending() bool { return v.pending != nil && v.pending.value == "go" }

func (v *fabricVoter) drive(ctx context.Context, members []fabricMember, running []int, states map[int]fabricState, aborted bool, out *fabricOutcome, log *logrus.Entry) {
	// A verdict some members already have is finished before an abort:
	// a member that got "go" releases, so the others must get "go" too,
	// not "abort" for the same attempt.
	if v.pending != nil && !v.finish(ctx, members, running, out, log) {
		return
	}
	if aborted {
		// An abort answers each member's own attempt as it votes.
		for _, i := range running {
			st, ok := states[i]
			if !ok || st.Vote == nil || st.Vote.Attempt <= v.decided {
				continue
			}
			if got, ok := v.abortSent[i]; ok && got >= st.Vote.Attempt {
				continue
			}
			if err := members[i].Verdict(ctx, st.Vote.Attempt, fabricVerdictAbort); err != nil {
				log.WithError(err).WithField("member", members[i].Name()).Warn("gpushare fabric: abort verdict; retrying")
				continue
			}
			v.abortSent[i] = st.Vote.Attempt
		}
		return
	}
	if v.pending == nil {
		attempt, allOK := -1, true
		for _, i := range running {
			st, ok := states[i]
			if !ok || st.Vote == nil {
				return
			}
			if attempt == -1 {
				attempt = st.Vote.Attempt
			}
			if st.Vote.Attempt != attempt {
				return
			}
			allOK = allOK && st.Vote.OK
		}
		if attempt <= v.decided {
			return
		}
		verdict := "retry"
		if allOK {
			verdict = "go"
		}
		v.pending, v.attempt = &fabricDelivery{what: "verdict", value: verdict, sent: map[int]bool{}}, attempt
	}
	v.finish(ctx, members, running, out, log)
}

// finish delivers the pending verdict and reports whether every running
// member now has it.
func (v *fabricVoter) finish(ctx context.Context, members []fabricMember, running []int, out *fabricOutcome, log *logrus.Entry) bool {
	p := v.pending
	if !p.deliver(running, func(i int) error { return members[i].Verdict(ctx, v.attempt, p.value) }, members, log) {
		return false
	}
	v.decided, v.pending = v.attempt, nil
	out.Attempts = v.attempt + 1
	out.Went = p.value == "go"
	log.WithFields(logrus.Fields{"attempt": v.attempt, "verdict": p.value}).Info("gpushare fabric: quiesce vote")
	return true
}

// memberStates asks the running members for their state in parallel, each
// request bounded by fabricMemberTimeout, so a member that does not answer
// delays a poll by one timeout, not one per member. A member that fails or
// has no session is left out.
func memberStates(ctx context.Context, members []fabricMember, running []int, log *logrus.Entry) map[int]fabricState {
	type res struct {
		i  int
		st fabricState
		ok bool
	}
	ch := make(chan res, len(running))
	for _, i := range running {
		go func(i int) {
			rctx, cancel := context.WithTimeout(ctx, fabricMemberTimeout)
			defer cancel()
			st, ok, err := members[i].State(rctx)
			if err != nil {
				log.WithError(err).WithField("member", members[i].Name()).Debug("gpushare fabric: state")
				ok = false
			}
			ch <- res{i, st, ok}
		}(i)
	}
	states := make(map[int]fabricState, len(running))
	for range running {
		if r := <-ch; r.ok {
			states[r.i] = r.st
		}
	}
	return states
}

// httpFabricMember reaches a member's session on its node's agent.
type httpFabricMember struct {
	client           *http.Client
	base             string // http://<node>:<port>
	session, ns, pod string
}

func (m *httpFabricMember) Name() string { return m.ns + "/" + m.pod }

func (m *httpFabricMember) url(suffix string) string {
	return fmt.Sprintf("%s/v1/gpushare/fabric/%s/%s/%s%s", m.base, url.PathEscape(m.session),
		url.PathEscape(m.ns), url.PathEscape(m.pod), suffix)
}

func (m *httpFabricMember) State(ctx context.Context) (fabricState, bool, error) {
	var st fabricState
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.url(""), http.NoBody)
	if err != nil {
		return st, false, err
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return st, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return st, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return st, false, fmt.Errorf("state %s: %s: %s", m.Name(), resp.Status, strings.TrimSpace(string(b)))
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return st, false, err
	}
	return st, true, nil
}

func (m *httpFabricMember) send(ctx context.Context, u string, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, fabricMemberTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s: %s: %s", m.Name(), resp.Status, strings.TrimSpace(string(b)))
	}
	return nil
}

func (m *httpFabricMember) Verdict(ctx context.Context, attempt int, verdict string) error {
	return m.send(ctx, m.url(fmt.Sprintf("/verdict?attempt=%d&verdict=%s", attempt, url.QueryEscape(verdict))), nil)
}

func (m *httpFabricMember) PutIn(ctx context.Context, data []byte) error {
	return m.send(ctx, m.url("/in"), data)
}

func (m *httpFabricMember) Unlock(ctx context.Context, verdict string) error {
	return m.send(ctx, m.url("/unlock?verdict="+url.QueryEscape(verdict)), nil)
}

func (m *httpFabricMember) Open(ctx context.Context, token, verdict string) error {
	return m.send(ctx, m.url("/open?token="+url.QueryEscape(token)+"&verdict="+url.QueryEscape(verdict)), nil)
}

// fabricHTTPClient carries the agent token to peers. No overall timeout: a
// member's checkpoint call runs as long as the suspend, dump and resume do,
// bounded by the caller's context.
var fabricHTTPClient = &http.Client{
	Transport: &authTransport{base: http.DefaultTransport},
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// GroupCheckpointRequest checkpoints the pods of one workload whose GPU
// memory is shared across nodes, as one fabric session.
type GroupCheckpointRequest struct {
	Members []CheckpointRequest `json:"members"`
	// LeaveRunning applies to every member: true suspends and resumes the
	// instance in place around the dump; false dumps it for a move, and
	// the network lock then holds the source pods' connections until the
	// pods are deleted.
	LeaveRunning bool `json:"leaveRunning"`
}

// GroupMemberResult is one member's outcome.
type GroupMemberResult struct {
	Namespace string            `json:"namespace"`
	PodName   string            `json:"podName"`
	Result    *CheckpointResult `json:"result,omitempty"`
	Error     string            `json:"error,omitempty"`
}

// GroupCheckpointResult reports a group checkpoint.
type GroupCheckpointResult struct {
	Session string              `json:"session"`
	Members []GroupMemberResult `json:"members"`
	Fabric  fabricOutcome       `json:"fabric"`
}

// groupCheckpoint drives a fabric session: it asks each member's agent to
// checkpoint its pod in the session and answers the votes and the
// exchange until all of them return.
func (a *Agent) groupCheckpoint(ctx context.Context, req GroupCheckpointRequest, log *logrus.Entry) (*GroupCheckpointResult, error) {
	if len(req.Members) < 2 {
		return nil, errors.New("a group checkpoint needs at least two members")
	}
	if a.kubeClient == nil {
		return nil, errors.New("a group checkpoint needs the kube client to find each member's node")
	}
	session := newFabricSession()
	log = log.WithField("fabricSession", session)

	members := make([]fabricMember, len(req.Members))
	bases := make([]string, len(req.Members))
	uids := make([]types.UID, len(req.Members))
	baselines := make([]memberBaseline, len(req.Members))
	for i, m := range req.Members {
		if m.Namespace == "" || m.PodName == "" {
			return nil, fmt.Errorf("member %d: namespace and podName required", i)
		}
		pod, err := a.kubeClient.CoreV1().Pods(m.Namespace).Get(ctx, m.PodName, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("member %s/%s: %w", m.Namespace, m.PodName, err)
		}
		uids[i] = pod.UID
		baselines[i] = baselineOf(pod)
		if pod.Spec.NodeName == "" {
			return nil, fmt.Errorf("member %s/%s is not scheduled", m.Namespace, m.PodName)
		}
		base, err := a.peerAgentURL(ctx, pod.Spec.NodeName)
		if err != nil {
			return nil, fmt.Errorf("member %s/%s: %w", m.Namespace, m.PodName, err)
		}
		bases[i] = base
		members[i] = &httpFabricMember{client: fabricHTTPClient, base: base, session: session, ns: m.Namespace, pod: m.PodName}
	}
	// Companions have no GPU state: no fabric session, no vote. Only the
	// GPU members take part in the fabric.
	var fabric []fabricMember
	var fabricIdx []int
	for i, m := range req.Members {
		if !m.Companion {
			fabric = append(fabric, members[i])
			fabricIdx = append(fabricIdx, i)
		}
	}

	res := &GroupCheckpointResult{Session: session, Members: make([]GroupMemberResult, len(req.Members))}
	var mu sync.Mutex
	finished := make([]bool, len(req.Members))
	var wg sync.WaitGroup
	for i, m := range req.Members {
		m.LeaveRunning = req.LeaveRunning
		if m.Companion {
			m.GroupSession = session
		} else {
			m.GPUShareFabricSession = session
			// In place, this driver answers the members' held resumes
			// (driveFabric's open phase, Open below).
			m.GPUShareSyncOpen = req.LeaveRunning
		}
		m.GPUShareGroupIndex, m.GPUShareGroupSize = i, len(req.Members)
		m.GroupFreeze = true
		wg.Add(1)
		go func(i int, m CheckpointRequest) {
			defer wg.Done()
			r, err := postCheckpoint(ctx, fabricHTTPClient, bases[i], m)
			mu.Lock()
			defer mu.Unlock()
			res.Members[i] = GroupMemberResult{Namespace: m.Namespace, PodName: m.PodName, Result: r}
			if err != nil {
				res.Members[i].Error = err.Error()
			}
			finished[i] = true
		}(i, m)
	}
	done := func(i int) bool {
		mu.Lock()
		defer mu.Unlock()
		return finished[fabricIdx[i]]
	}
	// Every member, GPU rank or companion, waits at the group's freeze
	// point once ready; released together, they stop together.
	freezeMembers := make([]freezeMember, len(req.Members))
	for i, m := range req.Members {
		i := i
		freezeMembers[i] = freezeMember{Base: bases[i], Namespace: m.Namespace, Pod: m.PodName, Done: func() bool {
			mu.Lock()
			defer mu.Unlock()
			return finished[i]
		}}
	}
	// The instance stops with its GPU ranks (instanceHold): their pods are
	// held when the vote goes, the companions' once every member waits at
	// the freeze point. The members are released from it only once all are
	// held.
	hold := newInstanceHold(ctx, fabricHTTPClient, session, req.Members, bases, log)
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			hold.wait()
			hold.cancel()
			releaseMembers(ctx, fabricHTTPClient, session, req.Members, bases, log)
		})
	}
	defer release()
	freezeDone := make(chan struct{})
	go func() {
		defer close(freezeDone)
		driveInstanceFreeze(ctx, fabricHTTPClient, session, freezeMembers, hold, log)
	}()
	// A companion that ends before the vote passed loses the capture: the
	// GPU ranks must not suspend for it.
	lostCompanion := func() bool {
		mu.Lock()
		defer mu.Unlock()
		for i := range req.Members {
			if req.Members[i].Companion && finished[i] {
				return true
			}
		}
		return false
	}
	// In place, the members open their GPU launch gates only once every
	// one has resumed (fabricOpener).
	res.Fabric = driveFabric(ctx, fabric, done, fabricDriveOpts{Vote: true, LostOther: lostCompanion, OnGo: hold.steps().onGo,
		VoteDeadline: gpushareQuiesceTimeout, Open: req.LeaveRunning, OpenDeadline: fabricOpenDeadline}, log)
	wg.Wait()
	<-freezeDone

	var failed []string
	replace := false
	for _, m := range res.Members {
		if m.Error != "" {
			failed = append(failed, memberFailure(m.Namespace+"/"+m.PodName, m.Error))
			replace = replace || sourceMustBeReplacedText(m.Error)
		}
	}
	if replace {
		// A member left suspended or locked is one engine with the others:
		// its controllers replace every pod of it, cold. (A member does not
		// replace itself in a group: checkpoint.go.)
		log.Error("group checkpoint: a member was left unusable; deleting every member's pod")
		a.deleteMembers(context.WithoutCancel(ctx), req.Members, uids, log)
	}
	// The members that resumed let their launches through before every
	// node had: each fails as stalled on its own node too; this names the
	// members that did not report resumed.
	var openErr error
	if res.Fabric.OpenAborted != "" {
		openErr = fmt.Errorf("group checkpoint %s: %w: the GPU launch gates were opened before every node resumed: %s",
			session, errResumeStalled, res.Fabric.OpenAborted)
	}
	if len(failed) > 0 {
		return res, errors.Join(membersFailed("group checkpoint", session, len(res.Members), failed), openErr)
	}
	if openErr != nil {
		return res, openErr
	}
	if req.LeaveRunning {
		// Every member resumed: the instance is seen serving again before
		// the capture succeeds (resume_guard.go). Released first: a held
		// pod's readiness probe fails.
		release()
		if err := watchMembersAfterResume(ctx, a.kubeClient, baselines, postResumeWatch); err != nil {
			log.WithError(err).Error("group checkpoint: the instance did not run again after its resume")
			return res, fmt.Errorf("group checkpoint %s: %w", session, err)
		}
	}
	return res, nil
}

// deleteMembers deletes the members' pods, each bound to the uid the
// driver found: a pod since replaced under the same name is left alone.
func (a *Agent) deleteMembers(ctx context.Context, members []CheckpointRequest, uids []types.UID, log *logrus.Entry) {
	for i := range members {
		m := &members[i]
		if err := a.deletePodRetrying(ctx, m.Namespace, m.PodName, uids[i]); err != nil {
			log.WithError(err).WithField("pod", m.Namespace+"/"+m.PodName).Error("group checkpoint: cannot delete a member's pod")
		}
	}
}

// postCheckpoint runs one member's checkpoint on its node's agent.
func postCheckpoint(ctx context.Context, client *http.Client, base string, req CheckpointRequest) (*CheckpointResult, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/checkpoint", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	hreq.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(hreq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var r CheckpointResult
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("decode checkpoint result: %w", err)
	}
	return &r, nil
}

func (a *Agent) groupCheckpointHandler(w http.ResponseWriter, r *http.Request) {
	var req GroupCheckpointRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}
	res, err := a.groupCheckpoint(r.Context(), req, logrus.WithField("op", "group-checkpoint"))
	w.Header().Set("Content-Type", "application/json")
	if err != nil && res == nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
	}
	_ = json.NewEncoder(w).Encode(res)
}
