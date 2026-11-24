// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/modelcache"
)

// modelCacheCmd wraps a pod's model download init
// (docs/design/node-model-cache.md). The node's cache of this download is
// mounted read-only at --cache. When it holds a complete generation, the
// helper copies it into the download volume at --to, checking every file
// against the manifest, and the download does not run. Otherwise, or when
// anything about the copy is wrong, it runs the download exactly as the
// pod would have. The volume is the pod's own, unchanged, so the download
// can always run; the cache never fails the init.
//
// It waits only while the agent is visibly fetching the key from a peer
// (a fresh heartbeat), and never past --wait seconds in all.
func modelCacheCmd(args []string) (int, error) {
	fs := flag.NewFlagSet("model-cache", flag.ContinueOnError)
	cache := fs.String("cache", "", "the node's cache directory for this download, read-only")
	to := fs.String("to", "", "the download volume to fill")
	wait := fs.Int("wait", 600, "most seconds to wait for a peer fetch in progress")
	stale := fs.Int("stale", 20, "seconds after which the fetch heartbeat counts as dead")
	poll := fs.Duration("poll", 2*time.Second, "how often to look again while waiting")
	maxEntries := fs.Int("max-entries", 500000, "most entries a tree may have to be reported for caching")
	planWait := fs.Int("plan-wait", 0, "single flight: most seconds to wait for the agent's plan for this pod (0: no plans)")
	maxWait := fs.Int("max-wait", 6*3600, "single flight: most seconds a pod told to wait waits in all")
	signal := fs.String("signal", "", "single flight: the signal volume where the helper tells the agent its download is over")
	stopWait := fs.Int("stop-wait", 600, "single flight: most seconds to wait for the agent to stop streaming into the volume before downloading")
	finalWait := fs.Int("final-wait", 120, "single flight: most seconds the downloading pod's init stays open for the agent's final manifest")
	var watch tryFlags
	fs.Var(&watch, "watch", "another writable volume a later container reads: if the download changes it, the download is not cached (repeatable)")
	if err := fs.Parse(args); err != nil {
		return 2, err
	}
	argv := fs.Args()
	if *cache == "" || *to == "" || len(argv) == 0 {
		return 2, errors.New("model-cache: --cache, --to and the download command are required")
	}
	w := waitRules{wait: time.Duration(*wait) * time.Second, stale: time.Duration(*stale) * time.Second, poll: *poll}
	if *planWait > 0 {
		w.uid, w.planWait, w.maxWait = os.Getenv(modelcache.PodUIDEnv), time.Duration(*planWait)*time.Second, time.Duration(*maxWait)*time.Second
	}
	switch seedFromCache(*cache, *to, w) {
	case filled:
		report(modelcache.InitSeeded)
		return 0, nil
	case streamStopped:
		// The agent wrote part of a tree. Say the pod downloads, wait for
		// the agent to stop writing, then start afresh in an empty volume.
		// Until the stream has stopped, the volume is left as it is: a
		// download over a copy still being written would mix the two.
		if !stopStream(*signal, *cache, w.uid, w.stale, time.Duration(*stopWait)*time.Second, w.poll) {
			return 1, errors.New("model-cache: the agent still writes into the volume after --stop-wait; the volume is left as it is, and the init's restart looks again")
		}
		emptyDir(*to)
	}
	fmt.Println("nvsnap: model cache: downloading")
	if *signal != "" {
		// Before the download writes anything: the agent never streams
		// into this volume, nor empties it to take the download over,
		// once it reads this.
		writeSignal(*signal, modelcache.SignalDownloading)
		// The agent reads the signal only between its writes, and its
		// stream may have begun after this helper decided to download
		// (before its plan arrived). Read after the signal is written,
		// a fresh stream heartbeat means the agent may still write or
		// empty the volume: wait for it to stop, then start afresh in an
		// empty volume. A download plan is written only once the agent
		// has emptied the volume and stopped writing into it.
		if w.uid != "" && modelcache.ReadPlan(*cache, w.uid) != modelcache.PlanDownload &&
			heartbeatFresh(filepath.Join(*cache, modelcache.StreamingFileOf(w.uid)), w.stale) {
			fmt.Println("nvsnap: model cache: the agent still streams into the volume; waiting for it to stop")
			if !stopStream(*signal, *cache, w.uid, w.stale, time.Duration(*stopWait)*time.Second, w.poll) {
				return 1, errors.New("model-cache: the agent still writes into the volume after --stop-wait; the volume is left as it is, and the init's restart looks again")
			}
			emptyDir(*to)
		}
	}
	before := treesOf(watch, *maxEntries)
	code, err := run(argv)
	if code == 0 {
		// What the download left, for the agent: it keeps a copy only if
		// the volume still has exactly this tree when it reads it. A
		// download that also wrote another volume a later container reads
		// is not cached: a seed would not produce that output.
		msg := modelcache.InitDownloaded
		if after := treesOf(watch, *maxEntries); after != before {
			fmt.Println("nvsnap: model cache: the download changed another volume; it is not cached")
		} else if ents, serr := modelcache.Scan(*to, *maxEntries); serr == nil {
			tree := modelcache.Fingerprint(ents)
			msg += " " + modelcache.TreeField + tree
			if w.uid != "" && modelcache.ReadPlan(*cache, w.uid) == modelcache.PlanDownload {
				if *signal != "" {
					// The agent reads the volume once this is there.
					writeSignal(*signal, tree)
				}
				if !holdForFinal(*cache, w.uid, tree, time.Duration(*finalWait)*time.Second, w.poll) {
					msg += " " + modelcache.FinalMismatch
				}
			}
		}
		report(msg)
	}
	return code, err
}

// terminationLog is where Kubernetes reads a container's termination
// message; a test points it elsewhere.
var terminationLog = "/dev/termination-log"

func report(msg string) { _ = os.WriteFile(terminationLog, []byte(msg), 0o644) }

// holdForFinal keeps the downloading pod's init open until the agent has
// read the volume for its final manifest (nothing else writes it while
// the init runs), or for at most wait. It reports false when the agent's
// manifest is of another tree than the download left.
func holdForFinal(cache, uid, tree string, wait, poll time.Duration) bool {
	p := filepath.Join(cache, modelcache.FinalFile(uid))
	deadline := time.Now().Add(wait)
	for {
		if b, err := os.ReadFile(p); err == nil {
			if strings.TrimSpace(string(b)) != tree {
				fmt.Println("nvsnap: model cache: the final manifest is not of the tree the download left")
				return false
			}
			fmt.Println("nvsnap: model cache: other nodes have the final manifest")
			return true
		}
		if !time.Now().Before(deadline) {
			if wait > 0 {
				fmt.Printf("nvsnap: model cache: no final manifest after %s\n", wait)
			}
			return true
		}
		time.Sleep(poll)
	}
}

// waitRules bound how long the helper waits for a copy instead of
// downloading.
type waitRules struct {
	// wait bounds a wait on the agent's fetch heartbeat without a plan;
	// stale is the heartbeat's age past which the fetch counts as dead.
	wait, stale, poll time.Duration
	// Single flight: uid is the pod's uid ("" without single flight);
	// planWait bounds the wait for a plan; maxWait bounds a wait plan.
	uid               string
	planWait, maxWait time.Duration
}

// outcome is how seedFromCache ended.
type outcome int

const (
	mustDownload  outcome = iota // the volume is as it was; download
	filled                       // the volume holds the model
	streamStopped                // the agent wrote part of a tree; empty, then download
)

// seedFromCache fills to from the cache, or waits for the agent to fill
// it, and says how it ended.
//
// With single flight the agent plans each pod. "download" runs the
// download at once. "wait" waits while the agent streams into the volume
// (StreamingFile fresh), however long the node it copies from takes, up to
// maxWait, and ends when the agent marks the volume done. With no plan
// within planWait, or no pod uid, the rules without single flight apply:
// seed a complete generation, waiting only while a fetch's heartbeat is
// fresh, and never past wait.
func seedFromCache(cache, to string, w waitRules) outcome {
	start := time.Now()
	waiting := false
	if ents, err := os.ReadDir(to); err != nil || len(ents) > 0 {
		if err == nil && w.uid != "" && modelcache.ReadPlan(cache, w.uid) == modelcache.PlanWait {
			// Restarted while the agent streams into the volume: what
			// is there is the stream's, and the stream goes on.
			waiting = true
			fmt.Println("nvsnap: model cache: restarted while another node's copy streams in; still waiting for it")
		} else {
			// A restarted init finds what its last run downloaded; the
			// download knows how to go on from it.
			fmt.Println("nvsnap: model cache: the volume already has content; the download goes on from it")
			return mustDownload
		}
	}
	for {
		plan := ""
		if w.uid != "" {
			plan = modelcache.ReadPlan(cache, w.uid)
		}
		switch {
		case plan == modelcache.PlanDownload:
			fmt.Println("nvsnap: model cache: this pod downloads; other nodes copy from it")
			if waiting {
				return streamStopped
			}
			return mustDownload
		case plan == modelcache.PlanWait:
			if !waiting {
				fmt.Println("nvsnap: model cache: another node downloads this model; the agent copies it here")
				waiting = true
			}
			if _, err := os.Stat(filepath.Join(cache, modelcache.DoneFile(w.uid))); err == nil {
				fmt.Printf("nvsnap: model cache: copied from another node in %s\n", time.Since(start).Round(time.Second))
				return filled
			}
			if !heartbeatFresh(filepath.Join(cache, modelcache.StreamingFileOf(w.uid)), w.stale) {
				fmt.Println("nvsnap: model cache: the copy stopped; downloading")
				return streamStopped
			}
			if time.Since(start) > w.maxWait {
				fmt.Printf("nvsnap: model cache: no copy after %s; downloading\n", w.maxWait)
				return streamStopped
			}
		case waiting:
			// The wait plan is gone (the agent restarted, or ended this
			// pod's flight) while the agent may have written into the
			// volume: stop its stream and empty the volume first.
			fmt.Println("nvsnap: model cache: no plan any more; downloading")
			return streamStopped
		case w.uid != "" && time.Since(start) < w.planWait && !hasCopy(cache):
			// The agent has not planned this pod yet. A node with a copy
			// gets no plan: the default rule seeds from it at once.
		default:
			if gen, m, ok := modelcache.LatestIn(cache); ok {
				began := time.Now()
				err := modelcache.Seed(gen, m, to)
				if err == nil {
					fmt.Printf("nvsnap: model cache: seeded %d bytes from %s in %s\n", m.Bytes, filepath.Base(gen), time.Since(began).Round(time.Millisecond))
					return filled
				}
				fmt.Println("nvsnap: model cache: the cached copy is unusable:", err)
				emptyDir(to)
				return mustDownload
			}
			if !heartbeatFresh(filepath.Join(cache, modelcache.IncomingFile), w.stale) {
				fmt.Println("nvsnap: model cache: no copy on this node")
				return mustDownload
			}
			if time.Since(start) > w.wait {
				fmt.Printf("nvsnap: model cache: no copy after %s\n", w.wait)
				return mustDownload
			}
		}
		time.Sleep(w.poll)
	}
}

// treesOf fingerprints the watched volumes, in order; a volume that
// cannot be read counts as changed.
func treesOf(paths []string, max int) string {
	var b strings.Builder
	for _, p := range paths {
		ents, err := modelcache.Scan(p, max)
		if err != nil {
			fmt.Fprintf(&b, "%s:unreadable %d;", p, time.Now().UnixNano())
			continue
		}
		fmt.Fprintf(&b, "%s:%s;", p, modelcache.Fingerprint(ents))
	}
	return b.String()
}

// writeSignal replaces the signal file whole (a temporary file renamed
// over it), so the agent reads the old value or the new, never part. It
// reports whether the signal was written.
func writeSignal(dir, content string) bool {
	p := filepath.Join(dir, modelcache.SignalFile)
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, []byte(content+"\n"), 0o644); err == nil {
		err = os.Rename(tmp, p)
		if err == nil {
			return true
		}
	}
	fmt.Println("nvsnap: model cache: cannot signal the agent")
	return false
}

// stopStream tells the agent the pod downloads (when there is a signal
// volume) and waits, at most max, until the agent's stream into the volume
// has stopped: its heartbeat is gone or stale. The agent stops its
// heartbeat only once its last write has returned, so a stale heartbeat
// means nothing writes the volume any more. A signal that could not be
// written is written again on every poll. It reports whether the stream
// stopped; false leaves the volume to the agent.
func stopStream(signal, cache, uid string, stale, max, poll time.Duration) bool {
	signalled := signal == "" || writeSignal(signal, modelcache.SignalDownloading)
	if uid == "" {
		return true
	}
	hb := filepath.Join(cache, modelcache.StreamingFileOf(uid))
	deadline := time.Now().Add(max)
	for heartbeatFresh(hb, stale) {
		if !time.Now().Before(deadline) {
			fmt.Printf("nvsnap: model cache: the agent's stream into the volume did not stop within %s\n", max)
			return false
		}
		time.Sleep(poll)
		if !signalled {
			signalled = writeSignal(signal, modelcache.SignalDownloading)
		}
	}
	return true
}

func hasCopy(cache string) bool {
	_, _, ok := modelcache.LatestIn(cache)
	return ok
}

func heartbeatFresh(p string, stale time.Duration) bool {
	fi, err := os.Stat(p)
	return err == nil && time.Since(fi.ModTime()) < stale
}

// emptyDir removes everything under dir, leaving dir: it is the volume's
// mount point.
func emptyDir(dir string) {
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		p := filepath.Join(dir, e.Name())
		// A directory a failed copy made read-only would keep its children.
		_ = filepath.Walk(p, func(q string, fi os.FileInfo, err error) error {
			if err == nil && fi.IsDir() {
				_ = os.Chmod(q, 0o700)
			}
			return nil
		})
		_ = os.RemoveAll(p)
	}
}
