// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/modelcache"
)

const cacheKey = "0123456789abcdef0123456789abcdef"

// cachedModel publishes a one-file generation in a cache root and returns
// the key directory, as the download init sees it mounted.
func cachedModel(t *testing.T) string {
	t.Helper()
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "weights.bin"), []byte("cached"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := &modelcache.Store{Root: t.TempDir()}
	if _, err := s.Harvest(cacheKey, src, modelcache.Limits{}); err != nil {
		t.Fatal(err)
	}
	return s.KeyDir(cacheKey)
}

// download is a fallback the test can see ran: it writes a marker file.
func download(dir string) []string {
	return []string{"/bin/sh", "-c", "echo downloaded > " + filepath.Join(dir, "weights.bin")}
}

func read(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestMain(m *testing.M) {
	terminationLog = filepath.Join(os.TempDir(), "nvsnap-helper-test-termination-log")
	os.Exit(m.Run())
}

func TestModelCacheSeedsFromACompleteGeneration(t *testing.T) {
	cache, to := cachedModel(t), t.TempDir()
	code, err := modelCacheCmd(append([]string{"--cache", cache, "--to", to, "--"}, download(to)...))
	if code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if got := read(t, filepath.Join(to, "weights.bin")); got != "cached" {
		t.Fatalf("weights.bin = %q, want the cached copy (the download ran)", got)
	}
	if got := read(t, terminationLog); got != modelcache.InitSeeded {
		t.Fatalf("termination message = %q", got)
	}
}

func TestModelCacheFallsBackOnACorruptGeneration(t *testing.T) {
	cache, to := cachedModel(t), t.TempDir()
	gen, _, _ := modelcache.LatestIn(cache)
	if err := os.WriteFile(filepath.Join(gen, modelcache.TreeDir, "weights.bin"), []byte("CACHED"), 0o644); err != nil {
		t.Fatal(err)
	}
	code, err := modelCacheCmd(append([]string{"--cache", cache, "--to", to, "--"}, download(to)...))
	if code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if got := read(t, filepath.Join(to, "weights.bin")); got != "downloaded\n" {
		t.Fatalf("weights.bin = %q, want the download's", got)
	}
}

func TestModelCacheDownloadsWithNoCache(t *testing.T) {
	to := t.TempDir()
	for _, cache := range []string{t.TempDir(), filepath.Join(t.TempDir(), "absent")} {
		_ = os.RemoveAll(filepath.Join(to, "weights.bin"))
		code, err := modelCacheCmd(append([]string{"--cache", cache, "--to", to, "--"}, download(to)...))
		if code != 0 || err != nil {
			t.Fatalf("code %d err %v", code, err)
		}
		if got := read(t, filepath.Join(to, "weights.bin")); got != "downloaded\n" {
			t.Fatalf("weights.bin = %q", got)
		}
	}
}

func TestModelCacheLeavesANonEmptyVolumeToTheDownload(t *testing.T) {
	cache, to := cachedModel(t), t.TempDir()
	if err := os.WriteFile(filepath.Join(to, "partial"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if code, err := modelCacheCmd(append([]string{"--cache", cache, "--to", to, "--"}, download(to)...)); code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if got := read(t, filepath.Join(to, "weights.bin")); got != "downloaded\n" {
		t.Fatalf("weights.bin = %q, want the download's", got)
	}
	if _, err := os.Stat(filepath.Join(to, "partial")); err != nil {
		t.Fatal("the helper removed what an earlier run of the download left")
	}
}

func TestModelCachePassesTheDownloadsExitCode(t *testing.T) {
	code, _ := modelCacheCmd([]string{"--cache", t.TempDir(), "--to", t.TempDir(), "--", "/bin/sh", "-c", "exit 7"})
	if code != 7 {
		t.Fatalf("code = %d, want the download's 7", code)
	}
}

func TestModelCacheDoesNotWaitWithoutAFreshHeartbeat(t *testing.T) {
	cache := t.TempDir()
	hb := filepath.Join(cache, modelcache.IncomingFile)
	if err := os.WriteFile(hb, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Minute)
	if err := os.Chtimes(hb, old, old); err != nil {
		t.Fatal(err)
	}
	to := t.TempDir()
	start := time.Now()
	if code, err := modelCacheCmd(append([]string{"--cache", cache, "--to", to, "--wait", "60", "--stale", "20", "--"}, download(to)...)); code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("waited %v on a stale heartbeat", d)
	}
}

func TestModelCacheWaitsForALiveFetchThenSeeds(t *testing.T) {
	cache := cachedModel(t)
	// Hide the generation behind a live heartbeat, then publish it.
	gen, _, _ := modelcache.LatestIn(cache)
	// Under a name LatestIn does not look at, so only the wait finds it.
	hidden := filepath.Join(cache, "hidden")
	if err := os.Rename(gen, hidden); err != nil {
		t.Fatal(err)
	}
	hb := filepath.Join(cache, modelcache.IncomingFile)
	if err := os.WriteFile(hb, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(1500 * time.Millisecond)
		_ = os.Rename(hidden, gen)
	}()
	to := t.TempDir()
	code, err := modelCacheCmd(append([]string{"--cache", cache, "--to", to, "--wait", "30", "--stale", "20", "--poll", "200ms", "--"}, download(to)...))
	if code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if got := read(t, filepath.Join(to, "weights.bin")); got != "cached" {
		t.Fatalf("weights.bin = %q, want the fetched copy", got)
	}
	if _, err := os.Stat(hidden); !os.IsNotExist(err) {
		t.Fatal("the seed did not wait for the generation to be published")
	}
}

func TestModelCacheReportsTheDownloadedTree(t *testing.T) {
	to := t.TempDir()
	if code, err := modelCacheCmd(append([]string{"--cache", t.TempDir(), "--to", to, "--"}, download(to)...)); code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	ents, err := modelcache.Scan(to, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := modelcache.DownloadedTree(read(t, terminationLog)); got != modelcache.Fingerprint(ents) {
		t.Fatalf("reported tree %q, want the volume's %q", got, modelcache.Fingerprint(ents))
	}
}

func TestModelCacheWaitIsCapped(t *testing.T) {
	cache := t.TempDir()
	hb := filepath.Join(cache, modelcache.IncomingFile)
	if err := os.WriteFile(hb, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	defer close(stop)
	go func() { // a heartbeat that never stops
		for {
			select {
			case <-stop:
				return
			case <-time.After(100 * time.Millisecond):
				now := time.Now()
				_ = os.Chtimes(hb, now, now)
			}
		}
	}()
	to := t.TempDir()
	start := time.Now()
	if code, err := modelCacheCmd(append([]string{"--cache", cache, "--to", to, "--wait", "2", "--poll", "100ms", "--"}, download(to)...)); code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if d := time.Since(start); d > 6*time.Second {
		t.Fatalf("waited %v past a 2 s cap", d)
	}
	if got := read(t, filepath.Join(to, "weights.bin")); got != "downloaded\n" {
		t.Fatalf("weights.bin = %q", got)
	}
}

func TestModelCacheNeedsACommand(t *testing.T) {
	if code, err := modelCacheCmd([]string{"--cache", t.TempDir(), "--to", t.TempDir()}); code == 0 || err == nil {
		t.Fatalf("no download command: code %d err %v, want an error", code, err)
	}
}

// A download that also writes another volume a later container reads is
// not reported for caching: a seed would skip that output.
func TestModelCacheDoesNotReportADownloadThatWroteAnotherVolume(t *testing.T) {
	to, other := t.TempDir(), t.TempDir()
	dl := []string{"/bin/sh", "-c", "echo w > " + filepath.Join(to, "w") + "; echo lic > " + filepath.Join(other, "license")}
	if code, err := modelCacheCmd(append([]string{"--cache", t.TempDir(), "--to", to, "--watch", other, "--"}, dl...)); code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if got := modelcache.DownloadedTree(read(t, terminationLog)); got != "" {
		t.Fatalf("reported tree %q for a download with another output", got)
	}
	// Reading the other volume is fine.
	to2 := t.TempDir()
	if code, err := modelCacheCmd(append([]string{"--cache", t.TempDir(), "--to", to2, "--watch", other, "--"}, download(to2)...)); code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if modelcache.DownloadedTree(read(t, terminationLog)) == "" {
		t.Fatal("a download that only read the other volume was not reported")
	}
}

// Single flight (docs/design/model-cache-single-flight.md).

const testUID = "6f1c2c1e-1d2b-4c47-9a0e-2b3f4c5d6e7f"

// publish writes name in dir by a rename, as the agent does. It returns
// its error: the fake agents run it off the test goroutine, where
// t.Fatal must not be called.
func publish(dir, name, content string) error {
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p+".tmp", []byte(content), 0o644); err != nil {
		return err
	}
	return os.Rename(p+".tmp", p)
}

// atomicWrite is publish on the test goroutine.
func atomicWrite(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := publish(dir, name, content); err != nil {
		t.Fatal(err)
	}
}

func writePlan(t *testing.T, cache, plan string) {
	t.Helper()
	atomicWrite(t, cache, modelcache.PlanFile(testUID), plan+"\n")
}

// fakeAgent runs an agent's part beside the helper. A step that fails
// stops the agent's heartbeat (when there is one), which ends any wait of
// the helper's, and is reported by check, on the test goroutine.
type fakeAgent struct {
	errc   chan error
	stopHB func()
}

func newFakeAgent(stopHB func()) *fakeAgent {
	return &fakeAgent{errc: make(chan error, 8), stopHB: stopHB}
}

func (a *fakeAgent) fail(err error) {
	if err == nil {
		return
	}
	a.errc <- err
	if a.stopHB != nil {
		a.stopHB()
	}
}

func (a *fakeAgent) check(t *testing.T) {
	t.Helper()
	select {
	case err := <-a.errc:
		t.Fatalf("the fake agent failed: %v", err)
	default:
	}
}

// heartbeat keeps cache/name fresh until the test ends or stop is called.
func heartbeat(t *testing.T, cache, name string) (stop func()) {
	t.Helper()
	hb := filepath.Join(cache, name)
	if err := os.WriteFile(hb, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(100 * time.Millisecond):
				now := time.Now()
				_ = os.Chtimes(hb, now, now)
			}
		}
	}()
	var once sync.Once
	stop = func() { once.Do(func() { close(done) }) }
	t.Cleanup(stop)
	return stop
}

func planArgs(cache, to string, extra ...string) []string {
	a := append([]string{"--cache", cache, "--to", to, "--plan-wait", "1", "--poll", "100ms", "--stale", "1", "--stop-wait", "3"}, extra...)
	return append(append(a, "--"), download(to)...)
}

func TestModelCacheDownloadPlanDownloadsAtOnce(t *testing.T) {
	t.Setenv(modelcache.PodUIDEnv, testUID)
	cache, to := t.TempDir(), t.TempDir()
	heartbeat(t, cache, modelcache.IncomingFile) // a fetch for another pod must not hold this one
	heartbeat(t, cache, modelcache.StreamingFileOf(testUID))
	writePlan(t, cache, modelcache.PlanDownload)
	start := time.Now()
	if code, err := modelCacheCmd(planArgs(cache, to, "--wait", "30", "--final-wait", "0")); code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("waited %v with a download plan", d)
	}
	if got := read(t, filepath.Join(to, "weights.bin")); got != "downloaded\n" {
		t.Fatalf("weights.bin = %q", got)
	}
}

// The downloading pod's init stays open until the agent has read the
// volume for the final manifest, so its engine cannot change the tree
// first. The helper checks the agent read the tree it downloaded.
func TestModelCacheDownloadPlanHoldsTheExitUntilTheFinalManifest(t *testing.T) {
	t.Setenv(modelcache.PodUIDEnv, testUID)
	cache, to, sig := t.TempDir(), t.TempDir(), t.TempDir()
	writePlan(t, cache, modelcache.PlanDownload)
	agent := newFakeAgent(nil) // a failure ends the hold at --final-wait
	go func() {
		// The agent waits for the helper's signal, then reads the tree.
		for i := 0; i < 100; i++ {
			if b, err := os.ReadFile(filepath.Join(sig, modelcache.SignalFile)); err == nil && len(b) > 0 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		time.Sleep(time.Second)
		ents, err := modelcache.Scan(to, 0)
		if err != nil {
			agent.fail(err)
			return
		}
		agent.fail(publish(cache, modelcache.FinalFile(testUID), modelcache.Fingerprint(ents)+"\n"))
	}()
	start := time.Now()
	code, err := modelCacheCmd(planArgs(cache, to, "--final-wait", "30", "--signal", sig))
	agent.check(t)
	if code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if d := time.Since(start); d < time.Second || d > 6*time.Second {
		t.Fatalf("exited after %v; want once the final manifest was ready", d)
	}
	msg := read(t, terminationLog)
	if strings.Contains(msg, modelcache.FinalMismatch) {
		t.Fatalf("termination message %q reports a mismatch", msg)
	}
	// The signal holds the tree the download left, as the message does.
	if got := strings.TrimSpace(read(t, filepath.Join(sig, modelcache.SignalFile))); got != modelcache.DownloadedTree(msg) {
		t.Fatalf("signal %q, want the reported tree %q", got, modelcache.DownloadedTree(msg))
	}
}

func TestModelCacheDownloadPlanReportsAFinalManifestOfAnotherTree(t *testing.T) {
	t.Setenv(modelcache.PodUIDEnv, testUID)
	cache, to := t.TempDir(), t.TempDir()
	writePlan(t, cache, modelcache.PlanDownload)
	atomicWrite(t, cache, modelcache.FinalFile(testUID), "not-this-tree\n")
	if code, err := modelCacheCmd(planArgs(cache, to, "--final-wait", "30")); code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if msg := read(t, terminationLog); !strings.Contains(msg, modelcache.FinalMismatch) {
		t.Fatalf("termination message %q; want %q", msg, modelcache.FinalMismatch)
	}
}

func TestModelCacheDownloadPlanHoldIsCapped(t *testing.T) {
	t.Setenv(modelcache.PodUIDEnv, testUID)
	cache, to := t.TempDir(), t.TempDir()
	writePlan(t, cache, modelcache.PlanDownload)
	start := time.Now()
	if code, err := modelCacheCmd(planArgs(cache, to, "--final-wait", "1")); code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("held %v past a 1 s cap", d)
	}
}

// A failed download is not held: there is nothing for others to copy.
func TestModelCacheDownloadPlanDoesNotHoldAFailedDownload(t *testing.T) {
	t.Setenv(modelcache.PodUIDEnv, testUID)
	cache, to := t.TempDir(), t.TempDir()
	writePlan(t, cache, modelcache.PlanDownload)
	start := time.Now()
	code, _ := modelCacheCmd([]string{"--cache", cache, "--to", to, "--plan-wait", "1", "--final-wait", "30", "--", "/bin/sh", "-c", "exit 3"})
	if code != 3 {
		t.Fatalf("code %d, want the download's 3", code)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("held a failed download %v", d)
	}
}

// A wait plan lasts while the agent streams, past --wait: the node it
// copies from downloads for as long as that takes. The agent fills the
// volume itself and says so with .done.
func TestModelCacheWaitPlanEndsWhenTheAgentFilledTheVolume(t *testing.T) {
	t.Setenv(modelcache.PodUIDEnv, testUID)
	cache, to := t.TempDir(), t.TempDir()
	agent := newFakeAgent(heartbeat(t, cache, modelcache.StreamingFileOf(testUID)))
	writePlan(t, cache, modelcache.PlanWait)
	go func() {
		time.Sleep(2500 * time.Millisecond)
		if err := os.WriteFile(filepath.Join(to, "weights.bin"), []byte("streamed"), 0o644); err != nil {
			agent.fail(err)
			return
		}
		agent.fail(publish(cache, modelcache.DoneFile(testUID), ""))
	}()
	code, err := modelCacheCmd(planArgs(cache, to, "--wait", "1", "--max-wait", "30"))
	agent.check(t)
	if code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if got := read(t, filepath.Join(to, "weights.bin")); got != "streamed" {
		t.Fatalf("weights.bin = %q, want the streamed copy", got)
	}
	if got := read(t, terminationLog); got != modelcache.InitSeeded {
		t.Fatalf("termination message = %q", got)
	}
}

func TestModelCacheWaitPlanFlippedToDownloadDownloads(t *testing.T) {
	t.Setenv(modelcache.PodUIDEnv, testUID)
	cache, to := t.TempDir(), t.TempDir()
	stop := heartbeat(t, cache, modelcache.StreamingFileOf(testUID))
	agent := newFakeAgent(stop)
	writePlan(t, cache, modelcache.PlanWait)
	go func() {
		time.Sleep(1500 * time.Millisecond)
		// The agent took the download over: it wrote the plan, then its
		// stream ended.
		agent.fail(publish(cache, modelcache.PlanFile(testUID), modelcache.PlanDownload+"\n"))
		stop()
	}()
	start := time.Now()
	code, err := modelCacheCmd(planArgs(cache, to, "--max-wait", "30", "--final-wait", "0"))
	agent.check(t)
	if code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if d := time.Since(start); d < time.Second || d > 6*time.Second {
		t.Fatalf("downloaded after %v; want soon after the plan changed", d)
	}
	if got := read(t, filepath.Join(to, "weights.bin")); got != "downloaded\n" {
		t.Fatalf("weights.bin = %q", got)
	}
}

// A stream that stops leaves part of a tree in the volume: the helper
// empties it before it downloads.
func TestModelCacheWaitPlanWithADeadStreamEmptiesTheVolumeAndDownloads(t *testing.T) {
	t.Setenv(modelcache.PodUIDEnv, testUID)
	cache, to := t.TempDir(), t.TempDir()
	stop := heartbeat(t, cache, modelcache.StreamingFileOf(testUID))
	writePlan(t, cache, modelcache.PlanWait)
	go func() {
		time.Sleep(500 * time.Millisecond)
		_ = os.WriteFile(filepath.Join(to, "partial.bin"), []byte("half"), 0o644)
		time.Sleep(500 * time.Millisecond)
		stop()
	}()
	start := time.Now()
	if code, err := modelCacheCmd(planArgs(cache, to, "--max-wait", "30", "--final-wait", "0")); code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if d := time.Since(start); d > 6*time.Second {
		t.Fatalf("waited %v on a dead stream", d)
	}
	if _, err := os.Stat(filepath.Join(to, "partial.bin")); !os.IsNotExist(err) {
		t.Fatal("the stream's partial file was left in the volume the download writes")
	}
	if got := read(t, filepath.Join(to, "weights.bin")); got != "downloaded\n" {
		t.Fatalf("weights.bin = %q", got)
	}
}

// Today's fetch heartbeat is not the stream's: it never holds a wait plan.
func TestModelCacheWaitPlanIsNotHeldByTheFetchHeartbeat(t *testing.T) {
	t.Setenv(modelcache.PodUIDEnv, testUID)
	cache, to := t.TempDir(), t.TempDir()
	heartbeat(t, cache, modelcache.IncomingFile)
	writePlan(t, cache, modelcache.PlanWait)
	start := time.Now()
	if code, err := modelCacheCmd(planArgs(cache, to, "--max-wait", "30", "--final-wait", "0")); code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("a wait plan waited %v on the fetch heartbeat", d)
	}
}

// No plan in time: an agent that is down or does not do single flight
// never holds a pod past --plan-wait.
func TestModelCacheNoPlanDownloadsAfterThePlanWait(t *testing.T) {
	t.Setenv(modelcache.PodUIDEnv, testUID)
	cache, to := t.TempDir(), t.TempDir()
	start := time.Now()
	if code, err := modelCacheCmd(planArgs(cache, to, "--final-wait", "0")); code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	d := time.Since(start)
	if d < 900*time.Millisecond || d > 5*time.Second {
		t.Fatalf("downloaded after %v; want after the 1 s plan wait", d)
	}
}

func TestModelCacheWaitPlanIsCappedByMaxWait(t *testing.T) {
	t.Setenv(modelcache.PodUIDEnv, testUID)
	cache, to, sig := t.TempDir(), t.TempDir(), t.TempDir()
	stop := heartbeat(t, cache, modelcache.StreamingFileOf(testUID))
	writePlan(t, cache, modelcache.PlanWait)
	go func() {
		// The stream stops once the helper says the pod downloads.
		for i := 0; i < 200; i++ {
			if b, err := os.ReadFile(filepath.Join(sig, modelcache.SignalFile)); err == nil && strings.TrimSpace(string(b)) == modelcache.SignalDownloading {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		stop()
	}()
	start := time.Now()
	if code, err := modelCacheCmd(planArgs(cache, to, "--max-wait", "2", "--final-wait", "0", "--signal", sig)); code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if d := time.Since(start); d > 6*time.Second {
		t.Fatalf("waited %v past a 2 s cap", d)
	}
}

// Without its pod's uid the helper cannot read a plan: it behaves as
// without single flight.
func TestModelCacheWithoutAPodUIDIgnoresPlans(t *testing.T) {
	t.Setenv(modelcache.PodUIDEnv, "")
	cache, to := t.TempDir(), t.TempDir()
	writePlan(t, cache, modelcache.PlanWait)
	start := time.Now()
	if code, err := modelCacheCmd(planArgs(cache, to)); code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("waited %v with no uid", d)
	}
}

// A node with a complete copy needs no plan: the helper seeds at once
// rather than wait out --plan-wait.
func TestModelCacheSeedsAtOnceWithACopyAndNoPlan(t *testing.T) {
	t.Setenv(modelcache.PodUIDEnv, testUID)
	cache, to := cachedModel(t), t.TempDir()
	start := time.Now()
	if code, err := modelCacheCmd(append([]string{"--cache", cache, "--to", to, "--plan-wait", "30", "--"}, download(to)...)); code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("seeded after %v; want at once", d)
	}
	if got := read(t, filepath.Join(to, "weights.bin")); got != "cached" {
		t.Fatalf("weights.bin = %q", got)
	}
}

// Another pod's stream on this node is not this pod's: its heartbeat
// stopping must not end this pod's wait, nor keep it waiting.
func TestModelCacheWaitPlanFollowsOnlyItsOwnStream(t *testing.T) {
	t.Setenv(modelcache.PodUIDEnv, testUID)
	cache, to := t.TempDir(), t.TempDir()
	heartbeat(t, cache, modelcache.StreamingFileOf("another-pod"))
	writePlan(t, cache, modelcache.PlanWait)
	start := time.Now()
	if code, err := modelCacheCmd(planArgs(cache, to, "--max-wait", "30", "--final-wait", "0")); code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("waited %v on another pod's stream", d)
	}
}

// Before it downloads, the helper says so in the signal volume, so the
// agent never streams into a volume a download writes.
func TestModelCacheSignalsDownloadingBeforeItDownloads(t *testing.T) {
	t.Setenv(modelcache.PodUIDEnv, testUID)
	cache, to, sig := t.TempDir(), t.TempDir(), t.TempDir()
	marker := filepath.Join(sig, modelcache.SignalFile)
	// The download records what the signal said while it ran.
	dl := []string{"/bin/sh", "-c", "cat " + marker + " > " + filepath.Join(to, "seen") + "; echo downloaded > " + filepath.Join(to, "weights.bin")}
	args := append([]string{"--cache", cache, "--to", to, "--plan-wait", "1", "--poll", "100ms", "--final-wait", "0", "--signal", sig, "--"}, dl...)
	if code, err := modelCacheCmd(args); code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if got := strings.TrimSpace(read(t, filepath.Join(to, "seen"))); got != modelcache.SignalDownloading {
		t.Fatalf("signal during the download = %q, want %q", got, modelcache.SignalDownloading)
	}
}

// An init restarted while the agent streams into its volume finds files in
// it: with a wait plan it keeps waiting for the stream, rather than run a
// download over the streamed files.
func TestModelCacheRestartedWaitingInitKeepsWaiting(t *testing.T) {
	t.Setenv(modelcache.PodUIDEnv, testUID)
	cache, to := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(to, "partial.bin"), []byte("streamed so far"), 0o644); err != nil {
		t.Fatal(err)
	}
	agent := newFakeAgent(heartbeat(t, cache, modelcache.StreamingFileOf(testUID)))
	writePlan(t, cache, modelcache.PlanWait)
	go func() {
		time.Sleep(time.Second)
		if err := os.WriteFile(filepath.Join(to, "weights.bin"), []byte("streamed"), 0o644); err != nil {
			agent.fail(err)
			return
		}
		if err := os.Remove(filepath.Join(to, "partial.bin")); err != nil {
			agent.fail(err)
			return
		}
		agent.fail(publish(cache, modelcache.DoneFile(testUID), ""))
	}()
	code, err := modelCacheCmd(planArgs(cache, to, "--max-wait", "30"))
	agent.check(t)
	if code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if got := read(t, filepath.Join(to, "weights.bin")); got != "streamed" {
		t.Fatalf("weights.bin = %q; want the stream's, not a download's", got)
	}
}

// A helper that gives up on a live stream says so, then waits for the
// agent to stop writing before it empties the volume.
func TestModelCacheGivingUpWaitsForTheStreamToStop(t *testing.T) {
	t.Setenv(modelcache.PodUIDEnv, testUID)
	cache, to, sig := t.TempDir(), t.TempDir(), t.TempDir()
	stop := heartbeat(t, cache, modelcache.StreamingFileOf(testUID))
	writePlan(t, cache, modelcache.PlanWait)
	agent := newFakeAgent(stop)
	var stoppedAt time.Time
	stopped := make(chan struct{})
	go func() {
		defer close(stopped) // after stoppedAt is set: the read below is ordered after it
		// The agent sees the signal and stops; meanwhile it still writes.
		for i := 0; i < 100; i++ {
			if b, err := os.ReadFile(filepath.Join(sig, modelcache.SignalFile)); err == nil && strings.TrimSpace(string(b)) == modelcache.SignalDownloading {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if err := os.WriteFile(filepath.Join(to, "late.bin"), []byte("in flight"), 0o644); err != nil {
			agent.fail(err)
			return
		}
		time.Sleep(time.Second)
		stoppedAt = time.Now()
		stop()
	}()
	code, err := modelCacheCmd(planArgs(cache, to, "--max-wait", "1", "--final-wait", "0", "--signal", sig))
	returned := time.Now()
	agent.check(t)
	if code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("the fake agent never stopped")
	}
	if stoppedAt.IsZero() || stoppedAt.After(returned) {
		t.Fatal("the helper downloaded before the stream stopped")
	}
	if _, err := os.Stat(filepath.Join(to, "late.bin")); !os.IsNotExist(err) {
		t.Fatal("a file the stream wrote before stopping is left in the volume")
	}
}

// A helper that downloads with no plan in time, while the agent already
// streams into the volume (it began after its last look at the signal):
// the helper signals, then waits for the stream to stop and empties the
// volume before it downloads. Otherwise the agent, which reads the signal
// only between writes or before it empties the volume to take the
// download over, could still write or delete under the download.
func TestModelCacheDownloadWithoutAPlanWaitsForALiveStream(t *testing.T) {
	t.Setenv(modelcache.PodUIDEnv, testUID)
	cache, to, sig := t.TempDir(), t.TempDir(), t.TempDir()
	stop := heartbeat(t, cache, modelcache.StreamingFileOf(testUID))
	agent := newFakeAgent(stop)
	var stoppedAt time.Time
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		// The agent streams before its plan is written; the helper's plan
		// wait (1 s) runs out first.
		time.Sleep(300 * time.Millisecond)
		if err := os.WriteFile(filepath.Join(to, "partial.bin"), []byte("streamed so far"), 0o644); err != nil {
			agent.fail(err)
			return
		}
		for i := 0; i < 200; i++ {
			if b, err := os.ReadFile(filepath.Join(sig, modelcache.SignalFile)); err == nil && strings.TrimSpace(string(b)) == modelcache.SignalDownloading {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		// It reads the signal, then finishes its last write.
		if err := os.WriteFile(filepath.Join(to, "late.bin"), []byte("in flight"), 0o644); err != nil {
			agent.fail(err)
			return
		}
		time.Sleep(time.Second)
		stoppedAt = time.Now()
		stop()
	}()
	code, err := modelCacheCmd(planArgs(cache, to, "--final-wait", "0", "--signal", sig))
	returned := time.Now()
	agent.check(t)
	if code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("the fake agent never stopped")
	}
	if stoppedAt.IsZero() || stoppedAt.After(returned) {
		t.Fatal("the helper downloaded before the stream stopped")
	}
	for _, name := range []string{"partial.bin", "late.bin"} {
		if _, err := os.Stat(filepath.Join(to, name)); !os.IsNotExist(err) {
			t.Fatalf("%s, which the stream wrote, is left under the download", name)
		}
	}
	if got := read(t, filepath.Join(to, "weights.bin")); got != "downloaded\n" {
		t.Fatalf("weights.bin = %q", got)
	}
}

// A wait plan that goes away (the agent restarted, or ended the pod's
// flight) while the agent streamed into the volume: the helper stops the
// stream and empties the volume before it downloads, as when a stream
// stops.
func TestModelCacheWaitPlanThatGoesAwayStopsTheStreamFirst(t *testing.T) {
	t.Setenv(modelcache.PodUIDEnv, testUID)
	cache, to, sig := t.TempDir(), t.TempDir(), t.TempDir()
	stop := heartbeat(t, cache, modelcache.StreamingFileOf(testUID))
	writePlan(t, cache, modelcache.PlanWait)
	agent := newFakeAgent(stop)
	go func() {
		time.Sleep(300 * time.Millisecond)
		if err := os.WriteFile(filepath.Join(to, "partial.bin"), []byte("streamed so far"), 0o644); err != nil {
			agent.fail(err)
			return
		}
		if err := os.Remove(filepath.Join(cache, modelcache.PlanFile(testUID))); err != nil {
			agent.fail(err)
			return
		}
		// The stream stops once it reads the signal.
		for i := 0; i < 200; i++ {
			if b, err := os.ReadFile(filepath.Join(sig, modelcache.SignalFile)); err == nil && strings.TrimSpace(string(b)) == modelcache.SignalDownloading {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		stop()
	}()
	code, err := modelCacheCmd(planArgs(cache, to, "--max-wait", "30", "--final-wait", "0", "--signal", sig))
	agent.check(t)
	if code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	if _, err := os.Stat(filepath.Join(to, "partial.bin")); !os.IsNotExist(err) {
		t.Fatal("the download ran over the stream's files")
	}
	if got := read(t, filepath.Join(to, "weights.bin")); got != "downloaded\n" {
		t.Fatalf("weights.bin = %q", got)
	}
}

// A stream that does not stop within --stop-wait (a long file mid-copy):
// the helper neither empties the volume nor downloads into it. It fails,
// and the init's restart looks again.
func TestModelCacheLeavesTheVolumeToAStreamThatHasNotStopped(t *testing.T) {
	t.Setenv(modelcache.PodUIDEnv, testUID)
	cache, to, sig := t.TempDir(), t.TempDir(), t.TempDir()
	heartbeat(t, cache, modelcache.StreamingFileOf(testUID)) // never stops
	writePlan(t, cache, modelcache.PlanWait)
	if err := os.WriteFile(filepath.Join(to, "partial.bin"), []byte("streamed so far"), 0o644); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	code, err := modelCacheCmd(append([]string{"--cache", cache, "--to", to, "--plan-wait", "1", "--poll", "100ms", "--stale", "1",
		"--max-wait", "1", "--stop-wait", "1", "--final-wait", "0", "--signal", sig, "--"}, download(to)...))
	if code == 0 || err == nil {
		t.Fatalf("code %d err %v; want a failure", code, err)
	}
	if d := time.Since(start); d > 8*time.Second {
		t.Fatalf("returned after %v", d)
	}
	if got := read(t, filepath.Join(to, "partial.bin")); got != "streamed so far" {
		t.Fatalf("partial.bin = %q; the volume was changed under the stream", got)
	}
	if _, err := os.Stat(filepath.Join(to, "weights.bin")); !os.IsNotExist(err) {
		t.Fatal("the download ran while the stream still wrote")
	}
}

// A signal the helper could not write is written again while it waits:
// otherwise the agent never learns the pod downloads, and never stops.
func TestModelCacheWritesTheSignalAgainUntilItLands(t *testing.T) {
	t.Setenv(modelcache.PodUIDEnv, testUID)
	cache, to := t.TempDir(), t.TempDir()
	sig := filepath.Join(t.TempDir(), "not-yet") // unwritable until it exists
	stop := heartbeat(t, cache, modelcache.StreamingFileOf(testUID))
	writePlan(t, cache, modelcache.PlanWait)
	agent := newFakeAgent(stop)
	saw := make(chan struct{})
	go func() {
		time.Sleep(1500 * time.Millisecond)
		if err := os.Mkdir(sig, 0o755); err != nil {
			agent.fail(err)
			return
		}
		for i := 0; i < 200; i++ {
			if b, err := os.ReadFile(filepath.Join(sig, modelcache.SignalFile)); err == nil && strings.TrimSpace(string(b)) == modelcache.SignalDownloading {
				close(saw)
				stop()
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
	code, err := modelCacheCmd(planArgs(cache, to, "--max-wait", "1", "--final-wait", "0", "--signal", sig, "--stop-wait", "20"))
	agent.check(t)
	if code != 0 || err != nil {
		t.Fatalf("code %d err %v", code, err)
	}
	select {
	case <-saw:
	default:
		t.Fatal("the agent never read the signal")
	}
}
