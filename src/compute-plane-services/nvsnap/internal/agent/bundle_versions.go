// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// Node bundle versions (docs/design/VERSIONED-NODE-BUNDLE.md). The staging
// init container stages each bundle version into its own immutable
// <root>/versions/<id> and records the id in <root>/current-bundle. Pods
// mount a version; captures record it; a restore placeholder mounts the
// version its checkpoint recorded, fetched from a peer agent when this node
// lacks it; the reaper removes versions nothing uses.

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/metrics"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/modelvolume"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

// bundleRoot is the node bundle root as the agent sees it (the chart
// mounts it at the same path as on the host).
func (a *Agent) bundleRoot() string {
	if r := a.config.Webhook.HostBundleRoot; r != "" {
		return r
	}
	return webhook.DefaultHostBundleRoot
}

// bundleVersionsDir holds the node's bundle versions.
func (a *Agent) bundleVersionsDir() string {
	return filepath.Join(a.bundleRoot(), webhook.BundleVersionsDir)
}

// currentBundle is the bundle version this agent staged, "" when the
// staging predates versions (pods then mount the legacy tree).
func (a *Agent) currentBundle() string {
	b, err := os.ReadFile(filepath.Join(a.bundleRoot(), "current-bundle"))
	if err != nil {
		return ""
	}
	id := strings.TrimSpace(string(b))
	if !validBundleID(id) {
		return ""
	}
	if _, err := os.Stat(filepath.Join(a.bundleVersionsDir(), id)); err != nil { //nolint:gosec // id passed validBundleID: hex, no separators
		return ""
	}
	return id
}

// validBundleID accepts the ids staging writes (an image version, or a
// content hash) and nothing that could name another path: letters,
// digits, '.', '_' and '-', not starting with '.'.
func validBundleID(id string) bool {
	if id == "" || len(id) > 128 || id[0] == '.' {
		return false
	}
	for _, c := range id {
		if (c < '0' || c > '9') && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && c != '.' && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

// podBundle is the bundle version a pod mounts, "" when unknown.
func (a *Agent) podBundle(ctx context.Context, ns, name string) string {
	if a.kubeClient == nil || ns == "" || name == "" {
		return ""
	}
	pod, err := a.kubeClient.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return ""
	}
	return webhook.PodBundleID(pod)
}

// podProcessConfig is the process configuration the webhook stamped on a
// pod at admission, "" when it has none. A pod that cannot be read is an
// error, not "none": a capture recorded without its configuration would
// restore into any configuration.
func (a *Agent) podProcessConfig(ctx context.Context, ns, name string) (string, error) {
	if a.kubeClient == nil || ns == "" || name == "" {
		return "", nil
	}
	pod, err := a.kubeClient.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	return pod.Annotations[webhook.ProcessConfigAnnotation], nil
}

// podGPUShareMounted reports whether the pod's container (any container
// when container is empty) has the gpushare library or store mounted.
// A pod that cannot be read reports false: the record then says what the
// dump itself found.
func (a *Agent) podGPUShareMounted(ctx context.Context, ns, name, container string) bool {
	if a.kubeClient == nil || ns == "" || name == "" {
		return false
	}
	pod, err := a.kubeClient.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return false
	}
	return webhook.GPUShareMounted(pod, container)
}

// bundleVersionMinAge keeps a version a while after it was staged or
// fetched: a pod admitted to mount it may not show yet.
const bundleVersionMinAge = time.Hour

// reapBundleVersions removes the bundle versions nothing on the node uses:
// not current, mounted by no pod of the node, recorded by no checkpoint on
// it, and older than bundleVersionMinAge. A staging or fetch left half
// done goes too, once as old.
func (a *Agent) reapBundleVersions(pods []corev1.Pod, now time.Time) {
	dir := a.bundleVersionsDir()
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	keep := map[string]bool{a.currentBundle(): true}
	for i := range pods {
		for _, id := range webhook.PodBundleIDs(&pods[i]) {
			keep[id] = true
		}
	}
	for id := range a.checkpointBundles() {
		keep[id] = true
	}
	for _, e := range ents {
		info, err := e.Info()
		if err != nil || !e.IsDir() || now.Sub(info.ModTime()) < bundleVersionMinAge {
			continue
		}
		if !strings.HasPrefix(e.Name(), ".") && keep[e.Name()] {
			continue
		}
		if err := a.removeBundleVersion(e.Name()); err != nil {
			a.log.WithError(err).WithField("bundle", e.Name()).Warn("node reaper: cannot remove an unused bundle version; keeping it")
			continue
		}
		a.log.WithField("bundle", e.Name()).Info("node reaper: removed an unused bundle version")
	}
}

// checkpointBundles are the bundle versions the checkpoints on this node
// recorded: their restores mount them.
func (a *Agent) checkpointBundles() map[string]bool {
	out := map[string]bool{}
	ents, err := os.ReadDir(a.config.CheckpointDir)
	if err != nil {
		return out
	}
	for _, e := range ents {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(a.config.CheckpointDir, e.Name(), "metadata.json"))
		if err != nil {
			continue
		}
		var md struct {
			Bundle string `json:"bundle"`
		}
		if json.Unmarshal(b, &md) == nil && validBundleID(md.Bundle) {
			out[md.Bundle] = true
		}
	}
	return out
}

// bundleHandler serves a bundle version of this node as a tar stream, for
// a peer whose restore needs it.
func (a *Agent) bundleHandler(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if !validBundleID(id) {
		http.Error(w, "invalid bundle id", http.StatusBadRequest)
		return
	}
	dir := filepath.Join(a.bundleVersionsDir(), id)
	if _, err := os.Stat(dir); err != nil { //nolint:gosec // id passed validBundleID: hex, no separators
		// Only a version that is not there is absent: a peer asking takes
		// that answer as final.
		if errors.Is(err, fs.ErrNotExist) {
			http.Error(w, bundleAbsentText, http.StatusNotFound)
		} else {
			http.Error(w, "cannot read the bundle version", http.StatusInternalServerError)
		}
		return
	}
	w.Header().Set("Content-Type", "application/x-tar")
	if err := writeBundleTar(w, dir); err != nil {
		a.log.WithError(err).WithField("bundle", id).Warn("serving a bundle version failed")
	}
}

// bundleAbsentText is the body of a peer's 404 for a version it does not
// have. A 404 without it (a route the peer does not serve) says nothing
// about the version.
const bundleAbsentText = "no such bundle version on this node"

// writeBundleTar writes the regular files, directories and symlinks under dir
// as a tar stream.
func writeBundleTar(w io.Writer, dir string) error {
	tw := tar.NewWriter(w)
	// dir is a version the agent staged or fetched itself; nothing else
	// writes under it while it is served.
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error { //nolint:gosec // see above
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil || rel == "." {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		link := ""
		if info.Mode()&os.ModeSymlink != 0 {
			if link, err = os.Readlink(p); err != nil {
				return err
			}
		} else if !info.Mode().IsRegular() && !info.IsDir() {
			return nil
		}
		hdr, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(rel)
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			f, err := os.Open(p) //nolint:gosec // the agent's own bundle version (see above)
			if err != nil {
				return err
			}
			_, err = io.Copy(tw, f)
			_ = f.Close()
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return tw.Close()
}

// bundleMaxBytes bounds what a fetched bundle version may hold.
const bundleMaxBytes = 4 << 30

// readBundleTar extracts a tar stream written by writeBundleTar into dir.
// Every write goes through an os.Root of dir, so neither a name nor a
// symlink the stream itself creates can place a file outside it; an entry
// whose name leaves dir fails the extraction, as does a stream larger than
// bundleMaxBytes.
func readBundleTar(r io.Reader, dir string) error {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	tr := tar.NewReader(r)
	var total int64
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		name := filepath.Clean(filepath.FromSlash(hdr.Name))
		if name == "." || filepath.IsAbs(name) || name == ".." || strings.HasPrefix(name, ".."+string(filepath.Separator)) {
			return fmt.Errorf("bundle entry %q leaves the bundle", hdr.Name)
		}
		// os.Root keeps every entry inside dir, but follows a link the
		// stream made inside it: a link "x -> ." and then a directory "x"
		// would chmod the extraction root itself. No entry may go through
		// a link: a directory entry is checked with its own name, any
		// other with its parents.
		through := filepath.Dir(name)
		if hdr.Typeflag == tar.TypeDir {
			through = name
		}
		if err := noLinkOnPath(root, through); err != nil {
			return fmt.Errorf("bundle entry %q: %w", hdr.Name, err)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			// Chmod after the create: the create's mode passes through the
			// agent's umask, and a version must arrive with the modes its
			// recorded digest covers.
			if err := root.MkdirAll(name, tarPerm(hdr.Mode)|0o700); err != nil {
				return err
			}
			if err := root.Chmod(name, tarPerm(hdr.Mode)|0o700); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := root.MkdirAll(filepath.Dir(name), 0o755); err != nil {
				return err
			}
			if err := root.Symlink(hdr.Linkname, name); err != nil {
				return err
			}
		case tar.TypeReg:
			if total += hdr.Size; hdr.Size < 0 || total > bundleMaxBytes {
				return fmt.Errorf("bundle larger than %d bytes", int64(bundleMaxBytes))
			}
			if err := root.MkdirAll(filepath.Dir(name), 0o755); err != nil {
				return err
			}
			f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, tarPerm(hdr.Mode))
			if err != nil {
				return err
			}
			_, err = io.CopyN(f, tr, hdr.Size)
			if err == nil {
				err = f.Chmod(tarPerm(hdr.Mode)) // past the umask, as for a directory
			}
			if cerr := f.Close(); err == nil {
				err = cerr
			}
			if err != nil {
				return err
			}
		}
	}
}

// noLinkOnPath fails when name, or a directory above it, is a symbolic
// link under root. Components that do not exist yet are fine.
func noLinkOnPath(root *os.Root, name string) error {
	if name == "." || name == "" {
		return nil
	}
	parts := strings.Split(name, string(filepath.Separator))
	for i := range parts {
		p := filepath.Join(parts[:i+1]...)
		fi, err := root.Lstat(p)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%s is a symbolic link", p)
		}
	}
	return nil
}

// tarPerm is the permission bits of a tar header's mode.
func tarPerm(mode int64) os.FileMode {
	if mode < 0 || mode > 0o777 {
		mode &= 0o777
	}
	return os.FileMode(uint32(mode)) & os.ModePerm //nolint:gosec // masked to 9 bits above
}

// ensurePlaceholderBundles fetches the bundle versions a pod scheduled to
// this node mounts and the node lacks: the pod cannot start without them
// (a restore placeholder mounts the version its checkpoint recorded).
//
// A companion (a pod without GPUs a capture may dump) starts without them,
// its tools directory empty (companion_tools.go): its version is fetched
// and mounted while it runs too, retried on its later events, so that it
// can be captured once that succeeds.
func (c *criuAutoRestorer) ensurePlaceholderBundles(ctx context.Context, pod *corev1.Pod) {
	if pod.Spec.NodeName != c.a.config.NodeName || pod.DeletionTimestamp != nil {
		return
	}
	if pod.Status.Phase != corev1.PodPending && !mountsAnyCompanionTools(pod) {
		return
	}
	for _, id := range webhook.PodBundleIDs(pod) {
		if !validBundleID(id) {
			continue
		}
		if _, err := os.Stat(filepath.Join(c.a.bundleVersionsDir(), id)); err == nil {
			c.ensureCompanionToolsFor(pod, id)
			continue
		}
		if _, refused := c.attempted.Load(bundleRefusedPrefix + bundleMissKey(pod.UID, id)); refused {
			// Not fetched for again; once it has waited too long, the pod
			// alone goes, so that it is not left waiting for good.
			c.considerStuckRefused(ctx, pod, id)
			continue
		}
		key := "bundle/" + id
		if _, busy := c.attempted.LoadOrStore(key, struct{}{}); busy {
			continue
		}
		go func(id string) {
			defer c.attempted.Delete(key) // a later event retries a failure
			log := c.a.log.WithFields(logrus.Fields{"bundle": id, "pod": pod.Namespace + "/" + pod.Name})
			fetch := c.fetchBundle
			if fetch == nil {
				fetch = c.a.fetchBundle
			}
			fctx, cancel := c.bundleFetchContext(ctx, pod, id)
			err := fetch(fctx, id)
			cancel()
			if err != nil {
				c.bundleFetchFailed(ctx, pod, id, err, log)
				return
			}
			c.bundleMissSince.Delete(bundleMissKey(pod.UID, id))
			c.bundleFailingSince.Delete(bundleMissKey(pod.UID, id))
			c.bundleServed.Delete(bundleMissKey(pod.UID, id))
			log.Info("fetched the bundle version a pod needs from a peer")
			c.ensureCompanionToolsFor(pod, id)
		}(id)
	}
}

// ensureCompanionToolsFor mounts version id on its companion directory when
// pod, a pod without GPUs scheduled here, mounts that directory: before it
// starts, rather than at the next node sweep.
func (c *criuAutoRestorer) ensureCompanionToolsFor(pod *corev1.Pod, id string) {
	if !mountsCompanionTools(pod, id) {
		return
	}
	if err := c.a.ensureCompanionTools(id); err != nil {
		c.a.log.WithError(err).WithFields(logrus.Fields{"bundle": id, "pod": pod.Namespace + "/" + pod.Name}).
			Warn("node bundle: cannot mount the version a pod without GPUs needs; it is not captured until it is")
	}
}

// bundleMissConfirm is how long a version must stay on no peer before a
// placeholder that needs it is given up: a node whose agent is between
// two pods is not asked, and may have it. Pod events (the informer's
// resync included) bring the fetch again within it.
var bundleMissConfirm = 2 * time.Minute

// bundleUndecidedMax bounds how long a placeholder waits for a version
// whose fetch keeps failing without a final answer (a peer that neither
// serves it nor is down long enough to count out, a copy that cannot be
// installed here). Past it the pod is deleted: a cold start is slow, a pod
// that never starts is an outage. An instance's rank 0 placeholder stuck
// on its version never runs, so its group's own wait does not start
// either.
var bundleUndecidedMax = 30 * time.Minute

// peerDownBound is how long a peer's node must be not Ready, or its agent
// pod not Ready, before the peer counts as unable to serve a version: a
// node that has been unreachable that long is not waited on, as an
// instance's placeholders are not (criuGroupPlaceholderWait).
var peerDownBound = criuGroupPlaceholderWait

// bundleMissKey keys the timers of pod uid's wait for version id.
func bundleMissKey(uid types.UID, id string) string { return string(uid) + "/" + id }

// forgetBundleWaits drops the timers and refusals kept for pod uid's
// versions.
func (c *criuAutoRestorer) forgetBundleWaits(uid types.UID) {
	prefix := string(uid) + "/"
	for _, m := range []*sync.Map{&c.bundleMissSince, &c.bundleFailingSince, &c.bundleServed} {
		m.Range(func(k, _ any) bool {
			if strings.HasPrefix(k.(string), prefix) {
				m.Delete(k)
			}
			return true
		})
	}
	c.attempted.Range(func(k, _ any) bool {
		if s, ok := k.(string); ok && strings.HasPrefix(s, bundleRefusedPrefix+prefix) {
			c.attempted.Delete(k)
		}
		return true
	})
}

// considerStuckRefused deletes pod, refused the restore of its checkpoint
// while it waited for version id, once it has waited bundleUndecidedMax.
func (c *criuAutoRestorer) considerStuckRefused(ctx context.Context, pod *corev1.Pod, id string) {
	f, ok := c.bundleFailingSince.Load(bundleMissKey(pod.UID, id))
	if !ok || time.Since(f.(time.Time)) < bundleUndecidedMax {
		return
	}
	if _, busy := c.attempted.LoadOrStore("bundle-stuck/"+string(pod.UID), struct{}{}); busy {
		return
	}
	go func() {
		defer c.attempted.Delete("bundle-stuck/" + string(pod.UID))
		log := c.a.log.WithFields(logrus.Fields{"bundle": id, "pod": pod.Namespace + "/" + pod.Name})
		c.deleteStuckPlaceholder(ctx, pod, "its restore is not admitted", log)
	}()
}

// bundleRefusedPrefix marks, in attempted, a pod and version whose restore
// the pod may not have: its version is not fetched for it again.
const bundleRefusedPrefix = "bundle-refused/"

// bundleSweepMin is the least time a fetch of a placeholder's version is
// given once its wait is past bundleUndecidedMax: it may still succeed.
const bundleSweepMin = time.Minute

// bundleFetchContext bounds a fetch of version id for restore placeholder
// pod by the end of its wait (bundleUndecidedMax from its first fetch),
// so that peers that accept a request and stall do not stretch the wait
// past it: the give-up is decided when the fetch returns. The wait starts
// with the first fetch. Any other pod's fetch is not bounded here.
func (c *criuAutoRestorer) bundleFetchContext(ctx context.Context, pod *corev1.Pod, id string) (context.Context, context.CancelFunc) {
	if !restoreNeedsBundle(pod, id) {
		return context.WithCancel(ctx)
	}
	now := time.Now()
	first, _ := c.bundleFailingSince.LoadOrStore(bundleMissKey(pod.UID, id), now)
	deadline := first.(time.Time).Add(bundleUndecidedMax)
	if deadline.Before(now.Add(bundleSweepMin)) {
		deadline = now.Add(bundleSweepMin)
	}
	return context.WithDeadline(ctx, deadline)
}

// bundleFetchFailed handles a failed fetch of version id for pod. A
// restore placeholder that cannot start without it is started cold once
// no peer has it for bundleMissConfirm, or once its fetch has failed for
// bundleUndecidedMax whatever the cause; any other pod waits for the next
// try.
func (c *criuAutoRestorer) bundleFetchFailed(ctx context.Context, pod *corev1.Pod, id string, err error, log *logrus.Entry) {
	if !restoreNeedsBundle(pod, id) {
		log.WithError(err).Warn("cannot fetch the bundle version a pod needs; it waits")
		return
	}
	k, now := bundleMissKey(pod.UID, id), time.Now()
	failing, _ := c.bundleFailingSince.LoadOrStore(k, now)
	if errors.Is(err, errBundleInstall) {
		// A peer has the version: whatever fails, the capture is not at
		// fault (bundleUnreachable).
		c.bundleServed.Store(k, struct{}{})
	}
	overdue := now.Sub(failing.(time.Time)) >= bundleUndecidedMax
	if errors.Is(err, errBundleNotOnAnyPeer) {
		since, _ := c.bundleMissSince.LoadOrStore(k, now)
		if now.Sub(since.(time.Time)) < bundleMissConfirm {
			log.WithError(err).Warn("no peer agent has the bundle version a restore placeholder needs; the pod starts cold if none has it a little later")
			return
		}
		c.restoreWithoutBundle(ctx, pod, id, err, overdue, log)
		return
	}
	c.bundleMissSince.Delete(k) // the miss must hold unbroken
	if !overdue {
		log.WithError(err).Warn("cannot fetch the bundle version a pod needs; it waits")
		return
	}
	log.WithError(err).Warn("CRIU restore: the placeholder's bundle version could not be fetched for too long; giving the restore up")
	c.restoreWithoutBundle(ctx, pod, id, fmt.Errorf("its fetch has failed for %s without a final answer: %w", bundleUndecidedMax, err), true, log)
}

// restoreWithoutBundle ends the restore of placeholder pod, which needs
// bundle version id that it cannot get (cause): the pod cannot start, so
// its restore would never run and its failure path never be reached. The
// pod is deleted (a group's placeholders all), so its replacement starts
// again. The checkpoint is blocked as well, so the replacement starts cold,
// only when the capture is at fault: the pod may restore it, the capture
// recorded the version, and no peer was seen to have it. Otherwise, once
// the pod has waited for bundleUndecidedMax (overdue), only the pod goes.
func (c *criuAutoRestorer) restoreWithoutBundle(ctx context.Context, pod *corev1.Pod, id string, cause error, overdue bool, log *logrus.Entry) {
	k := bundleMissKey(pod.UID, id)
	why, err := c.restoreRefusal(ctx, pod)
	if err != nil || why != "" {
		if err == nil {
			// As admitted: a pod naming a restore it may not have gets
			// nothing, not even its checkpoint blocked, nor further
			// fetches.
			c.attempted.Store(bundleRefusedPrefix+k, struct{}{})
		}
		if !overdue {
			log.WithError(err).WithField("reason", why).Warn("CRIU restore: the placeholder's bundle version is unavailable, and its restore is not admitted (or cannot be verified yet); leaving it as it is")
			return
		}
		c.deleteStuckPlaceholder(ctx, pod, "its restore is not admitted, or cannot be verified", log)
		return
	}
	block, reason := true, ""
	// Only the version its capture recorded is the capture's fault: a
	// mount the restore does not depend on, or a record gone since the
	// pod was admitted, says nothing about it.
	recorded, err := c.recordedBundle(ctx, pod)
	if err != nil || recorded != id {
		if !overdue {
			log.WithError(err).WithField("recorded", recorded).Warn("CRIU restore: the placeholder waits on a bundle version its capture record does not name (or cannot be read); it waits")
			return
		}
		block, reason = false, "its capture record does not name the version, or cannot be read"
	}
	if _, served := c.bundleServed.Load(k); served {
		block, reason = false, "a peer served the version, which could not be installed on this node"
	}
	ctx = context.WithoutCancel(ctx)
	ckpt := pod.Annotations[webhook.CRIURestoreAnnotation]
	key := pod.Annotations[webhook.CRIUGroupAnnotation]
	cause = fmt.Errorf("%s: %s: %w", metrics.BundleUnavailableText, id, cause)
	log = log.WithField("checkpoint", ckpt)
	if block {
		// Blocked first: a replacement admitted before the block would be
		// a placeholder of the same checkpoint, on a node that may lack
		// the version too. Unblocked, the pod is kept and this is tried
		// again on its next event.
		if err := c.markFailed(ctx, ckpt, cause); err != nil {
			log.WithError(err).Error("CRIU restore: the placeholder's bundle version is unavailable, and its checkpoint cannot be blocked; trying again on the pod's next event")
			return
		}
		log = log.WithField("checkpointBlocked", true)
	} else {
		log = log.WithFields(logrus.Fields{"checkpointBlocked": false, "notBlockedBecause": reason})
	}
	if isGroupPlaceholder(pod) {
		// The webhook admits no rank of an instance once any checkpoint
		// of its capture is blocked, so ending the group as not run
		// (failGroup) deletes placeholders whose replacements all start
		// cold; unblocked, their replacements restore again, within the
		// group's stalls. Every rank short of the version gets here; one
		// ends the group.
		if !c.claimGroupEnd(ctx, key, instanceIdentity(pod), time.Now(), log) {
			log.WithError(cause).Warn("CRIU group restore cannot run: a placeholder's bundle version is unavailable; another rank's agent ends the instance")
			return
		}
		log.WithError(cause).Error("CRIU group restore cannot run: a placeholder's bundle version is unavailable; ending the instance's restore")
		c.failGroup(ctx, groupLeaderOf(ctx, c.a, pod, key), key, false, cause, log)
		return
	}
	log.WithError(cause).Error("CRIU restore cannot run: the placeholder's bundle version is unavailable; deleting the pod so its replacement starts again")
	c.observeRestore(key, pod, c.a.nodeGPU(ctx, c.a.config.NodeName), 1, metrics.RestoreNotRun, cause, 0, log)
	// Deleted, not started cold in place (disposeFailedPlaceholder): the
	// pod cannot start until its spec no longer mounts the version, so a
	// pod nothing recreates is lost either way.
	if err := c.a.deletePodRetrying(ctx, pod.Namespace, pod.Name, pod.UID); err != nil {
		log.WithError(err).Error("CRIU restore: could not delete the placeholder; trying again on its next event")
	}
}

// deleteStuckPlaceholder deletes placeholder pod, which has waited for its
// bundle version for bundleUndecidedMax, and nothing else: no checkpoint
// is blocked and no record written, as the pod may not act on them
// (why). A pod that cannot start is not left waiting for good.
func (c *criuAutoRestorer) deleteStuckPlaceholder(ctx context.Context, pod *corev1.Pod, why string, log *logrus.Entry) {
	log.WithField("reason", why).Warn("CRIU restore: the placeholder has waited too long for its bundle version; deleting the pod only")
	if err := c.a.deletePodRetrying(context.WithoutCancel(ctx), pod.Namespace, pod.Name, pod.UID); err != nil {
		log.WithError(err).Error("CRIU restore: could not delete the placeholder; trying again on its next event")
	}
}

// groupBundleLostKey in a group record names the instance
// (instanceIdentity) whose restore an agent is ending for a lost bundle
// version, and when: "identity@RFC3339".
const groupBundleLostKey = "bundleLostEndedBy"

// groupEndClaimTTL is how long such a claim keeps other agents from ending
// the same instance: past it the claimant is taken to have stopped half
// way, and the next agent ends it.
const groupEndClaimTTL = 5 * time.Minute

// instanceIdentity names the instance pod is a rank of, as sameInstance
// tells instances apart, from the pod's own labels and owner: the same for
// every rank, whichever of them are still there.
func instanceIdentity(pod *corev1.Pod) string {
	if g := groveInstance(pod); g != "" {
		return "grove:" + g
	}
	if g := pod.Labels[lwsGroupKeyLabel]; g != "" {
		return "lws:" + g
	}
	if o := metav1.GetControllerOf(pod); o != nil {
		return "owner:" + string(o.UID)
	}
	return "pod:" + string(pod.UID)
}

// claimGroupEnd claims ending the restore of instance under group record
// key, so that one rank's agent ends it (one stall, one outcome), not
// every rank's that lacks the version. Without a record there is no one to
// share it with: claimed. A record that cannot be updated is not.
func (c *criuAutoRestorer) claimGroupEnd(ctx context.Context, key, instance string, now time.Time, log *logrus.Entry) bool {
	won := false
	err := c.updateGroupRecord(ctx, key, func(d map[string]string) bool {
		won = false
		v := d[groupBundleLostKey]
		if i := strings.LastIndex(v, "@"); i >= 0 && v[:i] == instance {
			if t, perr := time.Parse(time.RFC3339, v[i+1:]); perr == nil && now.Sub(t) < groupEndClaimTTL {
				return false
			}
		}
		d[groupBundleLostKey] = instance + "@" + now.UTC().Format(time.RFC3339)
		won = true
		return true
	})
	if apierrors.IsNotFound(err) {
		return true
	}
	if err != nil {
		log.WithError(err).Warn("CRIU group restore: cannot claim ending the instance; trying again on the placeholder's next event")
		return false
	}
	return won
}

// recordedBundle is the bundle version pod's checkpoint was captured with:
// its rank's in its group record, or its capture record's for a lone
// restore; "" when none was recorded.
func (c *criuAutoRestorer) recordedBundle(ctx context.Context, pod *corev1.Pod) (string, error) {
	if key := pod.Annotations[webhook.CRIUGroupAnnotation]; key != "" {
		if o, _, ok := podRank(pod, webhook.CRIUGroupOrdinalAnnotation, webhook.CRIUGroupSizeAnnotation); ok {
			cm, err := c.a.kubeClient.CoreV1().ConfigMaps(c.a.criuFailedNamespace()).Get(ctx, criuGroupConfigMapName(key), metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return "", nil
			}
			if err != nil {
				return "", err
			}
			return cm.Data[fmt.Sprintf("bundle.%d", o)], nil
		}
	}
	hash, err := c.checkpointHash(ctx, pod.Annotations[webhook.CRIURestoreAnnotation])
	if err != nil || hash == "" {
		return "", err
	}
	man, found, err := c.captureRecord(ctx, hash)
	if err != nil || !found {
		return "", err
	}
	return man.SourcePodMeta[webhook.BundleMeta], nil
}

// restoreNeedsBundle reports whether pod is a restore placeholder, not yet
// restored or given up, that cannot start without version id: it mounts it
// for its checkpoint, not only as the version the webhook mounts on every
// pod (its helper), which an agent rollout stages on every node. A
// companion starts without its version (companion_tools.go), so it never
// waits on it.
func restoreNeedsBundle(pod *corev1.Pod, id string) bool {
	if pod.Annotations[webhook.CRIURestoreAnnotation] == "" || pod.Annotations[webhook.CRIURestoreContainerAnnotation] == "" ||
		pod.Annotations[webhook.CRIURestoredAnnotation] != "" || coldStarted(pod) || pod.DeletionTimestamp != nil ||
		pod.Status.Phase != corev1.PodPending || isCompanion(pod) || mountsAnyCompanionTools(pod) {
		return false
	}
	needs := false
	for i := range pod.Spec.Volumes {
		v := &pod.Spec.Volumes[i]
		if v.HostPath == nil || !strings.HasSuffix(v.HostPath.Path, "/"+webhook.BundleVersionsDir+"/"+id) {
			continue
		}
		if v.Name == modelvolume.HelperVolumeName {
			return false
		}
		needs = true
	}
	return needs
}

// groupLeaderOf is the rank 0 placeholder of pod's instance under group
// key, whose restore ends the group's; pod itself when it cannot be found.
func groupLeaderOf(ctx context.Context, a *Agent, pod *corev1.Pod, key string) *corev1.Pod {
	pods, err := a.kubeClient.CoreV1().Pods(pod.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return pod
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if o, _, ok := podRank(p, webhook.CRIUGroupOrdinalAnnotation, webhook.CRIUGroupSizeAnnotation); ok && o == 0 &&
			p.DeletionTimestamp == nil && p.Annotations[webhook.CRIUGroupAnnotation] == key && !isCompanion(p) && groupMember(pod, p) {
			return p
		}
	}
	return pod
}

// errBundleNotOnPeer is a peer agent's answer that it does not have the
// bundle version asked for.
var errBundleNotOnPeer = errors.New("the peer agent does not have the bundle version")

// errPeerDown marks the failed fetch from a peer whose node, or agent, has
// not been Ready for longer than peerDownBound.
var errPeerDown = errors.New("the peer has not been ready for long")

// errBundleNotOnAnyPeer is a fetch that cannot succeed later either: every
// agent of this node's architecture but this one either answered that it
// does not have the version or has been down for longer than
// peerDownBound and did not serve it, while this node's agent was listed and no agent was being
// placed or replaced. A version is staged only as an agent's current one,
// and a placeholder's own (an older one its checkpoint recorded) is never
// staged again, so none of them will get it. With no such peer at all,
// nothing can serve it either: the same answer.
var errBundleNotOnAnyPeer = errors.New("no peer agent has the bundle version")

// fetchBundle copies bundle version id from the first peer agent that
// has it. It fails with errBundleNotOnAnyPeer only when no peer could have
// it; any other failure (a peer not answering for now, an unreadable
// node, an agent rollout) may pass.
func (a *Agent) fetchBundle(ctx context.Context, id string) error {
	pods, err := a.kubeClient.CoreV1().Pods(a.criuFailedNamespace()).List(ctx, metav1.ListOptions{LabelSelector: agentLabelSelector})
	if err != nil {
		return err
	}
	// A version holds one architecture's binaries: only a node of this
	// node's architecture has the right ones.
	_, arch, err := a.nodeArchOf(ctx, a.config.NodeName)
	if err != nil {
		return fmt.Errorf("this node's architecture: %w", err)
	}
	now := time.Now()
	var errs []error
	asked, absent := 0, 0
	unsettled, rev, selfListed := "", "", false
	listed := map[string]bool{}
	for i := range pods.Items {
		p := &pods.Items[i]
		node := p.Spec.NodeName
		if node == "" {
			// A new agent not placed yet may be the one for a node that
			// has the version.
			unsettled = "agent " + p.Name + " is not placed on a node yet"
			continue
		}
		n, na, err := a.nodeArchOf(ctx, node)
		if err != nil {
			// It may be of this architecture, and have the version.
			errs = append(errs, fmt.Errorf("%s: %w", node, err))
			continue
		}
		if na != arch {
			continue
		}
		self := node == a.config.NodeName
		selfListed = selfListed || self
		listed[node] = true
		down := !self && peerDown(n, p, now)
		// An agent being replaced may be the one that has the version, and
		// a node between its old agent and its new one is not listed at
		// all: no answer is final during a rollout. An agent down for long
		// is not waited on, whether it is crashing or stuck terminating.
		switch h := p.Labels["controller-revision-hash"]; {
		case down:
		case p.DeletionTimestamp != nil:
			unsettled = "agent " + p.Name + " is terminating"
		case rev != "" && h != rev:
			unsettled = "the agents run two revisions: a rollout is in progress"
		case rev == "":
			rev = h
		}
		if self {
			continue
		}
		// Every peer is asked, a down one too: its node's status comes
		// from its kubelet, and a node whose kubelet or link to the
		// control plane failed may still serve.
		asked++
		if err := a.fetchBundleFrom(ctx, node, id); err != nil {
			// Absent: it says so, or it is down for long and did not
			// serve. One that served the version has it, down or not.
			if errors.Is(err, errBundleNotOnPeer) || (down && !errors.Is(err, errBundleInstall)) {
				absent++
			}
			if down {
				err = fmt.Errorf("%w: %w", errPeerDown, err)
			}
			errs = append(errs, fmt.Errorf("%s: %w", node, err))
			continue
		}
		return nil
	}
	if !selfListed && unsettled == "" {
		// The list may be missing agents; with this one missing it is
		// surely not the whole set.
		unsettled = "this node's agent is not listed"
	}
	if unsettled == "" && absent == asked && len(errs) == absent {
		unsettled = a.agentMissingOn(ctx, arch, listed, now)
	}
	joined := errors.Join(errs...)
	if unsettled == "" && absent == asked && len(errs) == absent {
		return fmt.Errorf("%w: version %s: %d agents of this node's architecture asked or down, none has it: %w", errBundleNotOnAnyPeer, id, asked, joined)
	}
	if unsettled != "" {
		joined = errors.Join(joined, errors.New(unsettled))
	}
	return fmt.Errorf("no peer agent has bundle version %s: %w", id, joined)
}

// agentMissingOn returns why the agents listed (by node) may not be all of
// this architecture's: a Ready node of arch that the agent DaemonSet runs
// on but that has no agent listed (one not created yet, or deleted and
// not replaced), which may have the version. "" when there is none. A
// node not Ready for longer than peerDownBound could not serve anyway.
func (a *Agent) agentMissingOn(ctx context.Context, arch string, listed map[string]bool, now time.Time) string {
	nodes, err := a.kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "cannot list the nodes: " + err.Error()
	}
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if listed[n.Name] || n.Labels[corev1.LabelArchStable] != arch {
			continue
		}
		if peerDown(n, &corev1.Pod{}, now) {
			continue
		}
		places, err := a.placesAgent(n)
		if err != nil {
			return "agent placement: " + err.Error()
		}
		if places {
			return "node " + n.Name + " runs no listed agent"
		}
	}
	return ""
}

// peerDown reports whether a peer agent pod p, on node n, has been unable
// to serve for longer than peerDownBound: the node not Ready (or its
// status unknown), or the pod not Ready, since then.
func peerDown(n *corev1.Node, p *corev1.Pod, now time.Time) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady && c.Status != corev1.ConditionTrue && now.Sub(c.LastTransitionTime.Time) > peerDownBound {
			return true
		}
	}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionFalse && now.Sub(c.LastTransitionTime.Time) > peerDownBound {
			return true
		}
	}
	return false
}

func (a *Agent) fetchBundleFrom(ctx context.Context, node, id string) error {
	base, err := a.peerAgentURL(ctx, node)
	if err != nil {
		return err
	}
	fctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(fctx, http.MethodGet, base+"/v1/bundles/"+id, http.NoBody)
	if err != nil {
		return err
	}
	resp, err := fabricHTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		if strings.Contains(string(body), bundleAbsentText) {
			return fmt.Errorf("%w: %s", errBundleNotOnPeer, resp.Status)
		}
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s", resp.Status)
	}
	// From here the peer has the version: a failure says nothing about
	// whether it exists.
	if err := a.installBundle(fctx, resp.Body, id); err != nil {
		return fmt.Errorf("%w: %w", errBundleInstall, err)
	}
	return nil
}

// errBundleInstall is a fetch whose peer served the version, which could
// then not be installed on this node.
var errBundleInstall = errors.New("a peer served the bundle version, but it could not be installed here")

// installBundle extracts bundle version id from tar stream r, verifies it
// against the recorded digest and moves it into place.
func (a *Agent) installBundle(fctx context.Context, r io.Reader, id string) error {
	dir := a.bundleVersionsDir()
	if merr := os.MkdirAll(dir, 0o755); merr != nil {
		return merr
	}
	tmp, err := os.MkdirTemp(dir, "."+id+".fetch-")
	if err != nil {
		return err
	}
	if rerr := readBundleTar(r, tmp); rerr != nil {
		_ = os.RemoveAll(tmp)
		return rerr
	}
	// The peer is trusted for nothing: only contents with the digest the
	// staging agents recorded are installed.
	if verr := a.verifyBundle(fctx, id, a.nodeArch(fctx, a.config.NodeName), tmp); verr != nil {
		_ = os.RemoveAll(tmp)
		return verr
	}
	if cerr := os.Chmod(tmp, 0o755); cerr != nil {
		_ = os.RemoveAll(tmp)
		return cerr
	}
	if err := os.Rename(tmp, filepath.Join(dir, id)); err != nil {
		_ = os.RemoveAll(tmp)
		if _, serr := os.Stat(filepath.Join(dir, id)); serr == nil {
			return nil // another fetch won
		}
		return err
	}
	return nil
}

// nodeArch is a node's CPU architecture label, "" when unreadable.
func (a *Agent) nodeArch(ctx context.Context, node string) string {
	_, arch, _ := a.nodeArchOf(ctx, node)
	return arch
}

// nodeArchOf is a node and its CPU architecture label, or why that cannot
// be told.
func (a *Agent) nodeArchOf(ctx context.Context, node string) (*corev1.Node, string, error) {
	n, err := a.kubeClient.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
	if err != nil {
		return nil, "", err
	}
	arch := n.Labels[corev1.LabelArchStable]
	if arch == "" {
		return nil, "", fmt.Errorf("node %s has no %s label", node, corev1.LabelArchStable)
	}
	return n, arch, nil
}

// publishBuildInfo sets nvsnap_build_info to the bundle this agent serves
// ("unknown" before one is installed).
func (a *Agent) publishBuildInfo() { metrics.SetBuildInfo(a.currentBundle()) }
