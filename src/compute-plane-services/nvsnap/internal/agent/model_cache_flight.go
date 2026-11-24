// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	coordv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/modelcache"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/tracing"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

// Single flight (docs/design/model-cache-single-flight.md): per key, one
// pod in the cluster downloads, chosen by a Lease; every other pod of the
// key waits while its node's agent streams the download into its volume,
// from the downloading node or from a node already streaming (a relay).
// The pods learn their part from plan files in the key's directory.

// maxFollowAttempts bounds how often a waiting pod's stream starts over
// (a failed relay, a new holder) before the pod downloads.
const maxFollowAttempts = 20

// signalled reports whether f's helper said it downloads on its own.
func (c *nodeModelCache) signalled(f *flight) bool {
	_, err := os.Stat(filepath.Join(c.signalDir(f.vol), modelcache.SignalFile))
	return err == nil
}

// signalDir is the signal volume of the pod whose download volume is vol:
// a sibling emptyDir.
func (c *nodeModelCache) signalDir(vol string) string {
	return filepath.Join(filepath.Dir(vol), webhook.ModelCacheSignalVolume)
}

const (
	leasePrefix = "nvsnap-model-dl-"
	// feedsAnnotation, on the Lease, maps each streaming pod's identity to
	// the identity it streams from, so no source feeds more than maxFeeds.
	feedsAnnotation = "nvsnap.io/feeds"
	maxFeeds        = 3
	roleHolder      = "holder"
	roleFollower    = "follower"
	roleNone        = "none" // downloading on its own; left alone
)

func leaseName(key string) string { return leasePrefix + key }

// planned reports whether admission wrapped the pod's init for single
// flight: only such a pod's helper reads plans.
func planned(pod *corev1.Pod, d webhook.ModelCacheDownload) bool {
	for _, a := range pod.Spec.InitContainers[d.InitIndex].Command {
		if a == "--plan-wait" {
			return true
		}
		if a == "--" {
			return false
		}
	}
	return false
}

// flight is one single-flight pod on this node.
type flight struct {
	key, identity string
	uid           types.UID
	role          string
	vol           string
	src           *liveSource // what this node serves for the key, when this pod's volume is it
	cancel        context.CancelFunc
	finalized     bool // finalize started (it runs once, in the background)
	// tree and treeAt: once a stream is done, the fingerprint of the tree
	// the agent wrote and checked, and when; the node harvests that tree
	// as it would a download's.
	tree   string
	treeAt time.Time
}

// streamedTree returns the tree a done stream left in the pod's volume.
func (c *nodeModelCache) streamedTree(uid types.UID) (string, time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if f := c.flights[uid]; f != nil {
		return f.tree, f.treeAt
	}
	return "", time.Time{}
}

type leaseObs struct {
	holder, renew string
	at            time.Time
}

// flightPod handles a single-flight pod whose init has not finished. It
// returns false when the pod should take today's path instead (a peer
// has a complete copy, to fetch as today).
func (c *nodeModelCache) flightPod(ctx context.Context, pod *corev1.Pod, d webhook.ModelCacheDownload, st *corev1.ContainerStatus, have bool) bool {
	c.mu.Lock()
	f := c.flights[pod.UID]
	c.mu.Unlock()
	if f != nil {
		if f.role == roleHolder {
			c.holderStep(pod, f)
		}
		return true
	}
	if have || st == nil || st.State.Running == nil {
		// A node with a copy needs no plan: the helper seeds from it. A
		// pod whose init is not running yet takes no Lease: a holder that
		// cannot start would hold everyone.
		return true
	}
	if len(c.sources(ctx, d.Key)) > 0 {
		return false
	}
	vol, err := c.emptyDirWritePath(pod, d.Landing.VolumeName)
	if err != nil {
		return true
	}
	identity := c.node + "/" + string(pod.UID)
	log := c.log.WithFields(logrus.Fields{"key": d.Key, "pod": pod.Namespace + "/" + pod.Name})
	if c.holds(ctx, d.Key, identity) {
		// This agent restarted while its pod downloads: keep the Lease,
		// or it expires into a second download.
		log.Info("model cache: single flight: this pod still holds the download; keeping it")
		c.becomeHolder(ctx, d.Key, identity, pod.UID, vol, nil, log)
		return true
	}
	if !dirEmpty(vol) || c.downloadingOnItsOwn(pod) {
		// The helper gave up waiting for a plan and downloads.
		c.track(&flight{key: d.Key, identity: identity, uid: pod.UID, role: roleNone, vol: vol})
		return true
	}
	got, holder, err := c.acquire(ctx, d.Key, identity)
	if err != nil {
		log.WithError(err).Warn("model cache: single flight: the Lease is unavailable; the pod downloads on its own")
		return true
	}
	if got {
		c.becomeHolder(ctx, d.Key, identity, pod.UID, vol, nil, log)
		return true
	}
	c.becomeFollower(ctx, d.Key, identity, pod.UID, vol, holder, log)
	return true
}

func (c *nodeModelCache) track(f *flight) {
	c.mu.Lock()
	c.flights[f.uid] = f
	c.mu.Unlock()
}

func dirEmpty(dir string) bool {
	ents, err := os.ReadDir(dir)
	return err == nil && len(ents) == 0
}

// becomeHolder makes the pod the key's holder. With prev, the pod's
// flight until now, it does so only while prev is still the pod's flight
// (checked and replaced under one lock, so endFlights cannot remove it in
// between), and reports false otherwise.
func (c *nodeModelCache) becomeHolder(ctx context.Context, key, identity string, uid types.UID, vol string, prev *flight, log logrus.FieldLogger) bool {
	src := newLiveSource(vol, string(uid), c.timing)
	fctx, cancel := context.WithCancel(ctx)
	f := &flight{key: key, identity: identity, uid: uid, role: roleHolder, vol: vol, src: src, cancel: cancel}
	c.mu.Lock()
	if prev != nil && c.flights[uid] != prev {
		c.mu.Unlock()
		cancel()
		src.shutdown()
		return false
	}
	c.liveSources[key] = src
	c.flights[uid] = f
	c.mu.Unlock()
	if err := modelcache.WritePlan(c.store.KeyDir(key), string(uid), modelcache.PlanDownload); err != nil {
		log.WithError(err).Warn("model cache: single flight: writing the plan failed")
	}
	log.Info("model cache: single flight: this pod downloads; others stream from it")
	go c.renewLoop(fctx, f)
	return true
}

// holderStep finalizes once the helper signals its download is over.
func (c *nodeModelCache) holderStep(pod *corev1.Pod, f *flight) {
	if f.finalized {
		return
	}
	sig, err := c.emptyDirPath(pod, webhook.ModelCacheSignalVolume)
	if err != nil {
		return
	}
	b, err := os.ReadFile(filepath.Join(sig, modelcache.SignalFile))
	fp := strings.TrimSpace(string(b))
	if err != nil || fp == "" || fp == modelcache.SignalDownloading {
		return
	}
	f.finalized = true
	keyDir := c.store.KeyDir(f.key)
	log := c.log.WithFields(logrus.Fields{"key": f.key, "pod": pod.Namespace + "/" + pod.Name})
	// In the background: after an agent restart nothing is hashed yet, and
	// a tick must not wait on a pass over the model.
	go func() {
		start := time.Now()
		if err := f.src.finalize(fp); err != nil {
			log.WithError(err).Warn("model cache: single flight: no final manifest; streaming pods download")
			// Any value but the tree tells the helper to stop holding.
			_ = writeMarker(keyDir, modelcache.FinalFile(string(f.uid)), "failed")
			return
		}
		_ = writeMarker(keyDir, modelcache.FinalFile(string(f.uid)), fp)
		log.WithField("seconds", fmt.Sprintf("%.1f", time.Since(start).Seconds())).Info("model cache: single flight: final manifest served")
	}()
}

// downloadingOnItsOwn reports whether the pod's helper said it downloads:
// it gave up waiting for a plan.
func (c *nodeModelCache) downloadingOnItsOwn(pod *corev1.Pod) bool {
	sig, err := c.emptyDirPath(pod, webhook.ModelCacheSignalVolume)
	if err != nil {
		return false
	}
	_, err = os.Stat(filepath.Join(sig, modelcache.SignalFile))
	return err == nil
}

func writeMarker(dir, name, content string) error {
	if name == "" {
		return os.ErrInvalid
	}
	tmp := filepath.Join(dir, name+".tmp")
	if err := os.WriteFile(tmp, []byte(content+"\n"), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, name))
}

// renewLoop keeps the Lease while the holder downloads, and releases it
// once the node has published the key (others then fetch it as today),
// or the source failed. It stops on losing the Lease.
func (c *nodeModelCache) renewLoop(ctx context.Context, f *flight) {
	t := time.NewTicker(c.leaseDuration / 6)
	defer t.Stop()
	ok := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		// Released once others can fetch the copy: published and listed.
		if c.listed(ctx, f.key) || f.src.view().State == liveFailed {
			c.release(ctx, f.key, f.identity)
			return
		}
		err := c.renew(ctx, f.key, f.identity)
		switch {
		case err == nil:
			ok = time.Now()
		case errors.Is(err, errNotHolder):
			c.log.WithField("key", f.key).Warn("model cache: single flight: another pod holds the download now")
			return
		case time.Since(ok) > c.leaseDuration:
			c.log.WithError(err).WithField("key", f.key).Warn("model cache: single flight: could not renew the Lease")
			return
		}
	}
}

var errNotHolder = errors.New("model cache: the Lease is held by another")

// listed reports whether this node is a source of key: its copy is
// published and others can fetch it.
func (c *nodeModelCache) listed(ctx context.Context, key string) bool {
	if _, _, ok := c.store.Latest(key); !ok {
		return false
	}
	cm, err := c.kube.CoreV1().ConfigMaps(c.ns).Get(ctx, sourcesName(key), metav1.GetOptions{})
	return err == nil && cm.Data[c.node] != ""
}

// acquire takes the key's Lease for identity if it is free or expired,
// and returns who holds it otherwise. Expired is judged by how long this
// agent has seen the renew time stand still, as client-go's leader
// election does, never by comparing clocks across nodes.
func (c *nodeModelCache) acquire(ctx context.Context, key, identity string) (bool, string, error) {
	leases := c.kube.CoordinationV1().Leases(c.ns)
	dur := int32(c.leaseDuration / time.Second)
	if dur < 1 {
		dur = 1
	}
	now := metav1.NewMicroTime(time.Now())
	l, err := leases.Get(ctx, leaseName(key), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = leases.Create(ctx, &coordv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: leaseName(key), Namespace: c.ns,
			Labels: map[string]string{"app.kubernetes.io/component": modelCacheComponent}},
			Spec: coordv1.LeaseSpec{HolderIdentity: &identity, LeaseDurationSeconds: &dur, AcquireTime: &now, RenewTime: &now}}, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			if l, gerr := leases.Get(ctx, leaseName(key), metav1.GetOptions{}); gerr == nil && l.Spec.HolderIdentity != nil {
				return false, *l.Spec.HolderIdentity, nil
			}
			return false, "", nil
		}
		return err == nil, identity, err
	}
	if err != nil {
		return false, "", err
	}
	holder := ""
	if l.Spec.HolderIdentity != nil {
		holder = *l.Spec.HolderIdentity
	}
	if holder == identity {
		return true, holder, nil
	}
	renew := ""
	if l.Spec.RenewTime != nil {
		renew = l.Spec.RenewTime.String()
	}
	c.mu.Lock()
	o, seen := c.leaseSeen[key]
	if !seen || o.holder != holder || o.renew != renew {
		c.leaseSeen[key] = leaseObs{holder: holder, renew: renew, at: time.Now()}
		c.mu.Unlock()
		return false, holder, nil
	}
	c.mu.Unlock()
	if time.Since(o.at) <= c.leaseDuration {
		return false, holder, nil
	}
	l.Spec.HolderIdentity, l.Spec.AcquireTime, l.Spec.RenewTime = &identity, &now, &now
	l.Annotations = nil // the old holder's feeds
	if _, err := leases.Update(ctx, l, metav1.UpdateOptions{}); err != nil {
		if apierrors.IsConflict(err) {
			return false, holder, nil
		}
		return false, holder, err
	}
	return true, identity, nil
}

// holds reports whether identity holds the key's Lease.
func (c *nodeModelCache) holds(ctx context.Context, key, identity string) bool {
	l, err := c.kube.CoordinationV1().Leases(c.ns).Get(ctx, leaseName(key), metav1.GetOptions{})
	return err == nil && l.Spec.HolderIdentity != nil && *l.Spec.HolderIdentity == identity
}

// renew sets the Lease's renew time. A conflict (streaming pods record
// their sources on it) is retried; only another holder ends it.
func (c *nodeModelCache) renew(ctx context.Context, key, identity string) error {
	leases := c.kube.CoordinationV1().Leases(c.ns)
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		var l *coordv1.Lease
		if l, err = leases.Get(ctx, leaseName(key), metav1.GetOptions{}); err != nil {
			return err
		}
		if l.Spec.HolderIdentity == nil || *l.Spec.HolderIdentity != identity {
			return errNotHolder
		}
		now := metav1.NewMicroTime(time.Now())
		l.Spec.RenewTime = &now
		if _, err = leases.Update(ctx, l, metav1.UpdateOptions{}); !apierrors.IsConflict(err) {
			return err
		}
	}
	return err
}

// release deletes the key's Lease if identity still holds it, on that
// version only: a holder back from a partition cannot delete a
// successor's Lease.
func (c *nodeModelCache) release(ctx context.Context, key, identity string) {
	leases := c.kube.CoordinationV1().Leases(c.ns)
	l, err := leases.Get(ctx, leaseName(key), metav1.GetOptions{})
	if err != nil || l.Spec.HolderIdentity == nil || *l.Spec.HolderIdentity != identity {
		return
	}
	rv := l.ResourceVersion
	_ = leases.Delete(ctx, l.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{ResourceVersion: &rv}})
}

// pickSourceBackoff is the step of pickSource's backoff after a failed
// Lease read: 100, 200, 300, 400 ms, about 1 s in all.
const pickSourceBackoff = 100 * time.Millisecond

// pickSource chooses what identity streams from: the holder, or a pod
// already streaming, whichever feeds the fewest (at most maxFeeds), and
// records the choice on the Lease. With no room anywhere, the holder.
// The cap is best effort: when the Lease cannot be read or written after
// the retries, or names another holder, the pod streams from the holder
// unrecorded, since one more stream costs less than another download.
func (c *nodeModelCache) pickSource(ctx context.Context, key, identity, holder string, bad map[string]bool) string {
	leases := c.kube.CoordinationV1().Leases(c.ns)
	for attempt := 0; attempt < 5; attempt++ {
		l, err := leases.Get(ctx, leaseName(key), metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) && ctx.Err() == nil {
			// A transient read error is retried as a write conflict is:
			// a pod that streams unrecorded does not count against its
			// source's maxFeeds. The bound and backoff are small, not
			// measured: past them the pod streams from the holder anyway.
			select {
			case <-ctx.Done():
				return holder
			case <-time.After(time.Duration(attempt+1) * pickSourceBackoff):
			}
			continue
		}
		if err != nil || l.Spec.HolderIdentity == nil || *l.Spec.HolderIdentity != holder {
			return holder
		}
		feeds := map[string]string{}
		if s := l.Annotations[feedsAnnotation]; s != "" {
			_ = json.Unmarshal([]byte(s), &feeds)
		}
		delete(feeds, identity)
		// A source that failed this pod is no relay: its entry goes, so
		// no pod picks it again.
		for b := range bad {
			if b != holder {
				delete(feeds, b)
			}
		}
		count := map[string]int{holder: 0}
		for consumer, src := range feeds {
			count[src]++
			if _, ok := count[consumer]; !ok {
				count[consumer] = 0
			}
		}
		var cands []string
		for id := range count {
			if id != identity && !bad[id] && !feedsFrom(feeds, id, identity) {
				cands = append(cands, id)
			}
		}
		// Breadth first: the shallowest source with room, then the one
		// feeding fewest. Fewest alone builds a chain (a pod that just
		// joined feeds none), and each hop adds its lag.
		depth := func(id string) int { return feedDepth(feeds, id, holder) }
		sort.Slice(cands, func(i, j int) bool {
			ri, rj := count[cands[i]] < maxFeeds, count[cands[j]] < maxFeeds
			if ri != rj {
				return ri
			}
			if di, dj := depth(cands[i]), depth(cands[j]); di != dj {
				return di < dj
			}
			if ci, cj := count[cands[i]], count[cands[j]]; ci != cj {
				return ci < cj
			}
			return cands[i] < cands[j]
		})
		pick := holder
		if len(cands) > 0 && count[cands[0]] < maxFeeds {
			pick = cands[0]
		}
		feeds[identity] = pick
		b, _ := json.Marshal(feeds)
		if l.Annotations == nil {
			l.Annotations = map[string]string{}
		}
		l.Annotations[feedsAnnotation] = string(b)
		if _, err := leases.Update(ctx, l, metav1.UpdateOptions{}); apierrors.IsConflict(err) {
			continue
		}
		return pick
	}
	return holder
}

// feedDepth is how many hops id is from the holder (0 for the holder);
// a broken or cyclic path counts as deepest.
func feedDepth(feeds map[string]string, id, holder string) int {
	for d := 0; d <= len(feeds); d++ {
		if id == holder {
			return d
		}
		src, ok := feeds[id]
		if !ok {
			break
		}
		id = src
	}
	return len(feeds) + 1
}

// feedsFrom reports whether id streams, directly or through relays, from
// ancestor: picking id as ancestor's source would make a cycle.
func feedsFrom(feeds map[string]string, id, ancestor string) bool {
	for hops := 0; hops <= len(feeds); hops++ {
		src, ok := feeds[id]
		if !ok {
			return false
		}
		if src == ancestor {
			return true
		}
		id = src
	}
	return true // a cycle already: keep out of it
}

func (c *nodeModelCache) clientFor(key, source string) liveClient {
	node, uid := source, ""
	if i := strings.IndexByte(source, '/'); i >= 0 {
		node, uid = source[:i], source[i+1:]
	}
	if node == c.node {
		// Only the source pod's own volume: never this node's other one.
		c.mu.Lock()
		src := c.liveSources[key]
		c.mu.Unlock()
		if src != nil && src.uid == uid {
			return localClient{src}
		}
	}
	return &httpLiveClient{c: c, node: node, uid: uid, key: key}
}

func (c *nodeModelCache) becomeFollower(ctx context.Context, key, identity string, uid types.UID, vol, holder string, log logrus.FieldLogger) {
	keyDir := c.store.KeyDir(key)
	stopHB := c.heartbeatFile(key, modelcache.StreamingFileOf(string(uid)))
	if err := modelcache.WritePlan(keyDir, string(uid), modelcache.PlanWait); err != nil {
		stopHB()
		log.WithError(err).Warn("model cache: single flight: writing the plan failed")
		return
	}
	fctx, cancel := context.WithCancel(ctx)
	f := &flight{key: key, identity: identity, uid: uid, role: roleFollower, vol: vol, cancel: cancel}
	c.mu.Lock()
	if c.liveSources[key] == nil {
		// This pod's volume is a relay for later streaming pods.
		f.src = newLiveSource(vol, string(uid), c.timing)
		c.liveSources[key] = f.src
	}
	c.flights[uid] = f
	c.mu.Unlock()
	go c.follow(fctx, f, holder, stopHB, log)
}

// follow streams into the pod's volume until it is done, then marks it.
// When the source fails it takes the Lease if it can (the pod downloads,
// into its emptied volume), else streams from the new holder, keeping
// what it copied: only the final manifest decides what stays. When the
// holder published a copy, it copies that instead.
func (c *nodeModelCache) follow(ctx context.Context, f *flight, holder string, stopHB func(), log logrus.FieldLogger) {
	defer stopHB()
	// The helper may give up on its plan and download: never write then.
	s := &streamer{dst: f.vol, timing: c.timing, mayWrite: func() bool { return !c.signalled(f) }}
	deadline := time.Now().Add(c.followMax)
	bad := map[string]bool{}
	done := false
	defer func() {
		if !done && f.src != nil {
			// A stream that ended without the model is no relay.
			f.src.fail()
			c.mu.Lock()
			if c.liveSources[f.key] == f.src {
				delete(c.liveSources, f.key)
			}
			c.mu.Unlock()
		}
	}()
	for attempt := 0; ; attempt++ {
		if attempt >= maxFollowAttempts || time.Now().After(deadline) {
			log.Warn("model cache: single flight: no copy in time; the pod downloads")
			return
		}
		source := c.pickSource(ctx, f.key, f.identity, holder, bad)
		s.client = c.clientFor(f.key, source)
		sctx, cancel := context.WithDeadline(ctx, deadline)
		log.WithField("source", source).Info("model cache: single flight: streaming from another node")
		start := time.Now()
		err := s.run(sctx)
		cancel()
		if err == nil {
			done = true
			c.streamed(f, s, start, log)
			return
		}
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, errStall) || errors.Is(err, errHelperDownloads) {
			log.WithError(err).Warn("model cache: single flight: the pod downloads")
			return
		}
		if source != holder {
			// A relay failed: pick another, the holder at worst.
			bad[source] = true
			continue
		}
		if peers := c.sources(ctx, f.key); len(peers) > 0 {
			// The holder published a copy, and its stream is gone: do not
			// download it again, nor wait on the holder. Copy the published
			// copy from a node that lists it, checked against its manifest,
			// as a fetch from a peer does. With none readable, the pod
			// downloads.
			start = time.Now()
			if c.copyPublished(ctx, s, f.key, peers, deadline, log) {
				done = true
				c.streamed(f, s, start, log)
				return
			}
			log.Warn("model cache: single flight: no published copy could be read; the pod downloads")
			return
		}
		got, h, aerr := c.acquire(ctx, f.key, f.identity)
		if aerr != nil {
			log.WithError(aerr).Warn("model cache: single flight: the Lease is unavailable; the pod downloads")
			return
		}
		if got {
			c.promote(ctx, f, log)
			return
		}
		if h == "" {
			h = holder
		}
		holder = h
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.timing.poll):
		}
	}
}

// streamed records a done stream into f's volume and tells the helper.
func (c *nodeModelCache) streamed(f *flight, s *streamer, start time.Time, log logrus.FieldLogger) {
	if f.src != nil {
		f.src.setDone(*s.final)
	}
	if ents, serr := modelcache.ScanLive(f.vol, modelCacheMaxEntries); serr == nil {
		c.mu.Lock()
		f.tree, f.treeAt = modelcache.Fingerprint(modelcache.Entries(ents)), time.Now()
		c.mu.Unlock()
	}
	_ = writeMarker(c.store.KeyDir(f.key), modelcache.DoneFile(string(f.uid)), "")
	log.WithField("seconds", fmt.Sprintf("%.1f", time.Since(start).Seconds())).Info("model cache: single flight: streamed; the pod starts")
}

// copyPublished ends s from a published copy of key on one of peers,
// trying each in turn until the deadline. It reports whether s is done.
func (c *nodeModelCache) copyPublished(ctx context.Context, s *streamer, key string, peers []string, deadline time.Time, log logrus.FieldLogger) bool {
	for _, node := range peers {
		s.client = &publishedClient{c: c, node: node, key: key}
		sctx, cancel := context.WithDeadline(ctx, deadline)
		log.WithField("source", node).Info("model cache: single flight: copying the published copy")
		err := s.run(sctx)
		cancel()
		if err == nil {
			return true
		}
		if ctx.Err() != nil || time.Now().After(deadline) || errors.Is(err, errHelperDownloads) {
			return false
		}
		log.WithError(err).WithField("source", node).Warn("model cache: single flight: copying the published copy failed")
	}
	return false
}

// promote makes f's pod the holder after it took the Lease: it stops the
// pod's relay, empties its volume, and only then writes the download plan.
// When the volume cannot be emptied, the pod's helper downloads on its own,
// or f's flight ended meanwhile (its pod left the node), it releases the
// Lease instead: nobody would renew or serve it, and the other pods would
// wait out its expiry.
//
// The helper and promote never both touch the volume: promote runs inside
// follow, so the pod's streaming heartbeat is fresh throughout, and it
// reads the helper's signal only after that heartbeat started. The helper
// writes its signal before it reads the heartbeat, and while the heartbeat
// is fresh it waits, then empties the volume, before it downloads. So
// either promote sees the signal and leaves the volume, or the helper
// starts only after promote has returned.
func (c *nodeModelCache) promote(ctx context.Context, f *flight, log logrus.FieldLogger) {
	bg := context.WithoutCancel(ctx)
	if f.src != nil {
		f.src.fail()
		c.mu.Lock()
		if c.liveSources[f.key] == f.src {
			delete(c.liveSources, f.key)
		}
		c.mu.Unlock()
	}
	if c.signalled(f) {
		log.Info("model cache: single flight: the pod's helper downloads on its own; its volume is left to it")
		c.release(bg, f.key, f.identity)
		return
	}
	if err := emptyFlightVolume(f.vol); err != nil {
		log.WithError(err).Warn("model cache: single flight: emptying the volume failed; the pod downloads")
		c.release(bg, f.key, f.identity)
		return
	}
	if ctx.Err() != nil || !c.becomeHolder(bg, f.key, f.identity, f.uid, f.vol, f, log) {
		log.Info("model cache: single flight: the pod left before it could take the download over")
		c.release(bg, f.key, f.identity)
	}
}

// emptyFlightVolume is emptyVolume for a takeover; tests stub it.
var emptyFlightVolume = emptyVolume

func emptyVolume(dir string) error {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// endFlights stops the flights of pods no longer on the node: a holder's
// source fails (its streaming pods move on) and its Lease is released.
func (c *nodeModelCache) endFlights(ctx context.Context, live map[types.UID]bool) {
	c.mu.Lock()
	var gone []*flight
	for uid, f := range c.flights {
		if !live[uid] {
			gone = append(gone, f)
			delete(c.flights, uid)
		}
	}
	c.mu.Unlock()
	for _, f := range gone {
		if f.cancel != nil {
			f.cancel()
		}
		if f.src != nil {
			f.src.fail()
			c.mu.Lock()
			if c.liveSources[f.key] == f.src {
				delete(c.liveSources, f.key)
			}
			c.mu.Unlock()
		}
		if f.role == roleHolder {
			c.release(ctx, f.key, f.identity)
		}
		keyDir := c.store.KeyDir(f.key)
		modelcache.RemovePlan(keyDir, string(f.uid))
		for _, name := range []string{modelcache.FinalFile(string(f.uid)), modelcache.DoneFile(string(f.uid))} {
			if name != "" {
				_ = os.Remove(filepath.Join(keyDir, name))
			}
		}
	}
}

// removeStalePlans removes the plans, and their markers, of pods no longer
// on the node. endFlights does so only for the flights this agent knows,
// and an agent that restarted knows none: a plan left from before would
// pin its key against eviction for good. A plan of a pod still on the
// node stays, so its waiting init keeps the key.
func (c *nodeModelCache) removeStalePlans(live map[types.UID]bool) {
	for _, k := range c.store.Keys() {
		keyDir := c.store.KeyDir(k)
		for _, uid := range modelcache.Plans(keyDir) {
			if live[types.UID(uid)] {
				continue
			}
			modelcache.RemovePlan(keyDir, uid)
			for _, name := range []string{modelcache.FinalFile(uid), modelcache.DoneFile(uid)} {
				if name != "" {
					_ = os.Remove(filepath.Join(keyDir, name))
				}
			}
		}
	}
}

// Live routes. Behind the agent's token guard like every agent-to-agent
// route, under a prefix the cache's own routes do not match.

func (c *nodeModelCache) addLiveRoutes(r *mux.Router) {
	r.HandleFunc("/v1/model-cache-live/{key}", c.liveViewHandler).Methods("GET")
	r.HandleFunc("/v1/model-cache-live/{key}/file", c.liveFileHandler).Methods("GET")
}

func (c *nodeModelCache) liveSourceFor(w http.ResponseWriter, r *http.Request) *liveSource {
	key := mux.Vars(r)["key"]
	if !modelcache.ValidKey(key) {
		http.Error(w, "bad key", http.StatusBadRequest)
		return nil
	}
	c.mu.Lock()
	src := c.liveSources[key]
	c.mu.Unlock()
	if src == nil {
		http.NotFound(w, r)
		return nil
	}
	// The reader names the pod it chose; this node's source may be another
	// pod's volume (the node's first waiting pod is its only relay).
	if uid := r.URL.Query().Get("uid"); uid != "" && uid != src.uid {
		http.Error(w, "not this pod's source", http.StatusGone)
		return nil
	}
	return src
}

func (c *nodeModelCache) liveViewHandler(w http.ResponseWriter, r *http.Request) {
	if src := c.liveSourceFor(w, r); src != nil {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(src.view())
	}
}

func (c *nodeModelCache) liveFileHandler(w http.ResponseWriter, r *http.Request) {
	src := c.liveSourceFor(w, r)
	if src == nil {
		return
	}
	f, err := src.open(r.Context(), r.URL.Query().Get("path"))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = io.Copy(w, f)
}

// httpLiveClient reads a source on another node: the volume of the pod
// uid, which every request names.
type httpLiveClient struct {
	c              *nodeModelCache
	node, uid, key string
	fails          int
}

func (h *httpLiveClient) base(ctx context.Context) (string, error) {
	b, err := h.c.peerURL(ctx, h.node)
	return b + "/v1/model-cache-live/" + h.key, err
}

// query names the source's pod; the peer refuses a request for another.
func (h *httpLiveClient) query() string {
	if h.uid == "" {
		return ""
	}
	return "uid=" + url.QueryEscape(h.uid)
}

func (h *httpLiveClient) View(ctx context.Context) (liveView, error) {
	ctx, span := tracing.Tracer().Start(ctx, "model_cache.live_view")
	defer span.End()
	span.SetAttributes(attribute.String("nvsnap.model_cache.key", h.key), attribute.String("nvsnap.peer", h.node))
	var v liveView
	b, err := h.base(ctx)
	if err == nil {
		if q := h.query(); q != "" {
			b += "?" + q
		}
		err = h.c.getJSON(ctx, b, &v)
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		var se *peerStatusError
		if errors.As(err, &se) && se.code == http.StatusGone {
			// The node serves another pod's volume: no source here.
			return liveView{State: liveFailed}, nil
		}
		// A source that is gone is a failed one, after a few polls.
		if h.fails++; h.fails >= 5 {
			return liveView{State: liveFailed}, nil
		}
		return v, err
	}
	h.fails = 0
	return v, nil
}

func (h *httpLiveClient) Open(ctx context.Context, rel string) (io.ReadCloser, error) {
	ctx, span := tracing.Tracer().Start(ctx, "model_cache.live_file")
	span.SetAttributes(attribute.String("nvsnap.model_cache.key", h.key), attribute.String("nvsnap.peer", h.node))
	fail := func(err error) (io.ReadCloser, error) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
		return nil, err
	}
	b, err := h.base(ctx)
	if err != nil {
		return fail(err)
	}
	u := b + "/file?path=" + url.QueryEscape(rel)
	if q := h.query(); q != "" {
		u += "&" + q
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return fail(err)
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	resp, err := h.c.client.Do(req)
	if err != nil {
		return fail(err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return fail(fmt.Errorf("source answered %s for %s", resp.Status, rel))
	}
	// The span covers the transfer: it ends when the body is closed.
	return spanBody{ReadCloser: resp.Body, end: span.End}, nil
}

type spanBody struct {
	io.ReadCloser
	end func(...trace.SpanEndOption)
}

func (b spanBody) Close() error {
	err := b.ReadCloser.Close()
	b.end()
	return err
}

// publishedClient reads a complete copy a node published, through the
// routes a fetch from a peer uses. It is done from the start: its view is
// the copy's manifest, and a streamer copies every file it does not hold
// and checks it against that manifest.
type publishedClient struct {
	c         *nodeModelCache
	node, key string
	gen       string
}

func (p *publishedClient) View(ctx context.Context) (liveView, error) {
	base, err := p.c.peerURL(ctx, p.node)
	var pm modelPeerManifest
	if err == nil {
		err = p.c.getJSON(ctx, base+"/v1/model-cache/"+p.key, &pm)
	}
	if err == nil && (!strings.HasPrefix(pm.Gen, modelcache.GenPrefix) || !validPathSegmentOK(pm.Gen)) {
		err = fmt.Errorf("peer named generation %q", pm.Gen)
	}
	if err != nil {
		// A copy that cannot be read now is not waited for: the next node.
		p.c.log.WithError(err).WithField("peer", p.node).Warn("model cache: single flight: the published copy is unavailable")
		return liveView{State: liveFailed}, nil
	}
	p.gen = pm.Gen
	m := pm.Manifest
	return liveView{State: liveDone, Final: &m}, nil
}

func (p *publishedClient) Open(ctx context.Context, rel string) (io.ReadCloser, error) {
	if p.gen == "" {
		return nil, errors.New("model cache: no published generation chosen")
	}
	base, err := p.c.peerURL(ctx, p.node)
	if err != nil {
		return nil, err
	}
	u := base + "/v1/model-cache/" + p.key + "/" + p.gen + "/file?path=" + url.QueryEscape(rel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	resp, err := p.c.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("peer answered %s for %s", resp.Status, rel)
	}
	return resp.Body, nil
}
