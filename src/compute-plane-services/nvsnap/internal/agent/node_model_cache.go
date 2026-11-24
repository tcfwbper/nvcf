// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/metrics"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/modelcache"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/tracing"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

// The agent's half of the node-local model cache
// (docs/design/node-model-cache.md). Admission wraps a cold pod's model
// download init (webhook/node_model_cache.go); the agent on the pod's node
//
//   - fills the node's cache from a peer node that has the key, while the
//     init waits on its heartbeat, so the init seeds instead of downloading;
//   - harvests a pod's download volume into the cache after the init
//     downloaded it, so the next cold pod here, or on a node fetching from
//     here, does not;
//   - serves its generations to peers, and lists itself as a source of a
//     key in a ConfigMap peers read;
//   - keeps the cache within its disk budget, evicting the least recently
//     used key no pod on the node waits on.
//
// Every key is computed from the pod spec and the namespace's tenant
// (webhook.ResolveModelCacheDownload), never taken from a label the pod
// carries: a pod cannot have its files harvested under another tenant's
// or another model's key.
const (
	modelCacheSourcesPrefix = "nvsnap-model-cache-"
	modelCacheComponent     = "model-cache"
	modelCacheTick          = 10 * time.Second
	modelCacheHeartbeat     = 5 * time.Second
	modelCacheRetry         = 10 * time.Minute
	modelCacheReconcile     = 5 * time.Minute
	// modelCacheMaxEntries bounds a tree's entries: past it, a tree of
	// many tiny files costs more in inodes than its bytes say.
	modelCacheMaxEntries = 500000
	// modelCacheFloorPercent of the filesystem stays free whatever the
	// budget allows.
	modelCacheFloorPercent = 10
)

// NodeModelCacheConfig configures the cache on this node.
type NodeModelCacheConfig struct {
	Enabled bool
	// Root is the cache directory on the host, mounted at the same path.
	Root string
	// MaxPercent of Root's filesystem the cache may use.
	MaxPercent int
	// Wait bounds a download init's wait for a fetch in progress, seconds.
	Wait int
	// FetchTimeout bounds one fetch from one peer, all files included.
	FetchTimeout time.Duration
	// SingleFlight: one pod per key downloads, and the others stream its
	// download (docs/design/model-cache-single-flight.md).
	SingleFlight bool
	// PodsRoot is the kubelet pods directory, mounted read-write: single
	// flight writes into waiting pods' volumes. The host root the agent
	// reads pods through is read-only.
	PodsRoot string
}

// nodeModelCache is the agent's cache controller.
type nodeModelCache struct {
	store        *modelcache.Store
	kube         kubernetes.Interface
	node, ns     string
	podsRoot     string // kubelet pods directory, as the agent sees it
	tenants      webhook.TenantScoper
	main         int
	peerURL      func(ctx context.Context, node string) (string, error)
	client       *http.Client
	maxPercent   int
	fetchTimeout time.Duration
	log          logrus.FieldLogger
	// statfs reports (total, free) bytes of the cache filesystem.
	statfs func(path string) (int64, int64, error)

	mu        sync.Mutex
	busy      map[string]int       // key -> fills, harvests and transfers in flight
	evicting  map[string]bool      // keys an eviction holds: nothing new begins on them
	failed    map[string]time.Time // key -> last fill that found no usable peer
	harvested map[types.UID]bool   // pods whose volume was looked at
	counted   map[types.UID]bool   // pods whose init outcome was counted
	reserved  int64                // bytes promised to attempts in flight
	inUse     map[string]bool      // keys a pending or running pod on the node uses

	// Single flight (model_cache_flight.go).
	singleFlight  bool
	podsWrite     string // kubelet pods directory, writable
	followMax     time.Duration
	timing        streamTiming
	leaseDuration time.Duration
	flights       map[types.UID]*flight
	liveSources   map[string]*liveSource // key -> the volume this node serves for it
	leaseSeen     map[string]leaseObs
}

func newNodeModelCache(cfg NodeModelCacheConfig, kube kubernetes.Interface, node, ns string, tenants webhook.TenantScoper, main int, peerURL func(context.Context, string) (string, error), log logrus.FieldLogger) *nodeModelCache {
	pct := cfg.MaxPercent
	if pct <= 0 || pct > 90 {
		pct = 30
	}
	to := cfg.FetchTimeout
	if to <= 0 {
		to = 30 * time.Minute
	}
	return &nodeModelCache{
		store: &modelcache.Store{Root: cfg.Root}, kube: kube, node: node, ns: ns,
		podsRoot: cachedirPodsRoot, tenants: tenants, main: main, peerURL: peerURL,
		client: peerHTTPClient, maxPercent: pct, fetchTimeout: to, log: log, statfs: statfsBytes,
		busy: map[string]int{}, evicting: map[string]bool{}, failed: map[string]time.Time{}, harvested: map[types.UID]bool{}, counted: map[types.UID]bool{}, inUse: map[string]bool{},
		singleFlight: cfg.SingleFlight, podsWrite: cfg.PodsRoot, followMax: 5 * time.Hour,
		timing: defaultStreamTiming, leaseDuration: 30 * time.Second,
		flights: map[types.UID]*flight{}, liveSources: map[string]*liveSource{}, leaseSeen: map[string]leaseObs{},
	}
}

func statfsBytes(p string) (int64, int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(p, &st); err != nil {
		return 0, 0, err
	}
	return int64(st.Blocks) * st.Bsize, int64(st.Bavail) * st.Bsize, nil
}

// run starts the controller. No attempt is live when the agent starts, so
// every attempt directory left is abandoned.
func (c *nodeModelCache) run(ctx context.Context) {
	if err := os.MkdirAll(c.store.Root, 0o755); err != nil {
		c.log.WithError(err).Error("model cache: cannot create the cache root; the cache is off on this node")
		return
	}
	if err := c.store.CleanAttempts(); err != nil {
		c.log.WithError(err).Warn("model cache: cleaning abandoned attempts failed")
	}
	c.reconcileSources(ctx)
	t := time.NewTicker(modelCacheTick)
	defer t.Stop()
	last := time.Now()
	for {
		c.tick(ctx)
		// A registration that failed (API down, too many conflicts) is
		// made good here, and a node never stays listed for a key it lost.
		if time.Since(last) >= modelCacheReconcile {
			c.reconcileSources(ctx)
			last = time.Now()
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// begin marks key busy unless it already is; end undoes it. Nothing
// begins on a key being evicted, not even a reader: its files are going.
func (c *nodeModelCache) begin(key string, exclusive bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.evicting[key] || (exclusive && c.busy[key] > 0) {
		return false
	}
	c.busy[key]++
	return true
}

// endEvict ends evict's hold on key.
func (c *nodeModelCache) endEvict(key string) {
	c.mu.Lock()
	delete(c.evicting, key)
	c.mu.Unlock()
	c.end(key)
}

func (c *nodeModelCache) end(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.busy[key]--; c.busy[key] <= 0 {
		delete(c.busy, key)
	}
}

// tick looks at the pods on this node that use the cache.
func (c *nodeModelCache) tick(ctx context.Context) {
	pods, err := c.kube.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		LabelSelector: webhook.ModelCacheKeyLabel,
		FieldSelector: "spec.nodeName=" + c.node,
	})
	if err != nil {
		c.log.WithError(err).Warn("model cache: listing pods failed")
		return
	}
	inUse := map[string]bool{}
	seen := map[types.UID]bool{}
	live := map[types.UID]bool{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Spec.NodeName != c.node {
			continue
		}
		seen[pod.UID] = true
		if pod.Status.Phase != corev1.PodPending && pod.Status.Phase != corev1.PodRunning {
			continue
		}
		live[pod.UID] = true
		d, ok := webhook.ResolveModelCacheDownload(ctx, c.tenants, pod, c.main)
		if !ok || !d.Wrapped {
			continue
		}
		if d.Key != pod.Labels[webhook.ModelCacheKeyLabel] {
			c.log.WithFields(logrus.Fields{"pod": pod.Namespace + "/" + pod.Name, "label": pod.Labels[webhook.ModelCacheKeyLabel]}).
				Warn("model cache: the pod's key label is not its download's key; ignoring the pod")
			continue
		}
		inUse[d.Key] = true
		c.handlePod(ctx, pod, d)
	}
	c.mu.Lock()
	c.inUse = inUse
	for uid := range c.harvested {
		if !seen[uid] {
			delete(c.harvested, uid)
		}
	}
	for uid := range c.counted {
		if !seen[uid] {
			delete(c.counted, uid)
		}
	}
	c.mu.Unlock()
	for k := range inUse {
		if _, _, ok := c.store.Latest(k); ok {
			c.store.Touch(k)
		}
	}
	c.endFlights(ctx, live)
	c.removeStalePlans(live)
	c.evict(ctx, 0)
}

func (c *nodeModelCache) handlePod(ctx context.Context, pod *corev1.Pod, d webhook.ModelCacheDownload) {
	init := pod.Spec.InitContainers[d.InitIndex].Name
	var st *corev1.ContainerStatus
	for i := range pod.Status.InitContainerStatuses {
		if pod.Status.InitContainerStatuses[i].Name == init {
			st = &pod.Status.InitContainerStatuses[i]
		}
	}
	_, _, have := c.store.Latest(d.Key)
	if st != nil && st.State.Terminated != nil {
		c.countInit(pod.UID, st.State.Terminated)
	}
	switch {
	case st == nil || st.State.Terminated == nil:
		if c.singleFlight && planned(pod, d) && c.flightPod(ctx, pod, d, st, have) {
			return
		}
		// The init is yet to run, or running: bring the key here from a
		// peer, while it waits.
		if !have {
			c.mu.Lock()
			last, failed := c.failed[d.Key]
			c.mu.Unlock()
			if (!failed || time.Since(last) > modelCacheRetry) && c.begin(d.Key, true) {
				go func() {
					defer c.end(d.Key)
					c.fill(ctx, d.Key)
				}()
			}
		}
	case st.State.Terminated.ExitCode == 0:
		c.mu.Lock()
		done := c.harvested[pod.UID]
		c.harvested[pod.UID] = true
		c.mu.Unlock()
		if done || have {
			return
		}
		if !c.begin(d.Key, true) {
			c.mu.Lock()
			delete(c.harvested, pod.UID) // look again once the key is free
			c.mu.Unlock()
			return
		}
		tree, finished := modelcache.DownloadedTree(st.State.Terminated.Message), st.State.Terminated.FinishedAt.Time
		if tree == "" {
			// A streamed volume: the agent wrote and checked it, and knows
			// its tree.
			if t, at := c.streamedTree(pod.UID); t != "" {
				tree, finished = t, at
			}
		}
		go func() {
			defer c.end(d.Key)
			c.harvest(ctx, pod, d, finished, tree)
		}()
	}
}

// countInit counts a wrapped init's outcome once per pod.
func (c *nodeModelCache) countInit(uid types.UID, t *corev1.ContainerStateTerminated) {
	c.mu.Lock()
	done := c.counted[uid]
	c.counted[uid] = true
	c.mu.Unlock()
	if done {
		return
	}
	result := "failed"
	switch {
	case t.ExitCode != 0:
	case strings.Contains(t.Message, modelcache.InitSeeded):
		result = "seeded"
	default:
		result = "downloaded"
	}
	metrics.CountModelCacheInit(result)
}

// heartbeat keeps the key's fetch heartbeat fresh until stop is closed,
// then removes it.
func (c *nodeModelCache) heartbeat(key string) (stop func()) {
	return c.heartbeatFile(key, modelcache.IncomingFile)
}

// heartbeatFile keeps the key's file name fresh until stop is called,
// then removes it.
func (c *nodeModelCache) heartbeatFile(key, name string) (stop func()) {
	p := filepath.Join(c.store.KeyDir(key), name)
	_ = os.MkdirAll(c.store.KeyDir(key), 0o755)
	_ = os.WriteFile(p, nil, 0o644)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(modelCacheHeartbeat)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				now := time.Now()
				_ = os.Chtimes(p, now, now)
			}
		}
	}()
	return func() {
		close(done)
		wg.Wait()
		_ = os.Remove(p)
	}
}

// sourcesName is the ConfigMap listing the nodes that have key.
func sourcesName(key string) string { return modelCacheSourcesPrefix + key }

// sources lists the other nodes that have key, in random order.
func (c *nodeModelCache) sources(ctx context.Context, key string) []string {
	ctx, span := tracing.Tracer().Start(ctx, "model_cache.sources")
	defer span.End()
	span.SetAttributes(attribute.String("nvsnap.model_cache.key", key))
	cm, err := c.kube.CoreV1().ConfigMaps(c.ns).Get(ctx, sourcesName(key), metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return nil
	}
	var out []string
	for node := range cm.Data {
		if node != c.node {
			out = append(out, node)
		}
	}
	rand.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// register lists this node as a source of key's generation gen.
func (c *nodeModelCache) register(ctx context.Context, key, gen string) error {
	return c.updateSources(ctx, key, func(data map[string]string) bool {
		if data[c.node] == gen {
			return false
		}
		data[c.node] = gen
		return true
	})
}

// deregister removes this node from key's sources.
func (c *nodeModelCache) deregister(ctx context.Context, key string) error {
	return c.updateSources(ctx, key, func(data map[string]string) bool {
		if _, ok := data[c.node]; !ok {
			return false
		}
		delete(data, c.node)
		return true
	})
}

func (c *nodeModelCache) updateSources(ctx context.Context, key string, mutate func(map[string]string) bool) error {
	cms := c.kube.CoreV1().ConfigMaps(c.ns)
	for attempt := 0; attempt < 5; attempt++ {
		cm, err := cms.Get(ctx, sourcesName(key), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			data := map[string]string{}
			if !mutate(data) {
				return nil
			}
			cm = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: sourcesName(key), Namespace: c.ns,
				Labels: map[string]string{"app.kubernetes.io/component": modelCacheComponent}}, Data: data}
			if _, err = cms.Create(ctx, cm, metav1.CreateOptions{}); apierrors.IsAlreadyExists(err) {
				continue
			}
			return err
		}
		if err != nil {
			return err
		}
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		if !mutate(cm.Data) {
			return nil
		}
		if len(cm.Data) == 0 {
			// The last source went: the ConfigMap goes too, unless a node
			// listed itself since (the precondition fails, and we retry).
			rv := cm.ResourceVersion
			err = cms.Delete(ctx, cm.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{ResourceVersion: &rv}})
			if apierrors.IsConflict(err) {
				continue
			}
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if _, err = cms.Update(ctx, cm, metav1.UpdateOptions{}); apierrors.IsConflict(err) {
			continue
		}
		return err
	}
	return fmt.Errorf("model cache: sources of %s: too many conflicting updates", key)
}

// reconcileSources makes the ConfigMaps say what is on disk: the node is
// listed for every key it has, and for no other.
func (c *nodeModelCache) reconcileSources(ctx context.Context) {
	have := map[string]string{}
	for _, k := range c.store.Keys() {
		if gen, _, ok := c.store.Latest(k); ok {
			have[k] = filepath.Base(gen)
			if err := c.register(ctx, k, have[k]); err != nil {
				c.log.WithError(err).WithField("key", k).Warn("model cache: registering a cached key failed")
			}
		}
	}
	cms, err := c.kube.CoreV1().ConfigMaps(c.ns).List(ctx, metav1.ListOptions{LabelSelector: "app.kubernetes.io/component=" + modelCacheComponent})
	if err != nil {
		c.log.WithError(err).Warn("model cache: listing sources failed")
		return
	}
	for _, cm := range cms.Items {
		key := strings.TrimPrefix(cm.Name, modelCacheSourcesPrefix)
		if _, listed := cm.Data[c.node]; listed && have[key] == "" {
			_ = c.deregister(ctx, key)
		}
	}
}

// modelPeerManifest is what a peer serves for a key.
type modelPeerManifest struct {
	Gen      string              `json:"gen"`
	Manifest modelcache.Manifest `json:"manifest"`
}

// fill brings key to this node from the first peer that serves it whole.
func (c *nodeModelCache) fill(ctx context.Context, key string) {
	peers := c.sources(ctx, key)
	if len(peers) == 0 {
		return
	}
	stop := c.heartbeat(key)
	defer stop()
	log := c.log.WithField("key", key)
	for _, node := range peers {
		start := time.Now()
		gen, bytes, err := c.fillFrom(ctx, key, node)
		if err != nil {
			log.WithError(err).WithField("peer", node).Warn("model cache: fetch from peer failed")
			continue
		}
		secs := time.Since(start).Seconds()
		log.WithFields(logrus.Fields{"peer": node, "bytes": bytes, "seconds": fmt.Sprintf("%.1f", secs),
			"MBps": fmt.Sprintf("%.0f", float64(bytes)/1e6/secs)}).Info("model cache: fetched from peer")
		metrics.CountModelCacheOp("fetch", "ok")
		metrics.AddModelCacheBytes("fetch", bytes)
		if err := c.register(ctx, key, filepath.Base(gen)); err != nil {
			log.WithError(err).Warn("model cache: registering the fetched key failed")
		}
		return
	}
	metrics.CountModelCacheOp("fetch", "error")
	c.mu.Lock()
	c.failed[key] = time.Now()
	c.mu.Unlock()
}

func (c *nodeModelCache) fillFrom(ctx context.Context, key, node string) (gen string, bytes int64, err error) {
	ctx, span := tracing.Tracer().Start(ctx, "model_cache.peer_fetch")
	span.SetAttributes(attribute.String("nvsnap.model_cache.key", key), attribute.String("nvsnap.peer", node))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetAttributes(attribute.Int64("nvsnap.bytes", bytes))
		}
		span.End()
	}()
	ctx, cancel := context.WithTimeout(ctx, c.fetchTimeout)
	defer cancel()
	base, err := c.peerURL(ctx, node)
	if err != nil {
		return "", 0, err
	}
	var pm modelPeerManifest
	if err := c.getJSON(ctx, base+"/v1/model-cache/"+key, &pm); err != nil {
		return "", 0, err
	}
	if !strings.HasPrefix(pm.Gen, modelcache.GenPrefix) || !validPathSegmentOK(pm.Gen) {
		return "", 0, fmt.Errorf("peer named generation %q", pm.Gen)
	}
	if err := pm.Manifest.Check(modelcache.Limits{MaxEntries: modelCacheMaxEntries}); err != nil {
		return "", 0, err
	}
	release, err := c.reserve(ctx, modelcache.AllocEstimate(pm.Manifest.Entries))
	if err != nil {
		return "", 0, err
	}
	defer release()
	open := func(rel string) (modelcache.ReadCloser, error) {
		u := base + "/v1/model-cache/" + key + "/" + pm.Gen + "/file?path=" + url.QueryEscape(rel)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
		resp, err := c.client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("peer answered %s for %s", resp.Status, rel)
		}
		return resp.Body, nil
	}
	gen, err = c.store.Fetch(key, pm.Manifest, open, modelcache.Limits{MaxBytes: pm.Manifest.Bytes, MaxEntries: modelCacheMaxEntries})
	return gen, pm.Manifest.Bytes, err
}

func validPathSegmentOK(s string) bool { return validPathSegment("gen", s) == nil }

func (c *nodeModelCache) getJSON(ctx context.Context, u string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return &peerStatusError{code: resp.StatusCode, status: resp.Status}
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 256<<20)).Decode(v)
}

// peerStatusError is a peer's answer other than 200.
type peerStatusError struct {
	code   int
	status string
}

func (e *peerStatusError) Error() string { return "peer answered " + e.status }

// harvest copies a pod's finished download volume into the cache.
func (c *nodeModelCache) harvest(ctx context.Context, pod *corev1.Pod, d webhook.ModelCacheDownload, finishedAt time.Time, fingerprint string) {
	log := c.log.WithFields(logrus.Fields{"key": d.Key, "pod": pod.Namespace + "/" + pod.Name})
	if _, _, ok := c.store.Latest(d.Key); ok {
		return
	}
	if finishedAt.IsZero() || fingerprint == "" {
		// Without the download's own record of its tree, what the engine
		// changed since cannot be told apart.
		metrics.CountModelCacheOp("harvest", "skipped")
		return
	}
	src, err := c.emptyDirPath(pod, d.Landing.VolumeName)
	if err != nil {
		log.WithError(err).Warn("model cache: no volume to harvest")
		return
	}
	ents, err := modelcache.Scan(src, modelCacheMaxEntries)
	if errors.Is(err, modelcache.ErrTooLarge) {
		log.Info("model cache: not harvesting a tree of that many entries")
		metrics.CountModelCacheOp("harvest", "skipped")
		return
	}
	if err != nil {
		log.WithError(err).Warn("model cache: reading the volume failed")
		return
	}
	if len(ents) <= 1 { // the root alone
		return
	}
	var size int64
	for _, e := range ents {
		size += e.Size
	}
	release, err := c.reserve(ctx, modelcache.AllocEstimate(ents))
	if err != nil {
		log.WithError(err).WithField("bytes", size).Info("model cache: not harvesting")
		metrics.CountModelCacheOp("harvest", "skipped")
		return
	}
	defer release()
	start := time.Now()
	// Anything written after the init finished (one-second resolution,
	// so a second of slack) is the engine's, not the download's.
	gen, err := c.store.Harvest(d.Key, src, modelcache.Limits{MaxBytes: size, ExactBytes: true, MaxEntries: modelCacheMaxEntries,
		NotAfter: finishedAt.Add(time.Second), Fingerprint: fingerprint})
	if err != nil {
		log.WithError(err).Warn("model cache: harvest failed; the next cold pod downloads")
		metrics.CountModelCacheOp("harvest", "error")
		return
	}
	metrics.CountModelCacheOp("harvest", "ok")
	metrics.AddModelCacheBytes("harvest", size)
	log.WithFields(logrus.Fields{"bytes": size, "seconds": fmt.Sprintf("%.1f", time.Since(start).Seconds())}).Info("model cache: harvested the download")
	if err := c.register(ctx, d.Key, filepath.Base(gen)); err != nil {
		log.WithError(err).Warn("model cache: registering the harvested key failed")
	}
}

// emptyDirPath is the host path of the pod's emptyDir vol, as the agent
// sees it. vol must be an emptyDir of the pod's spec.
func (c *nodeModelCache) emptyDirPath(pod *corev1.Pod, vol string) (string, error) {
	for i := range pod.Spec.Volumes {
		v := &pod.Spec.Volumes[i]
		if v.Name == vol && v.EmptyDir != nil && filepath.Base(vol) == vol && filepath.IsLocal(vol) && pod.UID != "" {
			return filepath.Join(c.podsRoot, string(pod.UID), "volumes", "kubernetes.io~empty-dir", vol), nil
		}
	}
	return "", fmt.Errorf("volume %q is not an emptyDir of the pod", vol)
}

// emptyDirWritePath is the host path of the pod's emptyDir vol under the
// writable pods directory.
func (c *nodeModelCache) emptyDirWritePath(pod *corev1.Pod, vol string) (string, error) {
	p, err := c.emptyDirPath(pod, vol)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(c.podsRoot, p)
	if err != nil || c.podsWrite == "" {
		return "", fmt.Errorf("no writable pods directory")
	}
	return filepath.Join(c.podsWrite, rel), nil
}

// podsWritable checks the agent can write under dir, where single flight
// writes waiting pods' volumes.
func podsWritable(dir string) error {
	if dir == "" {
		return errors.New("no pods directory")
	}
	f, err := os.CreateTemp(dir, ".nvsnap-write-probe-")
	if err != nil {
		return err
	}
	name := f.Name()
	f.Close()
	return os.Remove(name)
}

var errNoRoom = errors.New("model cache: no room within the budget")

// reserve promises n bytes to an attempt, evicting unused keys to make
// room. The budget counts what is cached and what attempts in flight have
// been promised; the free-space floor is checked against the filesystem.
func (c *nodeModelCache) reserve(ctx context.Context, n int64) (func(), error) {
	total, _, err := c.statfs(c.store.Root)
	if err != nil {
		return nil, err
	}
	budget := total * int64(c.maxPercent) / 100
	if n < 0 {
		return nil, fmt.Errorf("%w: a reservation of %d bytes", errNoRoom, n)
	}
	if n > budget {
		return nil, fmt.Errorf("%w: %d bytes over a %d-byte budget", errNoRoom, n, budget)
	}
	c.evict(ctx, n)
	c.mu.Lock()
	defer c.mu.Unlock()
	used, err := c.store.Usage()
	if err != nil {
		return nil, err
	}
	_, free, err := c.statfs(c.store.Root)
	if err != nil {
		return nil, err
	}
	if used+c.reserved+n > budget || free-c.reserved-n < total*modelCacheFloorPercent/100 {
		return nil, fmt.Errorf("%w: %d bytes, %d used, %d promised, %d free", errNoRoom, n, used, c.reserved, free)
	}
	c.reserved += n
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			c.reserved -= n
			c.mu.Unlock()
		})
	}, nil
}

// evict removes least recently used keys until want more bytes fit in the
// budget. A key in use by a pending or running pod on the node, or busy
// with a fill, harvest or transfer, is never evicted: an init may be
// copying from it.
func (c *nodeModelCache) evict(ctx context.Context, want int64) {
	total, _, err := c.statfs(c.store.Root)
	if err != nil {
		return
	}
	budget := total * int64(c.maxPercent) / 100
	for {
		c.mu.Lock()
		used, err := c.store.Usage()
		if err != nil || used+c.reserved+want <= budget {
			c.mu.Unlock()
			return
		}
		var victim string
		var oldest time.Time
		for _, k := range c.store.Keys() {
			// A key with plans is mounted by a waiting init.
			if c.inUse[k] || c.busy[k] > 0 || len(modelcache.Plans(c.store.KeyDir(k))) > 0 {
				continue
			}
			if t := c.store.LastUsed(k); victim == "" || t.Before(oldest) {
				victim, oldest = k, t
			}
		}
		if victim == "" {
			c.mu.Unlock()
			return
		}
		// Held while it goes: no fill, harvest or transfer starts on it,
		// no peer request is served from it (begin), and peers stop being
		// sent here before the files go.
		c.busy[victim]++
		c.evicting[victim] = true
		c.mu.Unlock()
		if err := c.deregister(ctx, victim); err != nil {
			c.log.WithError(err).WithField("key", victim).Warn("model cache: deregistering before eviction failed; keeping the key")
			c.endEvict(victim)
			return
		}
		if err := c.store.EvictKey(victim); err != nil {
			c.log.WithError(err).WithField("key", victim).Warn("model cache: eviction failed")
			c.endEvict(victim)
			return
		}
		c.log.WithField("key", victim).Info("model cache: evicted the least recently used key")
		metrics.CountModelCacheOp("evict", "ok")
		c.endEvict(victim)
	}
}

// Peer routes. Behind the agent's token guard like every agent-to-agent
// route; the cache does not start unless the guard requires a token.

func (c *nodeModelCache) manifestHandler(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]
	if !modelcache.ValidKey(key) {
		http.Error(w, "bad key", http.StatusBadRequest)
		return
	}
	// A key being evicted is not offered: its files are going.
	c.mu.Lock()
	evicting := c.evicting[key]
	c.mu.Unlock()
	if evicting {
		http.NotFound(w, r)
		return
	}
	gen, m, ok := c.store.Latest(key)
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(modelPeerManifest{Gen: filepath.Base(gen), Manifest: m})
}

func (c *nodeModelCache) fileHandler(w http.ResponseWriter, r *http.Request) {
	v := mux.Vars(r)
	key, gen, rel := v["key"], v["gen"], r.URL.Query().Get("path")
	if !modelcache.ValidKey(key) || !strings.HasPrefix(gen, modelcache.GenPrefix) || !modelcache.ValidRel(rel) {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	// Busy while it streams: the key is not evicted under the transfer.
	// A key being evicted is not served.
	if !c.begin(key, false) {
		http.NotFound(w, r)
		return
	}
	defer c.end(key)
	dir := filepath.Join(c.store.KeyDir(key), gen)
	if _, err := os.Stat(filepath.Join(dir, modelcache.CompleteFile)); err != nil {
		http.NotFound(w, r)
		return
	}
	p, err := resolveWithinRoot(filepath.Join(dir, modelcache.TreeDir), rel)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	f, err := os.OpenFile(p, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil || !fi.Mode().IsRegular() {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, "", fi.ModTime(), f)
}

// singleFlightPlanWait is how long a wrapped init waits for its plan: a
// few agent ticks.
const singleFlightPlanWait = 60

// agentTenants scopes keys by the agent's tenant resolver.
type agentTenants struct{ a *Agent }

func (t agentTenants) Scope(_ context.Context, ns string) string { return t.a.tenantScope(ns) }

// nodeModelCacheOn reports whether this agent runs the cache: admission
// wraps download inits only where the agent fills and harvests them.
func (a *Agent) nodeModelCacheOn() bool {
	cfg := a.config.NodeModelCache
	return cfg.Enabled && cfg.Root != "" && a.config.AuthMode == AuthRequired && a.modelVolume == nil
}

// webhookNodeModelCache is the admission side's configuration, nil when off.
func (a *Agent) webhookNodeModelCache() *webhook.NodeModelCache {
	if !a.nodeModelCacheOn() {
		return nil
	}
	c := &webhook.NodeModelCache{Root: a.config.NodeModelCache.Root, Wait: a.config.NodeModelCache.Wait, Tenants: agentTenants{a}}
	// The helper must be the one that reads plans: admission mounts it
	// from its own bundle version, which a webhook without a bundle id
	// does not name.
	if a.config.NodeModelCache.SingleFlight && a.currentBundle() != "" && podsWritable(a.config.NodeModelCache.PodsRoot) == nil {
		c.PlanWait = singleFlightPlanWait
	}
	return c
}

// startNodeModelCache wires the cache into the agent: its controller and
// its peer routes. It refuses to start unless agent API auth is required:
// the routes serve a tenant's model to any caller that can reach the node.
func (a *Agent) startNodeModelCache(ctx context.Context, router *mux.Router) {
	cfg := a.config.NodeModelCache
	if !cfg.Enabled {
		return
	}
	log := a.log.WithField("subsys", "model-cache")
	if !a.nodeModelCacheOn() {
		log.WithFields(logrus.Fields{"auth_mode": a.config.AuthMode, "model_volume": a.modelVolume != nil, "root": cfg.Root}).
			Error("model cache: needs a cache root, --auth-mode=required and model volumes off; the cache is off")
		return
	}
	if a.config.NodeName == "" {
		log.Error("model cache: no node name; the cache is off")
		return
	}
	// The agent's client is built by a later start-up step
	// (startRootfsCapture); the cache does not depend on that order.
	kube := a.kubeClient
	if kube == nil {
		var err error
		if kube, err = buildKubeClient(); err != nil {
			log.WithError(err).Error("model cache: no kube client; the cache is off")
			return
		}
	}
	// The tenant resolver on this client, before the controller or
	// admission looks up a tenant (bindTenants).
	a.bindTenants(kube)
	// Main container 0, as the webhook's Mutator resolves it.
	c := newNodeModelCache(cfg, kube, a.config.NodeName, a.modelCacheNamespace(), agentTenants{a}, 0, a.peerAgentURL, log)
	router.HandleFunc("/v1/model-cache/{key}", c.manifestHandler).Methods("GET")
	router.HandleFunc("/v1/model-cache/{key}/{gen}/file", c.fileHandler).Methods("GET")
	if cfg.SingleFlight {
		if err := podsWritable(cfg.PodsRoot); err != nil {
			// Admission adds no plans either (webhookNodeModelCache).
			log.WithError(err).Error("model cache: single flight needs a writable kubelet pods directory; it is off")
			c.singleFlight = false
		} else {
			c.addLiveRoutes(router)
		}
	}
	go c.run(ctx)
	log.WithField("root", cfg.Root).Info("model cache: on")
}

func (a *Agent) modelCacheNamespace() string {
	if ns := a.config.RootfsCapture.CMNamespace; ns != "" {
		return ns
	}
	return "nvsnap-system"
}
