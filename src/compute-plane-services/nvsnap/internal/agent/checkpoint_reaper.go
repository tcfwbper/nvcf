// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// A CRIU checkpoint holds its pods' memory: a TB and more per node for a
// large model, on the node and again on L2. Two kinds are never restored
// again and are retired, here and on L2:
//   - a blocked checkpoint (its restore failed);
//   - an instance capture no complete group record names: one rank of a
//     capture that failed, or a capture since replaced. It gets
//     checkpointRetireAge first, past any capture and L2 promote still
//     running.
// A checkpoint a pod on this node is restoring from or restored from is
// kept either way, and so are the checkpoints of a capture still running
// (its record capturing). A record abandoned before its verification
// (verify_abandoned.go) names nothing: its namespace is gone, and it is
// restored only there. While the checkpoint filesystem is low
// (capture_disk.go) the age is pressureRetireAge. A retired instance
// capture's catalog row goes with it, through a tombstone retried within
// a budget after the sweep (a lone capture's row is its configuration's,
// which a later capture may share: nvsnap-server deletes by hash).

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/checkpointstore"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/tracing"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

const checkpointRetireAge = 3 * time.Hour

// retirableCheckpoints returns the checkpoints under the agent's
// checkpoint directory to retire, with their metadata: an unnamed instance
// capture once it is older than age.
func (a *Agent) retirableCheckpoints(ctx context.Context, pods []corev1.Pod, now time.Time, age time.Duration) (map[string]*CheckpointMetadata, error) {
	cms := a.kubeClient.CoreV1().ConfigMaps(a.criuFailedNamespace())
	blocked := map[string]bool{}
	if cm, err := cms.Get(ctx, criuRestoreFailedConfigMap, metav1.GetOptions{}); err == nil {
		for id := range cm.Data {
			blocked[id] = true
		}
	}
	records, err := cms.List(ctx, metav1.ListOptions{LabelSelector: "app.kubernetes.io/managed-by=nvsnap"})
	if err != nil {
		return nil, err // without the records nothing reads as unreferenced
	}
	named := map[string]bool{}
	present := map[string]bool{}
	for _, r := range records.Items {
		present[r.Name] = true
	}
	for _, r := range records.Items {
		// An alias outlives nothing: once its instance's record is gone it
		// would only send a rank to a lookup that fails.
		if strings.HasPrefix(r.Name, criuGroupConfigMapPrefix) && r.Data["state"] == criuGroupStateAlias && !present[criuGroupConfigMapName(r.Data["alias"])] {
			if err := cms.Delete(ctx, r.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				a.log.WithError(err).WithField("record", r.Name).Warn("checkpoint reaper: cannot delete an orphaned group alias")
			}
		}
	}
	for _, r := range records.Items {
		if !strings.HasPrefix(r.Name, criuGroupConfigMapPrefix) {
			continue
		}
		capturing := r.Data["state"] == criuGroupStateCapturing
		if !capturing && (!recordUsable(r.Data) || r.Data[groupAbandonedKey] != "") {
			continue
		}
		for k, v := range r.Data {
			if strings.HasPrefix(k, "checkpoint.") {
				named[v] = true
			}
		}
	}
	inUse := map[string]bool{}
	for _, p := range pods {
		if id := p.Annotations[webhook.CRIURestoreAnnotation]; id != "" && p.DeletionTimestamp == nil {
			inUse[id] = true
		}
	}
	ents, err := os.ReadDir(a.config.CheckpointDir)
	if err != nil {
		return nil, err
	}
	out := map[string]*CheckpointMetadata{}
	for _, e := range ents {
		id := e.Name()
		if !e.IsDir() || strings.HasPrefix(id, ".") || id == GPUSharePodStoresSubdir || inUse[id] {
			continue
		}
		b, err := os.ReadFile(filepath.Join(a.config.CheckpointDir, id, "metadata.json"))
		if err != nil {
			continue // not a checkpoint, or one being written
		}
		var md CheckpointMetadata
		if json.Unmarshal(b, &md) != nil {
			continue
		}
		instance := md.CatalogInfo != nil && md.CatalogInfo.InstanceCapture
		if blocked[id] || (instance && !named[id] && now.Sub(md.CreatedAt) > age) {
			out[id] = &md
		}
	}
	return out, nil
}

// retirableCachedirCaptures returns the cachedir captures under the
// agent's checkpoint directory to retire (criu_fallback.go): those no
// cachedir record names, older than checkpointRetireAge and seeding no pod
// on this node. A record whose configuration CRIU serves again, or whose
// group record is gone, is deleted first: its captures are never seeded
// from.
func (a *Agent) retirableCachedirCaptures(ctx context.Context, pods []corev1.Pod, now time.Time) ([]string, error) {
	cms := a.kubeClient.CoreV1().ConfigMaps(a.criuFailedNamespace())
	records, err := cms.List(ctx, metav1.ListOptions{LabelSelector: "app.kubernetes.io/managed-by=nvsnap"})
	if err != nil {
		return nil, err
	}
	groups := map[string]map[string]string{}
	for i := range records.Items {
		if r := &records.Items[i]; strings.HasPrefix(r.Name, criuGroupConfigMapPrefix) {
			groups[strings.TrimPrefix(r.Name, criuGroupConfigMapPrefix)] = r.Data
		}
	}
	// While the drivers keep CRIU off (driver_gate.go), every
	// configuration falls back with no group record of its own, and its
	// cachedir record is what seeds its pods.
	gated := a.driverGate(ctx) != ""
	named := map[string]bool{}
	for i := range records.Items {
		r := &records.Items[i]
		if !strings.HasPrefix(r.Name, cachedirRecordPrefix) {
			continue
		}
		key := strings.TrimPrefix(r.Name, cachedirRecordPrefix)
		g, ok := groups[key]
		if ok && g["state"] == criuGroupStateAlias {
			g, ok = groups[g["alias"]]
		}
		if !gated && (!ok || recordUsable(g)) {
			if err := cms.Delete(ctx, r.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				a.log.WithError(err).WithField("record", r.Name).Warn("checkpoint reaper: cannot delete a cachedir record no configuration falls back to")
			}
			continue
		}
		for k, v := range r.Data {
			if !strings.HasPrefix(k, "rank.") || v == "" {
				continue
			}
			if n, err := strconv.Atoi(strings.TrimPrefix(k, "rank.")); err == nil {
				named[webhook.FallbackCaptureID(key, n)] = true
			}
		}
	}
	for i := range pods {
		if id := pods[i].Annotations[webhook.FallbackCaptureAnnotation]; id != "" && pods[i].DeletionTimestamp == nil {
			named[id] = true
		}
	}
	ents, err := os.ReadDir(a.config.CheckpointDir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range ents {
		id := e.Name()
		if !e.IsDir() || !strings.HasPrefix(id, webhook.FallbackCapturePrefix) || named[id] {
			continue
		}
		if info, err := e.Info(); err == nil && now.Sub(info.ModTime()) > checkpointRetireAge {
			out = append(out, id)
		}
	}
	return out, nil
}

// reapCheckpoints retires this node's dead checkpoints and their L2 copies.
func (a *Agent) reapCheckpoints(ctx context.Context, pods []corev1.Pod, now time.Time) {
	age := checkpointRetireAge
	if a.checkpointDiskLow() {
		age = pressureRetireAge
	}
	// Local space first; the catalog retries after it, within a budget,
	// so an unreachable nvsnap-server cannot hold up reclamation.
	a.reapCheckpointsOlderThan(ctx, pods, now, age)
	rctx, cancel := context.WithTimeout(ctx, catalogRetryBudget)
	defer cancel()
	a.retryCatalogDrops(rctx, now)
}

// catalogRetryBudget bounds one sweep's catalog retries, and
// catalogRequestTimeout each request; what is left is tried on the next
// sweep. Variables for tests.
var (
	catalogRetryBudget    = 2 * time.Minute
	catalogRequestTimeout = 30 * time.Second
)

// reapCheckpointsOlderThan is reapCheckpoints with the age an unnamed
// instance capture must have.
func (a *Agent) reapCheckpointsOlderThan(ctx context.Context, pods []corev1.Pod, now time.Time, age time.Duration) {
	if ids, err := a.retirableCachedirCaptures(ctx, pods, now); err != nil {
		a.log.WithError(err).Warn("checkpoint reaper: cannot tell the dead cachedir captures; keeping all")
	} else {
		for _, id := range ids {
			if err := os.RemoveAll(filepath.Join(a.config.CheckpointDir, id)); err != nil {
				a.log.WithError(err).WithField("capture", id).Warn("checkpoint reaper: cannot remove the cachedir capture")
				continue
			}
			_ = os.RemoveAll(a.cachedirReadyDir(id))
			a.log.WithField("capture", id).Info("checkpoint reaper: retired a cachedir capture that is never seeded from again")
		}
	}
	dead, err := a.retirableCheckpoints(ctx, pods, now, age)
	if err != nil {
		a.log.WithError(err).Warn("checkpoint reaper: cannot tell the dead checkpoints; keeping all")
		return
	}
	backend, _ := a.l2Backend.(*checkpointstore.PerCapturePVCBackend)
	for id, md := range dead {
		log := a.log.WithField("checkpoint", id)
		// Only an instance capture's L2 copy is its alone: a lone capture's
		// is keyed by its configuration, which a later capture may share.
		instance := md.CatalogInfo != nil && md.CatalogInfo.InstanceCapture
		if instance && backend != nil && backend.Promoter != nil && md.Hash != "" {
			ns := md.PodNamespace
			if ns == "" {
				ns = backend.Namespace
			}
			if err := backend.Promoter.Delete(ctx, md.Hash, ns); err != nil {
				log.WithError(err).Warn("checkpoint reaper: cannot retire the L2 copy; keeping the checkpoint to try again")
				continue
			}
		}
		// An instance capture's catalog row and its other nodes' copies go
		// through nvsnap-server after the directory is gone: a tombstone
		// keeps the request, sent after the sweep (retryCatalogDrops).
		if instance {
			a.writeCatalogTombstone(id, log)
		}
		if err := os.RemoveAll(filepath.Join(a.config.CheckpointDir, id)); err != nil {
			log.WithError(err).Warn("checkpoint reaper: cannot remove the checkpoint")
			continue
		}
		log.Info("checkpoint reaper: retired a checkpoint that is never restored again")
	}
}

// catalogTombstoneDir holds, by checkpoint id, the retired checkpoints
// whose catalog row is still to be deleted; a dot directory, which the
// reaper's scan skips.
const catalogTombstoneDir = ".catalog-pending"

// catalogTombstoneMaxAge is how long a catalog delete is retried.
const catalogTombstoneMaxAge = 7 * 24 * time.Hour

func (a *Agent) writeCatalogTombstone(id string, log *logrus.Entry) {
	if a.config.CatalogURL == "" {
		return
	}
	dir := filepath.Join(a.config.CheckpointDir, catalogTombstoneDir)
	if err := os.MkdirAll(dir, 0o755); err == nil {
		// Its content is when it was first written (the retry's age); its
		// modification time, when it was last tried (the retry order).
		err = os.WriteFile(filepath.Join(dir, id), []byte(time.Now().UTC().Format(time.RFC3339)), 0o644)
		if err == nil {
			return
		}
	}
	log.Warn("checkpoint reaper: cannot record the catalog delete to retry; it is tried once")
}

func (a *Agent) clearCatalogTombstone(id string) {
	_ = os.Remove(filepath.Join(a.config.CheckpointDir, catalogTombstoneDir, id))
}

// retryCatalogDrops retries the catalog deletes of retired checkpoints
// that failed, least recently tried first, so a few that keep failing do
// not starve the rest; it drops one first written catalogTombstoneMaxAge
// ago.
func (a *Agent) retryCatalogDrops(ctx context.Context, now time.Time) {
	if a.config.CatalogURL == "" {
		return
	}
	dir := filepath.Join(a.config.CheckpointDir, catalogTombstoneDir)
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	type tomb struct {
		id            string
		tried, posted time.Time
	}
	tombs := make([]tomb, 0, len(ents))
	for _, e := range ents {
		info, err := e.Info()
		if err != nil {
			continue
		}
		t := tomb{id: e.Name(), tried: info.ModTime(), posted: info.ModTime()}
		if b, err := os.ReadFile(filepath.Join(dir, t.id)); err == nil {
			if p, err := time.Parse(time.RFC3339, strings.TrimSpace(string(b))); err == nil {
				t.posted = p
			}
		}
		tombs = append(tombs, t)
	}
	sort.Slice(tombs, func(i, j int) bool { return tombs[i].tried.Before(tombs[j].tried) })
	for _, t := range tombs {
		if ctx.Err() != nil {
			return // the sweep's budget is spent; the rest wait for the next
		}
		log := a.log.WithField("checkpoint", t.id)
		if a.dropCatalogRow(ctx, t.id, log) {
			a.clearCatalogTombstone(t.id)
			continue
		}
		if now.Sub(t.posted) > catalogTombstoneMaxAge {
			log.Warn("checkpoint reaper: gave up deleting the retired checkpoint's catalog row; delete it through nvsnap-server")
			a.clearCatalogTombstone(t.id)
			continue
		}
		_ = os.Chtimes(filepath.Join(dir, t.id), now, now) // tried: to the back of the queue
	}
}

// dropCatalogRow deletes a retired checkpoint's catalog row, and its
// copies on other nodes, through nvsnap-server (its delete cascades to
// them); kept rows would list a checkpoint that no longer exists. A
// failure leaves the row to nvsnap-server's own delete.
func (a *Agent) dropCatalogRow(ctx context.Context, id string, log *logrus.Entry) bool {
	if a.config.CatalogURL == "" {
		return true
	}
	ctx, span := tracing.Tracer().Start(ctx, "checkpoint.drop_catalog_row")
	defer span.End()
	span.SetAttributes(attribute.String("nvsnap.checkpoint_id", id))
	ctx, cancel := context.WithTimeout(ctx, catalogRequestTimeout)
	defer cancel()
	u := fmt.Sprintf("%s/api/v1/checkpoints/%s", a.config.CatalogURL, url.PathEscape(id))
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		span.RecordError(err)
		return false
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		span.RecordError(err)
		log.WithError(err).Warn("checkpoint reaper: cannot delete the retired checkpoint's catalog row; retried on a later sweep")
		return false
	}
	_ = resp.Body.Close()
	span.SetAttributes(attribute.Int("http.status_code", resp.StatusCode))
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		log.WithField("status", resp.StatusCode).Warn("checkpoint reaper: nvsnap-server kept the retired checkpoint's catalog row; retried on a later sweep")
		return false
	}
	return true
}

// podRestoringFrom returns a pod on this node that restores, or restored,
// from checkpoint id; "" for none.
func (a *Agent) podRestoringFrom(ctx context.Context, id string) (string, error) {
	if a.kubeClient == nil || a.config.NodeName == "" {
		return "", nil
	}
	pods, err := a.kubeClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + a.config.NodeName})
	if err != nil {
		return "", err
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.DeletionTimestamp == nil && p.Annotations[webhook.CRIURestoreAnnotation] == id {
			return p.Namespace + "/" + p.Name, nil
		}
	}
	return "", nil
}
