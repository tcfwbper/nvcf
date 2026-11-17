// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// Learned restore spans (docs/design/RESTORE-CLOCK-GPU-RELOAD.md, "Learned
// span").
//
// A restore through a GPU reload budgets its clocks with the checkpoint's
// last restore on the node (restoreSpanFile). Before that, it takes the
// estimate (restoreSpanEstimate), whose rates and slack were set on the
// slowest cluster seen: a 2-node Kimi-K3 rank whose whole restore took 97 s.
// Another cluster reloads the same rank much faster: a 2-node Kimi-K3.1
// rank of 826 GB of GPU memory restored in 33 s, waited out the estimate's
// 130 s at its resume, and its instance took 153 s instead of about 60.
// Every first restore of a capture, and every restore on a node new to the
// capture, paid that wait.
//
// So a first restore of a checkpoint on a node also records what it took
// in a cluster-wide ConfigMap, under its shape: its GPU product and the
// size of its group (a multi-node instance's fabric remap is a fixed cost a
// single pod does not have). A restore with no span of its own takes the
// largest recent one of its shape, scaled up to its checkpoint's size when
// that is larger, with a margin for being another checkpoint's, and never
// more than the estimate: a learned span only shortens the wait. The
// ConfigMap is per cluster, so each cluster learns its own nodes and
// fabric.

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

const (
	// restoreSpansConfigMap holds the cluster's recent first restore spans,
	// a key per shape (learnedSpanKey), each "bytes=ms" entries separated by
	// spaces, newest last.
	restoreSpansConfigMap = "nvsnap-restore-spans"
	// learnedSpanKeep is how many recent spans a shape keeps.
	learnedSpanKeep = 8
	// learnedSpanMargin, a fraction over the learned span, covers what
	// another checkpoint of the same size may take beyond it (more fabric
	// handles to remap, say). spanBudget adds its own tenth and 5 s.
	learnedSpanMarginPct = 20
	// learnedSpanTimeout bounds the ConfigMap read or write: a restore does
	// not wait on the API server for its budget.
	learnedSpanTimeout = 5 * time.Second
	// learnedSpanMinBytes is the smallest checkpoint a sample is taken
	// from: a smaller one's fixed costs, scaled up, would dwarf a large
	// checkpoint's span.
	learnedSpanMinBytes = 1 << 30
)

// spanSample is one restore's span and the size of its checkpoint.
type spanSample struct {
	bytes, ms int64
}

// learnedSpanKey is the ConfigMap key of a shape: the GPU product and the
// number of members of the group, 1 for a pod restored on its own; the
// characters a key does not allow become '_'.
func learnedSpanKey(gpu string, members int) string {
	if gpu == "" || members < 1 {
		return ""
	}
	b := []byte(gpu + ".m" + strconv.Itoa(members))
	for i, c := range b {
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.'
		if !ok {
			b[i] = '_'
		}
	}
	return string(b)
}

// parseSpanSamples reads a key's entries, skipping any that do not parse.
func parseSpanSamples(s string) []spanSample {
	var out []spanSample
	for _, f := range strings.Fields(s) {
		b, m, ok := strings.Cut(f, "=")
		if !ok {
			continue
		}
		bytes, err1 := strconv.ParseInt(b, 10, 64)
		ms, err2 := strconv.ParseInt(m, 10, 64)
		if err1 != nil || err2 != nil || bytes < learnedSpanMinBytes || ms <= 0 {
			continue
		}
		out = append(out, spanSample{bytes: bytes, ms: ms})
	}
	return out
}

// addSpanSample appends sample to a key's entries, keeping the newest
// learnedSpanKeep.
func addSpanSample(s string, sample spanSample) string {
	entries := strings.Fields(s)
	entries = append(entries, strconv.FormatInt(sample.bytes, 10)+"="+strconv.FormatInt(sample.ms, 10))
	if len(entries) > learnedSpanKeep {
		entries = entries[len(entries)-learnedSpanKeep:]
	}
	return strings.Join(entries, " ")
}

// learnedSpan is the span a restore of a checkpoint of bytes takes from
// samples: the largest of them, each scaled up to bytes when its checkpoint
// was smaller (never down), with learnedSpanMarginPct over it. False with no
// sample or no size.
func learnedSpan(samples []spanSample, bytes int64) (int64, bool) {
	if len(samples) == 0 || bytes <= 0 {
		return 0, false
	}
	var span int64
	for _, s := range samples {
		ms := s.ms
		if bytes > s.bytes {
			ms = int64(float64(s.ms) * float64(bytes) / float64(s.bytes))
		}
		span = max(span, ms)
	}
	return span + span*learnedSpanMarginPct/100, true
}

// learnedRestoreSpan is the learned span (learnedSpan) for a restore of a
// checkpoint of bytes in a group of members on gpu, from the cluster's
// ConfigMap; false when there is none or it cannot be read.
func (a *Agent) learnedRestoreSpan(ctx context.Context, gpu string, members int, bytes int64) (int64, bool) {
	key := learnedSpanKey(gpu, members)
	if a.kubeClient == nil || key == "" || bytes <= 0 {
		return 0, false
	}
	ctx, cancel := context.WithTimeout(ctx, learnedSpanTimeout)
	defer cancel()
	cm, err := a.kubeClient.CoreV1().ConfigMaps(a.agentNamespace()).Get(ctx, restoreSpansConfigMap, metav1.GetOptions{})
	if err != nil {
		return 0, false
	}
	return learnedSpan(parseSpanSamples(cm.Data[key]), bytes)
}

// recordLearnedSpan adds a first restore's span, without its wait for the
// clocks, and its checkpoint's size to the cluster's ConfigMap under its
// shape.
func (a *Agent) recordLearnedSpan(ctx context.Context, gpu string, members int, bytes int64, span time.Duration) error {
	key := learnedSpanKey(gpu, members)
	if a.kubeClient == nil || key == "" || bytes < learnedSpanMinBytes || span <= 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, learnedSpanTimeout)
	defer cancel()
	sample := spanSample{bytes: bytes, ms: span.Milliseconds()}
	cms := a.kubeClient.CoreV1().ConfigMaps(a.agentNamespace())
	// The ranks of an instance finish together: back off between tries.
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		cm, err := cms.Get(ctx, restoreSpansConfigMap, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			_, err = cms.Create(ctx, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: restoreSpansConfigMap, Labels: map[string]string{"app.kubernetes.io/managed-by": "nvsnap-agent"}},
				Data:       map[string]string{key: addSpanSample("", sample)},
			}, metav1.CreateOptions{})
			if apierrors.IsAlreadyExists(err) {
				return apierrors.NewConflict(corev1.Resource("configmaps"), restoreSpansConfigMap, err)
			}
			return err
		}
		if err != nil {
			return err
		}
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data[key] = addSpanSample(cm.Data[key], sample)
		_, err = cms.Update(ctx, cm, metav1.UpdateOptions{})
		return err
	})
}

// checkpointShape is a checkpoint's GPU product, the number of members of
// its group (1 on its own) and its size, from its metadata; empty and 0
// when it has none.
func checkpointShape(md *CheckpointMetadata) (gpu string, members int, size int64) {
	if md == nil || md.CatalogInfo == nil {
		return "", 0, 0
	}
	members = 1
	if md.GPUShareGroup != nil {
		members = md.GPUShareGroup.Size
	}
	return md.CatalogInfo.GPUType, members, md.CheckpointSize
}

// learnedGroupSpan is the span a group restore hands its GPU ranks when a
// rank has none of its own (groupSpanUnknown): the learned span of the
// largest rank's checkpoint, else groupSpanUnknown, for the estimate.
func (a *Agent) learnedGroupSpan(ctx context.Context, rankMeta []*CheckpointMetadata, log *logrus.Entry) int64 {
	var gpu string
	var members int
	var bytes int64
	for _, md := range rankMeta {
		g, m, b := checkpointShape(md)
		if g == "" || b <= 0 {
			return groupSpanUnknown
		}
		if gpu != "" && (g != gpu || m != members) {
			return groupSpanUnknown
		}
		gpu, members, bytes = g, m, max(bytes, b)
	}
	ms, ok := a.learnedRestoreSpan(ctx, gpu, members, bytes)
	if !ok {
		return groupSpanUnknown
	}
	log.WithFields(logrus.Fields{"gpu": gpu, "members": members, "bytes": bytes, "learnedSpanMs": ms}).
		Info("group restore: a rank has no restore of its checkpoint on its node; the ranks budget their clocks with the cluster's learned span")
	return ms
}
