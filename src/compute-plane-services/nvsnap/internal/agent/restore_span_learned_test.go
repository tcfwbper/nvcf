// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const gb = int64(1) << 30

// A rank of a 2-node Kimi-K3.1 instance of 5 members: an 882 GB checkpoint
// whose first restore on its node took 33.9 s.
const (
	kimiRankBytes   = 881759158843
	kimiRankSpanMS  = 33929
	kimiMembers     = 5
	kimiGPU         = "NVIDIA-GB300"
	kimiShapeKey    = "NVIDIA-GB300.m5"
	singleShapeKey  = "NVIDIA-GB300.m1"
	learnedMarginOf = 6 // learnedSpanMarginPct 20: 6/5
)

func TestLearnedSpanKey(t *testing.T) {
	for _, c := range []struct {
		gpu     string
		members int
		want    string
	}{
		{kimiGPU, 5, kimiShapeKey},
		{kimiGPU, 1, singleShapeKey},
		{"NVIDIA H100 80GB HBM3", 2, "NVIDIA_H100_80GB_HBM3.m2"},
		{"a/b:c", 1, "a_b_c.m1"},
		{"", 1, ""},
		{kimiGPU, 0, ""},
	} {
		if got := learnedSpanKey(c.gpu, c.members); got != c.want {
			t.Errorf("learnedSpanKey(%q, %d) = %q, want %q", c.gpu, c.members, got, c.want)
		}
	}
}

func TestSpanSamples(t *testing.T) {
	// Entries that do not parse, and samples of checkpoints too small to
	// scale from, are skipped.
	got := parseSpanSamples("1073741824=5 junk 7=x 1073741824=0 1=9000 -3=9 2147483648=12")
	want := []spanSample{{gb, 5}, {2 * gb, 12}}
	if len(got) != len(want) {
		t.Fatalf("parsed %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("parsed %v, want %v", got, want)
		}
	}
	s := ""
	for i := 1; i <= learnedSpanKeep+3; i++ {
		s = addSpanSample(s, spanSample{bytes: int64(i), ms: int64(i * 10)})
	}
	entries := strings.Fields(s)
	if len(entries) != learnedSpanKeep {
		t.Fatalf("kept %d samples, want %d: %q", len(entries), learnedSpanKeep, s)
	}
	if entries[0] != "4=40" || entries[len(entries)-1] != "11=110" {
		t.Errorf("kept %q, want the newest %d, oldest first", s, learnedSpanKeep)
	}
}

func TestLearnedSpan(t *testing.T) {
	if _, ok := learnedSpan(nil, 100*gb); ok {
		t.Error("a span learned from no sample")
	}
	if _, ok := learnedSpan([]spanSample{{100 * gb, 1000}}, 0); ok {
		t.Error("a span learned for a checkpoint of no size")
	}
	for _, c := range []struct {
		name    string
		samples []spanSample
		bytes   int64
		want    int64
	}{
		// The same size: the span with its margin.
		{"same size", []spanSample{{100 * gb, 10000}}, 100 * gb, 12000},
		// A smaller checkpoint is not scaled down.
		{"smaller checkpoint", []spanSample{{100 * gb, 10000}}, 10 * gb, 12000},
		// A larger one is scaled up by its size.
		{"larger checkpoint", []spanSample{{100 * gb, 10000}}, 200 * gb, 24000},
		// The largest of the samples, after scaling: a larger checkpoint's
		// span is not scaled down, a smaller one's is scaled up.
		{"largest sample", []spanSample{{100 * gb, 10000}, {400 * gb, 30000}, {50 * gb, 9000}}, 100 * gb, 36000},
		{"largest after scaling", []spanSample{{100 * gb, 10000}, {50 * gb, 9000}}, 100 * gb, 21600},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, ok := learnedSpan(c.samples, c.bytes)
			if !ok || got != c.want {
				t.Errorf("learnedSpan = %d, %v; want %d", got, ok, c.want)
			}
		})
	}
}

// The Kimi-K3.1 rank: its learned span on a node new to it, from the same
// checkpoint's first restore on another node (one unit, the checkpoint's
// size, on both sides), budgets about 50 s; the estimate it gets today is
// 130 s.
func TestLearnedSpanBudgetKimi(t *testing.T) {
	ms, ok := learnedSpan([]spanSample{{kimiRankBytes, kimiRankSpanMS}}, kimiRankBytes)
	if want := int64(kimiRankSpanMS * learnedMarginOf / 5); !ok || ms != want {
		t.Fatalf("learned %d, %v; want %d", ms, ok, want)
	}
	b := spanBudget(ms)
	if b < 45*time.Second || b > 55*time.Second {
		t.Errorf("budget %s from a learned %d ms, want about 50 s", b, ms)
	}
}

func spansAgent(t *testing.T, objs ...*corev1.ConfigMap) (*Agent, *fake.Clientset) {
	t.Helper()
	kc := fake.NewSimpleClientset()
	a := &Agent{kubeClient: kc}
	for _, o := range objs {
		o.Namespace = a.agentNamespace()
		if _, err := kc.CoreV1().ConfigMaps(o.Namespace).Create(context.Background(), o, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	return a, kc
}

func TestRecordAndReadLearnedSpan(t *testing.T) {
	ctx := context.Background()
	a, _ := spansAgent(t)
	if _, ok := a.learnedRestoreSpan(ctx, kimiGPU, kimiMembers, kimiRankBytes); ok {
		t.Fatal("a span learned before any was recorded")
	}
	if err := a.recordLearnedSpan(ctx, kimiGPU, kimiMembers, kimiRankBytes, kimiRankSpanMS*time.Millisecond); err != nil {
		t.Fatalf("record (create): %v", err)
	}
	if err := a.recordLearnedSpan(ctx, kimiGPU, kimiMembers, kimiRankBytes, 30*time.Second); err != nil {
		t.Fatalf("record (update): %v", err)
	}
	ms, ok := a.learnedRestoreSpan(ctx, kimiGPU, kimiMembers, kimiRankBytes)
	if want := int64(kimiRankSpanMS * learnedMarginOf / 5); !ok || ms != want {
		t.Errorf("learned %d, %v; want the larger sample with its margin, %d", ms, ok, want)
	}
	// Another shape learns nothing from these: another GPU product, or a
	// pod on its own, which has no fabric remap.
	if _, ok := a.learnedRestoreSpan(ctx, "NVIDIA-H100", kimiMembers, kimiRankBytes); ok {
		t.Error("a span learned from another GPU product's restores")
	}
	if _, ok := a.learnedRestoreSpan(ctx, kimiGPU, 1, kimiRankBytes); ok {
		t.Error("a single pod learned a span from a group's restores")
	}
	cm, err := a.kubeClient.CoreV1().ConfigMaps(a.agentNamespace()).Get(ctx, restoreSpansConfigMap, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Fields(cm.Data[kimiShapeKey]); len(got) != 2 {
		t.Errorf("recorded %q, want two samples", cm.Data[kimiShapeKey])
	}
	// Nothing is recorded without a GPU product, a group size, a size worth
	// scaling from, or a span.
	for _, c := range []struct {
		gpu     string
		members int
		bytes   int64
		d       time.Duration
	}{{"", 1, 10 * gb, time.Second}, {"g", 0, 10 * gb, time.Second}, {"g", 1, 1000, time.Second}, {"g", 1, 10 * gb, 0}} {
		if err := a.recordLearnedSpan(ctx, c.gpu, c.members, c.bytes, c.d); err != nil {
			t.Errorf("record %+v: %v", c, err)
		}
	}
	cm, _ = a.kubeClient.CoreV1().ConfigMaps(a.agentNamespace()).Get(ctx, restoreSpansConfigMap, metav1.GetOptions{})
	for k := range cm.Data {
		if k != kimiShapeKey {
			t.Errorf("a sample that should not be was recorded under %q", k)
		}
	}
}

// Ranks of one instance record at once: a write that loses the race, to a
// create or an update, is tried again, and no sample is lost.
func TestRecordLearnedSpanRetriesConflicts(t *testing.T) {
	ctx := context.Background()
	a, kc := spansAgent(t)
	createLost, updateLost := false, false
	kc.PrependReactor("create", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
		if createLost {
			return false, nil, nil
		}
		createLost = true
		// Another rank creates it first.
		other := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: restoreSpansConfigMap, Namespace: a.agentNamespace()},
			Data:       map[string]string{kimiShapeKey: addSpanSample("", spanSample{kimiRankBytes, 40000})},
		}
		if err := kc.Tracker().Add(other); err != nil {
			t.Fatal(err)
		}
		return true, nil, apierrors.NewAlreadyExists(corev1.Resource("configmaps"), restoreSpansConfigMap)
	})
	kc.PrependReactor("update", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
		if updateLost {
			return false, nil, nil
		}
		updateLost = true
		return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), restoreSpansConfigMap, nil)
	})
	if err := a.recordLearnedSpan(ctx, kimiGPU, kimiMembers, kimiRankBytes, kimiRankSpanMS*time.Millisecond); err != nil {
		t.Fatalf("record: %v", err)
	}
	if !createLost || !updateLost {
		t.Fatalf("the races did not happen: create %v, update %v", createLost, updateLost)
	}
	cm, err := kc.CoreV1().ConfigMaps(a.agentNamespace()).Get(ctx, restoreSpansConfigMap, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Fields(cm.Data[kimiShapeKey]); len(got) != 2 {
		t.Errorf("recorded %q, want the other rank's sample and this one's", cm.Data[kimiShapeKey])
	}
}

func TestLearnedGroupSpan(t *testing.T) {
	ctx := context.Background()
	md := func(gpu string, members int, size int64) *CheckpointMetadata {
		return &CheckpointMetadata{CheckpointSize: size, CatalogInfo: &CatalogInfo{GPUType: gpu},
			GPUShareGroup: &GPUShareGroupInfo{Size: members}}
	}
	a, _ := spansAgent(t, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: restoreSpansConfigMap},
		Data:       map[string]string{kimiShapeKey: addSpanSample("", spanSample{100 * gb, 10000})},
	})
	log := quietFabricLog()
	if got := a.learnedGroupSpan(ctx, []*CheckpointMetadata{md(kimiGPU, 5, 100*gb), md(kimiGPU, 5, 200*gb)}, log); got != 24000 {
		t.Errorf("span %d, want the largest rank's learned 24000", got)
	}
	for name, metas := range map[string][]*CheckpointMetadata{
		"a rank without metadata":  {md(kimiGPU, 5, 100*gb), nil},
		"a rank without a size":    {md(kimiGPU, 5, 100*gb), md(kimiGPU, 5, 0)},
		"ranks on two GPUs":        {md(kimiGPU, 5, 100*gb), md("NVIDIA-H100", 5, 100*gb)},
		"ranks of two group sizes": {md(kimiGPU, 5, 100*gb), md(kimiGPU, 3, 100*gb)},
		"a shape with no samples":  {md(kimiGPU, 3, 100*gb)},
	} {
		if got := a.learnedGroupSpan(ctx, metas, log); got != groupSpanUnknown {
			t.Errorf("%s: span %d, want unknown (the estimate)", name, got)
		}
	}
}

func TestHasRestoreSpan(t *testing.T) {
	dir := t.TempDir()
	if hasRestoreSpan(dir) {
		t.Fatal("a span on record in an empty directory")
	}
	if err := os.WriteFile(filepath.Join(dir, restoreSpanFile), []byte("soon\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if hasRestoreSpan(dir) {
		t.Error("a span that does not parse counts as on record")
	}
	if err := os.WriteFile(filepath.Join(dir, restoreSpanFile), []byte("1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !hasRestoreSpan(dir) {
		t.Error("the recorded span is not seen")
	}
}
