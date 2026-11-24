// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

// A redeploy that changes a member's process configuration (a new
// DYN_NAMESPACE_WORKER_SUFFIX) leaves a record its pods may not restore
// from: the next Ready instance captures again rather than start cold for
// good.

// stamped is p with the process configuration admission stamps, the env of
// its container ctr ("" for its GPU container) set to env.
func stamped(p corev1.Pod, ctr string, env ...corev1.EnvVar) *corev1.Pod {
	q := p.DeepCopy()
	if ctr == "" {
		ctr = gpuContainer(q)
	}
	for i := range q.Spec.Containers {
		if q.Spec.Containers[i].Name == ctr {
			q.Spec.Containers[i].Env = env
		}
	}
	if q.Annotations == nil {
		q.Annotations = map[string]string{}
	}
	q.Annotations[webhook.ProcessConfigAnnotation] = webhook.ProcessConfig(q)
	return q
}

func reconfigRecord(members []*corev1.Pod, ranks int, companionContainers map[string]string) map[string]string {
	data := map[string]string{}
	for i, m := range members {
		ctr := gpuContainer(m)
		if i >= ranks {
			ctr = companionContainers[m.Name]
		}
		recordProcessConfig(data, i, m, ctr, len(members))
	}
	return data
}

func TestProcessConfigChangedNamesTheChangedMember(t *testing.T) {
	r0 := stsRank("eng-0", "0", "2", "sts-c", true)
	r1 := stsRank("eng-1", "1", "2", "sts-c", true)
	suffix := func(v string) corev1.EnvVar { return corev1.EnvVar{Name: "DYN_NAMESPACE_WORKER_SUFFIX", Value: v} }
	old := []*corev1.Pod{stamped(r0, "", suffix("a1")), stamped(r1, "", suffix("a1"))}
	data := reconfigRecord(old, 2, nil)

	if got := processConfigChanged(data, old, 2, nil); len(got) != 0 {
		t.Fatalf("the instance the record was taken from: changed %q", got)
	}
	redeployed := []*corev1.Pod{stamped(r0, "", suffix("b7")), stamped(r1, "", suffix("b7"))}
	got := processConfigChanged(data, redeployed, 2, nil)
	if len(got) == 0 || !strings.Contains(strings.Join(got, " "), "DYN_NAMESPACE_WORKER_SUFFIX") {
		t.Fatalf("a redeploy with a new suffix: changed %q", got)
	}
	// A slot the record holds no configuration for (a capture from before
	// they were recorded) is not called changed: nothing says it is.
	delete(data, "processConfig.0")
	delete(data, "processConfig.1")
	if got := processConfigChanged(data, redeployed, 2, nil); len(got) != 0 {
		t.Fatalf("no recorded configuration: changed %q", got)
	}
}

func TestProcessConfigChangedComparesACompanionByItsContainer(t *testing.T) {
	r0 := stsRank("eng-0", "0", "1", "sts-c", true)
	etcd := stsRank("etcd-0", "0", "1", "sts-e", false)
	etcd.Name = "etcd-0"
	cc := map[string]string{"etcd-0": etcd.Spec.Containers[0].Name}
	old := []*corev1.Pod{stamped(r0, ""), stamped(etcd, cc["etcd-0"], corev1.EnvVar{Name: "ETCD_X", Value: "1"})}
	data := reconfigRecord(old, 1, cc)
	now := []*corev1.Pod{stamped(r0, ""), stamped(etcd, cc["etcd-0"], corev1.EnvVar{Name: "ETCD_X", Value: "2"})}
	if got := processConfigChanged(data, now, 1, cc); len(got) == 0 {
		t.Fatal("a companion's changed configuration is not seen")
	}
}

func TestClaimGroupRecord_ReplacesARecordOfAnotherConfiguration(t *testing.T) {
	c, kc, _ := groupFixture(t)
	ctx := context.Background()
	p := stsRank("eng-0", "0", "2", "sts-c", true)
	changed := func(map[string]string) bool { return true }
	same := func(map[string]string) bool { return false }

	old := time.Now().Add(-2 * recordReplaceCooldown).UTC().Format(time.RFC3339)
	setRecord(t, kc, map[string]string{"state": "complete", "captures": "3", "verifiedAt": old})
	if again, err := c.claimGroupRecord(ctx, "k1", 0, &p, 2, same); err != nil || again != nil {
		t.Fatalf("claim of a complete record of the same configuration = %v, %v", again, err)
	}
	claimed, err := c.claimGroupRecord(ctx, "k1", 0, &p, 2, changed)
	if err != nil || claimed == nil {
		t.Fatalf("claim of a complete record of another configuration = %v, %v; want it replaced", claimed, err)
	}
	// A new configuration gets its own attempts.
	if claimed.Data["state"] != criuGroupStateCapturing || claimed.Data["captures"] != "1" {
		t.Errorf("record = %v; want a first capture under way", claimed.Data)
	}
	for _, st := range []string{criuGroupStateCapturing, criuGroupStateVerifying} {
		setRecord(t, kc, map[string]string{"state": st, "captures": "1", "startedAt": "2999-01-01T00:00:00Z", "sourceNamespace": "fn"})
		if again, err := c.claimGroupRecord(ctx, "k1", 0, &p, 2, changed); err != nil || again != nil {
			t.Errorf("claim of a %s record = %v, %v; two configurations racing for one key: the first keeps it", st, again, err)
		}
	}
}

// A record younger than the cooldown is not replaced: a value that differs
// per instance rather than per deploy (or two configurations alternating
// during a rollout) costs at most one capture per cooldown, not one per
// instance.
func TestReplaceableRecordWaitsOutTheCooldown(t *testing.T) {
	now := time.Now()
	fresh := map[string]string{"state": criuGroupStateComplete, "verifiedAt": now.Add(-time.Minute).UTC().Format(time.RFC3339)}
	if replaceableRecord(fresh, now) {
		t.Fatal("replaced a record verified a minute ago")
	}
	old := map[string]string{"state": criuGroupStateComplete, "verifiedAt": now.Add(-2 * recordReplaceCooldown).UTC().Format(time.RFC3339)}
	if !replaceableRecord(old, now) {
		t.Fatal("kept a record past the cooldown")
	}
	failed := map[string]string{"state": criuGroupStateFailed, "startedAt": now.Add(-2 * recordReplaceCooldown).UTC().Format(time.RFC3339)}
	if !replaceableRecord(failed, now) {
		t.Fatal("a failed record of another configuration past the cooldown is not replaceable")
	}
}

// A companion is compared as admission compares it: its whole stamp, against
// any companion slot. Which of its containers the capture checkpointed may
// differ from instance to instance; an unchanged companion is not changed.
func TestProcessConfigChangedMatchesACompanionAgainstAnySlot(t *testing.T) {
	r0 := stsRank("eng-0", "0", "1", "sts-c", true)
	nats := stsRank("nats-0", "0", "1", "sts-n", false)
	nats.Name = "nats-0"
	etcd := stsRank("etcd-0", "0", "1", "sts-e", false)
	etcd.Name = "etcd-0"
	ctr := etcd.Spec.Containers[0].Name
	cc := map[string]string{"etcd-0": ctr, "nats-0": ctr}
	members := []*corev1.Pod{stamped(r0, ""), stamped(etcd, ctr, corev1.EnvVar{Name: "E", Value: "1"}), stamped(nats, ctr, corev1.EnvVar{Name: "N", Value: "1"})}
	data := reconfigRecord(members, 1, cc)
	data["companion.1"], data["companion.2"] = "true", "true"
	// The next instance lists its companions in the other order.
	swapped := []*corev1.Pod{members[0], members[2], members[1]}
	if got := processConfigChanged(data, swapped, 1, cc); len(got) != 0 {
		t.Fatalf("unchanged companions in another order: changed %q", got)
	}
}

// A failed record out of attempts sends the configuration's pods to the
// fallback; a pod of another process configuration is not that
// configuration's, and must reach the capture that replaces the record.
func TestCRIUFallbackSkipsARecordOfAnotherConfiguration(t *testing.T) {
	a, _ := fallbackAgent(t)
	own := fnKey(t, a)
	old := time.Now().Add(-2 * recordReplaceCooldown).UTC().Format(time.RFC3339)
	r0 := stsRank("eng-0", "0", "1", "sts-c", true)
	was := stamped(r0, "", corev1.EnvVar{Name: "DYN_NAMESPACE_WORKER_SUFFIX", Value: "a1"})
	now := stamped(r0, "", corev1.EnvVar{Name: "DYN_NAMESPACE_WORKER_SUFFIX", Value: "b7"})
	data := reconfigRecord([]*corev1.Pod{was}, 1, nil)
	data["state"], data["captures"], data["startedAt"], data[groupAgentBundleKey] = "failed", "3", old, fbBundle
	r := record(criuGroupConfigMapName(own), data)
	if _, err := a.kubeClient.CoreV1().ConfigMaps(a.criuFailedNamespace()).Create(context.Background(), &r, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, ok := a.criuFallbackFor(ctx, "fn", "cache://cfg", 0, webhook.ProcessConfig(was)); !ok {
		t.Fatal("the failed configuration itself does not fall back")
	}
	if fb, ok := a.criuFallbackFor(ctx, "fn", "cache://cfg", 0, webhook.ProcessConfig(now)); ok {
		t.Fatalf("a pod of another process configuration falls back: %+v", fb)
	}
	// Within the cooldown the record stays, and so does the fallback.
	r.Data["startedAt"] = time.Now().UTC().Format(time.RFC3339)
	if _, err := a.kubeClient.CoreV1().ConfigMaps(a.criuFailedNamespace()).Update(context.Background(), &r, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, ok := a.criuFallbackFor(ctx, "fn", "cache://cfg", 0, webhook.ProcessConfig(now)); !ok {
		t.Fatal("within the cooldown, another configuration's pod does not fall back")
	}
}

// The members' configurations written at the claim survive a capture that
// then fails: the record the failure persists is built from the same data.
func TestMemberConfigsAreInTheRecordSnapshot(t *testing.T) {
	r0 := stsRank("eng-0", "0", "1", "sts-c", true)
	m := stamped(r0, "", corev1.EnvVar{Name: "DYN_NAMESPACE_WORKER_SUFFIX", Value: "b7"})
	snapshot := map[string]string{"state": criuGroupStateCapturing}
	recordMemberConfigs(snapshot, []*corev1.Pod{m}, 1, nil)
	if snapshot["processConfig.0"] == "" {
		t.Fatal("the local record snapshot lacks the member's configuration")
	}
}

// reconfigFallbackRecord creates the failed (out of attempts, past the
// cooldown) record of fnKey with state and the configurations of members.
func reconfigFallbackRecord(t *testing.T, a *Agent, state string, members ...*corev1.Pod) {
	t.Helper()
	data := reconfigRecord(members, len(members), nil)
	old := time.Now().Add(-2 * recordReplaceCooldown).UTC().Format(time.RFC3339)
	data["state"], data["captures"], data["startedAt"], data[groupAgentBundleKey] = state, "3", old, fbBundle
	data["size"] = strconv.Itoa(len(members))
	r := record(criuGroupConfigMapName(fnKey(t, a)), data)
	if _, err := a.kubeClient.CoreV1().ConfigMaps(a.criuFailedNamespace()).Create(context.Background(), &r, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}

// A redeploy that changes only a worker's process configuration: the
// leader's slot still matches. Once a member of another configuration was
// admitted, the leader does not fall back either, or the instance (whose
// capture the leader leads) would never capture again and the
// configuration would start cold for good.
func TestCRIUFallbackLeaderFollowsAReconfiguredWorker(t *testing.T) {
	a, _ := fallbackAgent(t)
	ctx := context.Background()
	suffix := func(v string) corev1.EnvVar { return corev1.EnvVar{Name: "DYN_NAMESPACE_WORKER_SUFFIX", Value: v} }
	r0 := stamped(stsRank("eng-0", "0", "2", "sts-c", true), "", suffix("a1"))
	r1 := stamped(stsRank("eng-1", "1", "2", "sts-c", true), "", suffix("a1"))
	r1b := stamped(stsRank("eng-1", "1", "2", "sts-c", true), "", suffix("b7"))
	reconfigFallbackRecord(t, a, criuGroupStateFailed, r0, r1)

	if _, ok := a.criuFallbackFor(ctx, "fn", "cache://cfg", 0, webhook.ProcessConfig(r0)); !ok {
		t.Fatal("the failed configuration's leader does not fall back")
	}
	if fb, ok := a.criuFallbackFor(ctx, "fn", "cache://cfg", 1, webhook.ProcessConfig(r1b)); ok {
		t.Fatalf("a reconfigured worker falls back: %+v", fb)
	}
	if fb, ok := a.criuFallbackFor(ctx, "fn", "cache://cfg", 0, webhook.ProcessConfig(r0)); ok {
		t.Fatalf("after a reconfigured worker, the unchanged leader still falls back: %+v; the instance can never capture again", fb)
	}
	// And the capture the leader then leads replaces the record.
	if changed := processConfigChanged(getRecord(t, a, criuGroupConfigMapName(fnKey(t, a))), []*corev1.Pod{r0, r1b}, 2, nil); len(changed) == 0 {
		t.Fatal("the capture does not see the reconfigured worker")
	}
}

// A pod of another configuration that fails under the library replaces the
// record's configuration with its own: its replacement falls back without
// the library rather than fail under it again and again. A third
// configuration still reaches its capture.
func TestGPUShareFailureRecordsTheFailingConfiguration(t *testing.T) {
	a, _ := fallbackAgent(t)
	c := a.criuRestorer()
	ctx := context.Background()
	suffix := func(v string) corev1.EnvVar { return corev1.EnvVar{Name: "DYN_NAMESPACE_WORKER_SUFFIX", Value: v} }
	r0 := stsRank("eng-0", "0", "2", "sts-c", true)
	r1 := stsRank("eng-1", "1", "2", "sts-c", true)
	reconfigFallbackRecord(t, a, criuGroupStateGPUShareFailed, stamped(r0, "", suffix("a1")), stamped(r1, "", suffix("a1")))
	key := fnKey(t, a)

	b0, b1 := stamped(r0, "", suffix("b7")), stamped(r1, "", suffix("b7"))
	if fb, ok := a.criuFallbackFor(ctx, "fn", "cache://cfg", 0, webhook.ProcessConfig(b0)); ok {
		t.Fatalf("another configuration falls back: %+v", fb)
	}
	// It fails under the library, again on the same bundle.
	for i := 0; i < 2; i++ {
		if marked, err := c.markGPUShareFailed(ctx, key, fbBundle, "exit 1", "", b0, 0); err != nil || !marked {
			t.Fatalf("mark = %v, %v", marked, err)
		}
	}
	for ord, p := range []*corev1.Pod{b0, b1} {
		if fb, ok := a.criuFallbackFor(ctx, "fn", "cache://cfg", ord, webhook.ProcessConfig(p)); !ok || fb.Reason != webhook.FallbackGPUShareFailed {
			t.Fatalf("rank %d of the configuration that failed under the library: fallback %+v, %v; want %s", ord, fb, ok, webhook.FallbackGPUShareFailed)
		}
	}
	c0 := stamped(r0, "", suffix("c3"))
	if fb, ok := a.criuFallbackFor(ctx, "fn", "cache://cfg", 0, webhook.ProcessConfig(c0)); ok {
		t.Fatalf("a third configuration falls back: %+v", fb)
	}
}

// A failure under another configuration can rewrite the record between the
// admission's read and its reconfigured mark: the record then holds the
// admitted pod's own configuration. The mark must not land on it, or that
// configuration's pods skip their fallback and start under the library
// again until a capture replaces the record.
func TestMarkReconfiguredRechecksTheFreshRecord(t *testing.T) {
	a, cs := fallbackAgent(t)
	ctx := context.Background()
	suffix := func(v string) corev1.EnvVar { return corev1.EnvVar{Name: "DYN_NAMESPACE_WORKER_SUFFIX", Value: v} }
	r0 := stsRank("eng-0", "0", "2", "sts-c", true)
	r1 := stsRank("eng-1", "1", "2", "sts-c", true)
	reconfigFallbackRecord(t, a, criuGroupStateGPUShareFailed, stamped(r0, "", suffix("a1")), stamped(r1, "", suffix("a1")))
	b0, b1 := stamped(r0, "", suffix("b7")), stamped(r1, "", suffix("b7"))
	name := criuGroupConfigMapName(fnKey(t, a))
	ns := a.criuFailedNamespace()

	// The admission's first read sees the record of a1; meanwhile a failure
	// of b7 under the library rewrites it to b7 (markFailedConfiguration).
	first := true
	cs.PrependReactor("get", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		get := action.(k8stesting.GetAction)
		if !first || get.GetName() != name {
			return false, nil, nil
		}
		first = false
		obj, err := cs.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), ns, name)
		if err != nil {
			return true, nil, err
		}
		stale := obj.(*corev1.ConfigMap).DeepCopy()
		fresh := stale.DeepCopy()
		markFailedConfiguration(fresh.Data, b0, 0)
		if err := cs.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), fresh, ns); err != nil {
			return true, nil, err
		}
		return true, stale, nil
	})

	if fb, ok := a.criuFallbackFor(ctx, "fn", "cache://cfg", 0, webhook.ProcessConfig(b0)); ok {
		t.Fatalf("on the stale read, another configuration falls back: %+v", fb)
	}
	if at := getRecord(t, a, name)[groupReconfiguredKey]; at != "" {
		t.Fatalf("the record of the admitted pod's own configuration was marked reconfigured (%s)", at)
	}
	for ord, p := range []*corev1.Pod{b0, b1} {
		if fb, ok := a.criuFallbackFor(ctx, "fn", "cache://cfg", ord, webhook.ProcessConfig(p)); !ok || fb.Reason != webhook.FallbackGPUShareFailed {
			t.Fatalf("rank %d of the configuration that failed under the library: fallback %+v, %v; want %s", ord, fb, ok, webhook.FallbackGPUShareFailed)
		}
	}
}
