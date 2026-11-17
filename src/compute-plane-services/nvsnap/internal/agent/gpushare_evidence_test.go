// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// A pod replaced after failing under the library leaves on its record
// what tells a probe's kill from another: the last termination, its probes
// and grace period, and the pod's events. Its workload's log goes to the
// agent's log only, never to the record.
func TestConsiderGPUShareFailure_KeepsTheEvidence(t *testing.T) {
	a, cs := fallbackAgent(t)
	var agentLog bytes.Buffer
	l := logrus.New()
	l.SetOutput(&agentLog)
	a.log = l
	c := &criuAutoRestorer{a: a}
	ctx := context.Background()
	now := time.Now()
	pod := withLibrary(crashing(enginePod(0), 2, 137))
	term := pod.Status.ContainerStatuses[0].LastTerminationState.Terminated
	term.StartedAt, term.FinishedAt = metav1.NewTime(now.Add(-time.Minute)), metav1.NewTime(now)
	grace := int64(30)
	pod.Spec.TerminationGracePeriodSeconds = &grace
	pod.Spec.Containers[0].LivenessProbe = &corev1.Probe{
		ProbeHandler:  corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/health", Port: intstr.FromInt32(8000)}},
		PeriodSeconds: 10, TimeoutSeconds: 1, FailureThreshold: 3,
	}
	pod.OwnerReferences = []metav1.OwnerReference{ownedBy("ReplicaSet", true)}
	if _, err := cs.CoreV1().Pods("fn").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.CoreV1().Events("fn").Create(ctx, &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: "k", Namespace: "fn"},
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "fn", Name: "engine-0", UID: "uid-0", FieldPath: "spec.containers{engine}"},
		Reason:         "Killing", Message: "Container engine failed liveness probe, will be restarted", LastTimestamp: metav1.NewTime(now),
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	c.considerFallback(ctx, pod)

	raw := getRecord(t, a, criuGroupConfigMapName(fnKey(t, a)))[groupGPUShareEvidenceKey]
	var e gpushareEvidence
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		t.Fatalf("evidence %q: %v", raw, err)
	}
	if e.Pod != "fn/engine-0" || e.Container != "engine" || e.RestartCount != 2 {
		t.Errorf("evidence names %s %s restarts %d", e.Pod, e.Container, e.RestartCount)
	}
	if !strings.Contains(e.LastState, "reason Error, exit code 137") {
		t.Errorf("lastState %q", e.LastState)
	}
	if len(e.Probes) != 1 || e.Probes[0] != "liveness http /health:8000 initialDelay=0s period=10s timeout=1s failureThreshold=3" {
		t.Errorf("probes %q", e.Probes)
	}
	if e.GracePeriodS == nil || *e.GracePeriodS != 30 {
		t.Errorf("grace period %v", e.GracePeriodS)
	}
	if len(e.Events) != 1 || !strings.Contains(e.Events[0], "Killing spec.containers{engine}: Container engine failed liveness probe") {
		t.Errorf("events %q", e.Events)
	}
	// The fake clientset answers every log read with "fake logs".
	if strings.Contains(raw, "fake logs") {
		t.Error("the record keeps the workload's log")
	}
	if !strings.Contains(agentLog.String(), "the end of the failed run's log") || !strings.Contains(agentLog.String(), "fake logs") {
		t.Errorf("the agent's log lacks the failed run's log tail:\n%s", agentLog.String())
	}
}

// A pod nothing would recreate is left as it is, its logs with it: the
// agent does not copy them.
func TestConsiderGPUShareFailure_NoLogTailForAPodItKeeps(t *testing.T) {
	a, cs := fallbackAgent(t)
	var agentLog bytes.Buffer
	l := logrus.New()
	l.SetOutput(&agentLog)
	a.log = l
	c := &criuAutoRestorer{a: a}
	ctx := context.Background()
	pod := withLibrary(crashing(enginePod(0), 2, 137))
	if _, err := cs.CoreV1().Pods("fn").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	c.considerFallback(ctx, pod)
	if getRecord(t, a, criuGroupConfigMapName(fnKey(t, a)))[groupGPUShareEvidenceKey] == "" {
		t.Error("no evidence on the record")
	}
	if strings.Contains(agentLog.String(), "fake logs") {
		t.Error("the agent logged the log of a pod it leaves in place")
	}
}

// The record keeps the last gpushareEvidenceEvents events, oldest dropped
// first, each message bounded, and the whole within gpushareEvidenceMax.
func TestGPUShareEvidence_Bounded(t *testing.T) {
	pod := withLibrary(crashing(enginePod(0), 2, 137))
	base := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	var evs []corev1.Event
	for i := 49; i >= 0; i-- { // newest first: the order must not matter
		evs = append(evs, corev1.Event{Reason: fmt.Sprintf("R%02d", i), Message: strings.Repeat("m", 4096),
			LastTimestamp: metav1.NewTime(base.Add(time.Duration(i) * time.Second))})
	}
	e := newGPUShareEvidence(pod, "engine", pod.Status.ContainerStatuses[0].LastTerminationState.Terminated, evs)
	if len(e.Events) != gpushareEvidenceEvents || e.EventsDropped != 50-gpushareEvidenceEvents {
		t.Fatalf("kept %d events, dropped %d", len(e.Events), e.EventsDropped)
	}
	if !strings.Contains(e.Events[0], " R30") || !strings.Contains(e.Events[len(e.Events)-1], " R49") {
		t.Errorf("kept %q … %q, want R30 … R49", e.Events[0], e.Events[len(e.Events)-1])
	}
	for _, line := range e.Events {
		if len(line) > gpushareEventMessageMax+64 {
			t.Fatalf("event line of %d bytes", len(line))
		}
	}

	// Past the byte bound, the oldest events go first.
	big := gpushareEvidence{Pod: "fn/p", Container: "c"}
	for i := 0; i < 200; i++ {
		big.Events = append(big.Events, fmt.Sprintf("E%03d %s", i, strings.Repeat("x", 200)))
	}
	s := big.encode()
	if len(s) > gpushareEvidenceMax {
		t.Fatalf("encoded %d bytes, max %d", len(s), gpushareEvidenceMax)
	}
	var back gpushareEvidence
	if err := json.Unmarshal([]byte(s), &back); err != nil {
		t.Fatal(err)
	}
	if back.EventsDropped == 0 || !strings.HasPrefix(back.Events[len(back.Events)-1], "E199") {
		t.Errorf("dropped %d, last %q", back.EventsDropped, back.Events[len(back.Events)-1][:4])
	}
}

func TestDescribeProbe(t *testing.T) {
	for want, p := range map[string]*corev1.Probe{
		"startup tcp :8000 initialDelay=5s period=2s timeout=1s failureThreshold=600": {
			ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(8000)}}, InitialDelaySeconds: 5, PeriodSeconds: 2, TimeoutSeconds: 1, FailureThreshold: 600},
		"startup grpc :9000 initialDelay=0s period=0s timeout=0s failureThreshold=0": {
			ProbeHandler: corev1.ProbeHandler{GRPC: &corev1.GRPCAction{Port: 9000}}},
		"startup exec initialDelay=0s period=0s timeout=0s failureThreshold=0": {
			ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"secret-token"}}}},
	} {
		if got := describeProbe("startup", p); got != want {
			t.Errorf("describeProbe = %q, want %q", got, want)
		}
	}
}

// A probe path with no length limit, full of characters JSON escapes,
// still leaves valid JSON within the bound once every event is gone: the
// fields are cut, never the JSON.
func TestGPUShareEvidence_OversizedEscapedProbeStaysValidJSON(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "fn"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{
		Name: "inference",
		LivenessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
			Path: "/" + strings.Repeat("<&>", 20000), Port: intstr.FromInt(8000)}}},
	}}}}
	enc := newGPUShareEvidence(pod, "inference", &corev1.ContainerStateTerminated{ExitCode: 137}, nil).encode()
	if enc == "" || len(enc) > gpushareEvidenceMax {
		t.Fatalf("encoded %d bytes, want 1..%d", len(enc), gpushareEvidenceMax)
	}
	var back gpushareEvidence
	if err := json.Unmarshal([]byte(enc), &back); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}
	if back.Pod != "fn/p" || len(back.Probes) != 1 {
		t.Errorf("decoded %+v", back)
	}
}

// The log read is of the run lastFailure chose: the current run when it
// has terminated with a failure, else the previous one.
func TestFailedRunLogOptions(t *testing.T) {
	status := func(cur *corev1.ContainerStateTerminated) *corev1.Pod {
		return &corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name: "inference", RestartCount: 1,
			State:                corev1.ContainerState{Terminated: cur},
			LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}},
		}}}}
	}
	for name, tc := range map[string]struct {
		cur          *corev1.ContainerStateTerminated
		wantPrevious bool
	}{
		"current run failed":      {&corev1.ContainerStateTerminated{ExitCode: 137}, false},
		"current run running":     {nil, true},
		"current run exited zero": {&corev1.ContainerStateTerminated{ExitCode: 0}, true},
	} {
		o := failedRunLogOptions(status(tc.cur), "inference")
		if o.Previous != tc.wantPrevious {
			t.Errorf("%s: Previous %v, want %v", name, o.Previous, tc.wantPrevious)
		}
		if o.Container != "inference" || *o.TailLines != gpushareLogTailLines {
			t.Errorf("%s: options %+v", name, o)
		}
	}
}
