// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// Evidence of a pod that did not start under the gpushare library. The
// agent deletes such a pod so its replacement starts without the library
// (considerGPUShareFailure), and with it go its events and its failed
// runs' logs: what was left of one (exit code 137, reason Error) could
// not tell a probe's kill from any other. Before the delete, the mark on
// the configuration's group record keeps the failed container's last
// termination, its probes and the pod's last events (their messages bounded; a termination message or a probe failure can quote the workload),
// and the agent's own log keeps the end of the failed run's log.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/tracing"
)

const (
	// groupGPUShareEvidenceKey holds the evidence on the group record.
	groupGPUShareEvidenceKey = "gpushareEvidence"
	// gpushareEvidenceMax bounds it; the oldest events go first.
	gpushareEvidenceMax = 16 << 10
	// gpushareEvidenceEvents is how many of the pod's last events it keeps.
	gpushareEvidenceEvents = 20
	// gpushareEventMessageMax bounds each event's message.
	gpushareEventMessageMax = 256
	// gpushareEvidenceFieldMax bounds each other field when the evidence
	// is still too large without events.
	gpushareEvidenceFieldMax = 1 << 10
	// gpushareLogTailLines and gpushareLogTailMax bound the failed run's
	// log the agent logs.
	gpushareLogTailLines = 200
	gpushareLogTailMax   = 16 << 10
	// gpushareLogTailReadMax bounds the read of those lines.
	gpushareLogTailReadMax = 4 << 20
	// gpushareEvidenceTimeout bounds each read of the evidence.
	gpushareEvidenceTimeout = 5 * time.Second
)

// gpushareEvidence is what the record keeps of a pod's failed starts.
type gpushareEvidence struct {
	Pod           string   `json:"pod"`
	Container     string   `json:"container"`
	RestartCount  int32    `json:"restartCount"`
	LastState     string   `json:"lastState"`
	Probes        []string `json:"probes,omitempty"`
	GracePeriodS  *int64   `json:"terminationGracePeriodSeconds,omitempty"`
	Events        []string `json:"events,omitempty"`
	EventsDropped int      `json:"eventsDropped,omitempty"`
	// bounded: encode has cut the fields to gpushareEvidenceFieldMax.
	bounded bool
}

// newGPUShareEvidence is the evidence of container of pod, failed in its
// last run t, with the pod's events evs (any order).
func newGPUShareEvidence(pod *corev1.Pod, container string, t *corev1.ContainerStateTerminated, evs []corev1.Event) gpushareEvidence {
	e := gpushareEvidence{Pod: pod.Namespace + "/" + pod.Name, Container: container, GracePeriodS: pod.Spec.TerminationGracePeriodSeconds}
	for i := range pod.Status.ContainerStatuses {
		if cs := &pod.Status.ContainerStatuses[i]; cs.Name == container {
			e.RestartCount = cs.RestartCount
		}
	}
	if t != nil {
		e.LastState = fmt.Sprintf("reason %s, exit code %d, signal %d, ran %s to %s", t.Reason, t.ExitCode, t.Signal,
			t.StartedAt.UTC().Format(time.RFC3339), t.FinishedAt.UTC().Format(time.RFC3339))
		if m := strings.TrimSpace(t.Message); m != "" {
			e.LastState += ": " + truncateMiddle(m, terminationMessageMax)
		}
	}
	for i := range pod.Spec.Containers {
		if c := &pod.Spec.Containers[i]; c.Name == container {
			for _, p := range []struct {
				kind  string
				probe *corev1.Probe
			}{{"liveness", c.LivenessProbe}, {"readiness", c.ReadinessProbe}, {"startup", c.StartupProbe}} {
				if p.probe != nil {
					e.Probes = append(e.Probes, describeProbe(p.kind, p.probe))
				}
			}
		}
	}
	sorted := append([]corev1.Event(nil), evs...)
	sort.SliceStable(sorted, func(i, j int) bool { return eventTime(&sorted[i]).Before(eventTime(&sorted[j])) })
	if n := len(sorted); n > gpushareEvidenceEvents {
		e.EventsDropped = n - gpushareEvidenceEvents
		sorted = sorted[n-gpushareEvidenceEvents:]
	}
	for i := range sorted {
		ev := &sorted[i]
		line := fmt.Sprintf("%s %s", eventTime(ev).UTC().Format(time.RFC3339), ev.Reason)
		if ev.Count > 1 {
			line += fmt.Sprintf(" (x%d)", ev.Count)
		}
		if ev.InvolvedObject.FieldPath != "" {
			line += " " + ev.InvolvedObject.FieldPath
		}
		e.Events = append(e.Events, line+": "+truncateMiddle(strings.TrimSpace(ev.Message), gpushareEventMessageMax))
	}
	return e
}

// describeProbe is a probe's kind, handler and timing.
func describeProbe(kind string, p *corev1.Probe) string {
	h := "unknown"
	switch {
	case p.HTTPGet != nil:
		h = fmt.Sprintf("http %s:%s", p.HTTPGet.Path, p.HTTPGet.Port.String())
	case p.TCPSocket != nil:
		h = "tcp :" + p.TCPSocket.Port.String()
	case p.GRPC != nil:
		h = fmt.Sprintf("grpc :%d", p.GRPC.Port)
	case p.Exec != nil:
		h = "exec"
	}
	return fmt.Sprintf("%s %s initialDelay=%ds period=%ds timeout=%ds failureThreshold=%d",
		kind, h, p.InitialDelaySeconds, p.PeriodSeconds, p.TimeoutSeconds, p.FailureThreshold)
}

// encode is the evidence as the record keeps it, within
// gpushareEvidenceMax: the oldest events go first, then every event; then
// the remaining fields are bounded (a probe's HTTP path has no limit), and
// last the probes go. Fields are cut, never the JSON, which stays valid.
func (e gpushareEvidence) encode() string {
	for {
		b, err := json.Marshal(e)
		if err != nil {
			return ""
		}
		switch {
		case len(b) <= gpushareEvidenceMax:
			return string(b)
		case len(e.Events) > 0:
			e.Events = e.Events[1:]
			e.EventsDropped++
		case !e.bounded:
			e.bounded = true
			e.Pod = truncateMiddle(e.Pod, gpushareEvidenceFieldMax)
			e.Container = truncateMiddle(e.Container, gpushareEvidenceFieldMax)
			e.LastState = truncateMiddle(e.LastState, gpushareEvidenceFieldMax)
			for i := range e.Probes {
				e.Probes[i] = truncateMiddle(e.Probes[i], gpushareEvidenceFieldMax)
			}
		case len(e.Probes) > 0:
			e.Probes = nil
		default:
			return ""
		}
	}
}

// gpushareFailureEvidence reads the evidence of pod's failed container
// (lastFailure). "" when it has none. A read that fails leaves out what it
// would have read.
func (a *Agent) gpushareFailureEvidence(ctx context.Context, pod *corev1.Pod, log *logrus.Entry) string {
	name, t, _ := lastFailure(pod)
	if name == "" || t == nil || a.kubeClient == nil {
		return ""
	}
	var evs []corev1.Event
	ectx, cancel := context.WithTimeout(ctx, gpushareEvidenceTimeout)
	ectx, span := tracing.Tracer().Start(ectx, "gpushare.evidence.events", trace.WithSpanKind(trace.SpanKindClient))
	list, err := a.kubeClient.CoreV1().Events(pod.Namespace).List(ectx, metav1.ListOptions{FieldSelector: "involvedObject.uid=" + string(pod.UID)})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "list the pod's events")
	}
	span.End()
	cancel()
	if err != nil {
		log.WithError(err).Info("gpushare: cannot read the failed pod's events; its evidence goes without them")
	} else {
		evs = list.Items
	}
	return newGPUShareEvidence(pod, name, t, evs).encode()
}

// logFailedRunTail logs the end of container's previous run's log: the
// workload's own output, kept in the agent's log (and its retention) only,
// never on the record.
func (a *Agent) logFailedRunTail(ctx context.Context, pod *corev1.Pod, container string, log *logrus.Entry) {
	lctx, cancel := context.WithTimeout(ctx, gpushareEvidenceTimeout)
	defer cancel()
	lctx, span := tracing.Tracer().Start(lctx, "gpushare.evidence.log_tail", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	// The kubelet cuts LimitBytes from the start of the tail; the end, where
	// a failure says why, is kept here instead. The limit only bounds the
	// read.
	b, err := a.kubeClient.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, failedRunLogOptions(pod, container)).DoRaw(lctx)
	l := log.WithField("container", container)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "read the failed run's log")
		l.WithError(err).Info("gpushare: cannot read the failed run's log")
		return
	}
	l.WithField("logTail", lastBytes(string(b), gpushareLogTailMax)).Warn("gpushare: the end of the failed run's log, before the pod is replaced")
}

// failedRunLogOptions reads the log of the run lastFailure chose: the
// container's current run when that run has terminated with a failure
// (the kubelet has not restarted it), else its previous one.
func failedRunLogOptions(pod *corev1.Pod, container string) *corev1.PodLogOptions {
	lines, limit := int64(gpushareLogTailLines), int64(gpushareLogTailReadMax)
	previous := true
	for i := range pod.Status.ContainerStatuses {
		if cs := &pod.Status.ContainerStatuses[i]; cs.Name == container {
			if cur := cs.State.Terminated; cur != nil && (cur.ExitCode != 0 || cur.Signal != 0) {
				previous = false
			}
		}
	}
	return &corev1.PodLogOptions{Container: container, Previous: previous, TailLines: &lines, LimitBytes: &limit}
}

// keepEvidence puts a failed pod's evidence on marked record data d; none
// leaves what an earlier pod's mark kept.
func keepEvidence(d map[string]string, evidence string) {
	if evidence != "" {
		d[groupGPUShareEvidenceKey] = evidence
	}
}

// lastBytes is the end of s, at most n bytes, marked when cut.
func lastBytes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "… " + s[len(s)-n:]
}
