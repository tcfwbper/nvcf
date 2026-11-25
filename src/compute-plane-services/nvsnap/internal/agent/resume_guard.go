// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// After a leave-running capture resumes its workload, the capture is
// reported a success only once the workload is seen running again:
//
//   - On each member's node (dumpV2): every GPU process the capture
//     suspended still runs after the resume (membersAfterResume), and the
//     kernel reported no GPU fault over the capture (kmsg.go).
//   - On the group's driver (groupCheckpoint), once the members' holds
//     have ended: every member's pod is Ready again within
//     resumeWatchOpts.ReadyWait, and stays so for Steady, with none of its
//     containers restarted and the pod not replaced
//     (watchMembersAfterResume).
//
// The limit: neither watches the workload's GPU work. A workload whose
// GPU work hangs after the resume while its processes run and its
// readiness probe still passes (an engine's health endpoint answers from
// its API process) passes the pod watch; such a hang seen so far came
// with an Xid in the kernel log, which the node's check catches. A
// failure of either is a failed capture (metrics reasons gpu_xid and
// resume_stalled); neither deletes a pod.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/metrics"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// errResumeStalled marks a leave-running capture whose workload did not
// run again after its resume.
var errResumeStalled = errors.New(metrics.ResumeStalledText)

// membersAfterResume checks, after a leave-running resume, that every
// GPU process of the workload (session leader hostPID) the capture
// suspended (pids, namespace pids) still runs.
func membersAfterResume(procBase string, hostPID int, pids []int) error {
	now, _, _, err := gpushareTargets(procBase, hostPID)
	if err != nil {
		return fmt.Errorf("%w: cannot read the workload's GPU processes: %w", errResumeStalled, err)
	}
	var gone []string
	for _, p := range pids {
		if !slices.Contains(now, p) {
			gone = append(gone, fmt.Sprint(p))
		}
	}
	if len(gone) > 0 {
		return fmt.Errorf("%w: GPU process(es) %s of the workload ended after their resume", errResumeStalled, strings.Join(gone, ","))
	}
	return nil
}

// resumeWatchOpts bound the driver's watch of the members' pods after a
// leave-running group capture.
type resumeWatchOpts struct {
	// ReadyWait is how long a pod has to be Ready again: its readiness
	// probe fails while the capture holds it, and the kubelet sees it pass
	// again only at its next probe.
	ReadyWait time.Duration
	// Steady is how long every pod stays Ready, none of its containers
	// restarted, before the capture is reported a success.
	Steady time.Duration
	Poll   time.Duration
}

// postResumeWatch is the watch of a leave-running group capture.
var postResumeWatch = resumeWatchOpts{ReadyWait: 60 * time.Second, Steady: 10 * time.Second, Poll: 2 * time.Second}

// memberBaseline is a member's pod as it was when the capture began.
type memberBaseline struct {
	ns, name string
	uid      types.UID
	restarts map[string]int32
}

func baselineOf(p *corev1.Pod) memberBaseline {
	b := memberBaseline{ns: p.Namespace, name: p.Name, uid: p.UID, restarts: map[string]int32{}}
	for _, c := range watchedStatuses(p) {
		b.restarts[c.Name] = c.RestartCount
	}
	return b
}

// watchedStatuses are the statuses of the containers that run as long as
// the pod: its containers and its restartable init containers (sidecars,
// restartPolicy Always), which the kubelet restarts on their own. A
// one-shot init container has completed before the pod ran, and is left
// out.
func watchedStatuses(p *corev1.Pod) []corev1.ContainerStatus {
	st := slices.Clone(p.Status.ContainerStatuses)
	for i := range p.Status.InitContainerStatuses {
		if s := &p.Status.InitContainerStatuses[i]; restartableInit(p, s.Name) {
			st = append(st, *s)
		}
	}
	return st
}

func restartableInit(p *corev1.Pod, name string) bool {
	for i := range p.Spec.InitContainers {
		if c := &p.Spec.InitContainers[i]; c.Name == name {
			return c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways
		}
	}
	return false
}

// memberState reads one member's pod: fatal, when it ended or was
// replaced since the capture began; else whether it is Ready, and why not.
func memberState(ctx context.Context, cs kubernetes.Interface, b memberBaseline) (fatal string, ready bool, why string) {
	p, err := cs.CoreV1().Pods(b.ns).Get(ctx, b.name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return "the pod is gone", false, ""
	case err != nil:
		return "", false, "cannot be read: " + err.Error()
	case p.UID != b.uid:
		return "the pod was replaced", false, ""
	case p.DeletionTimestamp != nil:
		return "the pod is being deleted", false, ""
	}
	st := watchedStatuses(p)
	for i := range st {
		c := &st[i]
		if n, ok := b.restarts[c.Name]; ok && c.RestartCount > n {
			return fmt.Sprintf("container %s restarted%s", c.Name, lastTermination(c.LastTerminationState.Terminated)), false, ""
		}
		if t := c.State.Terminated; t != nil {
			return fmt.Sprintf("container %s ended%s", c.Name, lastTermination(t)), false, ""
		}
	}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			if c.Status == corev1.ConditionTrue {
				return "", true, ""
			}
			return "", false, "not Ready"
		}
	}
	return "", false, "not Ready (no condition)"
}

func lastTermination(t *corev1.ContainerStateTerminated) string {
	if t == nil {
		return ""
	}
	return fmt.Sprintf(" (%s, exit code %d)", t.Reason, t.ExitCode)
}

// watchMembersAfterResume watches the members' pods after a leave-running
// group capture resumed them: every one Ready within o.ReadyWait, then all
// of them Ready for o.Steady, with no container restarted or ended and no
// pod replaced. A failure, or a watch cut short, is errResumeStalled. Each
// pod read ends at the watch's deadline (the kube client has no timeout of
// its own); a read it ends counts as not Ready.
func watchMembersAfterResume(ctx context.Context, cs kubernetes.Interface, members []memberBaseline, o resumeWatchOpts) error {
	start := time.Now()
	deadline := start.Add(o.ReadyWait + o.Steady)
	readCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	var readySince time.Time
	for {
		allReady := true
		var notReady []string
		for _, b := range members {
			fatal, ready, why := memberState(readCtx, cs, b)
			if fatal != "" {
				return fmt.Errorf("%w: %s/%s: %s", errResumeStalled, b.ns, b.name, fatal)
			}
			if !ready {
				allReady = false
				notReady = append(notReady, fmt.Sprintf("%s/%s: %s", b.ns, b.name, why))
			}
		}
		now := time.Now()
		switch {
		case allReady && readySince.IsZero():
			readySince = now
		case !allReady:
			readySince = time.Time{}
		}
		if allReady && now.Sub(readySince) >= o.Steady {
			return nil
		}
		if !allReady && now.Sub(start) >= o.ReadyWait || now.After(deadline) {
			why := strings.Join(notReady, "; ")
			if why == "" {
				why = fmt.Sprintf("Ready, but not for %s on end", o.Steady)
			}
			return fmt.Errorf("%w: %s after the resume: %s", errResumeStalled, now.Sub(start).Round(time.Second), why)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: the watch after the resume was cut short (%w); the workload was not seen running", errResumeStalled, ctx.Err())
		case <-time.After(o.Poll):
		}
	}
}
