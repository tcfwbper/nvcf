// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// Shared companions of a Grove scaling group with several replicas.
//
// A Dynamo deployment whose worker scaling group (a Grove
// PodCliqueScalingGroup) runs more than one replica serves every replica
// from one etcd, one NATS and one frontend. An nvsnap instance is one
// replica (sameInstance), with the companions it talks to. So a capture
// of one replica would stop companions the other replicas still use, and
// every replica's restore would restore the same companions again: two
// restores into one etcd pod. Until an instance spans every replica of
// its scaling group, such a scaling group is neither captured nor
// restored as a group: its pods, and its companions, start cold.
//
// The check fails closed: when the replica count cannot be read, the
// pod is treated as one of several replicas.

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/modelvolume"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

// groveReadTries and groveReadBackoff bound the retries of a failed read
// of the scaling groups.
var (
	groveReadTries   = 3
	groveReadBackoff = 100 * time.Millisecond
)

// groveScalingGroups is the Grove scaling group resource.
var groveScalingGroups = schema.GroupVersionResource{Group: "grove.io", Version: "v1alpha1", Resource: "podcliquescalinggroups"}

// groveScalingGroupReplicas returns the replicas of each Grove scaling
// group in ns, by name; none, without an error, when the cluster has no
// Grove.
func (a *Agent) groveScalingGroupReplicas(ctx context.Context, ns string) (map[string]int, error) {
	if a.groveReplicas != nil {
		return a.groveReplicas(ctx, ns)
	}
	if a.dynClient == nil {
		return nil, fmt.Errorf("no client for custom resources")
	}
	var list *unstructured.UnstructuredList
	var err error
	// A few quick tries: a passing API error must not decide a pod's fate.
	for i := 0; i < groveReadTries; i++ {
		list, err = a.dynClient.Resource(groveScalingGroups).Namespace(ns).List(ctx, metav1.ListOptions{})
		if err == nil || apierrors.IsNotFound(err) || apierrors.IsForbidden(err) || ctx.Err() != nil {
			break
		}
		if i == groveReadTries-1 {
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(i+1) * groveReadBackoff):
		}
	}
	if apierrors.IsNotFound(err) {
		return map[string]int{}, nil // no Grove on this cluster
	}
	if err != nil {
		return nil, fmt.Errorf("list Grove scaling groups in %s: %w", ns, err)
	}
	out := make(map[string]int, len(list.Items))
	for _, it := range list.Items {
		n := int64(1) // the API default
		if r, found, err := unstructured.NestedInt64(it.Object, "spec", "replicas"); err == nil && found {
			n = r
		}
		out[it.GetName()] = int(n)
	}
	return out, nil
}

// groveReplicaFloor is the fewest replicas pod's own labels prove its
// scaling group has: one more than its replica index.
func groveReplicaFloor(pod *corev1.Pod) int {
	if r, err := strconv.Atoi(pod.Labels[webhook.GroveScalingGroupReplicaLabel]); err == nil && r >= 0 {
		return r + 1
	}
	return 1
}

// groveInstanceCount returns how many Grove instances (scaling group
// replicas) the namespace runs, which all use its companions: the
// replicas of all its scaling groups (several worker groups, prefill and
// decode groups, several replicas of a PodCliqueSet), and at least what
// pods' labels (some of the namespace's pods, with self) prove. err when
// they cannot be read, or self's scaling group is not found, while Grove
// pods are there.
func (a *Agent) groveInstanceCount(ctx context.Context, ns string, pods []corev1.Pod, self *corev1.Pod) (int, error) {
	floors := map[string]int{}
	note := func(p *corev1.Pod) {
		if groveInstance(p) == "" || p.DeletionTimestamp != nil {
			return
		}
		g := p.Labels[webhook.GroveScalingGroupLabel]
		floors[g] = max(floors[g], groveReplicaFloor(p))
	}
	for i := range pods {
		note(&pods[i])
	}
	if self != nil {
		note(self)
	}
	floor := 0
	for _, n := range floors {
		floor += n
	}
	replicas, err := a.groveScalingGroupReplicas(ctx, ns)
	if err != nil {
		if len(floors) == 0 {
			return 0, nil // no Grove pod here
		}
		return 0, err
	}
	if self != nil && groveInstance(self) != "" {
		if _, ok := replicas[self.Labels[webhook.GroveScalingGroupLabel]]; !ok {
			return 0, fmt.Errorf("scaling group %s not found", self.Labels[webhook.GroveScalingGroupLabel])
		}
	}
	if len(floors) > 0 && len(replicas) == 0 {
		return 0, fmt.Errorf("no scaling group found for the namespace's Grove pods")
	}
	sum := 0
	for _, n := range replicas {
		sum += n
	}
	return max(sum, floor), nil
}

// sharedCompanionsReason says why pod, a GPU rank, may not be captured or
// restored as a group: the namespace runs several Grove instances, which
// share their companions. "" for a pod of no scaling group, or the only
// instance of its namespace. err when that cannot be read yet: callers
// that can try again later do; admission fails closed.
//
// A Dynamo rank of no scaling group (a single-node prefill or decode
// worker, say) fails closed too: any other Dynamo GPU worker of another
// controller in pods shares the namespace's etcd, NATS and frontend with
// it, whatever labels Grove gave them. With no pods given (a check again
// just before a hold, or before a restore) the namespace's pods are listed
// now, so a worker that started since an earlier list is seen.
func (a *Agent) sharedCompanionsReason(ctx context.Context, pod *corev1.Pod, pods []corev1.Pod) (string, error) {
	if groveInstance(pod) == "" {
		// A rank that registers through Kubernetes shares no etcd or NATS:
		// its group is checked by the companions it captured
		// (kubeSharedCompanions).
		if webhook.DynamoComponent(pod) && !kubeDiscoveryRank(pod) {
			if pods == nil {
				list, err := a.kubeClient.CoreV1().Pods(pod.Namespace).List(ctx, metav1.ListOptions{})
				if err != nil {
					return "", fmt.Errorf("list the namespace's pods: %w", err)
				}
				pods = list.Items
			}
			if other := otherDynamoWorker(pod, pods); other != "" {
				return fmt.Sprintf("the namespace also runs Dynamo worker %s of another instance, which shares the companions (etcd, NATS, frontend); a group capture or restore of one would stop or restore what the other uses", other), nil
			}
		}
		return "", nil
	}
	n, err := a.groveInstanceCount(ctx, pod.Namespace, pods, pod)
	if err != nil {
		return "", fmt.Errorf("read the namespace's Grove scaling groups: %w", err)
	}
	if n > 1 {
		return fmt.Sprintf("the namespace runs %d Grove scaling group replicas, which share their companions (etcd, NATS, frontend); a group capture or restore of one would stop or restore what the others use", n), nil
	}
	return "", nil
}

// kubeSharedCompanions says why the group of pod, a rank that registers
// through Kubernetes and is of no scaling group, may not be captured or
// restored: it has companions (pods without GPUs it connects to) and the
// namespace runs a Dynamo GPU worker of another instance, which may use
// them. "" when the group has no companion: each worker of a
// disaggregated deployment (prefill, decode) registers through its own
// DynamoWorkerMetadata, so it is captured and restored apart from the
// others. With no pods given the namespace's pods are listed now.
func (a *Agent) kubeSharedCompanions(ctx context.Context, pod *corev1.Pod, pods []corev1.Pod, companions bool) (string, error) {
	if !companions || groveInstance(pod) != "" || !kubeDiscoveryRank(pod) {
		return "", nil
	}
	if pods == nil {
		list, err := a.kubeClient.CoreV1().Pods(pod.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return "", fmt.Errorf("list the namespace's pods: %w", err)
		}
		pods = list.Items
	}
	if other := otherDynamoWorker(pod, pods); other != "" {
		return fmt.Sprintf("the namespace also runs Dynamo worker %s of another instance, which may use this instance's companions; a group capture or restore of one would stop or restore what the other uses", other), nil
	}
	return "", nil
}

// recordHasCompanions reports a group record that captured companions.
func recordHasCompanions(d map[string]string) bool {
	for k, v := range d {
		if strings.HasPrefix(k, "companion.") && v == "true" {
			return true
		}
	}
	return false
}

// namespaceSharesCompanions says why a companion in ns may not restore
// with a group: the namespace runs several Grove instances, or their
// count cannot be read while Grove pods are there. "" otherwise.
func (a *Agent) namespaceSharesCompanions(ctx context.Context, ns string, pods []corev1.Pod) string {
	n, err := a.groveInstanceCount(ctx, ns, pods, nil)
	if err != nil {
		return fmt.Sprintf("cannot read the namespace's Grove scaling groups (%v); their replicas may share the companions", err)
	}
	if n > 1 {
		return fmt.Sprintf("the namespace runs %d Grove scaling group replicas, which share their companions", n)
	}
	return ""
}

// claimedByAnother reports whether p is a companion claimed by a group
// restore other than leader's whose leader is among pods and not being
// deleted: that restore's to restore or end, not leader's.
func claimedByAnother(p, leader *corev1.Pod, pods []corev1.Pod) bool {
	by := p.Annotations[companionClaimAnnotation]
	if by == "" || by == string(leader.UID) || !isCompanion(p) {
		return false
	}
	for i := range pods {
		if string(pods[i].UID) == by {
			return pods[i].DeletionTimestamp == nil
		}
	}
	return false
}

// companionClaimAnnotation names the leader (by pod UID) of the group
// restore that restores a companion placeholder.
const companionClaimAnnotation = "nvsnap.io/criu-companion-claimed-by"

// errCompanionClaimed is a companion placeholder another group restore
// has claimed.
type errCompanionClaimed struct{ pod, by string }

func (e errCompanionClaimed) Error() string {
	return fmt.Sprintf("companion %s is claimed by the restore of %s", e.pod, e.by)
}

// leaderAlive reports whether a pod of ns with the given UID exists and
// is not being deleted.
func leaderAlive(ctx context.Context, kc kubernetes.Interface, ns, uid string) (bool, error) {
	pods, err := kc.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	for i := range pods.Items {
		if string(pods.Items[i].UID) == uid {
			return pods.Items[i].DeletionTimestamp == nil, nil
		}
	}
	return false, nil
}

// claimReadHook, when set, runs between a claim's read and its write; for
// tests.
var claimReadHook func(pod string)

// claimCompanions claims each companion among members for leader's group
// restore, so no other restore restores it: the claim is written with
// the pod's resourceVersion, so of two leaders claiming at once one
// wins. A companion already claimed by leader stays claimed; one claimed
// by a leader that is gone or being deleted is taken over.
func claimCompanions(ctx context.Context, kc kubernetes.Interface, members []*corev1.Pod, leader *corev1.Pod) error {
	for _, m := range members {
		if m == nil || !isCompanion(m) {
			continue
		}
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			p, err := kc.CoreV1().Pods(m.Namespace).Get(ctx, m.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			switch by := p.Annotations[companionClaimAnnotation]; by {
			case string(leader.UID):
				return nil
			case "":
			default:
				alive, aerr := leaderAlive(ctx, kc, p.Namespace, by)
				if aerr != nil {
					return aerr
				}
				if alive {
					return errCompanionClaimed{pod: p.Name, by: by}
				}
				// Its leader is gone: the restore it drove is over.
			}
			if claimReadHook != nil {
				claimReadHook(p.Name)
			}
			// The resourceVersion makes the patch conditional: a pod
			// changed since the read (another leader's claim) is a
			// conflict, and the claim is read again.
			patch, err := json.Marshal(map[string]any{"metadata": map[string]any{
				"resourceVersion": p.ResourceVersion,
				"annotations":     map[string]string{companionClaimAnnotation: string(leader.UID)},
			}})
			if err != nil {
				return err
			}
			_, err = kc.CoreV1().Pods(p.Namespace).Patch(ctx, p.Name, types.MergePatchType, patch, metav1.PatchOptions{})
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// deleteCompanionUnlessClaimed deletes companion p, as leader's restore
// ends, unless another live leader has claimed it since p was read: the
// delete carries p's resourceVersion, which a claim changes, and a
// conflict is answered by reading the pod again.
func deleteCompanionUnlessClaimed(ctx context.Context, kc kubernetes.Interface, p, leader *corev1.Pod, log *logrus.Entry) error {
	uid, rv := p.UID, p.ResourceVersion
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		opts := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}
		if rv != "" {
			opts.Preconditions.ResourceVersion = &rv
		}
		err = kc.CoreV1().Pods(p.Namespace).Delete(ctx, p.Name, opts)
		if err == nil || apierrors.IsNotFound(err) {
			return nil
		}
		if !apierrors.IsConflict(err) {
			continue
		}
		cur, gerr := kc.CoreV1().Pods(p.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(gerr) || (gerr == nil && cur.UID != uid) {
			return nil // gone, or replaced: not this restore's to delete
		}
		if gerr != nil {
			err = gerr
			continue
		}
		if by := cur.Annotations[companionClaimAnnotation]; by != "" && by != string(leader.UID) {
			alive, aerr := leaderAlive(ctx, kc, cur.Namespace, by)
			if aerr != nil {
				err = aerr
				continue
			}
			if alive {
				log.WithFields(logrus.Fields{"placeholder": cur.Name, "claimedBy": by}).
					Info("CRIU group restore: a companion is now another restore's; not deleting it")
				return nil
			}
		}
		rv = cur.ResourceVersion
	}
	return err
}

// otherDynamoWorker names a Dynamo GPU worker in pods, not being deleted,
// of another instance than pod's (sameInstance): it shares the
// namespace's companions. "" when there is none.
func otherDynamoWorker(pod *corev1.Pod, pods []corev1.Pod) string {
	// An instance of one pod (a replica of a Deployment, whose replicas
	// share their controller) has no other ranks.
	_, size, ranked := podRank(pod, modelvolume.CacheOrdinalAnnotation, modelvolume.CacheGroupSizeAnnotation)
	alone := ranked && size == 1
	for i := range pods {
		p := &pods[i]
		if p.UID == pod.UID || p.DeletionTimestamp != nil || !webhook.DynamoComponent(p) || !requestsGPU(p) {
			continue
		}
		if !alone && sameInstance(pod, p) {
			continue // a rank of its own instance (LeaderWorkerSet group, controller)
		}
		return p.Name
	}
	return ""
}

// requestsGPU reports whether any of p's containers asks for a GPU.
func requestsGPU(p *corev1.Pod) bool {
	for i := range p.Spec.Containers {
		if gpuRequestCount(&p.Spec.Containers[i]) > 0 {
			return true
		}
	}
	return false
}
