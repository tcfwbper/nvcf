// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// Dynamo components that register through Kubernetes.
//
// With DYN_DISCOVERY_BACKEND=kubernetes a Dynamo worker has no etcd to
// capture with it. It writes its registration once, at startup, into a
// DynamoWorkerMetadata object in its namespace: named after its pod (and
// container), owned by its pod, listing its endpoints with the addresses
// it serves them at. A frontend routes to a worker when the worker's
// container is ready and an object of that name, owned by that pod,
// exists.
//
// A restored worker never registers again, and its replacement pod has
// another name, UID and IP. So a capture keeps each rank's registration
// objects and the rank's address (registrationsOf); the restore writes
// them again for the placeholder pod it restores into, named after it,
// owned by it, with the captured address replaced by the placeholder's
// wherever the objects name it (writeRegistrations): the endpoints'
// transports, and the URLs of the model card's files on the worker's
// system port, from which a frontend that has not loaded the model yet
// fetches its tokenizer. The worker's instance
// IDs in the objects stay those it was captured with: the restored
// process answers to them.
//
// Both ends fail closed. A rank whose registration cannot be read, or
// whose transports or URLs name an address other than its pod's, is not
// captured. A restore whose registrations cannot be written does not run:
// its pods start cold, rather than run ready with no route to them.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"net"
	"net/url"
	"regexp"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

// dynamoWorkerMetadata is the resource a Dynamo component registers
// itself as under Kubernetes discovery.
var dynamoWorkerMetadata = schema.GroupVersionResource{Group: "nvidia.com", Version: "v1alpha1", Resource: "dynamoworkermetadatas"}

// kubeRegistrationKey in a group record holds rank i's registrations
// (rankRegistrations, JSON).
const kubeRegistrationKey = "kubeRegistrations.%d"

// kubeRegistrationBudget bounds the registrations one capture keeps in
// its group record, which shares a ConfigMap's 1 MiB with the rest of
// the record. A Dynamo worker's object is a few KiB (its model card).
const kubeRegistrationBudget = 256 << 10

// kubeDiscoveryRank reports a Dynamo component that registers as
// Kubernetes objects in its namespace.
func kubeDiscoveryRank(pod *corev1.Pod) bool {
	return webhook.DynamoComponent(pod) && webhook.DynamoDiscovery(pod) == "kubernetes"
}

// workerRegistration is one DynamoWorkerMetadata object of a rank.
type workerRegistration struct {
	// Suffix is the object's name after its pod's name: "" for the pod's
	// main container, "-<container>" for another.
	Suffix string `json:"suffix"`
	// Controller is whether the pod is the object's controlling owner.
	Controller bool            `json:"controller"`
	Spec       json.RawMessage `json:"spec"`
}

// rankRegistrations is what a capture keeps of a rank's registration.
type rankRegistrations struct {
	PodIP   string               `json:"podIP"`
	Objects []workerRegistration `json:"objects"`
}

// registrationsOf reads the DynamoWorkerMetadata objects pod owns, to
// keep with its capture.
func (a *Agent) registrationsOf(ctx context.Context, pod *corev1.Pod) (rankRegistrations, error) {
	var out rankRegistrations
	if a.dynClient == nil {
		return out, fmt.Errorf("no client for custom resources")
	}
	ip := pod.Status.PodIP
	if net.ParseIP(ip).To4() == nil {
		return out, fmt.Errorf("pod %s has no IPv4 address (%q)", pod.Name, ip)
	}
	if err := readyOnlyOnceRestored(pod); err != nil {
		return out, err
	}
	list, err := a.dynClient.Resource(dynamoWorkerMetadata).Namespace(pod.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return out, fmt.Errorf("list DynamoWorkerMetadata in %s: %w", pod.Namespace, err)
	}
	out.PodIP = ip
	for i := range list.Items {
		o := &list.Items[i]
		owner, controller := false, false
		for _, r := range o.GetOwnerReferences() {
			if r.UID == pod.UID {
				owner, controller = true, r.Controller != nil && *r.Controller
			}
		}
		if !owner {
			continue
		}
		if !strings.HasPrefix(o.GetName(), pod.Name) {
			return out, fmt.Errorf("DynamoWorkerMetadata %s is owned by pod %s but not named after it", o.GetName(), pod.Name)
		}
		spec, found, err := unstructured.NestedFieldNoCopy(o.Object, "spec")
		if err != nil || !found {
			return out, fmt.Errorf("DynamoWorkerMetadata %s has no spec", o.GetName())
		}
		raw, err := json.Marshal(spec)
		if err != nil {
			return out, fmt.Errorf("DynamoWorkerMetadata %s: %w", o.GetName(), err)
		}
		if other := foreignAddress(raw, ip); other != "" {
			// An address the restore would not know to replace.
			return out, fmt.Errorf("DynamoWorkerMetadata %s serves at %s, not at its pod's address %s", o.GetName(), other, ip)
		}
		out.Objects = append(out.Objects, workerRegistration{Suffix: strings.TrimPrefix(o.GetName(), pod.Name), Controller: controller, Spec: raw})
	}
	if len(out.Objects) == 0 {
		return out, fmt.Errorf("pod %s owns no DynamoWorkerMetadata: it has not registered", pod.Name)
	}
	return out, nil
}

// readyOnlyOnceRestored fails for a rank a frontend could route to before
// its restore runs. In container mode a frontend takes a container's own
// readiness, which nvsnap's readiness gate does not reach: a container
// without a readiness probe is ready while it is still a placeholder, and
// its registration is written before the restore.
func readyOnlyOnceRestored(pod *corev1.Pod) error {
	container := false
	for _, c := range pod.Spec.Containers {
		explicit := false
		for _, e := range c.Env {
			if e.Name == "DYN_KUBE_DISCOVERY_MODE" {
				explicit = true
				if e.ValueFrom != nil || e.Value == "container" {
					container = true // by reference: cannot tell, so as container mode
				}
			}
		}
		if !explicit && len(c.EnvFrom) > 0 {
			container = true // envFrom may set it: cannot tell, so as container mode
		}
	}
	if !container {
		return nil // pod mode: the pod's readiness, which the gate holds
	}
	name := gpuContainer(pod)
	for _, c := range pod.Spec.Containers {
		if c.Name == name && c.ReadinessProbe == nil {
			return fmt.Errorf("container %s of pod %s has no readiness probe: in container discovery mode it would be routed to before its restore", name, pod.Name)
		}
	}
	return nil
}

// recordRegistrations reads every Kubernetes-discovery rank's
// registrations into data, a group record's capture outcome.
func (a *Agent) recordRegistrations(ctx context.Context, ranks []*corev1.Pod) (map[string]string, error) {
	data := map[string]string{}
	total := 0
	for i, r := range ranks {
		if !kubeDiscoveryRank(r) {
			continue
		}
		regs, err := a.registrationsOf(ctx, r)
		if err != nil {
			return nil, err
		}
		b, err := json.Marshal(regs)
		if err != nil {
			return nil, err
		}
		if total += len(b); total > kubeRegistrationBudget {
			return nil, fmt.Errorf("the ranks' registrations take more than %d bytes", kubeRegistrationBudget)
		}
		data[fmt.Sprintf(kubeRegistrationKey, i)] = string(b)
	}
	return data, nil
}

// writeRegistrations writes, for every rank of a group restore, the
// registrations its capture kept (record, the group record's data), as
// the placeholder pod's.
func (a *Agent) writeRegistrations(ctx context.Context, record map[string]string, ranks []*corev1.Pod) error {
	for i, p := range ranks {
		v := record[fmt.Sprintf(kubeRegistrationKey, i)]
		if v == "" {
			if kubeDiscoveryRank(p) {
				return fmt.Errorf("rank %d (%s) registers through Kubernetes, and its capture kept no registration", i, p.Name)
			}
			continue
		}
		if a.dynClient == nil {
			return fmt.Errorf("no client for custom resources")
		}
		var regs rankRegistrations
		if err := json.Unmarshal([]byte(v), &regs); err != nil {
			return fmt.Errorf("rank %d: registrations: %w", i, err)
		}
		if err := a.instanceIDsFree(ctx, p, regs); err != nil {
			return fmt.Errorf("rank %d: %w", i, err)
		}
		ip := p.Status.PodIP
		if net.ParseIP(ip).To4() == nil {
			return fmt.Errorf("placeholder %s has no IPv4 address (%q)", p.Name, ip)
		}
		for _, o := range regs.Objects {
			spec, err := replacePodIP(o.Spec, regs.PodIP, ip)
			if err != nil {
				return fmt.Errorf("rank %d: %w", i, err)
			}
			if err := a.applyRegistration(ctx, p, p.Name+o.Suffix, o.Controller, spec); err != nil {
				return err
			}
		}
	}
	return nil
}

// applyRegistration creates DynamoWorkerMetadata name in pod's namespace,
// owned by pod, with spec; an object of that name pod already owns is
// overwritten (a restore driven again), any other is an error.
func (a *Agent) applyRegistration(ctx context.Context, pod *corev1.Pod, name string, controller bool, spec map[string]any) error {
	no := false
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": dynamoWorkerMetadata.GroupVersion().String(),
		"kind":       "DynamoWorkerMetadata",
		"metadata":   map[string]any{"name": name, "namespace": pod.Namespace},
		"spec":       spec,
	}}
	obj.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: "v1", Kind: "Pod", Name: pod.Name, UID: pod.UID,
		Controller: &controller, BlockOwnerDeletion: &no,
	}})
	api := a.dynClient.Resource(dynamoWorkerMetadata).Namespace(pod.Namespace)
	_, err := api.Create(ctx, obj, metav1.CreateOptions{})
	if !apierrors.IsAlreadyExists(err) {
		if err != nil {
			return fmt.Errorf("create DynamoWorkerMetadata %s/%s: %w", pod.Namespace, name, err)
		}
		return nil
	}
	cur, err := api.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get DynamoWorkerMetadata %s/%s: %w", pod.Namespace, name, err)
	}
	mine := false
	for _, r := range cur.GetOwnerReferences() {
		mine = mine || r.UID == pod.UID
	}
	if !mine {
		return fmt.Errorf("DynamoWorkerMetadata %s/%s exists and is not the placeholder's", pod.Namespace, name)
	}
	obj.SetResourceVersion(cur.GetResourceVersion())
	if _, err := api.Update(ctx, obj, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update DynamoWorkerMetadata %s/%s: %w", pod.Namespace, name, err)
	}
	return nil
}

// replacePodIP returns spec (JSON) with the IPv4 address old replaced by
// new in every string, as a whole address only (10.0.0.1 is not part of
// 10.0.0.12).
func replacePodIP(spec json.RawMessage, old, new string) (map[string]any, error) {
	// Numbers decoded exactly: instance IDs are integers of up to 53 bits.
	d := json.NewDecoder(bytes.NewReader(spec))
	d.UseNumber()
	var v map[string]any
	if err := d.Decode(&v); err != nil {
		return nil, fmt.Errorf("registration spec: %w", err)
	}
	walkStrings(v, false, func(s string, _ bool) string { return replaceIPv4(s, old, new) })
	return v, nil
}

// foreignAddress returns an IPv4 address other than ip that spec names
// where a peer would connect to it: in a string under a "transport" key,
// or in a URL. "" when there is none.
func foreignAddress(spec json.RawMessage, ip string) string {
	var v any
	if err := json.Unmarshal(spec, &v); err != nil {
		return "unparseable spec"
	}
	other := ""
	walkStrings(v, false, func(s string, transport bool) string {
		if !transport && !strings.Contains(s, "://") {
			return s
		}
		toks := ipv4Tokens(s)
		if other == "" && strings.Contains(s, "://") {
			// A URL's host, with or without a port: a name the restore
			// cannot move.
			if u, err := url.Parse(s); err == nil {
				if h := u.Hostname(); h != "" && h != ip && net.ParseIP(h) == nil {
					other = s
				}
			}
		}
		if len(toks) == 0 && other == "" && hostPort.MatchString(s) {
			// A host named, not addressed: the restore cannot move it.
			other = s
		}
		for _, a := range toks {
			if a != ip && other == "" {
				other = a
			}
		}
		return s
	})
	return other
}

// hostPort matches a host name with a port, as a transport or URL names a
// peer by name.
var hostPort = regexp.MustCompile(`(^|//)[A-Za-z][-A-Za-z0-9.]*:[0-9]+`)

// walkStrings replaces, in place, every string of v with f's result; f
// learns whether the string is under a "transport" key.
func walkStrings(v any, in bool, f func(s string, transport bool) string) any {
	switch t := v.(type) {
	case map[string]any:
		for k, e := range t {
			t[k] = walkStrings(e, in || k == "transport", f)
		}
		return t
	case []any:
		for i, e := range t {
			t[i] = walkStrings(e, in, f)
		}
		return t
	case string:
		return f(t, in)
	case json.Number:
		// As an unstructured object holds numbers.
		if i, err := t.Int64(); err == nil {
			return i
		}
		f, _ := t.Float64()
		return f
	default:
		return v
	}
}

// ipv4Tokens returns the dotted-quad IPv4 addresses in s that are not part
// of a longer run of digits and dots.
func ipv4Tokens(s string) []string {
	var out []string
	i := 0
	for i < len(s) {
		if !isDigitOrDot(s[i]) || (i > 0 && isDigitOrDot(s[i-1])) {
			i++
			continue
		}
		j := i
		for j < len(s) && isDigitOrDot(s[j]) {
			j++
		}
		if ip := net.ParseIP(s[i:j]); ip != nil && ip.To4() != nil && strings.Count(s[i:j], ".") == 3 {
			out = append(out, s[i:j])
		}
		i = j
	}
	return out
}

// replaceIPv4 replaces each whole occurrence of the address old in s.
func replaceIPv4(s, old, new string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		if strings.HasPrefix(s[i:], old) && (i == 0 || !isDigitOrDot(s[i-1])) &&
			(i+len(old) == len(s) || !isDigitOrDot(s[i+len(old)])) {
			b.WriteString(new)
			i += len(old)
			continue
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

func isDigitOrDot(c byte) bool { return c == '.' || (c >= '0' && c <= '9') }

// writeGroupRegistrations is writeRegistrations for group record key.
func (c *criuAutoRestorer) writeGroupRegistrations(ctx context.Context, key string, ranks []*corev1.Pod) error {
	needed := false
	for _, p := range ranks {
		needed = needed || kubeDiscoveryRank(p)
	}
	cm, err := c.a.kubeClient.CoreV1().ConfigMaps(c.a.criuFailedNamespace()).Get(ctx, criuGroupConfigMapName(key), metav1.GetOptions{})
	if err != nil {
		if !needed && apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("group record: %w", err)
	}
	return c.a.writeRegistrations(ctx, cm.Data, ranks)
}

// instanceIDsFree fails when another live pod of p's namespace answers
// to an instance ID that regs does. The restored process answers to the
// IDs it was captured with: two replicas restored from one capture would
// both claim them, and a frontend routes an ID to one of them. The first
// restore keeps them; a later one starts cold.
//
// Two restores at once must not both pass: each ID is claimed first with
// an object only one pod can create (claimInstanceID). Then the
// registrations of the namespace are checked, for a pod that registered
// the ID itself (the captured pod, before its capture stopped it). A pod
// gone or terminating holds nothing: the captured pod is deleted as its
// replacement is admitted, and its objects outlive it until the garbage
// collector removes them.
func (a *Agent) instanceIDsFree(ctx context.Context, p *corev1.Pod, regs rankRegistrations) error {
	mine := map[string]bool{}
	var ids []string
	for _, o := range regs.Objects {
		for _, id := range instanceIDs(o.Spec) {
			if !mine[id] {
				mine[id] = true
				ids = append(ids, id)
			}
		}
	}
	if len(ids) == 0 {
		return fmt.Errorf("the captured registration names no instance ID")
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := a.claimInstanceID(ctx, p, id); err != nil {
			return err
		}
	}
	list, err := a.dynClient.Resource(dynamoWorkerMetadata).Namespace(p.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list DynamoWorkerMetadata in %s: %w", p.Namespace, err)
	}
	for i := range list.Items {
		o := &list.Items[i]
		raw, err := json.Marshal(o.Object["spec"])
		if err != nil {
			continue
		}
		shared := ""
		for _, id := range instanceIDs(raw) {
			if mine[id] {
				shared = id
				break
			}
		}
		if shared == "" {
			continue
		}
		live, err := a.ownerLive(ctx, p, o.GetNamespace(), o.GetOwnerReferences())
		if err != nil {
			return err
		}
		if live {
			return fmt.Errorf("instance %s of the capture is registered by DynamoWorkerMetadata %s of a running pod (another replica restored from the same capture)", shared, o.GetName())
		}
	}
	return nil
}

// instanceClaimPrefix names the ConfigMap that claims a Dynamo instance ID
// in a namespace for the pod that owns it.
const instanceClaimPrefix = "nvsnap-dynamo-instance-"

// claimInstanceID makes p the holder of instance ID id in its namespace:
// a ConfigMap of that name, owned by p, so it goes when p goes. Creating
// it is atomic; one a pod gone or terminating holds is taken over with a
// compare-and-swap update. Held by another live pod, it fails.
func (a *Agent) claimInstanceID(ctx context.Context, p *corev1.Pod, id string) error {
	if a.kubeClient == nil {
		return fmt.Errorf("no Kubernetes client")
	}
	no := false
	name := instanceClaimPrefix + id
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: p.Namespace,
		Labels:          map[string]string{"app.kubernetes.io/managed-by": "nvsnap"},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: p.Name, UID: p.UID, BlockOwnerDeletion: &no}},
	}, Data: map[string]string{"pod": p.Name}}
	api := a.kubeClient.CoreV1().ConfigMaps(p.Namespace)
	_, err := api.Create(ctx, cm, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("claim instance %s: %w", id, err)
	}
	cur, err := api.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("claim instance %s: %w", id, err)
	}
	for _, r := range cur.OwnerReferences {
		if r.UID == p.UID {
			return nil // this pod's, from an earlier attempt
		}
	}
	live, err := a.ownerLive(ctx, p, p.Namespace, cur.OwnerReferences)
	if err != nil {
		return err
	}
	if live {
		return fmt.Errorf("instance %s of the capture is held by pod %s (another replica restored from the same capture)", id, cur.Data["pod"])
	}
	cm.ResourceVersion = cur.ResourceVersion
	if _, err := api.Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("claim instance %s from a pod that is gone: %w", id, err)
	}
	return nil
}

// ownerLive reports whether a pod among refs, other than p, exists in ns
// with that UID and is not terminating.
func (a *Agent) ownerLive(ctx context.Context, p *corev1.Pod, ns string, refs []metav1.OwnerReference) (bool, error) {
	for _, r := range refs {
		if r.Kind != "Pod" || r.UID == p.UID {
			continue
		}
		if a.kubeClient == nil {
			return true, nil // cannot tell: held
		}
		o, err := a.kubeClient.CoreV1().Pods(ns).Get(ctx, r.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return false, fmt.Errorf("get pod %s/%s: %w", ns, r.Name, err)
		}
		if o.UID == r.UID && o.DeletionTimestamp == nil {
			return true, nil
		}
	}
	return false, nil
}

// deleteRegistrations removes the DynamoWorkerMetadata objects pod uid
// owns in ns: a placeholder that starts cold in place registers anew, and
// the restored registration (the capture's instance IDs at its address)
// must not route to it meanwhile.
func (a *Agent) deleteRegistrations(ctx context.Context, ns string, uid types.UID) error {
	if a.dynClient == nil {
		return nil
	}
	api := a.dynClient.Resource(dynamoWorkerMetadata).Namespace(ns)
	list, err := api.List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for i := range list.Items {
		for _, r := range list.Items[i].GetOwnerReferences() {
			if r.UID != uid {
				continue
			}
			// Bound to the object listed: one of the same name a later
			// restore wrote is not removed (Conflict).
			oid := list.Items[i].GetUID()
			if err := api.Delete(ctx, list.Items[i].GetName(), metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &oid}}); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
				return err
			}
		}
	}
	return nil
}

// instanceIDs returns the values of every "instance_id" key in spec.
func instanceIDs(spec json.RawMessage) []string {
	d := json.NewDecoder(bytes.NewReader(spec))
	d.UseNumber()
	var v any
	if d.Decode(&v) != nil {
		return nil
	}
	var out []string
	var walk func(any)
	walk = func(v any) {
		switch t := v.(type) {
		case map[string]any:
			for k, e := range t {
				if k == "instance_id" {
					out = append(out, fmt.Sprint(e))
					continue
				}
				walk(e)
			}
		case []any:
			for _, e := range t {
				walk(e)
			}
		}
	}
	walk(v)
	return out
}

// coldInPlaceAllowed reports whether a placeholder refused for its node may
// start cold in place. One that registers through Kubernetes registers anew
// as it starts, and the restored registration must not route to it
// meanwhile: it is removed first, and when it cannot be, the pod is deleted
// instead (the registration is the pod's, so it goes with it).
func (a *Agent) coldInPlaceAllowed(ctx context.Context, ns string, uid types.UID, kube bool, log *logrus.Entry) bool {
	if !kube {
		return true
	}
	if err := a.deleteRegistrations(ctx, ns, uid); err != nil {
		log.WithError(err).Warn("CRIU auto-restore: cannot remove the restored registration; deleting the pod instead of starting it cold in place")
		return false
	}
	return true
}
