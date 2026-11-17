// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package webhook

// Process configuration of a CRIU capture. A CRIU restore brings back the
// captured process with the command line and environment it was started
// with; the new pod's are never read. The configuration hash a capture is
// found by (cacheURI) leaves out what differs between the roles and ranks
// of one model's pods (rootfsonly roleEnvPrefixes, the KV-transfer flags),
// so the pods can share one model download. Two pods with the same hash
// can therefore start different processes: a rollout that changes only a
// DYN_* variable keeps the hash, and a restore would resume the old
// settings without a word (an in-place update of a Dynamo worker changed
// only DYN_NAMESPACE_WORKER_SUFFIX). The webhook records each pod's
// process configuration as admission receives it, the capture keeps the
// captured container's with each checkpoint, and a pod whose container
// differs from its checkpoint's starts cold.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

const (
	// ProcessConfigAnnotation is a pod's process configuration as
	// admission received it, before the webhook's own patches (which the
	// restored process carries from its capture either way).
	ProcessConfigAnnotation = "nvsnap.io/process-config"
	// ProcessConfigMeta keeps it in a single pod's capture record.
	ProcessConfigMeta = "process_config"

	processConfigVersion = "v1"
	// processConfigNamespace stands for the pod's namespace in an env
	// value: every instance has its own, and an instance's restore runs in
	// a new one, which NamesOwnInstance and the Dynamo profile handle.
	processConfigNamespace = "$(namespace)"
	// processConfigWhole is the field of a container recorded as one
	// digest of all its fields (compactContainer).
	processConfigWhole = "*"
	// processConfigEnvOrder is the field holding the order of a
	// container's env declarations, recorded when a value references
	// another variable.
	processConfigEnvOrder = "envOrder"
	// processConfigAnnotationMax bounds the annotation (a pod's
	// annotations may total 256 KiB); past it, each container is one
	// digest.
	processConfigAnnotationMax = 32 << 10
)

// placementEnv is env a controller sets to place a pod in its instance,
// not to configure its process: replicas and ranks of one configuration
// differ in it, and restore from each other's checkpoints by design (the
// restored process keeps its capture's placement either way). The rank a
// group's pod restores is matched by its ordinal instead.
var (
	placementEnvPrefixes = []string{"GROVE_"}
	placementEnvExact    = map[string]bool{"NODE_RANK": true, "RANK": true, "GROUP_RANK": true, "LOCAL_RANK": true,
		"LWS_WORKER_INDEX": true, "LWS_LEADER_ADDRESS": true, "JOB_COMPLETION_INDEX": true}
)

func placementEnv(name string) bool {
	if placementEnvExact[name] {
		return true
	}
	for _, p := range placementEnvPrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// ProcessConfig returns the process configuration of each of the pod's
// containers: its command, args, image, working directory and environment
// variables, as "<container>/<field>=<digest>" items in a stable order. A
// digest, not the value, so the annotation and the record hold no secret
// a pod passes as a literal. "" for a pod without containers.
func ProcessConfig(pod *corev1.Pod) string {
	var ns *regexp.Regexp
	if pod.Namespace != "" {
		ns = regexp.MustCompile(`(^|[^a-z0-9])` + regexp.QuoteMeta(pod.Namespace) + `([^a-z0-9]|$)`)
	}
	byContainer := map[string][]string{}
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		var items []string
		add := func(field, s string) {
			items = append(items, c.Name+"/"+field+"="+digest(s))
		}
		addJSON := func(field string, v any) {
			b, _ := json.Marshal(v)
			add(field, string(b))
		}
		addJSON("command", c.Command)
		addJSON("args", c.Args)
		add("image", c.Image)
		add("workingDir", c.WorkingDir)
		if len(c.EnvFrom) > 0 {
			addJSON("envFrom", c.EnvFrom)
		}
		var order []string
		refs := false
		for _, e := range c.Env {
			if !placementEnv(e.Name) {
				order = append(order, e.Name)
				refs = refs || strings.Contains(e.Value, "$(")
			}
			switch {
			case placementEnv(e.Name):
			case e.ValueFrom != nil:
				addJSON("env/"+e.Name, e.ValueFrom)
			default:
				v := e.Value
				// The namespace is normalized in env values only, where a
				// pod names its instance (NamesOwnInstance); a command or
				// image naming a word that is also the namespace is kept.
				if ns != nil && strings.Contains(v, pod.Namespace) {
					v = ns.ReplaceAllString(v, "${1}"+processConfigNamespace+"${2}")
				}
				add("env/"+e.Name, v)
			}
		}
		// Kubernetes expands $(NAME) only from variables declared before
		// it, so with a reference the declaration order is configuration
		// too. Only then: a container without one keeps the fingerprint
		// it had.
		if refs {
			add(processConfigEnvOrder, strings.Join(order, ","))
		}
		byContainer[c.Name] = items
	}
	pc := joinProcessConfig(byContainer)
	if len(pc) > processConfigAnnotationMax {
		for name, items := range byContainer {
			byContainer[name] = compactContainer(name, items)
		}
		pc = joinProcessConfig(byContainer)
	}
	return pc
}

func digest(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:6])
}

func joinProcessConfig(byContainer map[string][]string) string {
	var all []string
	for _, items := range byContainer {
		all = append(all, items...)
	}
	if len(all) == 0 {
		return ""
	}
	sort.Strings(all)
	return processConfigVersion + " " + strings.Join(all, " ")
}

// splitProcessConfig returns a configuration's version and its items by
// container.
func splitProcessConfig(pc string) (version string, byContainer map[string][]string) {
	f := strings.Fields(pc)
	if len(f) == 0 {
		return "", nil
	}
	m := map[string][]string{}
	for _, it := range f[1:] {
		if ctr, _, ok := strings.Cut(it, "/"); ok {
			m[ctr] = append(m[ctr], it)
		}
	}
	return f[0], m
}

// isCompact reports whether a container's items are one digest of them all.
func isCompact(name string, items []string) bool {
	return len(items) == 1 && strings.HasPrefix(items[0], name+"/"+processConfigWhole+"=")
}

// compactContainer turns a container's items into one digest of them all,
// "<container>/*=<digest>"; items already compact stay as they are.
func compactContainer(name string, items []string) []string {
	if isCompact(name, items) {
		return items
	}
	s := append([]string(nil), items...)
	sort.Strings(s)
	return []string{name + "/" + processConfigWhole + "=" + digest(strings.Join(s, " "))}
}

// ProcessConfigOf returns the part of a configuration that is about
// container: what a checkpoint of that container records. "" when the
// configuration has no such container.
func ProcessConfigOf(pc, container string) string {
	_, m := splitProcessConfig(pc)
	if len(m[container]) == 0 {
		return ""
	}
	return joinProcessConfig(map[string][]string{container: m[container]})
}

// CompactProcessConfig returns a configuration with each container as one
// digest, for a record without room for every field: a pod still matches
// or not, but a mismatch cannot say which field changed.
func CompactProcessConfig(pc string) string {
	v, m := splitProcessConfig(pc)
	if v == "" {
		return ""
	}
	for name, items := range m {
		m[name] = compactContainer(name, items)
	}
	return joinProcessConfig(m)
}

// ProcessConfigChanges names what differs between a checkpoint's recorded
// process configuration and a pod's, compared on the recorded containers
// only: the changed fields, as "<container>: env X", "<container>: args"
// and so on. None when the record holds no configuration (a capture from
// before it was recorded) or the two agree. A container recorded as one
// digest is compared as one.
func ProcessConfigChanges(recorded, current string) []string {
	if recorded == "" || recorded == current {
		return nil
	}
	rv, r := splitProcessConfig(recorded)
	cv, c := splitProcessConfig(current)
	if rv != cv {
		return []string{"the recorded configuration's format (" + rv + ")"}
	}
	var out []string
	for name, ritems := range r {
		citems := c[name]
		if len(citems) == 0 {
			out = append(out, name+" (no such container)")
			continue
		}
		// One side compact (a pod past the annotation bound compacts every
		// container): compare both as one digest.
		if isCompact(name, ritems) || isCompact(name, citems) {
			if compactContainer(name, citems)[0] != compactContainer(name, ritems)[0] {
				out = append(out, name+": the process configuration (recorded as one digest)")
			}
			continue
		}
		rm, cm := itemMap(ritems), itemMap(citems)
		for k, v := range rm {
			if w, ok := cm[k]; !ok {
				out = append(out, describeField(k)+" (removed)")
			} else if w != v {
				out = append(out, describeField(k))
			}
		}
		for k := range cm {
			if _, ok := rm[k]; !ok {
				out = append(out, describeField(k)+" (added)")
			}
		}
	}
	sort.Strings(out)
	return out
}

func itemMap(items []string) map[string]string {
	m := make(map[string]string, len(items))
	for _, it := range items {
		if k, v, ok := strings.Cut(it, "="); ok {
			m[k] = v
		}
	}
	return m
}

// describeField turns "<container>/env/NAME" into "<container>: env NAME"
// and "<container>/args" into "<container>: args".
func describeField(k string) string {
	ctr, field, ok := strings.Cut(k, "/")
	if !ok {
		return k
	}
	return ctr + ": " + strings.Replace(field, "env/", "env ", 1)
}

// processConfigPatches stamps a pod with its process configuration as
// received. Every pod, not only GPU ones: a group capture takes the
// instance's companions (an etcd, a frontend) too. The webhook sees a pod
// once (no reinvocation), so a stamp the pod already has came with it (a
// template that copied another pod's annotations): it is replaced when it
// does not match the spec, so a capture never records another
// configuration than the one it runs.
func (m *Mutator) processConfigPatches(pod *corev1.Pod) []PatchOp {
	if !m.CRIUCapture {
		return nil
	}
	pc := ProcessConfig(pod)
	supplied, has := pod.Annotations[ProcessConfigAnnotation]
	if has && supplied == pc {
		return nil
	}
	if pc == "" {
		if has {
			return []PatchOp{{Op: "remove", Path: "/metadata/annotations/" + jsonPointerEscape(ProcessConfigAnnotation)}}
		}
		return nil
	}
	if has {
		m.logger().WithField("pod", pod.Namespace+"/"+pod.Name).
			Warn("the pod came with a process-config stamp that does not match its spec; replacing it")
		return []PatchOp{{Op: "replace", Path: "/metadata/annotations/" + jsonPointerEscape(ProcessConfigAnnotation), Value: pc}}
	}
	return newMetaPatcher(pod).annotation(ProcessConfigAnnotation, pc)
}
