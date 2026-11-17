// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package fnstatus names the function version a capture record belongs to
// and projects the records onto one status per function version: what
// nvsnap holds for it and what admission does with its next pod. The
// agents stamp the names (Identity); nvsnap-server keeps the NvsnapFunction
// objects (Project). The records stay the only source of truth.
package fnstatus

import (
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// The labels the agents put on a group record. A record's key is scoped
// by the function version (or the namespace, outside NVCF), so a record
// belongs to exactly one.
const (
	LabelFunctionVersion = "nvsnap.io/function-version-id"
	LabelFunction        = "nvsnap.io/function-id"
	LabelTenant          = "nvsnap.io/tenant"
	LabelNamespace       = "nvsnap.io/namespace"
)

// Record names and states, as the agent writes them (internal/agent).
const (
	GroupRecordPrefix = "nvsnap-criu-group-"
	// What the webhook and agents put on a restore's pods, which the
	// restore history reads (internal/agent's TestFnstatusMatchesTheAgent
	// keeps them the webhook's).
	GroupLabel               = "nvsnap.io/criu-group"
	GroupAnnotation          = "nvsnap.io/criu-group"
	RestoreAnnotation        = "nvsnap.io/criu-restore"
	CompanionAnnotation      = "nvsnap.io/criu-group-companion"
	OrdinalAnnotation        = "nvsnap.io/criu-group-ordinal"
	RestoredAnnotation       = "nvsnap.io/criu-restored"
	ColdStartedAnnotation    = "nvsnap.io/criu-cold-started"
	TeardownExitedAnnotation = "nvsnap.io/criu-teardown-exited"
	// The labels that tell an instance's pods from another's in one
	// namespace, as the agent reads them.
	GroveScalingGroupLabel        = "grove.io/podcliquescalinggroup"
	GroveScalingGroupReplicaLabel = "grove.io/podcliquescalinggroup-replica-index"
	LWSGroupKeyLabel              = "leaderworkerset.sigs.k8s.io/group-key"
	CachedirRecordPrefix          = "nvsnap-cachedir-"

	StateAlias          = "alias"
	StateCapturing      = "capturing"
	StateVerifying      = "verifying"
	StateComplete       = "complete"
	StateFailed         = "failed"
	StateRestoreFailed  = "restore-failed"
	StateGPUShareFailed = "gpushare-failed"
	// StateStalled is a complete capture whose restores stopped getting to
	// run MaxStalls times: admission starts its pods cold, keeping it.
	StateStalled = "stalled"

	// StateAbandoned is a capture being verified whose namespace went
	// before a restore of it served (the agent marked its record
	// AbandonedKey): nothing can verify it, and the configuration's next
	// deployment captures it again.
	StateAbandoned = "abandoned"
	// AbandonedKey is the agent's groupAbandonedKey.
	AbandonedKey = "abandonedAt"

	// MaxStalls is the agent's criuGroupMaxStalls.
	MaxStalls = 3

	MethodCRIU     = "criu"
	MethodCachedir = "cachedir"
)

// Namespace labels the platform puts on a function's namespaces, in the
// order they are read (the agent's criuScopeLabels for the version).
var (
	versionLabels  = []string{"function-version-id", "FUNCTION_VERSION_ID"}
	functionLabels = []string{"function-id", "FUNCTION_ID"}
)

// Identity returns the labels naming the function version of namespace ns,
// from its labels nsLabels; tenantLabel is the namespace label naming its
// tenant. Outside NVCF only the namespace is named. A value that is not a
// valid label value is left out.
func Identity(ns string, nsLabels map[string]string, tenantLabel string) map[string]string {
	out := map[string]string{}
	set := func(k, v string) {
		if v != "" && len(validation.IsValidLabelValue(v)) == 0 {
			out[k] = v
		}
	}
	set(LabelNamespace, ns)
	set(LabelFunctionVersion, first(nsLabels, versionLabels))
	set(LabelFunction, first(nsLabels, functionLabels))
	if tenantLabel != "" {
		set(LabelTenant, nsLabels[tenantLabel])
	}
	return out
}

func first(labels map[string]string, keys []string) string {
	for _, k := range keys {
		if v := labels[k]; v != "" {
			return v
		}
	}
	return ""
}

// ObjectName is the name of the status of the function version a record
// with labels belongs to: the version id, or "ns-<namespace>" outside
// NVCF; "" for a record not named yet.
func ObjectName(labels map[string]string) string {
	if v := labels[LabelFunctionVersion]; v != "" {
		return v
	}
	if ns := labels[LabelNamespace]; ns != "" {
		return "ns-" + ns
	}
	return ""
}

// Configuration is one configuration (group record) of a function version.
type Configuration struct {
	Key string `json:"key"`
	// State is the record's state; StateStalled for a complete one
	// admission no longer restores.
	State string `json:"state"`
	// Method is how admission starts the configuration's next pod:
	// MethodCRIU (restore), MethodCachedir (seeded caches), or "" (cold).
	Method     string   `json:"method,omitempty"`
	Ranks      int      `json:"ranks"`
	Nodes      []string `json:"nodes,omitempty"`
	Captures   int      `json:"captures,omitempty"`
	StartedAt  string   `json:"startedAt,omitempty"`
	VerifiedAt string   `json:"verifiedAt,omitempty"`
	// Restores counts the recent restores the record keeps (at most 10);
	// LastRestoreAt and LastRestoreSeconds are the newest.
	Restores           int     `json:"restores,omitempty"`
	LastRestoreAt      string  `json:"lastRestoreAt,omitempty"`
	LastRestoreSeconds float64 `json:"lastRestoreSeconds,omitempty"`
	RestoreFailures    int     `json:"restoreFailures,omitempty"`
	GPUFaults          int     `json:"gpuFaults,omitempty"`
	Stalls             int     `json:"stalls,omitempty"`
	// Cachedir is "complete/ranks" of the cachedir capture, when there is
	// one.
	Cachedir string `json:"cachedir,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

// Status is what nvsnap holds for one function version. The top fields
// summarise its best configuration.
type Status struct {
	FunctionVersionID string          `json:"functionVersionID,omitempty"`
	FunctionID        string          `json:"functionID,omitempty"`
	Tenant            string          `json:"tenant,omitempty"`
	Namespace         string          `json:"namespace,omitempty"`
	State             string          `json:"state"`
	Method            string          `json:"method,omitempty"`
	Ranks             int             `json:"ranks"`
	VerifiedAt        string          `json:"verifiedAt,omitempty"`
	LastRestoreSecs   float64         `json:"lastRestoreSeconds,omitempty"`
	Reason            string          `json:"reason,omitempty"`
	Configurations    []Configuration `json:"configurations"`
	// History is the version's restores (history.go), kept by the server
	// across passes; Project leaves it empty.
	History
}

// reasonMax bounds a reason in the status. The record keeps the whole
// reason (up to 16 KiB); the status says what failed (reasonSummary).
const reasonMax = 300

// Project returns the status of every function version the records name,
// by object name. records are the ConfigMaps of the records' namespace;
// aliases, records not named yet and anything else are skipped.
func Project(records []corev1.ConfigMap) map[string]Status {
	cachedir := map[string]map[string]string{}
	for i := range records {
		if key, ok := strings.CutPrefix(records[i].Name, CachedirRecordPrefix); ok {
			cachedir[key] = records[i].Data
		}
	}
	out := map[string]Status{}
	for i := range records {
		r := &records[i]
		key, ok := strings.CutPrefix(r.Name, GroupRecordPrefix)
		if !ok || r.Data["state"] == StateAlias {
			continue
		}
		name := ObjectName(r.Labels)
		if name == "" {
			continue
		}
		s := out[name]
		s.FunctionVersionID = r.Labels[LabelFunctionVersion]
		s.FunctionID = r.Labels[LabelFunction]
		s.Tenant = r.Labels[LabelTenant]
		s.Namespace = r.Labels[LabelNamespace]
		s.Configurations = append(s.Configurations, configuration(key, r.Data, cachedir[key]))
		out[name] = s
	}
	for name := range out {
		s := out[name]
		summarise(&s)
		out[name] = s
	}
	return out
}

// configuration reads group record d of key and its cachedir record c.
func configuration(key string, d, c map[string]string) Configuration {
	cfg := Configuration{Key: key, State: d["state"], StartedAt: d["startedAt"], VerifiedAt: d["verifiedAt"]}
	cfg.Ranks, _ = strconv.Atoi(d["size"])
	cfg.Captures, _ = strconv.Atoi(d["captures"])
	cfg.RestoreFailures, _ = strconv.Atoi(d["restoreFailures"])
	cfg.GPUFaults, _ = strconv.Atoi(d["gpuFaults"])
	cfg.Stalls, _ = strconv.Atoi(d["stalls"])
	for i := range cfg.Ranks {
		if n := d["node."+strconv.Itoa(i)]; n != "" {
			cfg.Nodes = append(cfg.Nodes, n)
		}
	}
	if h := strings.Fields(d["restores"]); len(h) > 0 {
		cfg.Restores = len(h)
		if s := RecordStamps(map[string]string{"restores": h[len(h)-1]}); len(s) == 1 {
			cfg.LastRestoreAt = s[0].At.UTC().Format(time.RFC3339)
			cfg.LastRestoreSeconds = s[0].Seconds
		}
	}
	if c != nil {
		cfg.Cachedir = cachedirRanks(c, cfg.Ranks)
	}
	switch {
	case cfg.State == StateVerifying && d[AbandonedKey] != "":
		cfg.State = StateAbandoned
	case Restorable(d):
		cfg.Method = MethodCRIU
	case cfg.State == StateComplete && cfg.Stalls >= MaxStalls:
		cfg.State = StateStalled
	}
	if cfg.Method == "" && fallsBack(cfg.State) && cfg.Cachedir != "" {
		if done, total, _ := strings.Cut(cfg.Cachedir, "/"); done == total {
			cfg.Method = MethodCachedir
		}
	}
	cfg.Reason = reasonSummary(firstOf(d["reason"], d["lastRestoreFailure"], d["gpushareFailure"]))
	return cfg
}

// Restorable reports whether admission restores the configuration of group
// record d (the agent's parseCRIUGroup): a complete capture, or one being
// verified (for its own instance only), that has not stalled, with a
// checkpoint for every rank.
func Restorable(d map[string]string) bool {
	if d["state"] != StateComplete && (d["state"] != StateVerifying || d["sourceNamespace"] == "") {
		return false
	}
	if n, _ := strconv.Atoi(d["stalls"]); n >= MaxStalls {
		return false
	}
	size, err := strconv.Atoi(d["size"])
	if err != nil || size < 1 {
		return false
	}
	for i := range size {
		if d["checkpoint."+strconv.Itoa(i)] == "" {
			return false
		}
	}
	return true
}

// fallsBack reports a state whose pods admission seeds from the
// cachedir capture, when there is a complete one.
func fallsBack(state string) bool {
	return state == StateFailed || state == StateRestoreFailed || state == StateGPUShareFailed
}

// cachedirRanks is "complete/ranks" of cachedir record c, for a
// configuration of ranks ranks (0: as many as the record names).
func cachedirRanks(c map[string]string, ranks int) string {
	done, named := 0, 0
	for k, v := range c {
		if strings.HasPrefix(k, "rank.") {
			named++
			if v == StateComplete {
				done++
			}
		}
	}
	if ranks < named {
		ranks = named
	}
	return strconv.Itoa(done) + "/" + strconv.Itoa(ranks)
}

// stateOrder ranks states from the most useful to the least: the summary
// is the best configuration's.
var stateOrder = map[string]int{
	StateComplete: 0, StateVerifying: 1, StateCapturing: 2, StateStalled: 3,
	StateAbandoned: 4, StateRestoreFailed: 5, StateFailed: 6, StateGPUShareFailed: 7,
}

func rank(c Configuration) int {
	r, ok := stateOrder[c.State]
	if !ok {
		r = len(stateOrder)
	}
	// A configuration that falls back to a complete cachedir capture
	// still starts faster than a cold one.
	if c.Method == MethodCachedir {
		r -= len(stateOrder)
	}
	if c.Method == MethodCRIU {
		r -= 2 * len(stateOrder)
	}
	return r
}

// summarise orders s's configurations, best first, and sets the summary
// from the first.
func summarise(s *Status) {
	sort.SliceStable(s.Configurations, func(i, j int) bool {
		ri, rj := rank(s.Configurations[i]), rank(s.Configurations[j])
		if ri != rj {
			return ri < rj
		}
		return s.Configurations[i].Key < s.Configurations[j].Key
	})
	best := s.Configurations[0]
	s.State, s.Method, s.Ranks = best.State, best.Method, best.Ranks
	s.VerifiedAt, s.LastRestoreSecs = best.VerifiedAt, best.LastRestoreSeconds
	s.Reason = ""
	if best.State != StateComplete {
		s.Reason = best.Reason
	}
}

func firstOf(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}

// groupFailure is a group operation's failure: "group checkpoint s1: 2 of
// 2 members failed: ns/pod-0: <its error>; ns/pod-1: …" (the agent's
// membersFailed and memberFailure).
var groupFailure = regexp.MustCompile(`^(group \S+ \S+: \d+ of \d+ members failed): ([^:]+): (.*)$`)

// nextMember starts the next member's part of a group failure.
var nextMember = regexp.MustCompile(`; [a-z0-9][a-z0-9.-]*/[a-z0-9][a-z0-9.-]*: `)

// toolErrorReply is a process's error reply as nvsnap-gpu-suspend prints
// it ("pid=1152 quiesce: err busy (GPU work did not drain)"), with the
// count the agent adds; it ends at the next clause.
var toolErrorReply = regexp.MustCompile(`pid=\d+ [^:|;]+: err .*?(?: \(\d+ times\))?(?: \(command:| \| |; |$)`)

// reasonSummary is the record's reason as the status shows it, at most
// reasonMax bytes. A long group failure keeps its count, its first
// member's name and that member's cause: a process's error reply when the
// GPU tool printed one, else the start of the member's error. The first
// 200 bytes of such a reason used to hold the count, the pod's name and
// "500 Internal Server Error: checkpoint fail" and nothing of why.
func reasonSummary(s string) string {
	if len(s) <= reasonMax {
		return s
	}
	// A member's error can hold a tool's output lines; the patterns match
	// within one line.
	m := groupFailure.FindStringSubmatch(strings.ReplaceAll(s, "\n", " "))
	if m == nil {
		return truncate(s)
	}
	member := m[3]
	if loc := nextMember.FindStringIndex(member); loc != nil {
		member = member[:loc[0]]
	}
	// The member's agent answered its driver with an HTTP error.
	cause := strings.TrimPrefix(member, "500 Internal Server Error: ")
	if r := toolErrorReply.FindString(member); r != "" {
		cause = strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(r, " (command:"), " | "), "; "))
	}
	return truncate(m[1] + "; " + m[2] + ": " + cause)
}

// truncate bounds s to reasonMax bytes, cutting at a rune boundary.
func truncate(s string) string {
	if len(s) <= reasonMax {
		return s
	}
	i := reasonMax
	for i > 0 && !utf8.RuneStart(s[i]) {
		i--
	}
	return s[:i]
}
