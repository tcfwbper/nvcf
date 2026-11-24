// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package modelcache

import (
	"os"
	"path/filepath"
	"strings"
)

// Single flight (docs/design/model-cache-single-flight.md): the agent
// tells each waiting download init whether its pod downloads or waits
// for a copy streamed from the node that does, in a plan file in the
// key's directory, which the init mounts read-only.
const (
	// PodUIDEnv carries a wrapped download init's pod uid, from the
	// downward API: the helper reads its pod's plan by it.
	PodUIDEnv = "NVSNAP_POD_UID"
	// PlanPrefix starts a pod's plan file's name; the pod's uid follows.
	PlanPrefix = ".plan-"
	// PlanDownload: the pod runs its download.
	PlanDownload = "download"
	// PlanWait: the pod waits while the agent streams into its volume.
	PlanWait = "wait"
	// StreamingPrefix starts the heartbeat the agent refreshes while it
	// streams into a waiting pod's volume; the pod's uid follows, so one
	// pod's stream ending never ends another's wait. It is not
	// IncomingFile: a helper from before single flight waits on that one
	// for at most its --wait.
	StreamingPrefix = ".streaming-"
	// FinalPrefix and DonePrefix start the names of a pod's end markers;
	// the pod's uid follows. Final: the agent read the downloading pod's
	// volume for the final manifest, and holds the tree's Fingerprint.
	// Done: the agent filled a waiting pod's volume and checked it.
	FinalPrefix = ".final-"
	DonePrefix  = ".done-"
	// FinalMismatch, in a download init's termination message, says the
	// agent's final manifest is not of the tree the download left.
	FinalMismatch = "final-manifest-mismatch"
)

// StreamingFileOf names pod uid's streaming heartbeat, or "" for a uid
// that is not one.
func StreamingFileOf(uid string) string { return marker(StreamingPrefix, uid) }

// FinalFile and DoneFile name pod uid's end markers, or "" for a uid that
// is not one.
func FinalFile(uid string) string { return marker(FinalPrefix, uid) }
func DoneFile(uid string) string  { return marker(DonePrefix, uid) }

func marker(prefix, uid string) string {
	if !validUID(uid) {
		return ""
	}
	return prefix + uid
}

// validUID accepts a Kubernetes uid: letters, digits and dashes, as the
// API server makes them. Anything else could name a path.
func validUID(uid string) bool {
	if uid == "" || len(uid) > 64 {
		return false
	}
	for _, r := range uid {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-') {
			return false
		}
	}
	return true
}

// PlanFile is the name of pod uid's plan file in a key's directory, or ""
// for a uid that is not one.
func PlanFile(uid string) string { return marker(PlanPrefix, uid) }

// ReadPlan returns pod uid's plan in keyDir, or "" when there is none or
// it is not one the helper knows.
func ReadPlan(keyDir, uid string) string {
	name := PlanFile(uid)
	if name == "" {
		return ""
	}
	b, err := os.ReadFile(filepath.Join(keyDir, name))
	if err != nil {
		return ""
	}
	switch p := strings.TrimSpace(string(b)); p {
	case PlanDownload, PlanWait:
		return p
	}
	return ""
}

// WritePlan sets pod uid's plan in keyDir. It writes a temporary file and
// renames it, so a reader sees the old plan or the new one, never part.
func WritePlan(keyDir, uid, plan string) error {
	name := PlanFile(uid)
	if name == "" {
		return os.ErrInvalid
	}
	if err := os.MkdirAll(keyDir, 0o755); err != nil {
		return err
	}
	tmp := filepath.Join(keyDir, name+".tmp")
	if err := os.WriteFile(tmp, []byte(plan+"\n"), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(keyDir, name))
}

// RemovePlan removes pod uid's plan from keyDir.
func RemovePlan(keyDir, uid string) {
	if name := PlanFile(uid); name != "" {
		_ = os.Remove(filepath.Join(keyDir, name))
	}
}

// Plans lists the uids that have a plan in keyDir.
func Plans(keyDir string) []string {
	ents, err := os.ReadDir(keyDir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range ents {
		n := e.Name()
		if uid := strings.TrimPrefix(n, PlanPrefix); uid != n && !strings.HasSuffix(n, ".tmp") && validUID(uid) {
			out = append(out, uid)
		}
	}
	return out
}
