// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Lifecycle metrics: what captures, restores and admissions did and how
// long they took, kept by the cluster's Prometheus so the history outlives
// the agents. Every label takes a bounded set of values: no namespace, pod,
// function or checkpoint (those are in the summary log lines).

// Capture and restore methods.
const (
	MethodCRIU     = "criu"
	MethodCachedir = "cachedir"
	MethodCold     = "cold"
)

// Capture outcomes.
const (
	CaptureCaptured       = "captured"        // dumped; its restore verifies it
	CaptureVerified       = "verified"        // its restored pods served
	CaptureUnverified     = "unverified"      // its restored pods never served
	CaptureFailed         = "failed"          // the capture itself failed
	CaptureRefused        = "refused"         // the preflight refused the configuration
	CaptureGPUShareFailed = "gpushare-failed" // the pod never started under the library
	CaptureAbandoned      = "abandoned"       // its namespace went before a restore of it served
)

// Restore outcomes.
const (
	RestoreRestored = "restored"
	RestoreFailed   = "failed"  // ran and failed
	RestoreNotRun   = "not-run" // could not run (fetch, placeholders); tried again
	RestoreRefused  = "refused" // the pod may not restore what it names
	// RestoreGPUFault ran and failed on a GPU the probe found faulty: not
	// counted against the capture.
	RestoreGPUFault = "gpu-fault"
)

// Teardown outcomes.
const (
	TeardownReleased = "released" // every rank of the restore exited; the pod went
	TeardownDeadline = "deadline" // the grace period ran out first; the pod was killed
)

// GPU probe results: the probe of a GPU a failed pod was given.
const (
	ProbeHealthy = "healthy"
	ProbeFaulty  = "faulty"
	ProbeUnknown = "unknown" // the probe could not tell; the failure counts
)

// Admission decisions. AdmitMissed is counted by the agents, not the
// webhook: a pod of an instance that the webhook never mutated (it failed
// open, or the pod opts out).
const (
	AdmitRestore  = "restore"
	AdmitFallback = "fallback"
	AdmitCold     = "cold"
	AdmitMissed   = "missed"
)

var (
	secondsBuckets = []float64{1, 2, 5, 10, 20, 30, 45, 60, 90, 120, 180, 240, 300, 450, 600, 900, 1200, 1800, 2700, 3600}
	bytesBuckets   = []float64{1e6, 1e7, 1e8, 1e9, 5e9, 1e10, 2.5e10, 5e10, 1e11, 2.5e11, 5e11, 1e12, 2e12}

	// CaptureOutcomes counts captures by method and outcome; reason is the
	// class of a failure or refusal ("" otherwise).
	CaptureOutcomes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "capture_outcomes_total",
		Help:      "Captures by method, outcome and failure reason, GPU product and instance size.",
	}, []string{"method", "outcome", "reason", "gpu", "ranks"})

	// CapturePhaseSeconds is the time a capture spent in a phase: per rank
	// "dump" (freeze, CRIU, GPU state, snapshot), "snapshot" (replay mounts
	// and rootfs diff, the tree stopped) and "rank" (all of it); per instance "group" (the driver's dump of every
	// member) and "verify" (capture start to its restored pods serving).
	CapturePhaseSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "capture_phase_seconds",
		Help:      "Time a capture spent in each phase.",
		Buckets:   secondsBuckets,
	}, []string{"method", "phase", "gpu", "ranks"})

	// CaptureBytes is the size of a rank's capture by part: "gpu" (GPU
	// state), "memory" (process images), "rootfs" (rootfs diff), "cachedir".
	CaptureBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "capture_bytes",
		Help:      "Size of a rank's capture by part.",
		Buckets:   bytesBuckets,
	}, []string{"method", "part", "gpu"})

	// RestoreOutcomes counts restores by method and outcome.
	RestoreOutcomes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "restore_outcomes_total",
		Help:      "Restores by method, outcome and failure reason, GPU product and instance size.",
	}, []string{"method", "outcome", "reason", "gpu", "ranks"})

	// RestorePhaseSeconds is the time a restore spent in a phase: per rank
	// "criu", "gpu" (GPU state reload and resume), "clock_wait" (the part of
	// "gpu" spent waiting for the restored clocks to reach the dump) and
	// "rank" (the node's whole restore); per instance
	// "group" (placeholders running to every rank restored) and "ready"
	// (restored to the pod Ready).
	RestorePhaseSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "restore_phase_seconds",
		Help:      "Time a restore spent in each phase.",
		Buckets:   secondsBuckets,
	}, []string{"method", "phase", "gpu", "ranks"})

	// RestoreFetches counts checkpoint fetches to a node by where the bytes
	// came from ("rails", "peer", "fsstore", "volume"). A checkpoint already
	// on the node is no fetch.
	RestoreFetches = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "restore_fetches_total",
		Help:      "Checkpoint fetches to a node by source and result.",
	}, []string{"source", "result"})

	// RestoreFetchSeconds is the time a successful fetch took, by source.
	RestoreFetchSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "restore_fetch_seconds",
		Help:      "Time a checkpoint fetch to a node took, by source.",
		Buckets:   secondsBuckets,
	}, []string{"source"})

	// PodReadySeconds is a GPU pod's time from creation to Ready, by how
	// it started: restored (criu), seeded from a cachedir capture
	// (cachedir), or cold. Cold pods are the baseline the others save on.
	PodReadySeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "pod_ready_seconds",
		Help:      "GPU pod time from creation to Ready, by how it started.",
		Buckets:   secondsBuckets,
	}, []string{"method", "gpu", "ranks"})

	// Admissions counts the webhook's decision for a GPU pod of a captured
	// configuration: restore, fall back to the cachedir, or start cold, and
	// why.
	Admissions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "admissions_total",
		Help:      "Webhook decisions for GPU pods of captured configurations, and why.",
	}, []string{"decision", "reason"})

	// WebhookRequests counts the admission requests this agent's webhook
	// answered, by HTTP status, and WebhookSeconds their time. Against the
	// API server's own count of its calls, a gap is calls that never
	// reached an agent.
	WebhookRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "webhook_requests_total",
		Help:      "Admission requests this agent's webhook answered, by HTTP status.",
	}, []string{"code"})

	// WebhookSeconds is the time this agent's webhook took to answer.
	WebhookSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "webhook_request_seconds",
		Help:      "Time this agent's webhook took to answer an admission request.",
		Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	})

	// GPUProbes counts the probes of the GPUs a failed pod was given, by
	// result and, for a faulty GPU, the driver's error (a fixed set of
	// names). A faulty GPU's failure is not counted against nvsnap.
	GPUProbes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "gpu_probes_total",
		Help:      "Probes of the GPUs of a pod that failed, by GPU product, result and CUDA error.",
	}, []string{"gpu", "result", "error"})

	// PodStartSeconds is a GPU pod's time from being scheduled, its GPUs
	// given to it, to Ready, by how it started: the scale-up time. Unlike
	// PodReadySeconds it leaves out the time the pod waited for GPUs (a
	// restored instance's pods wait for the instance it replaces to go).
	PodStartSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "pod_start_seconds",
		Help:      "GPU pod time from scheduled to Ready (scale-up), by how it started.",
		Buckets:   secondsBuckets,
	}, []string{"method", "gpu", "ranks"})

	// TeardownOutcomes counts how restored ranks' group teardowns ended:
	// released (every rank of the restore exited, then the pod went) or
	// deadline (the pod's grace period ran out first, so it was killed).
	TeardownOutcomes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "teardown_outcomes_total",
		Help:      "Group teardowns of restored ranks by outcome: released, or deadline (killed at the end of the grace period).",
	}, []string{"outcome", "gpu", "ranks"})

	// TeardownSeconds is a released rank's time from its delete to every
	// rank of its restore having exited.
	TeardownSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "teardown_seconds",
		Help:      "Time from a restored rank's delete to every rank of its restore having exited.",
		Buckets:   secondsBuckets,
	}, []string{"gpu", "ranks"})

	// CaptureRecords is the number of capture records (the cached
	// captures restores use) by state and the agent bundle that made them;
	// an alias of a record is state "alias". Exported by one agent of the
	// cluster (the inventory leader).
	CaptureRecords = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "capture_records",
		Help:      "Capture records by state and the agent bundle that made them (inventory leader only).",
	}, []string{"state", "bundle"})

	// CaptureRecordOldestSeconds is the age of the oldest capture record
	// in each state: a record stuck verifying shows here.
	CaptureRecordOldestSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "capture_record_oldest_seconds",
		Help:      "Age of the oldest capture record in each state (inventory leader only).",
	}, []string{"state"})

	// CaptureRecordsUnrestored is the number of complete (verified)
	// capture records no restore has used since their verifying restore.
	CaptureRecordsUnrestored = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "capture_records_unrestored",
		Help:      "Verified capture records not restored since they were verified (inventory leader only).",
	})

	// InventoryLeader is 1 on the agent that exports the capture-record
	// inventory, and InventoryScanTimestamp the time its last scan of the
	// records succeeded.
	InventoryLeader = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "inventory_leader",
		Help:      "1 on the agent that exports the capture-record inventory.",
	})
	InventoryScanTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "inventory_scan_timestamp_seconds",
		Help:      "Unix time of the inventory leader's last successful scan of the capture records.",
	})

	// BuildInfo is 1 for the agent bundle this agent serves; join on it to
	// compare releases.
	BuildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "build_info",
		Help:      "The agent bundle version this agent serves.",
	}, []string{"version"})
)

// lifecycleCollectors are registered with the agent's.
var lifecycleCollectors = []prometheus.Collector{
	CaptureOutcomes, CapturePhaseSeconds, CaptureBytes,
	RestoreOutcomes, RestorePhaseSeconds, RestoreFetches, RestoreFetchSeconds,
	PodReadySeconds, PodStartSeconds, TeardownOutcomes, TeardownSeconds,
	CaptureRecords, CaptureRecordOldestSeconds, CaptureRecordsUnrestored, InventoryLeader, InventoryScanTimestamp,
	Admissions, WebhookRequests, WebhookSeconds, GPUProbes, BuildInfo,
}

// Ranks buckets an instance size: the label stays bounded for instances of
// tens of ranks.
func Ranks(n int) string {
	switch {
	case n <= 0:
		return "unknown"
	case n == 1:
		return "1"
	case n <= 4:
		return "2-4"
	case n <= 8:
		return "5-8"
	case n <= 16:
		return "9-16"
	default:
		return "17+"
	}
}

// labelMax bounds a free-form label value (a GPU product name).
const labelMax = 64

// GPU is the label value of a GPU product name.
func GPU(product string) string {
	product = strings.TrimSpace(product)
	if product == "" {
		return "unknown"
	}
	if len(product) > labelMax {
		product = product[:labelMax]
	}
	return product
}

// The texts Reason classes as the workload's own doing, as the agent
// writes them into a capture's error (lower case: Reason matches them in
// the lowered text).
const (
	// WorkloadExitedText: the container's process was gone when its dump
	// failed.
	WorkloadExitedText = "the workload exited during the capture"
	// NoGPUProcessText: a multi-node instance's pod had no process on the
	// GPU under the gpushare library to capture.
	NoGPUProcessText = "no process of this pod runs on the gpu"
)

// Reason classes a failure's text into a bounded label value.
func Reason(text string) string {
	t := strings.ToLower(text)
	switch {
	case t == "":
		return ""
	// The workload's own doing, checked first: the rest of the text is
	// what the capture ran into because of it.
	case strings.Contains(t, WorkloadExitedText):
		return "workload_exited"
	case strings.Contains(t, NoGPUProcessText):
		return "no_gpu_process"
	case strings.Contains(t, "mptcp"):
		return "mptcp"
	case strings.Contains(t, "no nvsnap agent"):
		return "no_agent"
	case strings.Contains(t, "rdma") || strings.Contains(t, "infiniband"):
		return "rdma"
	case strings.Contains(t, "gpushare"):
		return "gpushare"
	case strings.Contains(t, "ensure-local") || strings.Contains(t, "could not be brought"):
		return "fetch"
	case strings.Contains(t, "deadline exceeded") || strings.Contains(t, "timed out") || strings.Contains(t, "timeout"):
		return "timeout"
	case strings.Contains(t, "criu"):
		return "criu"
	default:
		return "other"
	}
}

// count adds one to v's series of labels, held on its first sight
// (first_sight.go).
func count(name string, v *prometheus.CounterVec, labels ...string) {
	sight.observe(name, labels, func() { v.WithLabelValues(labels...) }, func() { v.WithLabelValues(labels...).Inc() })
}

// observe records x in h's series of labels, held on its first sight.
func observe(name string, h *prometheus.HistogramVec, x float64, labels ...string) {
	sight.observe(name, labels, func() { h.WithLabelValues(labels...) }, func() { h.WithLabelValues(labels...).Observe(x) })
}

// ObserveCapture counts a capture's outcome.
func ObserveCapture(method, outcome, reason, gpu string, ranks int) {
	count("capture_outcomes", CaptureOutcomes, method, outcome, reason, GPU(gpu), Ranks(ranks))
}

// ObserveCapturePhase records the time a capture spent in phase.
func ObserveCapturePhase(method, phase, gpu string, ranks int, d time.Duration) {
	if d < 0 {
		return
	}
	observe("capture_phase", CapturePhaseSeconds, d.Seconds(), method, phase, GPU(gpu), Ranks(ranks))
}

// ObserveCaptureBytes records the size of a part of a rank's capture; an
// empty part is not recorded.
func ObserveCaptureBytes(method, part, gpu string, n int64) {
	if n <= 0 {
		return
	}
	observe("capture_bytes", CaptureBytes, float64(n), method, part, GPU(gpu))
}

// ObserveRestore counts a restore's outcome.
func ObserveRestore(method, outcome, reason, gpu string, ranks int) {
	count("restore_outcomes", RestoreOutcomes, method, outcome, reason, GPU(gpu), Ranks(ranks))
}

// ObserveRestorePhase records the time a restore spent in phase.
func ObserveRestorePhase(method, phase, gpu string, ranks int, d time.Duration) {
	if d < 0 {
		return
	}
	observe("restore_phase", RestorePhaseSeconds, d.Seconds(), method, phase, GPU(gpu), Ranks(ranks))
}

// ObserveFetch records a checkpoint fetch from source that took d.
func ObserveFetch(source string, ok bool, d time.Duration) {
	if source == "" {
		source = "unknown"
	}
	result := "ok"
	if !ok {
		result = "failed"
	}
	count("restore_fetches", RestoreFetches, source, result)
	if ok {
		observe("restore_fetch", RestoreFetchSeconds, d.Seconds(), source)
	}
}

// ObservePodReady records a GPU pod's time from creation to Ready.
func ObservePodReady(method, gpu string, ranks int, d time.Duration) {
	if d < 0 {
		return
	}
	observe("pod_ready", PodReadySeconds, d.Seconds(), method, GPU(gpu), Ranks(ranks))
}

// ObservePodStart records a GPU pod's time from being scheduled (its GPUs
// given to it) to Ready.
func ObservePodStart(method, gpu string, ranks int, d time.Duration) {
	if d < 0 {
		return
	}
	observe("pod_start", PodStartSeconds, d.Seconds(), method, GPU(gpu), Ranks(ranks))
}

// ObserveTeardown records how a restored rank's group teardown ended
// (TeardownReleased or TeardownDeadline) and, released, how long from
// its delete it took.
func ObserveTeardown(outcome, gpu string, ranks int, d time.Duration) {
	count("teardown_outcomes", TeardownOutcomes, outcome, GPU(gpu), Ranks(ranks))
	if outcome == TeardownReleased && d >= 0 {
		observe("teardown", TeardownSeconds, d.Seconds(), GPU(gpu), Ranks(ranks))
	}
}

// CountAdmission counts a webhook decision.
func CountAdmission(decision, reason string) {
	count("admissions", Admissions, decision, reason)
}

// ObserveGPUProbe counts the probe of one GPU.
func ObserveGPUProbe(gpu, result, cudaError string) {
	count("gpu_probes", GPUProbes, GPU(gpu), result, cudaError)
}

// ObserveWebhook records an admission request answered with code in d.
func ObserveWebhook(code int, d time.Duration) {
	count("webhook_requests", WebhookRequests, strconv.Itoa(code))
	WebhookSeconds.Observe(d.Seconds())
}

// SetBuildInfo marks version as the bundle this agent serves.
func SetBuildInfo(version string) {
	if version == "" {
		version = "unknown"
	}
	BuildInfo.Reset()
	BuildInfo.WithLabelValues(version).Set(1)
}
