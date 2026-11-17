#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Generates deploy/helm/nvsnap/files/grafana-dashboard-nvsnap-history.json:
#   scripts/grafana-dashboard-history.py deploy/helm/nvsnap/files/grafana-dashboard-nvsnap-history.json
# Edit this, not the JSON. Every query filters on $cluster (remote-written
# series carry the cluster label; "All" also matches series without it).
import json, sys
out = sys.argv[1]
DS = {"type": "prometheus", "uid": "${datasource}"}
C = 'cluster=~"$cluster"'
G = C + ', gpu=~"$gpu", ranks=~"$ranks"'
panels, pid, y = [], [0], [0]

def nid():
    pid[0] += 1
    return pid[0]

def row(title):
    panels.append({"type": "row", "title": title, "id": nid(), "collapsed": False, "panels": [],
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": y[0]}})
    y[0] += 1

def tgt(expr, legend, ref="A", instant=False):
    t = {"datasource": DS, "expr": expr, "legendFormat": legend, "refId": ref}
    if instant:
        t["instant"] = True
    return t

# Count panels draw one bar per step: increase() over $__interval, so the
# bars add up to the events. MIN_STEP keeps each window at least four 30 s
# scrapes wide, where $__interval would otherwise shrink below two and
# return nothing; $__rate_interval would overlap the windows and count an
# event in more than one bar.
MIN_STEP = "2m"

def ts(title, desc, targets, x, w=12, h=8, unit=None, stack=False, bars=False, overrides=None, min_step=False):
    d = {"unit": unit} if unit else {}
    custom = {}
    if stack:
        custom["stacking"] = {"mode": "normal"}
    if bars:
        custom.update({"drawStyle": "bars", "fillOpacity": 60})
    if custom:
        d["custom"] = custom
    panels.append({"type": "timeseries", "title": title, "description": desc, "id": nid(), "datasource": DS,
                   "gridPos": {"h": h, "w": w, "x": x, "y": y[0]},
                   "fieldConfig": {"defaults": d, "overrides": overrides or []},
                   **({"interval": MIN_STEP} if bars or min_step else {}),
                   "options": {"legend": {"displayMode": "table", "placement": "bottom", "calcs": ["mean", "max", "lastNotNull"]},
                               "tooltip": {"mode": "multi"}},
                   "targets": targets})

def stat(title, desc, expr, x, w=3, unit=None, thresholds=None, decimals=None):
    d = {"unit": unit or "short", "color": {"mode": "thresholds"},
         "thresholds": {"mode": "absolute", "steps": thresholds or [{"color": "text", "value": None}]}}
    if decimals is not None:
        d["decimals"] = decimals
    panels.append({"type": "stat", "title": title, "description": desc, "id": nid(), "datasource": DS,
                   "gridPos": {"h": 4, "w": w, "x": x, "y": y[0]},
                   "fieldConfig": {"defaults": d, "overrides": []},
                   "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                               "colorMode": "value", "graphMode": "none", "textMode": "value", "justifyMode": "center"},
                   "targets": [tgt(expr, "", instant=True)]})

def q(p, metric, by, sel=G, rng="$__rate_interval"):
    return f'histogram_quantile({p}, sum by (le, {by}) (rate({metric}_bucket{{{sel}}}[{rng}])))'

RED = [{"color": "green", "value": None}, {"color": "red", "value": 1}]

row("At a glance (the selected time range)")
stat("Restores", "Instances restored from a capture.", f'sum(increase(nvsnap_restore_outcomes_total{{outcome="restored", {G}}}[$__range])) or vector(0)', 0, decimals=0)
stat("Restores failed or not run", "Restores that failed, or could not run and were tried again.", f'sum(increase(nvsnap_restore_outcomes_total{{outcome=~"failed|not-run", {G}}}[$__range])) or vector(0)', 3, thresholds=RED, decimals=0)
stat("Scale-up p50, restored", "Restored GPU pods, scheduled to Ready.", f'histogram_quantile(0.5, sum by (le) (increase(nvsnap_pod_start_seconds_bucket{{method="criu", {G}}}[$__range])))', 6, unit="s")
stat("Scale-up p50, cold", "Cold-started GPU pods, scheduled to Ready: the baseline.", f'histogram_quantile(0.5, sum by (le) (increase(nvsnap_pod_start_seconds_bucket{{method="cold", {G}}}[$__range])))', 9, unit="s")
stat("Teardowns killed", "Restored ranks killed at the end of their grace period before every rank of their restore exited.", f'sum(increase(nvsnap_teardown_outcomes_total{{outcome="deadline", {G}}}[$__range])) or vector(0)', 12, thresholds=RED, decimals=0)
stat("Captures failed", "Captures that failed, never started under the GPU library, or restored without serving.", f'sum(increase(nvsnap_capture_outcomes_total{{outcome=~"failed|gpushare-failed|unverified", {G}}}[$__range])) or vector(0)', 15, thresholds=RED, decimals=0)
stat("Verified captures cached", "Capture records restores can use (state complete), now.", f'sum(max by (cluster, bundle) (nvsnap_capture_records{{state="complete", {C}}}))', 18, decimals=0)
stat("NvSnap alerts firing", "NvSnap alerts firing now (docs/operate/RUNBOOK.md).", f'count(ALERTS{{alertname=~"Nvsnap.*", alertstate="firing", {C}}}) or vector(0)', 21, thresholds=RED, decimals=0)
y[0] += 4

row("Scale-up: scheduled to Ready, by how the pod started")
ts("Scheduled to Ready, p50", "A GPU pod's time from being scheduled (given its GPUs) to Ready: the scale-up time. criu: restored; cachedir: seeded from a cache; cold: the baseline.", [tgt(q(0.5, "nvsnap_pod_start_seconds", "method"), "{{method}}")], 0, unit="s")
ts("Scheduled to Ready, p95", "As p50, the slowest 5%.", [tgt(q(0.95, "nvsnap_pod_start_seconds", "method"), "{{method}}")], 12, unit="s")
y[0] += 8
ts("Creation to Ready, p50", "From the pod's creation, so with the time it waited for GPUs (a restore's pods wait for the instance they replace to go).", [tgt(q(0.5, "nvsnap_pod_ready_seconds", "method"), "{{method}}")], 0, unit="s")
ts("Pod starts by method", "GPU pods that became Ready, by how they started.", [tgt(f'sum by (method) (increase(nvsnap_pod_ready_seconds_count{{{G}}}[$__interval]))', "{{method}}")], 12, stack=True, bars=True)
y[0] += 8

row("Restore")
ts("Restore outcomes", "restored; failed (ran and failed); not-run (could not run, tried again); refused; gpu-fault (a GPU the probe found faulty).", [tgt(f'sum by (outcome, reason) (increase(nvsnap_restore_outcomes_total{{{G}}}[$__interval]))', "{{outcome}} {{reason}}")], 0, stack=True, bars=True)
ts("Restore phases, p50", "criu: process restore; gpu: GPU state reload and resume; clock_wait: the part of gpu spent waiting for the restored clocks; rank: a node's whole restore; group: placeholders running to every rank restored; ready: restored to Ready.", [tgt(q(0.5, "nvsnap_restore_phase_seconds", "phase", 'method="criu", ' + G), "{{phase}}")], 12, unit="s")
y[0] += 8
ts("Restore phases, p95", "As p50, the slowest 5%.", [tgt(q(0.95, "nvsnap_restore_phase_seconds", "phase", 'method="criu", ' + G), "{{phase}}")], 0, unit="s")
ts("Checkpoint fetch to a node", "p50 time by where the bytes came from (left axis), and failed fetches (bars, right axis). A checkpoint already on the node is no fetch.",
   [tgt(q(0.5, "nvsnap_restore_fetch_seconds", "source", C), "p50 {{source}}"),
    tgt(f'sum by (source) (increase(nvsnap_restore_fetches_total{{result="failed", {C}}}[$__interval]))', "failed {{source}}", "B")], 12, unit="s",
   overrides=[{"matcher": {"id": "byFrameRefID", "options": "B"},
               "properties": [{"id": "unit", "value": "short"}, {"id": "decimals", "value": 0},
                              {"id": "custom.axisPlacement", "value": "right"}, {"id": "custom.axisLabel", "value": "failed fetches"},
                              {"id": "custom.drawStyle", "value": "bars"}, {"id": "custom.fillOpacity", "value": 60}]}], min_step=True)
y[0] += 8

row("Teardown of restored instances")
ts("Delete to every rank exited, p50 and p95", "A released restored rank's time from its delete to every rank of its restore having exited (mostly the engine's own graceful shutdown).",
   [tgt(q(0.5, "nvsnap_teardown_seconds", "ranks"), "p50 {{ranks}} ranks"), tgt(q(0.95, "nvsnap_teardown_seconds", "ranks"), "p95 {{ranks}} ranks", "B")], 0, unit="s")
ts("Teardown outcomes", "released: every rank exited, then the pod went. deadline: the grace period ran out first and the pod was killed (NvsnapTeardownKilled).", [tgt(f'sum by (outcome) (increase(nvsnap_teardown_outcomes_total{{{G}}}[$__interval]))', "{{outcome}}")], 12, stack=True, bars=True)
y[0] += 8

row("Capture")
ts("Capture outcomes", "captured; verified (its restored pods served); unverified; failed; refused; gpushare-failed; abandoned (its namespace went before it was verified).", [tgt(f'sum by (method, outcome, reason) (increase(nvsnap_capture_outcomes_total{{{G}}}[$__interval]))', "{{method}} {{outcome}} {{reason}}")], 0, stack=True, bars=True)
ts("Capture phases, p50", "dump, snapshot and rank per node; group (the driver's dump of every member) and verify (capture start to its restored pods serving) per instance.", [tgt(q(0.5, "nvsnap_capture_phase_seconds", "method, phase"), "{{method}} {{phase}}")], 12, unit="s")
y[0] += 8
ts("Capture size by part, p50", "A rank's capture by part: gpu (GPU state), memory (process images), rootfs (rootfs diff), cachedir.", [tgt(q(0.5, "nvsnap_capture_bytes", "method, part", 'cluster=~"$cluster", gpu=~"$gpu"'), "{{method}} {{part}}")], 0, unit="bytes")
ts("Capture phases, p95", "As p50, the slowest 5%.", [tgt(q(0.95, "nvsnap_capture_phase_seconds", "method, phase"), "{{method}} {{phase}}")], 12, unit="s")
y[0] += 8

row("Cached captures (the capture records)")
ts("Capture records by state", "complete: verified, restores use it. verifying: restored once to be verified. abandoned: its namespace went first. failed, gpushare-failed, restore-failed: not used. alias: names another record.", [tgt(f'sum by (state) (max by (cluster, state, bundle) (nvsnap_capture_records{{{C}}}))', "{{state}}")], 0, stack=True)
ts("Capture records by agent bundle", "Records by the agent release that captured them (aliases left out).", [tgt(f'sum by (bundle) (max by (cluster, state, bundle) (nvsnap_capture_records{{state!="alias", {C}}}))', "{{bundle}}")], 12, stack=True)
y[0] += 8
ts("Oldest record by state", "Age of the oldest record in each state. A capture verifying for hours is stuck (NvsnapCaptureStuckVerifying).", [tgt(f'max by (state) (nvsnap_capture_record_oldest_seconds{{{C}}})', "{{state}}")], 0, unit="s")
ts("Verified captures not restored since", "Complete records that no restore has used since their verification: cached, but not paying off.", [tgt(f'max by (cluster) (nvsnap_capture_records_unrestored{{{C}}})', "{{cluster}}")], 12)
y[0] += 8

row("Agents and webhook")
ts("Agents scraped", "nvsnap-agent pods Prometheus reaches, of those it knows (NvsnapAgentDown, NvsnapAgentMetricsMissing).", [tgt(f'sum by (cluster) (up{{job="nvsnap-agent", {C}}})', "up {{cluster}}"), tgt(f'count by (cluster) (up{{job="nvsnap-agent", {C}}})', "known {{cluster}}", "B")], 0, w=8)
ts("Agent bundle", "Agents by the bundle version they serve.", [tgt(f'count by (version) (nvsnap_build_info{{{C}}})', "{{version}}")], 8, w=8, stack=True)
ts("Inventory scan age", "Seconds since the inventory leader last scanned the capture records (NvsnapInventoryStale past 600).", [tgt(f'time() - max by (cluster) (nvsnap_inventory_scan_timestamp_seconds{{{C}}} > 0)', "{{cluster}}")], 16, w=8, unit="s")
y[0] += 8
ts("Webhook answers by status", "Admission requests the agents' webhook answered. It fails open: errors start pods cold.", [tgt(f'sum by (code) (rate(nvsnap_webhook_requests_total{{{C}}}[$__rate_interval]))', "{{code}}")], 0, w=8, unit="reqps")
ts("Webhook latency, p95", "Time the webhook took to answer.", [tgt(f'histogram_quantile(0.95, sum by (le) (rate(nvsnap_webhook_request_seconds_bucket{{{C}}}[$__rate_interval])))', "p95")], 8, w=8, unit="s")
ts("Admissions by decision and reason", "restore, fallback (to a cache), cold, and missed (ranks the webhook never mutated).", [tgt(f'sum by (decision, reason) (increase(nvsnap_admissions_total{{{C}}}[$__interval]))', "{{decision}} {{reason}}")], 16, w=8, stack=True, bars=True)
y[0] += 8

def var(name, label, query, all_=True):
    return {"type": "query", "name": name, "label": label, "datasource": DS,
            "query": {"query": query, "refId": name}, "definition": query,
            "includeAll": all_, "multi": True, "allValue": ".*",
            "current": {"selected": True, "text": ["All"], "value": ["$__all"]}, "refresh": 2, "hide": 0, "sort": 1}

dash = {
    "uid": "nvsnap-history", "title": "nvSnap capture and restore history",
    "description": "NvSnap captures, restores, scale-up, teardown and the cached captures, from the agents' metrics (docs/operate/METRICS.md; alerts: docs/operate/RUNBOOK.md).",
    "tags": ["nvsnap"], "timezone": "utc", "schemaVersion": 39, "version": 1, "editable": True,
    "time": {"from": "now-7d", "to": "now"}, "refresh": "5m", "annotations": {"list": []}, "links": [],
    "templating": {"list": [
        {"type": "datasource", "name": "datasource", "label": "Prometheus", "query": "prometheus", "current": {}, "hide": 0},
        var("cluster", "Cluster", 'label_values(nvsnap_build_info, cluster)'),
        # Every lifecycle series a GPU product or rank count occurs in: a
        # series exists only once its event has, so restores alone would
        # leave out a product that has only captured or cold-started.
        var("gpu", "GPU", 'label_values({__name__=~"nvsnap_restore_outcomes_total|nvsnap_capture_outcomes_total|nvsnap_pod_ready_seconds_count"}, gpu)'),
        var("ranks", "Ranks", 'label_values({__name__=~"nvsnap_restore_outcomes_total|nvsnap_capture_outcomes_total|nvsnap_pod_ready_seconds_count"}, ranks)'),
    ]},
    "panels": panels,
}
json.dump(dash, open(out, "w"), indent=2)
open(out, "a").write("\n")
print(len(panels), "panels")
