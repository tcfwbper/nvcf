// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/metrics"
)

// kmsgRecord is one /dev/kmsg record: "prio,seq,usec,flags;message", then
// its dictionary lines, each led by a space.
func kmsgRecord(seq int, msg string) string {
	return fmt.Sprintf("4,%d,%d,-;%s\n SUBSYSTEM=pci\n DEVICE=+pci:0008:06:00.0\n", seq, 1000+seq, msg)
}

func appendFile(t *testing.T, path, s string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(s); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// As the driver logs it (a node's kernel log, 2026-11-22).
const xid137 = "NVRM: Xid (PCI:0008:06:00): 137, pid=524770, name=nvsnap-gpu-susp, RLW_RXPIPE interrupt hit on link 15 on GPU0: PRIV Error"

// The cursor reads only what the kernel logged after it was opened, the
// messages without their dictionary lines.
func TestKmsgCursor_ReadsOnlyNewRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kmsg")
	if err := os.WriteFile(path, []byte(kmsgRecord(1, "NVRM: Xid (PCI:0008:06:00): 137, pid=1, name=old, before the capture")), 0o644); err != nil {
		t.Fatal(err)
	}
	c, err := openKmsgAt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	appendFile(t, path, kmsgRecord(2, "eth0: link up")+kmsgRecord(3, xid137))
	lines, truncated := c.drain(time.Second, 1<<20)
	if truncated {
		t.Error("a short log read as truncated")
	}
	if len(lines) != 2 || lines[0] != "eth0: link up" || lines[1] != xid137 {
		t.Fatalf("read %q, want the two new messages", lines)
	}
	if more, _ := c.drain(time.Second, 1<<20); len(more) != 0 {
		t.Errorf("a second drain read %q again", more)
	}
}

// A drain stops at its byte budget and says so.
func TestKmsgCursor_BoundedBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kmsg")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	c, err := openKmsgAt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var b strings.Builder
	for i := range 2000 {
		b.WriteString(kmsgRecord(i, xid137))
	}
	appendFile(t, path, b.String())
	lines, truncated := c.drain(time.Second, 64<<10)
	if !truncated {
		t.Error("a log over the budget read as whole")
	}
	if n := len(strings.Join(lines, "\n")); n > 64<<10 {
		t.Errorf("read %d bytes, over the 64 KiB budget", n)
	}
	if len(lines) == 0 {
		t.Error("read nothing within the budget")
	}
}

func TestKmsgCursor_NoDevice(t *testing.T) {
	if _, err := openKmsgAt(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("a missing kernel log opened")
	}
}

// The GPU faults of a capture: Xid 137 on the workload's GPUs; other Xids,
// other GPUs and other messages are not. Without the workload's GPUs, the
// lines naming the GPU tool count.
func TestXidFaults(t *testing.T) {
	lines := []string{
		"eth0: link up",
		xid137,
		"NVRM: Xid (PCI:0009:06:00): 137, pid=524770, name=nvsnap-gpu-susp, RLW_RXPIPE interrupt hit on link 6 on GPU0: PRIV Error",
		"NVRM: Xid (PCI:0018:06:00): 137, pid=99, name=other-job, RLW_RXPIPE interrupt hit on link 1 on GPU0: PRIV Error",
		"NVRM: Xid (PCI:0008:06:00): 45, pid=1964987, name=VLLM::Worker, channel 0x00000002",
	}
	f := xidFaults(lines, map[string]bool{"0008:06:00": true, "0009:06:00": true})
	if len(f) != 2 || f[0] != lines[1] || f[1] != lines[2] {
		t.Errorf("faults on the workload's GPUs: %q", f)
	}
	f = xidFaults(lines, nil)
	if len(f) != 2 || f[0] != lines[1] || f[1] != lines[2] {
		t.Errorf("faults naming the GPU tool: %q", f)
	}
	if f := xidFaults(lines[:1], nil); len(f) != 0 {
		t.Errorf("faults in a quiet log: %q", f)
	}
}

// The error of a capture with GPU faults names the first and how many,
// is classed gpu_xid, and is no reason to replace the workload.
func TestXidFaultError(t *testing.T) {
	faults := []string{xid137, xid137, xid137}
	err := xidFaultError(faults, false)
	if !errors.Is(err, errXidFault) {
		t.Fatalf("not a GPU fault: %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "3 line(s)") || !strings.Contains(msg, "PCI:0008:06:00") {
		t.Errorf("error does not say what and how many: %q", msg)
	}
	if got := metrics.Reason("CRIU dump failed (criu-v2): " + msg); got != "gpu_xid" {
		t.Errorf("reason %q, want gpu_xid", got)
	}
	if sourceMustBeReplaced(err) || sourceMustBeReplacedText(msg) {
		t.Error("a GPU fault replaces the workload's pods")
	}
	if !strings.Contains(xidFaultError(faults, true).Error(), "more") {
		t.Error("a truncated read does not say there may be more")
	}
}

// The driver's GPU directories are named by PCI address: the address of
// each UUID, as the kernel log writes it (without the function).
func TestDriverGPUPCI(t *testing.T) {
	dir := t.TempDir()
	for pci, uuid := range map[string]string{"0008:06:00.0": "GPU-aaa", "0009:06:00.0": "GPU-bbb"} {
		if err := os.MkdirAll(filepath.Join(dir, pci), 0o755); err != nil {
			t.Fatal(err)
		}
		info := "Model: \t\t NVIDIA GB300\nGPU UUID: \t " + uuid + "\nDevice Minor: \t 0\n"
		if err := os.WriteFile(filepath.Join(dir, pci, "information"), []byte(info), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	got := gpuPCIAddrs(dir, "GPU-bbb\nGPU-zzz\n")
	if len(got) != 1 || !got["0009:06:00"] {
		t.Errorf("addresses %v, want only 0009:06:00", got)
	}
	if got := gpuPCIAddrs(dir, ""); got != nil {
		t.Errorf("no GPUs gave %v", got)
	}
}
