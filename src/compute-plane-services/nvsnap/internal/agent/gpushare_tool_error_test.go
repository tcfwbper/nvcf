// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
)

// exitError is a real *exec.ExitError (status 1).
func exitError(t *testing.T) error {
	t.Helper()
	err := exec.CommandContext(t.Context(), "sh", "-c", "exit 1").Run()
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		t.Fatalf("no exit error: %v", err)
	}
	return err
}

// quiesceNeverDrained is the shape of a 2-node rank's suspend that timed
// out on its quiesce: every 250 ms the first pid quiesced, the second was
// busy, and all were resumed (as recorded on a production cluster).
func quiesceNeverDrained(attempts int) string {
	var b strings.Builder
	for i := 0; i < attempts; i++ {
		b.WriteString("pid=1150 quiesce: ok quiesced\npid=1152 quiesce: err busy (GPU work did not drain)\n")
		for _, p := range []string{"1150", "1152", "1154", "1156"} {
			b.WriteString("pid=" + p + " resume: ok resumed (0 multicast bind(s), 0 multicast mapping(s))\n")
		}
	}
	return b.String()
}

var suspendArgs = []string{"--timeout-ms", "120000", "--store", "/nvsnap-gpushare", "--ckpt-dir", "/nvsnap-gpushare",
	"--fabric-map", "/nvsnap-gpushare/.fabric-s1", "suspend", "1", "934", "1150", "1152", "1154", "1156"}

// TestToolError_LeadsWithWhatFailed: the busy pid, and how often, comes
// first and survives a group record's cut of one member's error, where the
// command line and the last lines (all "resume: ok") used to fill it.
func TestToolError_LeadsWithWhatFailed(t *testing.T) {
	err := &toolError{args: suspendArgs, err: exitError(t), out: quiesceNeverDrained(480)}
	msg := err.Error()
	want := "nvsnap-gpu-suspend suspend: exit status 1: pid=1152 quiesce: err busy (GPU work did not drain) (480 times)"
	if !strings.HasPrefix(msg, want) {
		t.Fatalf("error starts %q, want %q", msg[:min(len(msg), len(want)+40)], want)
	}
	if !strings.Contains(memberFailure("ns/rank-0", "500 Internal Server Error: checkpoint failed: CRIU dump failed (criu-v2): gpushare suspend: "+msg), "err busy (GPU work did not drain) (480 times)") {
		t.Error("a group record's cut of the member's error loses the busy pid")
	}
	if !strings.Contains(msg, "(command: nvsnap-gpu-suspend --timeout-ms 120000") || !strings.Contains(msg, "; output: pid=") {
		t.Errorf("error lost the command or the output's last lines: %q", msg)
	}
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		t.Error("the exit error is not reachable through the tool error")
	}
}

func TestToolFailures(t *testing.T) {
	cases := map[string]struct{ out, want string }{
		"none":     {"pid=1 quiesce: ok quiesced\npid=1 resume: ok resumed\n", ""},
		"distinct": {"pid=1 remap /in: err not in the fabric map\nremap failed; pids left SIGSTOPped\n", "pid=1 remap /in: err not in the fabric map | remap failed; pids left SIGSTOPped"},
		"rollback": {"rolling back 2 pid(s) to RUNNING\nsuspend failed and so did the rollback\n", "rolling back 2 pid(s) to RUNNING | suspend failed and so did the rollback"},
	}
	for name, c := range cases {
		if got := toolFailures(c.out); got != c.want {
			t.Errorf("%s: %q, want %q", name, got, c.want)
		}
	}
	var many strings.Builder
	for i := 0; i < toolFailureMax+3; i++ {
		many.WriteString("pid=" + strings.Repeat("1", i+1) + " load: err out of memory\n")
	}
	if got := toolFailures(many.String()); !strings.HasSuffix(got, "| 3 more") {
		t.Errorf("more than %d distinct lines: %q, want a count of the rest", toolFailureMax, got)
	}
}

func TestToolVerb(t *testing.T) {
	for want, args := range map[string][]string{
		"suspend": suspendArgs,
		"resume":  {"--hold-gate", "--clock-at", "7", "--gpu-map", "/g", "resume", "1", "2"},
		"gpus":    {"gpus"},
		"":        {"--timeout-ms", "5"},
	} {
		if got := toolVerb(args); got != want {
			t.Errorf("toolVerb(%v) = %q, want %q", args, got, want)
		}
	}
}

// TestSuspendFailure_ToolError: a toolError still tells a rollback that
// failed (left suspended) from one that worked.
func TestSuspendFailure_ToolError(t *testing.T) {
	ok := &toolError{args: suspendArgs, err: exitError(t), out: quiesceNeverDrained(2)}
	if errors.Is(suspendFailure(ok), errSourceLeftSuspended) {
		t.Error("a suspend that rolled back reads as left suspended")
	}
	bad := &toolError{args: suspendArgs, err: exitError(t), out: "rolling back 4 pid(s) to RUNNING\n" + suspendRollbackFailed + "\n"}
	if !errors.Is(suspendFailure(bad), errSourceLeftSuspended) {
		t.Error("a suspend whose rollback failed does not read as left suspended")
	}
}

// TestWriteToolOutput_KeptWithTheFailedCapture: the whole output of the
// failed suspend is kept with the failed capture's logs.
func TestWriteToolOutput_KeptWithTheFailedCapture(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "ckpt-1")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	out := quiesceNeverDrained(3)
	err := errors.Join(errors.New("CRIU dump failed (criu-v2)"), &toolError{args: suspendArgs, err: exitError(t), out: out})
	writeToolOutput(dir, err)
	keepFailureLogs(root, dir, logrus.NewEntry(logrus.New()))
	got, rerr := os.ReadFile(filepath.Join(root, failureLogsDir, "ckpt-1", "nvsnap-gpu-suspend-suspend.log"))
	if rerr != nil {
		t.Fatalf("the suspend's output was not kept: %v", rerr)
	}
	if string(got) != out {
		t.Errorf("kept %d bytes, want the whole output (%d)", len(got), len(out))
	}
	// Another error: nothing written.
	other := t.TempDir()
	writeToolOutput(other, errors.New("criu dump: exit status 1"))
	if ents, _ := os.ReadDir(other); len(ents) != 0 {
		t.Errorf("wrote %d files for an error that is not the tool's", len(ents))
	}
}
