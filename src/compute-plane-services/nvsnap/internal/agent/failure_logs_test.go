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

	"github.com/sirupsen/logrus"
)

func TestKeepFailureLogs(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "abc__20261016-171936")
	for name, body := range map[string]string{"dump.log": "Error (criu/x.c:1): boom", "pages-1.img": "image", "sub/restore.log": "r"} {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	keepFailureLogs(root, dir, logrus.NewEntry(logrus.New()))
	kept := filepath.Join(root, failureLogsDir, "abc__20261016-171936")
	if b, err := os.ReadFile(filepath.Join(kept, "dump.log")); err != nil || string(b) != "Error (criu/x.c:1): boom" {
		t.Fatalf("dump.log not kept: %q %v", b, err)
	}
	if _, err := os.Stat(filepath.Join(kept, "sub", "restore.log")); err != nil {
		t.Errorf("a nested log was not kept: %v", err)
	}
	if _, err := os.Stat(filepath.Join(kept, "pages-1.img")); !os.IsNotExist(err) {
		t.Errorf("an image was kept with the logs: %v", err)
	}
}

func TestPruneFailureLogs(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	for i := range 5 {
		p := filepath.Join(dir, fmt.Sprintf("c%d", i))
		if err := os.Mkdir(p, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, now, now.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	pruneFailureLogs(dir, 3)
	ents, _ := os.ReadDir(dir)
	if len(ents) != 3 || ents[0].Name() != "c2" || ents[2].Name() != "c4" {
		t.Errorf("kept %v, want the three newest", ents)
	}
}

// A failed capture's error says where the tool's whole output is kept:
// the error carries only its last lines.
func TestKeepFailedCapture_SaysWhereTheToolOutputIs(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "abc__20261122-233816")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	for i := range 40 {
		fmt.Fprintf(&out, "pid=%d hold: ok held\n", 811+i%4)
	}
	te := &toolError{args: suspendArgs, err: exitError(t), out: out.String()}
	err := keepFailedCapture(root, dir, fmt.Errorf("CRIU dump failed (criu-v2): gpushare suspend: %w", te), logrus.NewEntry(logrus.New()))
	kept := filepath.Join(root, failureLogsDir, "abc__20261122-233816", "nvsnap-gpu-suspend-suspend.log")
	if !strings.Contains(err.Error(), kept) {
		t.Fatalf("error %q does not say where the output is (%s)", err, kept)
	}
	if !errors.As(err, &te) {
		t.Error("the tool's error is lost")
	}
	if b, rerr := os.ReadFile(kept); rerr != nil || string(b) != out.String() {
		t.Errorf("kept output differs (%d of %d bytes): %v", len(b), out.Len(), rerr)
	}
	// The tool's output could not be kept, the other logs were: the error
	// does not claim it was.
	root2 := t.TempDir()
	dir2 := filepath.Join(root2, "abc__20261122-233817")
	if err := os.MkdirAll(dir2, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir2, "dump.log"), []byte("criu"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A directory where the tool's log would be copied: its copy fails.
	if err := os.MkdirAll(filepath.Join(root2, failureLogsDir, "abc__20261122-233817", "nvsnap-gpu-suspend-suspend.log"), 0o755); err != nil {
		t.Fatal(err)
	}
	err2 := keepFailedCapture(root2, dir2, fmt.Errorf("gpushare suspend: %w", te), logrus.NewEntry(logrus.New()))
	if strings.Contains(err2.Error(), "whole output is kept") {
		t.Errorf("error %q says the tool's output is kept, but its copy failed", err2)
	}
	if _, serr := os.Stat(filepath.Join(root2, failureLogsDir, "abc__20261122-233817", "dump.log")); serr != nil {
		t.Errorf("the other logs were not kept: %v", serr)
	}
	// Without a tool error, nothing is added.
	plain := errors.New("criu: exit status 1")
	if got := keepFailedCapture(root, dir, plain, logrus.NewEntry(logrus.New())); got.Error() != plain.Error() {
		t.Errorf("a failure without tool output became %q", got)
	}
}
