// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// A failed capture's directory is removed at once: nothing in it can be
// restored. Its logs (criu's dump.log above all) are the only account of
// why it failed, and the error carries only their tail, so they are kept
// under <checkpoint dir>/.failed-logs/<capture>, the newest
// failureLogsKept of them.

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/sirupsen/logrus"
)

const (
	failureLogsDir  = ".failed-logs"
	failureLogsKept = 20
	// failureLogMax bounds one kept log file.
	failureLogMax = 64 << 20
)

// writeToolOutput writes the whole output of the nvsnap-gpu-suspend run
// that failed the capture (a toolError in err) into dir, as
// nvsnap-gpu-suspend-<command>.log, for keepFailureLogs to keep: the error
// carries only what failed and the output's last lines.
// It returns the file's name, "" when it wrote none.
func writeToolOutput(dir string, err error) string {
	var te *toolError
	if !errors.As(err, &te) || te.out == "" {
		return ""
	}
	name := filepath.Base(gpushareToolName + "-" + toolVerb(te.args) + ".log")
	if werr := os.WriteFile(filepath.Join(dir, name), []byte(te.out), 0o644); werr != nil { //nolint:gosec // dir is the capture's own directory
		return ""
	}
	return name
}

// keepFailedCapture keeps the logs of the failed capture in dir (failed
// with err) under root, the tool's whole output with them, and returns err
// told where that output is kept: err carries only its last lines. It
// says so only when that output itself was kept, not only other logs.
func keepFailedCapture(root, dir string, err error, log *logrus.Entry) error {
	name := writeToolOutput(dir, err)
	kept := keepFailureLogs(root, dir, log)
	if name == "" || kept == "" {
		return err
	}
	if fi, serr := os.Lstat(filepath.Join(kept, name)); serr != nil || !fi.Mode().IsRegular() {
		return err
	}
	return fmt.Errorf("%w (the tool's whole output is kept on the node in %s)", err, filepath.Join(kept, name))
}

// keepFailureLogs copies the *.log files of the failed capture in dir into
// root's failed-logs directory and drops the oldest beyond
// failureLogsKept. It returns the directory they are kept in, "" when
// none was kept.
func keepFailureLogs(root, dir string, log *logrus.Entry) string {
	dst := filepath.Join(root, failureLogsDir, filepath.Base(dir))
	n := 0
	_ = filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(d.Name(), ".log") {
			return nil
		}
		rel, rerr := filepath.Rel(dir, p)
		if rerr != nil {
			return nil
		}
		if cerr := copyLogFile(p, filepath.Join(dst, rel)); cerr != nil {
			log.WithError(cerr).WithField("file", p).Warn("cannot keep a failed capture's log")
			return nil
		}
		n++
		return nil
	})
	if n == 0 {
		return ""
	}
	log.WithFields(logrus.Fields{"logs": n, "dir": dst}).Info("kept the failed capture's logs")
	pruneFailureLogs(filepath.Join(root, failureLogsDir), failureLogsKept)
	return dst
}

// copyLogFile copies src to dst; a copy that fails leaves no dst.
func copyLogFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	if merr := os.MkdirAll(filepath.Dir(dst), 0o755); merr != nil {
		return merr
	}
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, io.LimitReader(in, failureLogMax)); err != nil {
		_ = out.Close()
		_ = os.Remove(dst)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(dst)
		return err
	}
	return nil
}

// pruneFailureLogs keeps the keep most recently written entries of dir.
func pruneFailureLogs(dir string, keep int) {
	ents, err := os.ReadDir(dir)
	if err != nil || len(ents) <= keep {
		return
	}
	type entry struct {
		name string
		mod  int64
	}
	all := make([]entry, 0, len(ents))
	for _, e := range ents {
		if info, ierr := e.Info(); ierr == nil {
			all = append(all, entry{e.Name(), info.ModTime().UnixNano()})
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].mod > all[j].mod })
	for _, e := range all[min(keep, len(all)):] {
		_ = os.RemoveAll(filepath.Join(dir, e.name))
	}
}
