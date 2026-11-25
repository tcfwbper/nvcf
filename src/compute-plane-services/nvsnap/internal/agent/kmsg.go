// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// GPU faults in the node's kernel log over a capture. A leave-running
// group capture can report success while the workload it resumed never
// finishes another step: its GPUs spin, and its engine times out minutes
// later (2026-11-22, a 2-node NVLink group). Each such cycle came with a
// burst of Xid 137 (an NVLink access the GPU refused, "PRIV Error") on
// every GPU of one node, logged by the driver during the GPU tool's
// resume; the cycles that passed had none. The agent, privileged, reads
// the kernel log from /dev/kmsg: it opens it before the suspend,
// positioned at its end, and once the resume is done reads what was
// logged since. An Xid 137 on the workload's GPUs fails the capture.
// Without /dev/kmsg (an agent that is not privileged) the check is off,
// and the agent's log says so.

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

// kmsgPath is the kernel log device.
const kmsgPath = "/dev/kmsg"

// kernelGPUFaultsLog is the file a failed capture keeps the kernel's
// fault lines in, with its other logs (failure_logs.go).
const kernelGPUFaultsLog = "kernel-gpu-faults.log"

// A capture's read of the kernel log is bounded in time and in bytes: a
// node that logs a storm must neither hold up the capture nor fill the
// agent's memory.
const (
	kmsgDrainTime  = 2 * time.Second
	kmsgDrainBytes = 1 << 20
	// kmsgRecordMax is the largest record the kernel returns in one read.
	kmsgRecordMax = 8192
)

// errXidFault marks a capture during which the kernel reported a fault on
// the workload's GPUs (metrics.GPUFaultText, in lower case).
var errXidFault = errors.New("the kernel reported a GPU fault during the capture")

// kmsgCursor reads the kernel log from where it was opened.
type kmsgCursor struct {
	fd    int
	carry []byte // a line read in part (a regular file, in tests)
}

// openKmsgAt opens the kernel log at path, positioned after its last
// record: a drain returns only what is logged from now on.
func openKmsgAt(path string) (*kmsgCursor, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	if _, err := syscall.Seek(fd, 0, io.SeekEnd); err != nil {
		_ = syscall.Close(fd)
		return nil, &os.PathError{Op: "seek", Path: path, Err: err}
	}
	return &kmsgCursor{fd: fd}, nil
}

// drain returns the messages logged since the last drain (or the open),
// without their headers and dictionary lines. It reads at most maxBytes,
// for at most budget; truncated reports that it stopped at either, or
// that the kernel overwrote records before they were read.
func (c *kmsgCursor) drain(budget time.Duration, maxBytes int) (lines []string, truncated bool) {
	buf := make([]byte, kmsgRecordMax)
	deadline := time.Now().Add(budget)
	read := 0
	for {
		if read >= maxBytes || time.Now().After(deadline) {
			return lines, true
		}
		n, err := syscall.Read(c.fd, buf[:min(len(buf), maxBytes-read)])
		switch {
		case errors.Is(err, syscall.EPIPE):
			// Records were overwritten before this read; the next read
			// continues from the oldest left.
			truncated = true
			continue
		case errors.Is(err, syscall.EINTR):
			continue
		case err != nil || n <= 0:
			// EAGAIN: nothing more logged; 0: the end of a file.
			return lines, truncated
		}
		read += n
		c.carry = append(c.carry, buf[:n]...)
		for {
			i := strings.IndexByte(string(c.carry), '\n')
			if i < 0 {
				break
			}
			if msg, ok := kmsgMessage(string(c.carry[:i])); ok {
				lines = append(lines, msg)
			}
			c.carry = c.carry[i+1:]
		}
	}
}

// kmsgMessage is the message of one line of the kernel log: a record
// "prio,seq,usec,flags;message", or a dictionary line (led by a space),
// which has none.
func kmsgMessage(line string) (string, bool) {
	if line == "" || line[0] == ' ' {
		return "", false
	}
	if _, msg, ok := strings.Cut(line, ";"); ok {
		return msg, true
	}
	return line, true
}

// Close closes the cursor.
func (c *kmsgCursor) Close() { _ = syscall.Close(c.fd) }

// xidLine is the driver's report of an Xid: the GPU's PCI address, the
// Xid, and the name of the process the driver names.
var xidLine = regexp.MustCompile(`NVRM: Xid \(PCI:([0-9A-Fa-f:.]+)\): (\d+), pid=[^,]*, name=([^,]*),`)

// xidCaptureFaults are the Xids that fail a capture they happen during:
// 137, an NVLink access the GPU refused (a peer wrote to memory it was
// not, or no longer, allowed to).
var xidCaptureFaults = map[string]bool{"137": true}

// gpuToolComm is the GPU tool's name as the kernel records it (15 bytes).
var gpuToolComm = gpushareToolName[:min(len(gpushareToolName), 15)]

// xidFaults returns the lines of lines that report a capture fault on one
// of the GPUs at the PCI addresses pcis; without pcis (the workload's GPUs
// could not be told), the ones that name the GPU tool.
func xidFaults(lines []string, pcis map[string]bool) []string {
	var out []string
	for _, l := range lines {
		m := xidLine.FindStringSubmatch(l)
		if len(m) < 4 || !xidCaptureFaults[m[2]] {
			continue
		}
		if len(pcis) > 0 {
			if !pcis[strings.ToLower(m[1])] {
				continue
			}
		} else if strings.TrimSpace(m[3]) != gpuToolComm {
			continue
		}
		out = append(out, l)
	}
	return out
}

// xidFaultError is the failure of a capture with faults (at least one):
// how many lines, and the first.
func xidFaultError(faults []string, truncated bool) error {
	more := ""
	if truncated {
		more = "; the kernel log was read only in part, there may be more"
	}
	return fmt.Errorf("%w: %d line(s) of Xid 137 (an NVLink access the GPU refused) in the node's kernel log, the first: %s%s",
		errXidFault, len(faults), faults[0], more)
}

// gpuPCIAddrs returns the PCI addresses, as the kernel log writes them
// (domain:bus:device, without the function), of the GPUs gpus lists (one
// UUID per line, as `nvsnap-gpu-suspend gpus` prints them), from the
// driver's directory of GPUs (one per address). nil when gpus lists none.
func gpuPCIAddrs(driverDir, gpus string) map[string]bool {
	want := map[string]bool{}
	for _, u := range strings.Fields(gpus) {
		want[u] = true
	}
	if len(want) == 0 {
		return nil
	}
	out := map[string]bool{}
	entries, _ := os.ReadDir(driverDir)
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(driverDir, e.Name(), "information"))
		if err != nil {
			continue
		}
		for _, l := range strings.Split(string(b), "\n") {
			k, v, ok := strings.Cut(l, ":")
			if !ok || strings.TrimSpace(k) != "GPU UUID" || !want[strings.TrimSpace(v)] {
				continue
			}
			addr := strings.ToLower(e.Name())
			if i := strings.LastIndexByte(addr, '.'); i > 0 {
				addr = addr[:i]
			}
			out[addr] = true
		}
	}
	return out
}

// gpuFaultWatch watches the node's kernel log over one capture. Without
// a kernel log to read, or nil, it watches nothing.
type gpuFaultWatch struct {
	c   *kmsgCursor
	log *logrus.Entry
}

// startGPUFaultWatch starts watching the node's kernel log, before the
// capture suspends the workload.
func startGPUFaultWatch(log *logrus.Entry) *gpuFaultWatch {
	return startGPUFaultWatchAt(kmsgPath, log)
}

func startGPUFaultWatchAt(path string, log *logrus.Entry) *gpuFaultWatch {
	c, err := openKmsgAt(path)
	if err != nil {
		log.WithError(err).Warn("cannot read the node's kernel log: a GPU fault during this capture goes unseen")
		return &gpuFaultWatch{log: log}
	}
	return &gpuFaultWatch{c: c, log: log}
}

// check reads what the kernel logged since the watch started (or since
// the last check) and fails on a capture fault on the GPUs gpus lists
// (UUIDs; driverDir maps them to addresses). It keeps the kernel's fault
// lines in logDir, with the failed capture's other logs.
func (w *gpuFaultWatch) check(driverDir, gpus, logDir string) error {
	if w == nil || w.c == nil {
		return nil
	}
	lines, truncated := w.c.drain(kmsgDrainTime, kmsgDrainBytes)
	faults := xidFaults(lines, gpuPCIAddrs(driverDir, gpus))
	if len(faults) == 0 {
		if truncated {
			w.log.Warn("the node's kernel log was read only in part over the capture; a GPU fault may have gone unseen")
		}
		return nil
	}
	if logDir != "" {
		if err := os.WriteFile(filepath.Join(logDir, kernelGPUFaultsLog), []byte(strings.Join(faults, "\n")+"\n"), 0o644); err != nil { //nolint:gosec // the capture's own directory
			w.log.WithError(err).Warn("cannot keep the kernel's GPU fault lines")
		}
	}
	return xidFaultError(faults, truncated)
}

func (w *gpuFaultWatch) close() {
	if w != nil && w.c != nil {
		w.c.Close()
	}
}
