// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// The mounts a dump's processes use, read from the dump itself, for a
// group capture's decision on which setup its placeholders may skip
// (webhook/unused_volumes.go). The dump records every file its processes
// hold: open descriptors, memory mappings, cwd and root, the executable,
// deleted files (ghosts), and path-bound unix sockets, each with the id of
// the mount it is on, and the mount table those ids resolve in.
//
// Anything the reading cannot account for (a descriptor kind that names a
// file another way, a mount id the table does not have, more than one
// mount namespace) makes the use unknown, and the group skips nothing.

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// pathlessFileTypes are the descriptor kinds of a dump's files.img that
// name no file of a mount: sockets on the network, pipes, anonymous
// descriptors. A FIFO's path is a regular-file entry of its own.
var pathlessFileTypes = map[string]bool{
	"INETSK": true, "PACKETSK": true, "NETLINKSK": true,
	"PIPE": true, "FIFO": true, "EVENTFD": true, "EVENTPOLL": true,
	"SIGNALFD": true, "TIMERFD": true, "MEMFD": true, "IO_URING": true,
	"TUNF": true, "NS": true, "BPFMAP": true, "PIDFD": true,
}

// critTimeout bounds one crit decode; a member's capture waits on it.
const critTimeout = 2 * time.Minute

// critShow decodes a CRIU image to JSON; a variable for tests.
var critShow = func(img string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), critTimeout)
	defer cancel()
	// The image is a file of the agent's own dump directory.
	return exec.CommandContext(ctx, "crit", "show", img).Output() //nolint:gosec // fixed binary; img is a path the agent wrote

}

// dumpUsedMounts returns the mountpoints, in the dumped container's view,
// of the mounts the processes of the dump in dir use.
func dumpUsedMounts(dir string) ([]string, error) {
	mps, err := filepath.Glob(filepath.Join(dir, "mountpoints-*.img"))
	if err != nil {
		return nil, err
	}
	if len(mps) != 1 {
		return nil, fmt.Errorf("%d mount namespaces in the dump; one is understood", len(mps))
	}
	mounts, err := critShow(mps[0])
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", filepath.Base(mps[0]), err)
	}
	files, err := critShow(filepath.Join(dir, "files.img"))
	if err != nil {
		return nil, fmt.Errorf("decode files.img: %w", err)
	}
	return usedMounts(files, mounts)
}

// usedMounts is dumpUsedMounts on the decoded files.img and mount table.
func usedMounts(filesJSON, mountsJSON []byte) ([]string, error) {
	var mt struct {
		Entries []struct {
			MntID      int64  `json:"mnt_id"`
			Mountpoint string `json:"mountpoint"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(mountsJSON, &mt); err != nil {
		return nil, fmt.Errorf("mount table: %w", err)
	}
	byID := map[int64]string{}
	for _, e := range mt.Entries {
		byID[e.MntID] = path.Clean(e.Mountpoint)
	}
	var fs struct {
		Entries []struct {
			Type string `json:"type"`
			Reg  *struct {
				Name  string `json:"name"`
				MntID *int64 `json:"mnt_id"`
			} `json:"reg"`
			Usk *struct {
				Name  string `json:"name"`
				MntID *int64 `json:"mnt_id"`
			} `json:"usk"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(filesJSON, &fs); err != nil {
		return nil, fmt.Errorf("files: %w", err)
	}
	used := map[string]bool{}
	for _, e := range fs.Entries {
		switch {
		case e.Type == "REG" && e.Reg != nil:
			if e.Reg.MntID == nil || *e.Reg.MntID < 0 {
				return nil, fmt.Errorf("file %q has no mount id", e.Reg.Name)
			}
			mp, ok := byID[*e.Reg.MntID]
			if !ok {
				return nil, fmt.Errorf("file %q is on mount %d, not in the dump's mount table", e.Reg.Name, *e.Reg.MntID)
			}
			used[mp] = true
		case e.Type == "UNIXSK" && e.Usk != nil:
			// Abstract (a leading NUL, which crit writes "=00") or unbound:
			// on no mount. A bound one, by an absolute or a relative name,
			// carries the id of the mount it is on.
			if e.Usk.Name == "" || strings.HasPrefix(e.Usk.Name, "=00") {
				continue
			}
			if e.Usk.MntID == nil || *e.Usk.MntID < 0 {
				return nil, fmt.Errorf("unix socket %q has no mount id", e.Usk.Name)
			}
			mp, ok := byID[*e.Usk.MntID]
			if !ok {
				return nil, fmt.Errorf("unix socket %q is on mount %d, not in the dump's mount table", e.Usk.Name, *e.Usk.MntID)
			}
			used[mp] = true
		case pathlessFileTypes[e.Type]:
		default:
			return nil, fmt.Errorf("descriptor kind %s is not understood", e.Type)
		}
	}
	out := make([]string, 0, len(used))
	for mp := range used {
		out = append(out, mp)
	}
	sort.Strings(out)
	return out, nil
}
