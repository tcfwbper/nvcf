// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package diskguard

import (
	"io/fs"
	"syscall"
)

// diskUsage is the space info's file takes on disk, in 512-byte blocks as
// the kubelet counts an emptyDir, with its identity when another name may
// link to it.
func diskUsage(info fs.FileInfo) (bytes int64, id fileID, linked bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return info.Size(), fileID{}, false
	}
	return int64(st.Blocks) * 512, fileID{dev: uint64(st.Dev), ino: uint64(st.Ino)}, st.Nlink > 1 && !info.IsDir()
}
