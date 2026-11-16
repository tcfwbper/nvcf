// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build !(linux || darwin)

package diskguard

import "io/fs"

// diskUsage falls back to a file's length where its blocks are not known.
func diskUsage(info fs.FileInfo) (bytes int64, id fileID, linked bool) {
	return info.Size(), fileID{}, false
}
