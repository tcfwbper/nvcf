#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Builds the released `stargate` and `pylon` binaries for one release tag from
# that tag's own source, lockfile, and toolchain, in a temporary worktree.
# Set CARGO_TARGET_DIR to share dependency builds between tags.

set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: scripts/build_release_binaries.sh <release-tag> <output-directory>" >&2
  exit 2
fi
tag="$1"
out="$2"
repo="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
commit="$(git -C "$repo" rev-parse --verify --end-of-options "refs/tags/$tag^{commit}")"
worktree="$(mktemp -d "${TMPDIR:-/tmp}/stargate-release-src.XXXXXX")"
cleanup() {
  git -C "$repo" worktree remove --force "$worktree" >/dev/null 2>&1 || rm -rf "$worktree"
}
trap cleanup EXIT

git -C "$repo" worktree add --detach --force "$worktree" "$commit" >/dev/null
mkdir -p "$out"
out="$(cd "$out" && pwd)"
(
  cd "$worktree/src/libraries/rust/stargate"
  cargo build --locked --release -p stargate -p pylon --bins
  target="${CARGO_TARGET_DIR:-target}"
  cp "$target/release/stargate" "$target/release/pylon" "$out/"
)
printf '%s\n' "$commit" > "$out/COMMIT"
echo "built $tag ($commit) into $out"
