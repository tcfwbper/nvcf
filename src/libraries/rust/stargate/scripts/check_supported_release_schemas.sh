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

# Selects the supported Stargate releases from the GitHub release inventory,
# fetches their tags, and checks the candidate Stargate schemas against each.
# Writes the selection JSON to <baseline-json>. Requires gh, jq, git, and buf.

set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: scripts/check_supported_release_schemas.sh <owner/repo> <baseline-json>" >&2
  exit 2
fi
release_repo="$1"
baseline="$2"
scripts="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
inventory="$(mktemp)"
trap 'rm -f "$inventory"' EXIT

gh api --paginate "repos/$release_repo/releases?per_page=100" \
  --jq '.[] | {tag_name,published_at,draft,prerelease} | @json' > "$inventory"
python3 "$scripts/select_compatibility_baseline.py" < "$inventory" > "$baseline"
cat "$baseline"

release_refs=()
while IFS= read -r tag; do
  release_refs+=("refs/tags/$tag:refs/tags/$tag")
done < <(jq -er '.supported_tags[]' "$baseline")
git fetch --no-tags --force "https://github.com/$release_repo.git" "${release_refs[@]}"
# Check every supported tag, including off-main backports.
while IFS= read -r tag; do
  bash "$scripts/check_buf_breaking.sh" "refs/tags/$tag"
done < <(jq -er '.supported_tags[]' "$baseline")
