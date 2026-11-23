#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Select the supported releases whose binaries the release matrix runs.

Reads the JSON written by select_compatibility_baseline.py on stdin and prints
one tag per line. The selection keeps the first supported release (by
publication order) for each distinct Stargate/Pylon wire contract, plus the
newest supported release. The oldest supported release is always first.
Contract files must be readable at every tag; fetch the tags first.
"""

import hashlib
import json
import subprocess
import sys


CONTRACT_PATHS = (
    "src/libraries/rust/stargate/crates/proto/proto/stargate.proto",
    "src/libraries/rust/stargate/crates/protocol/quic.capnp",
    "src/libraries/rust/stargate/crates/protocol/src/tunnel_contract.rs",
)


def select_matrix(supported_tags, contract_digest):
    if not supported_tags:
        raise ValueError("no supported releases to select from")
    selected = []
    seen = set()
    for tag in supported_tags:
        digest = contract_digest(tag)
        if digest not in seen:
            seen.add(digest)
            selected.append(tag)
    if supported_tags[-1] not in selected:
        selected.append(supported_tags[-1])
    return selected


def git_contract_digest(tag):
    digest = hashlib.sha256()
    for path in CONTRACT_PATHS:
        result = subprocess.run(
            ["git", "rev-parse", "--verify", "--quiet", f"refs/tags/{tag}:{path}"],
            capture_output=True, text=True, check=False,
        )
        # A contract file that did not exist yet is part of that release's contract.
        digest.update(f"{path}={result.stdout.strip() or 'absent'}\n".encode())
    if not subprocess.run(["git", "rev-parse", "--verify", "--quiet", f"refs/tags/{tag}^{{commit}}"],
                          capture_output=True, check=False).returncode == 0:
        raise ValueError(f"release tag is not fetched: {tag}")
    return digest.hexdigest()


def main():
    baseline = json.load(sys.stdin)
    for tag in select_matrix(baseline["supported_tags"], git_contract_digest):
        print(tag)


if __name__ == "__main__":
    main()
