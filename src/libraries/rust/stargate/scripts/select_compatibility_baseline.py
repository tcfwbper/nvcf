#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import datetime
import json
import pathlib
import re
import sys


SUPPORT_DAYS = 60
# Releases leaving the window within this many days are reported so deployment
# inventory can be checked before coverage drops.
EXPIRY_NOTICE_DAYS = 14
RELEASE_TAG = re.compile(r"src/libraries/rust/stargate/v[0-9]+\.[0-9]+\.[0-9]+")


def select_release(releases, retained, now):
    cutoff = now - datetime.timedelta(days=SUPPORT_DAYS)
    stable = {}
    for release in releases:
        tag = release["tag_name"]
        if not RELEASE_TAG.fullmatch(tag) or release["draft"] or release["prerelease"]:
            continue
        published = datetime.datetime.fromisoformat(release["published_at"].replace("Z", "+00:00"))
        if published.tzinfo is None:
            raise ValueError(f"release publication date must include a timezone: {tag}")
        if published <= now:
            stable[tag] = (published, release)
    for tag in retained:
        if tag not in stable:
            raise ValueError(f"retained release has no published stable release metadata: {tag}")
    supported = [entry for tag, entry in stable.items() if entry[0] >= cutoff or tag in retained]
    if not supported:
        raise ValueError(f"no stable release in the {SUPPORT_DAYS}-day window or retained release list")
    supported.sort(key=lambda entry: (entry[0], entry[1]["tag_name"]))
    published, release = supported[0]
    notice = now + datetime.timedelta(days=EXPIRY_NOTICE_DAYS)
    expiring = []
    for entry_published, entry in supported:
        leaves_at = entry_published + datetime.timedelta(days=SUPPORT_DAYS)
        if entry["tag_name"] not in retained and leaves_at <= notice:
            expiring.append({
                "tag": entry["tag_name"],
                "published_at": entry_published.isoformat(),
                "leaves_window_at": leaves_at.isoformat(),
            })
    return {
        "tag": release["tag_name"],
        "supported_tags": [entry[1]["tag_name"] for entry in supported],
        "published_at": published.isoformat(),
        "support_days": SUPPORT_DAYS,
        "as_of": now.isoformat(),
        "cutoff": cutoff.isoformat(),
        "expiry_notice_days": EXPIRY_NOTICE_DAYS,
        "expiring": expiring,
    }


def main():
    releases = [json.loads(line) for line in sys.stdin if line.strip()]
    policy = pathlib.Path(__file__).resolve().parents[1] / "compatibility_retained_releases.json"
    retained = json.loads(policy.read_text())
    if not isinstance(retained, list) or any(not isinstance(tag, str) for tag in retained):
        raise ValueError("retained releases must be a JSON array of stable release tags")
    print(json.dumps(select_release(releases, retained, datetime.datetime.now(datetime.timezone.utc))))


if __name__ == "__main__":
    main()
