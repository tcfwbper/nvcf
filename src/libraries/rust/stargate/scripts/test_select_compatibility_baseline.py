#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import datetime
import unittest

from select_compatibility_baseline import EXPIRY_NOTICE_DAYS, SUPPORT_DAYS, select_release


NOW = datetime.datetime(2026, 11, 9, 12, tzinfo=datetime.timezone.utc)
PREFIX = "src/libraries/rust/stargate/v"


def release(version, age, **kwargs):
    return {
        "tag_name": PREFIX + version,
        "published_at": (NOW - datetime.timedelta(days=age)).isoformat(),
        "draft": False,
        "prerelease": False,
        **kwargs,
    }


class CompatibilityBaselineTest(unittest.TestCase):
    def test_oldest_publication_within_inclusive_60_days(self):
        self.assertEqual(SUPPORT_DAYS, 60)
        rows = [release("0.9.0", 61), release("0.8.0", 60), release("0.7.0", 3)]
        result = select_release(rows, [], NOW)
        self.assertEqual(result["tag"], PREFIX + "0.8.0")
        self.assertEqual(result["cutoff"], (NOW - datetime.timedelta(days=60)).isoformat())

    def test_reports_unretained_releases_leaving_the_window_soon(self):
        self.assertEqual(EXPIRY_NOTICE_DAYS, 14)
        rows = [release("0.1.0", 200), release("0.8.0", 59), release("0.9.0", 46),
                release("0.10.0", 45), release("0.11.0", 1)]
        result = select_release(rows, [PREFIX + "0.1.0"], NOW)
        self.assertEqual([entry["tag"] for entry in result["expiring"]],
                         [PREFIX + "0.8.0", PREFIX + "0.9.0"])
        self.assertEqual(result["expiring"][0]["leaves_window_at"],
                         (NOW + datetime.timedelta(days=1)).isoformat())

    def test_excludes_drafts_prereleases_other_components_and_future_dates(self):
        rows = [
            release("0.1.0", 60, draft=True),
            release("0.2.0", 59, prerelease=True),
            release("0.3.0-rc.1", 58),
            release("0.4.0", 57, tag_name="other/component/v0.4.0"),
            release("0.5.0", -1),
            release("0.6.0", 56),
        ]
        self.assertEqual(select_release(rows, [], NOW)["tag"], PREFIX + "0.6.0")

    def test_retained_release_does_not_expire(self):
        rows = [release("0.1.0", 200), release("0.8.0", 59)]
        self.assertEqual(select_release(rows, [PREFIX + "0.1.0"], NOW)["tag"], PREFIX + "0.1.0")
        self.assertEqual(select_release(rows, [], NOW)["tag"], PREFIX + "0.8.0")

    def test_retained_only_inventory_still_selects_a_baseline(self):
        self.assertEqual(select_release([release("0.1.0", 200)], [PREFIX + "0.1.0"], NOW)["tag"],
                         PREFIX + "0.1.0")

    def test_missing_inventory_and_unknown_retained_release_fail(self):
        for rows, retained in [([], []), ([release("0.1.0", 61)], []),
                               ([release("0.1.0", 1)], [PREFIX + "0.2.0"])]:
            with self.subTest(rows=rows, retained=retained), self.assertRaises(ValueError):
                select_release(rows, retained, NOW)

    def test_bad_publication_date_fails(self):
        for date in ["invalid", "2026-11-01T00:00:00"]:
            with self.subTest(date=date), self.assertRaises(ValueError):
                select_release([release("0.1.0", 1, published_at=date)], [], NOW)

    def test_inventory_includes_later_backports_and_retained_tags(self):
        rows = [release("0.18.3", 1), release("0.29.2", 20),
                release("0.18.2", 2), release("0.1.0", 200),
                release("0.0.1", 201)]
        result = select_release(rows, [PREFIX + "0.1.0"], NOW)
        self.assertEqual(result["supported_tags"], [PREFIX + version for version in
                         ["0.1.0", "0.29.2", "0.18.2", "0.18.3"]])
        self.assertEqual(result["tag"], result["supported_tags"][0])


if __name__ == "__main__":
    unittest.main()
