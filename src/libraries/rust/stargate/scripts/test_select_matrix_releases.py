#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import pathlib
import subprocess
import tempfile
import unittest

import select_matrix_releases
from select_matrix_releases import CONTRACT_PATHS, select_matrix


class SelectMatrixTest(unittest.TestCase):
    def test_keeps_first_release_per_contract_and_the_newest(self):
        digests = {"a": "1", "b": "1", "c": "2", "d": "2", "e": "1"}
        self.assertEqual(select_matrix(list("abcde"), digests.__getitem__), ["a", "c", "e"])
        self.assertEqual(select_matrix(list("abcd"), digests.__getitem__), ["a", "c", "d"])

    def test_single_release_and_unchanged_contract(self):
        self.assertEqual(select_matrix(["a"], lambda tag: "x"), ["a"])
        self.assertEqual(select_matrix(["a", "b", "c"], lambda tag: "x"), ["a", "c"])

    def test_empty_inventory_fails(self):
        with self.assertRaises(ValueError):
            select_matrix([], lambda tag: "x")


class GitDigestTest(unittest.TestCase):
    def setUp(self):
        self.repo = tempfile.TemporaryDirectory()
        self.addCleanup(self.repo.cleanup)
        self.cwd = os.getcwd()
        os.chdir(self.repo.name)
        self.addCleanup(os.chdir, self.cwd)
        env = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.com",
               "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.com"}
        self.git = lambda *args: subprocess.run(["git", *args], check=True, capture_output=True,
                                                env={**os.environ, **env})
        self.git("init", "-q")

    def commit_and_tag(self, tag, contents):
        for path in CONTRACT_PATHS:
            file = pathlib.Path(path)
            if path in contents:
                file.parent.mkdir(parents=True, exist_ok=True)
                file.write_text(contents[path])
            elif file.exists():
                file.unlink()
        self.git("add", "-A")
        self.git("commit", "-q", "--allow-empty", "-m", tag)
        self.git("tag", tag)

    def test_digest_tracks_every_contract_file_and_absence(self):
        proto, capnp, tunnel = CONTRACT_PATHS
        self.commit_and_tag("v1", {proto: "a", capnp: "b"})
        self.commit_and_tag("v2", {proto: "a", capnp: "b"})
        self.commit_and_tag("v3", {proto: "a", capnp: "b", tunnel: "c"})
        self.commit_and_tag("v4", {proto: "a2", capnp: "b", tunnel: "c"})
        digest = select_matrix_releases.git_contract_digest
        self.assertEqual(digest("v1"), digest("v2"))
        self.assertNotEqual(digest("v2"), digest("v3"))
        self.assertNotEqual(digest("v3"), digest("v4"))
        self.assertEqual(select_matrix(["v1", "v2", "v3", "v4"], digest), ["v1", "v3", "v4"])

    def test_missing_tag_fails(self):
        self.commit_and_tag("v1", {})
        with self.assertRaises(ValueError):
            select_matrix_releases.git_contract_digest("v9")


if __name__ == "__main__":
    unittest.main()
