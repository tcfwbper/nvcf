#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""nvsnap-gpu-suspend quiesces ranks caught mid-collective, and gives up a
quiesce that never succeeds on time.

No GPU: the tool is built against a stub libcuda.so.1, and a fake workload
answers its control socket the way libnvsnap_gpushare does under steady
traffic ("quiesce: err busy (GPU work did not drain)", after waiting up to
2 s for the work to drain). The test checks that

  * the --timeout-ms deadline is wall-clock time, not a count of attempts:
    an attempt takes far longer than the 250 ms pause between attempts;
  * with --fabric-map, an "abort" verdict from the vote driver ends the
    suspend at once, rather than counting as a retry;
  * with a library that has "hold", the tool lets a drained rank (behind)
    launch one held call at a time until a busy rank (ahead, its collective
    waiting for the one behind) drains, then quiesces both, and resumes
    them when the suspend fails later;
  * a held rank that stops answering "poll" ends the catch-up within its
    10 s, not after the 120 s a single request may take;
  * a verbs exchange that fails leaves no directory behind.

Run: python3 -I tests/gpushare/host/test_quiesce_deadline.py
(CUDA_INC names the directory with cuda.h; default /usr/local/cuda/include.)
"""

import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.normpath(os.path.join(HERE, "../../../docker/agent/gpushare"))
CUDA_INC = os.environ.get("CUDA_INC", "/usr/local/cuda/include")

STUB = r"""
typedef int CUresult;
CUresult cuInit(unsigned f) { (void)f; return 0; }
CUresult cuGetErrorName(CUresult r, const char **s) { (void)r; *s = "stub"; return 0; }
#define STUB_FN(n) CUresult n(void) { return 0; }
STUB_FN(cuCheckpointProcessLock)
STUB_FN(cuCheckpointProcessCheckpoint)
STUB_FN(cuCheckpointProcessRestore)
STUB_FN(cuCheckpointProcessUnlock)
/* The suspend must fail after the quiesce (the fake ranks cannot be
 * checkpointed): fail here, explicitly. */
CUresult cuCheckpointProcessGetState(int pid, int *state) { (void)pid; *state = 0; return 999; }
STUB_FN(cuCheckpointProcessGetRestoreThreadId)
"""

# How long the fake workload takes to answer "quiesce busy": the library
# waits up to 2 s for GPU work to drain before it says so.
BUSY_REPLY_S = 0.6


def build(tmp):
    with open(os.path.join(tmp, "stub.c"), "w") as f:
        f.write(STUB)
    subprocess.run(["gcc", "-shared", "-fPIC", "-o", os.path.join(tmp, "libcuda.so.1"),
                    os.path.join(tmp, "stub.c")], check=True)
    tool = os.path.join(tmp, "nvsnap-gpu-suspend")
    subprocess.run(["gcc", "-O2", "-Wall", "-Wextra", "-Werror", "-I", SRC, "-I", CUDA_INC, "-o", tool,
                    os.path.join(SRC, "nvsnap-gpu-suspend.c"), "-ldl", "-lpthread"], check=True)
    return tool


WORKLOAD = r"""
import os, socket, sys, time
log = sys.argv[2] if len(sys.argv) > 2 else None
s = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
s.bind("\0nvsnap-gpushare.%d" % os.getpid())
s.listen(16)
print("ready", flush=True)
while True:
    c, _ = s.accept()
    cmd = c.recv(4096).rstrip(b"\0").decode()
    if cmd == "quiesce":
        time.sleep(float(sys.argv[1]))
        rep = "err busy (GPU work did not drain)"
        if log:
            with open(log, "a") as f:
                f.write("busy\n")
    elif cmd.startswith("resume"):
        rep = "ok resumed"
    else:
        rep = "err unknown command"
    c.send(rep.encode() + b"\0")
    c.close()
"""


# Two ranks of one engine, caught by "hold" mid-collective. The rank ahead
# ("ahead") is busy until the rank behind ("behind") has let STEPS held
# launches through; the one behind is drained with launches held.
RANK = r"""
import os, socket, sys
role, state, steps = sys.argv[1], sys.argv[2], int(sys.argv[3])
log = open(state + "." + role + ".log", "a")
def stepped():
    try:
        with open(state) as f:
            return int(f.read() or 0)
    except OSError:
        return 0
s = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
s.bind("\0nvsnap-gpushare.%d" % os.getpid())
s.listen(16)
print("ready", flush=True)
unanswered = []
while True:
    c, _ = s.accept()
    cmd = c.recv(4096).rstrip(b"\0").decode()
    log.write(cmd + "\n"); log.flush()
    idle = role in ("behind", "ibvbroken") or stepped() >= steps
    if role == "silent" and cmd in ("poll", "step"):
        unanswered.append(c)  # held, and stopped answering
        continue
    if role == "broken":
        rep = "err broken"
    elif role == "ibvbroken" and cmd == "ibv":
        rep = "ok ibv 1"
    elif role == "ibvbroken" and cmd.startswith("fabmap "):
        rep = "err broken"
    elif cmd == "hold":
        rep = "ok held"
    elif cmd == "poll":
        rep = ("ok idle 3" if role == "behind" else "ok idle 0") if idle else "ok busy"
    elif cmd == "step":
        if role == "behind":
            n = stepped() + 1
            with open(state + ".tmp", "w") as f:
                f.write(str(n))
            os.rename(state + ".tmp", state)
            rep = "ok stepped"
        else:
            rep = "ok idle 0" if idle else "ok busy"
    elif cmd == "quiesce":
        rep = "ok quiesced" if idle else "err busy (GPU work did not drain)"
    elif cmd.startswith("resume"):
        rep = "ok resumed"
    elif cmd.split(" ")[0] in ("release", "load", "remap"):
        rep = "ok"
    else:
        rep = "err unknown command"
    c.send(rep.encode() + b"\0")
    c.close()
"""


def rank(role, state, steps):
    p = subprocess.Popen([sys.executable, "-I", "-c", RANK, role, state, str(steps)], stdout=subprocess.PIPE, text=True)
    if p.stdout.readline().strip() != "ready":
        p.kill()
        raise RuntimeError("fake rank did not start")
    return p


def busy_workload(log=None):
    """A process whose control socket always answers quiesce busy (each
    reply logged to LOG)."""
    p = subprocess.Popen([sys.executable, "-I", "-c", WORKLOAD, str(BUSY_REPLY_S), *([log] if log else [])],
                         stdout=subprocess.PIPE, text=True)
    if p.stdout.readline().strip() != "ready":
        p.kill()
        raise RuntimeError("fake workload did not start")
    return p


class QuiesceDeadline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not shutil.which("gcc") or not os.path.exists(os.path.join(CUDA_INC, "cuda.h")):
            raise unittest.SkipTest("needs gcc and cuda.h (CUDA_INC)")
        cls.tmp = tempfile.mkdtemp()
        cls.tool = build(cls.tmp)
        cls.env = dict(os.environ, LD_LIBRARY_PATH=cls.tmp)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def setUp(self):
        self.busy_log = os.path.join(tempfile.mkdtemp(dir=self.tmp), "busy")
        w = busy_workload(self.busy_log)
        self.addCleanup(w.stdout.close)
        self.addCleanup(w.wait)
        self.addCleanup(w.kill)
        self.pid = w.pid

    def run_tool(self, *args, timeout=60):
        t0 = time.monotonic()
        p = subprocess.run([self.tool, *args], env=self.env, capture_output=True, text=True, timeout=timeout)
        return p, time.monotonic() - t0

    def test_timeout_is_wall_clock(self):
        # 1 s at one attempt per (0.6 s reply + 0.25 s pause): counting
        # attempts (4 of 250 ms) ran about 3.4 s; the deadline stops at ~1.7 s.
        p, took = self.run_tool("--timeout-ms", "1000", "suspend", str(self.pid))
        self.assertLess(took, 2.5, "a 1 s quiesce timeout ran %.1f s:\n%s" % (took, p.stdout))
        self.assertNotEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("err busy (GPU work did not drain)", p.stdout)
        self.assertIn("not quiesced after", p.stdout)

    def test_fabric_abort_ends_the_suspend(self):
        fabric = tempfile.mkdtemp(dir=self.tmp)
        stop = threading.Event()
        verdicts = []

        def driver():
            # Answer the first vote "retry" and the second "abort", as the
            # agent does once its quiesce deadline has passed.
            seen = -1
            while not stop.is_set():
                try:
                    with open(os.path.join(fabric, "q")) as f:
                        attempt = int(f.read().split()[0])
                except (OSError, ValueError, IndexError):
                    attempt = -1
                if attempt > seen:
                    seen = attempt
                    v = "retry" if attempt == 0 else "abort"
                    verdicts.append(v)
                    with open(os.path.join(fabric, "d.tmp"), "w") as f:
                        f.write("%d %s\n" % (attempt, v))
                    os.rename(os.path.join(fabric, "d.tmp"), os.path.join(fabric, "d"))
                time.sleep(0.05)

        t = threading.Thread(target=driver, daemon=True)
        t.start()
        self.addCleanup(stop.set)
        # A long timeout: only the abort can end the suspend this soon.
        p, took = self.run_tool("--timeout-ms", "120000", "--fabric-map", fabric, "suspend", str(self.pid))
        self.assertNotEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertEqual(verdicts[:2], ["retry", "abort"])
        self.assertIn("the vote was aborted at attempt 1: rolling back", p.stdout)
        self.assertLess(took, 10, "the suspend went on %.1f s after the abort:\n%s" % (took, p.stdout))


class CatchUp(QuiesceDeadline):
    def setUp(self):
        self.state = os.path.join(tempfile.mkdtemp(dir=self.tmp), "stepped")
        self.ranks = []
        for role in ("ahead", "behind"):
            w = rank(role, self.state, 5)
            self.addCleanup(w.stdout.close)
            self.addCleanup(w.wait)
            self.addCleanup(w.kill)
            self.ranks.append(w.pid)

    def commands(self, role):
        with open(self.state + "." + role + ".log") as f:
            return f.read().split("\n")

    def test_drained_rank_catches_up_then_both_quiesce(self):
        p, took = self.run_tool("--timeout-ms", "10000", "suspend", *map(str, self.ranks))
        self.assertIn("catch-up: 5 launch(es) let through", p.stdout, p.stdout + p.stderr)
        self.assertIn("quiesced", p.stdout)
        self.assertLess(took, 8, p.stdout)
        for role in ("ahead", "behind"):
            cmds = self.commands(role)
            self.assertIn("hold", cmds)
            self.assertIn("quiesce", cmds)
            # The stub driver cannot checkpoint the fake ranks, so the suspend
            # fails after the quiesce: every rank must be resumed.
            self.assertTrue(any(c.startswith("resume") for c in cmds), "%s not resumed: %s" % (role, cmds))
        self.assertNotIn("step", self.commands("ahead"), "a busy rank was stepped past its peers")

    # The deadline and abort tests do not apply to two ranks that catch up.
    test_timeout_is_wall_clock = None
    test_fabric_abort_ends_the_suspend = None


class PartialHold(CatchUp):
    """The first rank's control thread fails; the second accepts "hold".
    The rollback must still resume the second."""

    def setUp(self):
        self.state = os.path.join(tempfile.mkdtemp(dir=self.tmp), "stepped")
        self.ranks = []
        for role in ("broken", "behind"):
            w = rank(role, self.state, 0)
            self.addCleanup(w.stdout.close)
            self.addCleanup(w.wait)
            self.addCleanup(w.kill)
            self.ranks.append(w.pid)

    def test_every_rank_resumed(self):
        p, _ = self.run_tool("--timeout-ms", "10000", "suspend", *map(str, self.ranks))
        self.assertNotEqual(p.returncode, 0, p.stdout + p.stderr)
        cmds = self.commands("behind")
        self.assertIn("hold", cmds)
        self.assertTrue(any(c.startswith("resume") for c in cmds), "held rank not resumed: %s" % cmds)

    test_drained_rank_catches_up_then_both_quiesce = None


class SilentRank(CatchUp):
    """A held rank stops answering "poll" (and "step"); the other is ahead
    and busy. The catch-up ends within its own 10 s, and the rollback
    resumes both."""

    def setUp(self):
        self.state = os.path.join(tempfile.mkdtemp(dir=self.tmp), "stepped")
        self.ranks = []
        for role in ("ahead", "silent"):
            w = rank(role, self.state, 5)
            self.addCleanup(w.stdout.close)
            self.addCleanup(w.wait)
            self.addCleanup(w.kill)
            self.ranks.append(w.pid)

    def test_catch_up_keeps_its_budget(self):
        try:
            p, took = self.run_tool("--timeout-ms", "5000", "suspend", *map(str, self.ranks), timeout=90)
        except subprocess.TimeoutExpired as e:
            self.fail("the catch-up waited on a silent rank past 90 s: %s" % e.stdout)
        self.assertNotEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("catch-up:", p.stdout, p.stdout + p.stderr)
        self.assertLess(took, 40, p.stdout + p.stderr)
        for role in ("ahead", "silent"):
            self.assertTrue(any(c.startswith("resume") for c in self.commands(role)), role)

    test_drained_rank_catches_up_then_both_quiesce = None


class FailedVerbsExchange(CatchUp):
    """A rank with NCCL's verbs whose "fabmap" fails, during the rollback of
    a suspend that failed after release: the exchange directory goes."""

    def setUp(self):
        self.state = os.path.join(tempfile.mkdtemp(dir=self.tmp), "stepped")
        w = rank("ibvbroken", self.state, 0)
        self.addCleanup(w.stdout.close)
        self.addCleanup(w.wait)
        self.addCleanup(w.kill)
        self.ranks = [w.pid]

    def test_no_directory_left(self):
        import glob
        before = set(glob.glob("/tmp/nvsnap-ibv-*"))
        p, _ = self.run_tool("--timeout-ms", "10000", "suspend", *map(str, self.ranks))
        self.assertNotEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertTrue(any(c.startswith("fabmap ") for c in self.commands("ibvbroken")), p.stdout + p.stderr)
        self.assertEqual(set(glob.glob("/tmp/nvsnap-ibv-*")) - before, set(), p.stdout + p.stderr)

    test_drained_rank_catches_up_then_both_quiesce = None


class Deadlines(QuiesceDeadline):
    def test_vote_wait_ends_at_the_deadline(self):
        # No driver answers: the node gives up at its own deadline
        # (--timeout-ms plus a minute with --fabric-map, plus the 20 s it
        # waits for a verdict after taking its vote back), not after 600 s.
        fabric = tempfile.mkdtemp(dir=self.tmp)
        p, took = self.run_tool("--timeout-ms", "1000", "--fabric-map", fabric, "suspend", str(self.pid),
                                timeout=120)
        self.assertNotEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("no verdict", p.stdout + p.stderr)
        self.assertLess(took, 100, p.stdout + p.stderr)

    def test_timeout_zero_keeps_trying(self):
        # --timeout-ms 0 is "wait forever": still retrying after several
        # busy attempts.
        p = subprocess.Popen([self.tool, "--timeout-ms", "0", "suspend", str(self.pid)], env=self.env,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        time.sleep(4)
        alive = p.poll() is None
        p.kill()
        out = p.communicate()[0]
        self.assertTrue(alive, "gave up with --timeout-ms 0:\n" + out)
        # Alive because it tries again, not because its first try hangs.
        with open(self.busy_log) as f:
            self.assertGreaterEqual(len(f.read().split()), 2, "fewer than two busy attempts in 4 s:\n" + out)

    def test_fabric_refuses_timeout_zero(self):
        # Across nodes every node needs its own deadline: with none it holds
        # its ranks forever if the driver never answers. Refused before it
        # talks to any rank or writes a vote.
        fabric = tempfile.mkdtemp(dir=self.tmp)
        p, took = self.run_tool("--timeout-ms", "0", "--fabric-map", fabric, "suspend", str(self.pid),
                                timeout=30)
        self.assertEqual(p.returncode, 1, p.stdout + p.stderr)
        self.assertIn("--fabric-map needs --timeout-ms above 0", p.stderr)
        self.assertEqual(os.listdir(fabric), [])
        self.assertLess(took, 2, p.stdout + p.stderr)

    test_timeout_is_wall_clock = None
    test_fabric_abort_ends_the_suspend = None


class WithdrawVote(QuiesceDeadline):
    """A node that voted "ok" and reaches its own deadline takes the vote
    back before rolling back, and follows a "go" the driver sent before it
    saw the vote taken back."""

    def setUp(self):
        w = rank("behind", os.path.join(tempfile.mkdtemp(dir=self.tmp), "stepped"), 0)
        self.addCleanup(w.stdout.close)
        self.addCleanup(w.wait)
        self.addCleanup(w.kill)
        self.pid = w.pid

    def test_ok_vote_is_taken_back_at_the_deadline(self):
        fabric = tempfile.mkdtemp(dir=self.tmp)
        stop = threading.Event()
        votes = []

        def driver():
            # Late: it answers only once the node has taken its vote back,
            # as a driver that decided "go" on the "ok" just before.
            while not stop.is_set():
                try:
                    with open(os.path.join(fabric, "q")) as f:
                        v = f.read().split()
                except OSError:
                    v = []
                if v and (not votes or votes[-1] != v):
                    votes.append(v)
                if v and v[1] == "busy" and ["0", "ok"] in votes:
                    with open(os.path.join(fabric, "d.tmp"), "w") as f:
                        f.write("%s go\n" % v[0])
                    os.rename(os.path.join(fabric, "d.tmp"), os.path.join(fabric, "d"))
                    return
                time.sleep(0.05)

        t = threading.Thread(target=driver, daemon=True)
        t.start()
        self.addCleanup(stop.set)
        p, _ = self.run_tool("--timeout-ms", "1000", "--fabric-map", fabric, "suspend", str(self.pid), timeout=120)
        self.assertEqual(votes[:2], [["0", "ok"], ["0", "busy"]], p.stdout + p.stderr)
        self.assertIn('"go" for attempt 0 came after the deadline', p.stdout, p.stdout + p.stderr)
        self.assertNotIn("no verdict", p.stdout + p.stderr)

    test_timeout_is_wall_clock = None
    test_fabric_abort_ends_the_suspend = None


if __name__ == "__main__":
    unittest.main(verbosity=2)
