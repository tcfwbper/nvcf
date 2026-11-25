#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""An in-place resume across nodes opens the GPU launch gates only once
every node has resumed (nvsnap-gpu-suspend resume --sync-open).

No GPU: the tool is built against a stub libcuda.so.1 whose checkpoint
calls succeed, and a fake rank answers its control socket the way
libnvsnap_gpushare does, from a thread named as the library's control
thread (the tool's ptrace freeze leaves it running). A thread here plays
the agent's fabric driver on the session directory: it answers the vote
"go", hands the node's handle list back as DIR/in and, per test, answers
the node's DIR/resumed marker in DIR/o. Each test runs the sequence the
agent runs for a capture left running: suspend, stop (the holder hands the
processes over stopped and exits), resume. The test checks that

  * with --sync-open the resume holds the launches and takes the tool's
    lease on them in one request ("resume hold lease"; a library without
    it gets "resume hold", then "lease"), writes DIR/resumed, and sends
    "open" only after the driver's "<token> open", and only for its own
    token (a verdict for another token, as from an earlier round, is
    ignored);
  * a process whose library has no "resume hold" (it opens at its resume)
    makes the resume say it was not synced;
  * an "abort" verdict, or no verdict by the --sync-open deadline (also
    when the verdict's name is a FIFO no one writes), still opens the gates
    (no node is left with its launches held), and the resume says so ("GPU launches let through before every node
    resumed");
  * without --sync-open the resume is as before: plain "resume", no
    marker, no wait.

The fake rank must let the holder ptrace it: it allows any tracer
(PR_SET_PTRACER), which Yama's ptrace_scope 1 honours; under scope 2 or 3
the test is skipped.

Run: python3 -I tests/gpushare/host/test_sync_open.py
(CUDA_INC names the directory with cuda.h; default /usr/local/cuda/include.)
"""

import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.normpath(os.path.join(HERE, "../../../docker/agent/gpushare"))
CUDA_INC = os.environ.get("CUDA_INC", "/usr/local/cuda/include")
UNSYNCED = "GPU launches let through before every node resumed"

# One process's checkpoint state, kept by the holder that drives it.
STUB = r"""
typedef int CUresult;
enum { RUNNING = 0, LOCKED = 1, CHECKPOINTED = 2 };  /* CUprocessState */
static int state = RUNNING;
CUresult cuInit(unsigned f) { (void)f; return 0; }
CUresult cuGetErrorName(CUresult r, const char **s) { *s = r ? "stub error" : "CUDA_SUCCESS"; return 0; }
CUresult cuCheckpointProcessLock(int pid, void *a) { (void)pid; (void)a; state = LOCKED; return 0; }
CUresult cuCheckpointProcessCheckpoint(int pid, void *a) { (void)pid; (void)a; state = CHECKPOINTED; return 0; }
CUresult cuCheckpointProcessRestore(int pid, void *a) { (void)pid; (void)a; state = LOCKED; return 0; }
CUresult cuCheckpointProcessUnlock(int pid, void *a) { (void)pid; (void)a; state = RUNNING; return 0; }
CUresult cuCheckpointProcessGetState(int pid, int *s) { (void)pid; *s = state; return 0; }
CUresult cuCheckpointProcessGetRestoreThreadId(int pid, int *tid) { (void)pid; *tid = 0; return 0; }
"""

RANK = r"""
import ctypes, os, socket, sys, threading, time
log = open(sys.argv[1], "a")
old_lib = len(sys.argv) > 2 and sys.argv[2] == "old"  # a library without "resume hold"
# A library with "resume hold" and "lease", but not "resume hold lease".
no_hold_lease = old_lib or (len(sys.argv) > 2 and sys.argv[2] == "nolease")
# A library that cannot take the lease: it resumes without the hold.
lease_fails = len(sys.argv) > 2 and sys.argv[2] == "leasefails"
# A library whose "open" finds the state lock busy.
open_busy = len(sys.argv) > 2 and sys.argv[2] == "openbusy"
# A library whose NCCL release refused after the fence: it answers every
# command that would let it run again with an error (gpushare.c, DOOMED_REPLY).
doomed = len(sys.argv) > 2 and sys.argv[2] == "doomed"
DOOMED = "err this process refused its RDMA release after its peers were fenced; it cannot run again (replace it)"
libc = ctypes.CDLL(None, use_errno=True)
libc.prctl(0x59616d61, ctypes.c_ulong(-1 & 0xffffffffffffffff), 0, 0, 0)  # PR_SET_PTRACER, any
s = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
s.bind("\0nvsnap-gpushare.%d" % os.getpid())
s.listen(16)
def serve():
    libc.prctl(15, b"nvsnap-gpushare", 0, 0, 0)  # PR_SET_NAME: the library's control thread
    held = refused = False
    while True:
        c, _ = s.accept()
        cmd = c.recv(4096).rstrip(b"\0").decode()
        log.write("%.6f %s\n" % (time.monotonic(), cmd)); log.flush()
        verb = cmd.split(" ")[0]
        if doomed and verb == "release":
            refused = True
            rep = "err NCCL has not acknowledged 1 async event(s) of a queue pair or completion queue"
        elif refused and verb in ("resume", "open", "hold", "quiesce"):
            rep = DOOMED
        elif cmd == "hold":
            rep = "ok held"
        elif cmd == "poll":
            rep = "ok idle 0"
        elif cmd == "quiesce":
            rep = "ok quiesced"
        elif cmd == "resume hold" and not old_lib:
            held, rep = True, "ok resumed (0 multicast bind(s), 0 multicast mapping(s)); launches held until open"
        elif cmd == "resume hold lease" and lease_fails:
            held, rep = False, ("ok resumed (0 multicast bind(s), 0 multicast mapping(s)); no lease (pidfd_open: "
                                "Too many open files): launches let through")
        elif cmd == "resume hold lease" and not no_hold_lease:
            held, rep = True, "ok resumed (0 multicast bind(s), 0 multicast mapping(s)); launches held until open"
        elif cmd == "resume":
            held, rep = False, "ok resumed (0 multicast bind(s), 0 multicast mapping(s))"
        elif cmd == "lease" and not old_lib:
            rep = "ok leased"
        elif cmd == "open" and open_busy:
            rep = "err busy (state lock)"
        elif cmd == "open":
            rep = "ok opened" if held else "ok not held"
            held = False
        elif verb in ("release", "load", "fabmap", "remap"):
            rep = "ok"
        else:
            rep = "err unknown command"
        c.send(rep.encode() + b"\0")
        c.close()
threading.Thread(target=serve, daemon=True).start()
print("ready", flush=True)
while True:
    time.sleep(0.05)
"""


def build(tmp):
    with open(os.path.join(tmp, "stub.c"), "w") as f:
        f.write(STUB)
    subprocess.run(["gcc", "-shared", "-fPIC", "-o", os.path.join(tmp, "libcuda.so.1"),
                    os.path.join(tmp, "stub.c")], check=True)
    tool = os.path.join(tmp, "nvsnap-gpu-suspend")
    subprocess.run(["gcc", "-O2", "-Wall", "-Wextra", "-Werror", "-I", SRC, "-I", CUDA_INC, "-o", tool,
                    os.path.join(SRC, "nvsnap-gpu-suspend.c"), "-ldl", "-lpthread"], check=True)
    return tool


def write_atomic(path, text):
    with open(path + ".tmp", "w") as f:
        f.write(text)
    os.rename(path + ".tmp", path)


class Driver(threading.Thread):
    """The agent's side of one node's session directory. verdict: "open",
    "abort" or None (never answers the resumed marker); delay: seconds
    between seeing the marker and answering it; stale: first write a
    verdict for another token; fifo: put a FIFO with no writer where the
    verdict goes (the directory is the workload's to write)."""

    def __init__(self, fabric, verdict, delay=0.0, stale=False, fifo=False):
        super().__init__(daemon=True)
        self.fabric, self.verdict, self.delay, self.stale, self.fifo = fabric, verdict, delay, stale, fifo
        self.stop = threading.Event()
        self.token = None
        self.stale_at = self.verdict_at = None

    def read(self, name):
        try:
            with open(os.path.join(self.fabric, name)) as f:
                return f.read()
        except OSError:
            return None

    def run(self):
        voted = handed = False
        seen = None
        while not self.stop.is_set():
            q = self.read("q")
            if q and not voted:
                write_atomic(os.path.join(self.fabric, "d"), "%s go\n" % q.split()[0])
                voted = True
            out = self.read("out")
            if out is not None and not handed:
                write_atomic(os.path.join(self.fabric, "in"), out)
                handed = True
            r = self.read("resumed")
            if r and seen is None:
                seen = time.monotonic()
                self.token = r.strip()
                if self.stale:
                    write_atomic(os.path.join(self.fabric, "o"), "999999-7 open\n")
                    self.stale_at = time.monotonic()
                if self.fifo:
                    os.mkfifo(os.path.join(self.fabric, "o"))
            if seen is not None and self.verdict and self.verdict_at is None and time.monotonic() - seen >= self.delay:
                self.verdict_at = time.monotonic()
                write_atomic(os.path.join(self.fabric, "o"), "%s %s\n" % (self.token, self.verdict))
            time.sleep(0.02)


class SyncOpen(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not shutil.which("gcc") or not os.path.exists(os.path.join(CUDA_INC, "cuda.h")):
            raise unittest.SkipTest("needs gcc and cuda.h (CUDA_INC)")
        try:
            with open("/proc/sys/kernel/yama/ptrace_scope") as f:
                if int(f.read()) > 1:
                    raise unittest.SkipTest("Yama ptrace_scope above 1: the holder cannot freeze the fake rank")
        except OSError:
            pass
        cls.tmp = tempfile.mkdtemp()
        cls.tool = build(cls.tmp)
        cls.env = dict(os.environ, LD_LIBRARY_PATH=cls.tmp)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def setUp(self):
        self.start_rank()

    def start_rank(self, *extra):
        d = tempfile.mkdtemp(dir=self.tmp)
        self.log = os.path.join(d, "rank.log")
        self.fabric = os.path.join(d, "fabric")
        os.mkdir(self.fabric)
        p = subprocess.Popen([sys.executable, "-I", "-c", RANK, self.log, *extra], stdout=subprocess.PIPE, text=True)
        if p.stdout.readline().strip() != "ready":
            p.kill()
            raise RuntimeError("fake rank did not start")
        self.addCleanup(p.stdout.close)
        self.addCleanup(p.wait)
        self.addCleanup(p.kill)
        self.pid = p.pid

    def tool_run(self, *args, timeout=60):
        return subprocess.run([self.tool, *args], env=self.env, capture_output=True, text=True, timeout=timeout)

    def cycle(self, driver, sync_ms):
        """Suspend, stop and resume in place (resume with --sync-open
        sync_ms unless None), as the agent does for a capture left running;
        returns the resume, its duration and the rank's commands after the
        stop, as (time, command)."""
        driver.start()
        self.addCleanup(driver.stop.set)
        s = self.tool_run("--timeout-ms", "10000", "--fabric-map", self.fabric, "suspend", str(self.pid))
        self.assertEqual(s.returncode, 0, "suspend failed:\n" + s.stdout + s.stderr)
        self.assertIn("suspended; holder pid=", s.stdout, s.stdout + s.stderr)
        holder = int(s.stdout.split("holder pid=")[1].split()[0])
        self.addCleanup(self.kill_holder, holder)
        st = self.tool_run("stop", str(self.pid))
        self.assertEqual(st.returncode, 0, "stop failed:\n" + st.stdout + st.stderr)
        with open(self.log) as f:
            mark = len(f.readlines())
        sync = ["--sync-open", str(sync_ms)] if sync_ms is not None else []
        t0 = time.monotonic()
        r = self.tool_run("--fabric-map", self.fabric, *sync, "resume", str(self.pid), timeout=120)
        took = time.monotonic() - t0
        with open(self.log) as f:
            cmds = [(float(t), c) for t, c in (l.rstrip("\n").split(" ", 1) for l in f.readlines()[mark:])]
        return r, took, cmds

    def kill_holder(self, pid):
        # A holder left by a failed test blocks SIGTERM (it waits for it).
        # Only if pid is still this test's tool: a holder that exited may
        # have had its pid reused.
        try:
            with open("/proc/%d/cmdline" % pid, "rb") as f:
                if f.read().split(b"\0")[0] != self.tool.encode():
                    return
            os.kill(pid, 9)
        except OSError:
            pass

    def at(self, cmds, name):
        return [t for t, c in cmds if c == name]

    def test_open_waits_for_the_verdict(self):
        drv = Driver(self.fabric, "open", delay=1.5)
        r, _, cmds = self.cycle(drv, 30000)
        out = r.stdout + r.stderr
        self.assertEqual(r.returncode, 0, out)
        names = [c for _, c in cmds]
        # One request holds the launches and takes this tool's lease on them.
        self.assertIn("resume hold lease", names, "the in-place resume did not hold the launches: %s\n%s" % (names, out))
        self.assertNotIn("resume", names, "a plain resume opened the gates at once: %s" % names)
        self.assertNotIn("lease", names, "a lease taken apart from the held resume: %s" % names)
        self.assertEqual(len(self.at(cmds, "open")), 1, "%s\n%s" % (names, out))
        self.assertIsNotNone(drv.verdict_at, "the node never wrote its resumed marker:\n" + out)
        self.assertGreaterEqual(self.at(cmds, "open")[0], drv.verdict_at,
                                "the gates opened before every node had resumed:\n" + out)
        self.assertLess(self.at(cmds, "resume hold lease")[0], drv.verdict_at)
        self.assertRegex(drv.token, r"^[0-9]+-[0-9]+$")
        self.assertNotIn(UNSYNCED, out)
        self.assertIn("sync-open: every node resumed", out)

    def test_verdict_for_another_token_is_ignored(self):
        drv = Driver(self.fabric, "open", delay=1.5, stale=True)
        r, _, cmds = self.cycle(drv, 30000)
        out = r.stdout + r.stderr
        self.assertEqual(r.returncode, 0, out)
        opens = self.at(cmds, "open")
        self.assertEqual(len(opens), 1, out)
        self.assertIsNotNone(drv.stale_at, out)
        self.assertGreaterEqual(opens[0], drv.verdict_at, "a verdict for another token opened the gates:\n" + out)

    def test_abort_still_opens(self):
        drv = Driver(self.fabric, "abort")
        r, took, cmds = self.cycle(drv, 30000)
        out = r.stdout + r.stderr
        self.assertEqual(r.returncode, 0, out)
        self.assertEqual(len(self.at(cmds, "open")), 1, "an aborted node left its launches held:\n" + out)
        self.assertIn(UNSYNCED, out)
        self.assertIn("abort", out)
        self.assertLess(took, 20, "the abort did not end the wait:\n" + out)

    def test_no_verdict_opens_at_the_deadline(self):
        drv = Driver(self.fabric, None)
        r, took, cmds = self.cycle(drv, 2000)
        out = r.stdout + r.stderr
        self.assertEqual(r.returncode, 0, out)
        self.assertEqual(len(self.at(cmds, "open")), 1, "a node with no verdict left its launches held:\n" + out)
        self.assertIn(UNSYNCED, out)
        self.assertIn("no verdict", out)
        self.assertGreaterEqual(took, 2.0, "opened before its deadline with no verdict:\n" + out)
        self.assertLess(took, 20, out)

    def test_fifo_for_a_verdict_does_not_block_past_the_deadline(self):
        drv = Driver(self.fabric, None, fifo=True)
        r, took, cmds = self.cycle(drv, 2000)
        out = r.stdout + r.stderr
        self.assertEqual(r.returncode, 0, out)
        self.assertEqual(len(self.at(cmds, "open")), 1, "a FIFO for a verdict left the launches held:\n" + out)
        self.assertIn("no verdict", out)
        self.assertLess(took, 20, "the wait for a verdict blocked on a FIFO:\n" + out)

    def test_resume_through_the_holder_is_not_synced(self):
        # The stop failed after a good suspend: the holder is still there,
        # and the agent's resume (with --sync-open) goes through it. That
        # resume opens at once, so it must say it was not synced.
        drv = Driver(self.fabric, "open")
        drv.start()
        self.addCleanup(drv.stop.set)
        s = self.tool_run("--timeout-ms", "10000", "--fabric-map", self.fabric, "suspend", str(self.pid))
        self.assertEqual(s.returncode, 0, "suspend failed:\n" + s.stdout + s.stderr)
        holder = int(s.stdout.split("holder pid=")[1].split()[0])
        self.addCleanup(self.kill_holder, holder)
        r = self.tool_run("--fabric-map", self.fabric, "--sync-open", "30000", "resume", str(self.pid), timeout=120)
        out = r.stdout + r.stderr
        self.assertEqual(r.returncode, 0, out)
        self.assertIn(UNSYNCED, out, "a resume through the holder was reported synced")

    def test_without_sync_open_resumes_as_before(self):
        drv = Driver(self.fabric, "open")
        r, _, cmds = self.cycle(drv, None)
        out = r.stdout + r.stderr
        self.assertEqual(r.returncode, 0, out)
        names = [c for _, c in cmds]
        self.assertIn("resume", names, out)
        self.assertNotIn("resume hold", names, out)
        self.assertNotIn("resume hold lease", names, out)
        self.assertNotIn("open", names, out)
        self.assertNotIn("lease", names, out)
        self.assertFalse(os.path.exists(os.path.join(self.fabric, "resumed")), "a marker without --sync-open")
        self.assertNotIn("sync-open", out)

    def test_library_without_resume_hold_lease_is_not_held(self):
        # A held resume and a separate "lease" would leave the launches
        # held with no lease between the two requests: such a library is
        # resumed without the hold, and the resume says it was not synced.
        self.start_rank("nolease")
        drv = Driver(self.fabric, "open")
        r, _, cmds = self.cycle(drv, 30000)
        out = r.stdout + r.stderr
        self.assertEqual(r.returncode, 0, out)
        names = [c for _, c in cmds]
        self.assertIn("resume", names, out)
        self.assertNotIn("resume hold", names, "launches held with no lease: %s" % names)
        self.assertNotIn("lease", names, names)
        self.assertIn(UNSYNCED, out)
        self.assertIn('without "resume hold lease"', out)

    def test_open_that_fails_is_not_reported_let_through(self):
        self.start_rank("openbusy")
        drv = Driver(self.fabric, "open")
        r, _, cmds = self.cycle(drv, 30000)
        out = r.stdout + r.stderr
        self.assertNotEqual(r.returncode, 0, out)
        self.assertNotIn("every node resumed: launches let through", out,
                         "the resume said the launches were let through when an open failed")
        self.assertIn("not every pid's GPU launches were let through", out)

    def test_lease_that_cannot_be_taken_is_not_synced(self):
        self.start_rank("leasefails")
        drv = Driver(self.fabric, "open")
        r, _, cmds = self.cycle(drv, 30000)
        out = r.stdout + r.stderr
        self.assertEqual(r.returncode, 0, out)
        self.assertIn(UNSYNCED, out, "a resume whose launches went through without a lease was reported synced")
        self.assertIn("no lease", out)

    def test_library_without_resume_hold_is_not_synced(self):
        self.start_rank("old")
        drv = Driver(self.fabric, "open")
        r, _, cmds = self.cycle(drv, 30000)
        out = r.stdout + r.stderr
        self.assertEqual(r.returncode, 0, out)
        self.assertIn("resume", [c for _, c in cmds], out)
        self.assertIn(UNSYNCED, out, "a process that opened at its resume was reported synced")
        self.assertIn('without "resume hold lease"', out, out)

    def test_release_refused_after_the_fence_fails_the_rollback(self):
        # The process cannot run again: the suspend's rollback fails, and
        # says so in the words the agent replaces the workload on
        # (internal/agent/gpushare.go, suspendRollbackFailed).
        self.start_rank("doomed")
        s = self.tool_run("--timeout-ms", "10000", "suspend", str(self.pid))
        out = s.stdout + s.stderr
        self.assertNotEqual(s.returncode, 0, out)
        self.assertIn("suspend failed and so did the rollback", out)
        self.assertIn("cannot run again", out)

    def test_sync_open_usage_is_checked(self):
        for args, want in ((["--sync-open", "1000", "resume"], "--sync-open goes with --fabric-map"),
                           (["--fabric-map", self.fabric, "--sync-open", "1000", "suspend"],
                            "--sync-open goes with resume"),
                           (["--fabric-map", self.fabric, "--sync-open", "1000", "--hold-gate", "resume"],
                            "--sync-open does not go with --hold-gate"),
                           (["--fabric-map", self.fabric, "--sync-open", "12x", "resume"],
                            "--sync-open needs a number of milliseconds above 0"),
                           (["--fabric-map", self.fabric, "--sync-open", "0", "resume"],
                            "--sync-open needs a number of milliseconds above 0")):
            r = self.tool_run(*args, str(self.pid))
            self.assertEqual(r.returncode, 1, "%s: %s" % (args, r.stdout + r.stderr))
            self.assertIn(want, r.stderr, args)


if __name__ == "__main__":
    unittest.main(verbosity=2)
