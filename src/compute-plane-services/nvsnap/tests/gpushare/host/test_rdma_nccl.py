#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""mx-pause and nixl-pause leave NCCL's RDMA to "release".

A worker can load its weights through ModelExpress (or move KV over NIXL)
and run NCCL over RDMA through libnvsnap_ibv. After the pause, NCCL's
device files are still open: the library closes them at "release", right
after. The pause's check, that no RDMA is left for the dump to fail on,
must not count them, or such a worker can never be captured; anything
else's must still count.

No NIC and no root: the library is built with GPUSHARE_RDMA_DIR naming a
temporary directory, whose files stand for /dev/infiniband's, and with
GPUSHARE_RDMA_EVENT naming a timerfd for a verbs event file. A fake
libnvsnap_ibv opens and maps one device file, and opens an event file, as
NCCL's context would. The test checks that

  * NCCL's open files and mapping are left out of the pause's count;
  * another file open on the same device, another event file, or a
    mapping of another device, still counts;
  * a mapping of NCCL's device whose file another user closed cannot be
    told from NCCL's (the dump fails on it instead);
  * a library without nvsnap_ibv_fds leaves nothing out;
  * end to end, mx-pause passes in a worker whose only RDMA is NCCL's, and
    refuses one with another file open (test_nixl_host.py: nixl-pause);
  * "quiesce" refuses the same RDMA the pauses refuse, before anything
    stops, and passes with NCCL's only;
  * a check that cannot read the process's files fails the pause;
  * so does one that cannot look at a file the process has open;
  * once a quiesce has held NCCL's verbs calls, every path that lets the
    app run again without a release ("hold" again, the gates opening) lets
    them run too.

Run: python3 -I tests/gpushare/host/test_rdma_nccl.py
(CUDA_INC names the directory with cuda.h; default /usr/local/cuda/include.)
"""

import os
import selectors
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from test_mx_host import CUDA_INC, SRC  # noqa: E402
from test_mx_host import STUB as MX_STUB  # noqa: E402

# Also the calls "quiesce" makes to drain the GPU (one device, idle).
STUB = MX_STUB + r"""
CUresult cuDeviceGetCount(int *n) { *n = 1; return 0; }
CUresult cuDevicePrimaryCtxGetState(int d, unsigned *f, int *a) { (void)d; *f = 0; *a = 1; return 0; }
CUresult cuDevicePrimaryCtxRetain(void **c, int d) { (void)d; *c = (void *)1; return 0; }
CUresult cuDevicePrimaryCtxRelease(int d) { (void)d; return 0; }
CUresult cuCtxPushCurrent(void *c) { (void)c; return 0; }
CUresult cuCtxPopCurrent(void **c) { *c = (void *)1; return 0; }
CUresult cuCtxSynchronize(void) { return 0; }
"""

# A fake libnvsnap_ibv: at load it opens IBDIR/uverbs0 and maps it, and
# opens an event file (a timerfd), as NCCL's context would; "suspend"
# closes them and "restore" opens them again. -DOLD leaves out
# nvsnap_ibv_fds (a library from before it).
SHIM = r"""
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/timerfd.h>
#include <unistd.h>

static int fd0 = -1, ev0 = -1;
static void *map0;

static void up(void)
{
    char p[512];
    snprintf(p, sizeof(p), "%s/uverbs0", getenv("IBDIR"));
    fd0 = open(p, O_RDWR | O_CLOEXEC);
    map0 = fd0 < 0 ? MAP_FAILED : mmap(NULL, 4096, PROT_READ, MAP_SHARED, fd0, 0);
    ev0 = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC);
}
__attribute__((constructor)) static void init(void) { up(); }

int nvsnap_ibv_fence(char *r, size_t n) { snprintf(r, n, "ok"); return 0; }
int nvsnap_ibv_suspend(char *r, size_t n)
{
    if (getenv("REFUSE")) { snprintf(r, n, "err refused"); return -1; }
    if (map0 != MAP_FAILED) munmap(map0, 4096);
    if (fd0 >= 0) close(fd0);
    if (ev0 >= 0) close(ev0);
    fd0 = ev0 = -1;
    map0 = MAP_FAILED;
    snprintf(r, n, "ok released 1 device(s)");
    return 0;
}
int nvsnap_ibv_restore(char *r, size_t n) { if (fd0 < 0) up(); snprintf(r, n, "ok"); return 0; }
int nvsnap_ibv_map(FILE *fp) { (void)fp; return 0; }
int nvsnap_ibv_connect(const char *p, char *r, size_t n) { (void)p; snprintf(r, n, "ok"); return 0; }
int shim_opens;  /* nvsnap_ibv_open calls */
void nvsnap_ibv_open(void) { shim_opens++; }
int nvsnap_ibv_count(void) { return 1; }
#ifndef NODOOM
/* REFUSE: the release refused after the fence; NCCL's calls stay held.
 * nvsnap_ibv_doom_if_fenced: gpushare's release could not run after the
 * fence (state lock busy): held for good too. */
int shim_doom_calls;
int nvsnap_ibv_doomed(void) { return getenv("REFUSE") != NULL || shim_doom_calls > 0; }
int nvsnap_ibv_doom_if_fenced(void) { shim_doom_calls++; return 1; }
#endif
#ifndef OLD
int nvsnap_ibv_fds(int *fds, int max)
{
    static int calls;
    /* FLIP: every other call finds no device, as if NCCL had just closed
     * one, and its number might have gone to another user. */
    if (fd0 < 0 || (getenv("FLIP") && calls++ % 2)) return 0;
    if (max > 0) fds[0] = fd0;
    if (max > 1) fds[1] = ev0;
    return 2;
}
#endif
"""

# Calls the library's own functions on a process set up as CASE says:
#   owned       NCCL's file and mapping only
#   foreignfd   + another file open on NCCL's device (UCX, say)
#   foreignmap  + a mapping of another device, its file closed
#   samemap     + a mapping of NCCL's device, its file closed
#   foreignev   + another event file
#   (FLIP set: NCCL's list changes during the check)
#   noshim      no libnvsnap_ibv; a file open as in "owned"
# then prints the pause's count (pause_open, pause_mapped).
PROBE = r"""
#include "gpushare.c"
#include <sys/timerfd.h>

static int closed_gate(struct gate *g) { return __atomic_load_n(&g->closed, __ATOMIC_SEQ_CST); }
/* "release" from another thread than the one holding the state lock. */
static void *release_t(void *rep) { do_release(rep, GPUSHARE_REPLY_MAX, NULL); return NULL; }

static void open_map(const char *dev, int keep)
{
    char p[512];
    snprintf(p, sizeof(p), "%s/%s", getenv("IBDIR"), dev);
    int fd = open(p, O_RDWR);
    if (fd < 0 || mmap(NULL, 4096, PROT_READ, MAP_SHARED, fd, 0) == MAP_FAILED) exit(7);
    if (!keep) close(fd);
}

int main(int argc, char **argv)
{
    const char *c = argc > 2 ? argv[1] : "";
    if (strcmp(c, "noshim") && !dlopen(argv[2], RTLD_NOW | RTLD_LOCAL)) { puts(dlerror()); return 3; }
    if (!strcmp(c, "refused")) {
        /* Quiesce (unless UNFENCED), then a release whose NCCL verbs
         * refuse (REFUSE), or that finds the state lock busy (BUSY), then
         * every command that would let the process run again. */
        char rep[GPUSHARE_REPLY_MAX], r2[GPUSHARE_REPLY_MAX];
        void *sh = dlopen(argv[2], RTLD_NOW | RTLD_NOLOAD);
        int *opens = dlsym(sh, "shim_opens"), *dooms = dlsym(sh, "shim_doom_calls");
        int q = 0;
        if (!getenv("UNFENCED")) {
            do_quiesce(rep, sizeof(rep));
            q = !strcmp(rep, "ok quiesced");
        } else {
            hold_launches();
        }
        pthread_t locker;
        if (getenv("BUSY")) {
            pthread_mutex_lock(&mu);  /* another thread's: the release cannot take it */
            pthread_create(&locker, NULL, release_t, rep);
            pthread_join(locker, NULL);
            pthread_mutex_unlock(&mu);
            printf("release_reply=%s\n", !strcmp(rep, "err busy (state lock)") ? "busy" : "other");
        } else {
            do_release(rep, sizeof(rep), NULL);
        }
        int rel = !strncmp(rep, "ok", 2), o0 = *opens, refused = 0;
        const char *cmds[] = { "resume", "resume hold", "open", "hold", "step", "quiesce", "release" };
        for (int i = 0; i < 7; i++) {
            r2[0] = 0;
            if (i == 0) do_resume(r2, sizeof(r2), 0);
            else if (i == 1) do_resume(r2, sizeof(r2), 1);
            else if (i == 2) do_open(r2, sizeof(r2));
            else if (i == 3) do_hold(r2, sizeof(r2));
            else if (i == 4) do_poll(r2, sizeof(r2), 1);
            else if (i == 5) do_quiesce(r2, sizeof(r2));
            else do_release(r2, sizeof(r2), NULL);
            int no = !!strstr(r2, "cannot run again");
            refused += no;
            if (!no) fprintf(stderr, "%s: %s\n", cmds[i], r2);
        }
        gate_open();
        ibv_unfence();
        printf("quiesce_ok=%d release_ok=%d refused=%d launch_closed=%d memory_closed=%d verbs_opens=%d doom_calls=%d\n",
               q, rel, refused, closed_gate(&lgate), closed_gate(&mgate), *opens - o0, dooms ? *dooms : -1);
        return 0;
    }
    if (!strcmp(c, "noshim")) {
        char p[512];
        snprintf(p, sizeof(p), "%s/uverbs0", getenv("IBDIR"));
        if (open(p, O_RDWR) < 0) return 4;
    }
    if (!strcmp(c, "foreignfd")) {
        char p[512];
        snprintf(p, sizeof(p), "%s/uverbs0", getenv("IBDIR"));
        if (open(p, O_RDWR) < 0) return 4;
    }
    if (!strcmp(c, "foreignmap")) open_map("uverbs1", 0);
    if (!strcmp(c, "samemap")) open_map("uverbs0", 0);
    if (!strcmp(c, "foreignev") && timerfd_create(CLOCK_MONOTONIC, 0) < 0) return 5;
    int o, m;
    int scanned = rdma_files(&o, &m, 1) == 0;
    printf("scanned=%d pause_open=%d pause_mapped=%d\n", scanned, o, m);
    char rep[GPUSHARE_REPLY_MAX];
    do_py(rep, sizeof(rep), mx_py, sizeof(mx_py), "mx", "pause", "");
    printf("mx_pause_ok=%d\n", !strncmp(rep, "ok", 2));
    do_quiesce(rep, sizeof(rep));
    int q = !strcmp(rep, "ok quiesced");
    printf("quiesce_ok=%d\n", q);
    /* After a quiesce, the paths that let the app run without a release. */
    int *opens = dlsym(RTLD_DEFAULT, "shim_opens");
    void *h = strcmp(c, "noshim") ? dlopen(argv[2], RTLD_NOW | RTLD_NOLOAD) : NULL;
    if (h) opens = dlsym(h, "shim_opens");
    if (q && opens) {
        int o0 = *opens;
        do_hold(rep, sizeof(rep));
        int o1 = *opens;
        do_quiesce(rep, sizeof(rep));
        gate_open();
        printf("opens_at_hold=%d opens_at_open=%d\n", o1 - o0, *opens - o1);
    }
    return 0;
}
"""

# A worker for the end-to-end cases: the library preloaded, the fake
# libnvsnap_ibv loaded as NCCL loads it (dlopen, local), a fake
# ModelExpress 0.7 loader and a fake vLLM NIXL connector whose pause frees
# nothing of its own. argv: LOG FOREIGN ("1": also opens another file on
# NCCL's device, as UCX left behind).
WORKER = r"""
import ctypes, enum, os, sys, time, types
LOG, FOREIGN = sys.argv[1], sys.argv[2] == "1"
ctypes.CDLL(os.environ["SHIM"], mode=os.RTLD_LOCAL)
if FOREIGN:
    keep = os.open(os.path.join(os.environ["IBDIR"], "uverbs0"), os.O_RDWR)

class Ctx:
    p2p_enabled = True
    nixl_manager = object()
    mx_client = None

def mod(name, **kw):
    m = types.ModuleType(name)
    m.__dict__.update(kw)
    sys.modules[name] = m
    return m

def pause_serving(ctx):
    ctx.nixl_manager = ctx.mx_client = None

def resume_serving(ctx, model):
    ctx.nixl_manager = object()

mod("modelexpress")
mod("modelexpress.lifecycle", pause_serving=pause_serving, resume_serving=resume_serving)
sys.modules["modelexpress"].lifecycle = sys.modules["modelexpress.lifecycle"]
mod("modelexpress.engines.vllm.loader", _loader_registry={0: types.SimpleNamespace(_ctx=Ctx())},
    _nixl_managers={})

h = ctypes.c_ulonglong()
fd = os.open(os.devnull, os.O_RDONLY)
imp = ctypes.CDLL("libcuda.so.1").cuMemImportFromShareableHandle
r = imp(ctypes.byref(h), ctypes.c_void_p(fd), 1)
print("ready" if r == 0 else "import failed: %d" % r, flush=True)
while True:
    time.sleep(0.1)
"""


def rdma_defines(ib):
    """Build flags: IB's files stand for RDMA devices, a timerfd for a verbs event file."""
    return ["-DGPUSHARE_RDMA_DIR=\"%s/\"" % ib, "-DGPUSHARE_RDMA_EVENT=\"anon_inode:[timerfd]\""]


class RdmaNCCL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not shutil.which("gcc") or not os.path.exists(os.path.join(CUDA_INC, "cuda.h")):
            raise unittest.SkipTest("needs gcc and cuda.h (CUDA_INC)")
        cls.tmp = tempfile.mkdtemp()
        cls.ib = os.path.join(cls.tmp, "ib")
        os.mkdir(cls.ib)
        for dev in ("uverbs0", "uverbs1"):
            with open(os.path.join(cls.ib, dev), "wb") as f:
                f.write(b"\0" * 4096)
        t = cls.tmp

        def cc(*args):
            subprocess.run(["gcc", "-O2", "-Wall", "-Wextra", "-Werror", "-I", SRC, "-I", CUDA_INC,
                            *rdma_defines(cls.ib), *args], check=True)
        for name, text in (("stub.c", STUB), ("shim.c", SHIM), ("probe.c", PROBE)):
            with open(os.path.join(t, name), "w") as f:
                f.write(text)
        cc("-shared", "-fPIC", "-o", os.path.join(t, "libcuda.so.1"), os.path.join(t, "stub.c"))
        for out, extra in (("libnvsnap_ibv.so", []), ("old/libnvsnap_ibv.so", ["-DOLD"]),
                           ("nodoom/libnvsnap_ibv.so", ["-DNODOOM"])):
            os.makedirs(os.path.dirname(os.path.join(t, out)), exist_ok=True)
            cc("-shared", "-fPIC", "-Wl,-soname,libnvsnap_ibv.so", *extra, "-o", os.path.join(t, out),
               os.path.join(t, "shim.c"))
        cc("-Wno-unused-function", "-o", os.path.join(t, "probe"), os.path.join(t, "probe.c"),
           os.path.join(t, "libcuda.so.1"), "-Wl,-rpath," + t, "-ldl", "-lpthread")
        # A process whose files cannot be listed.
        cc("-Wno-unused-function", "-DGPUSHARE_PROC_SELF=\"%s/noproc\"" % t, "-o", os.path.join(t, "probe-noproc"),
           os.path.join(t, "probe.c"), os.path.join(t, "libcuda.so.1"), "-Wl,-rpath," + t, "-ldl", "-lpthread")
        # A process whose file table lists a file that cannot be looked at
        # (a regular file stands for it: readlink fails), and one whose
        # table, "." and ".." included, reads cleanly.
        for name, target in (("badproc", None), ("fakeproc", "/dev/null")):
            fd = os.path.join(t, name, "fd")
            os.makedirs(fd)
            if target:
                os.symlink(target, os.path.join(fd, "3"))
            else:
                open(os.path.join(fd, "3"), "w").close()
            open(os.path.join(t, name, "maps"), "w").close()
            cc("-Wno-unused-function", "-DGPUSHARE_PROC_SELF=\"%s/%s\"" % (t, name), "-o",
               os.path.join(t, "probe-" + name), os.path.join(t, "probe.c"), os.path.join(t, "libcuda.so.1"),
               "-Wl,-rpath," + t, "-ldl", "-lpthread")
        cc("-fpatchable-function-entry=4", "-shared", "-fPIC", "-o", os.path.join(t, "libnvsnap_gpushare.so"),
           os.path.join(SRC, "gpushare.c"), "-ldl", "-lpthread")
        cc("-o", os.path.join(t, "nvsnap-gpu-suspend"), os.path.join(SRC, "nvsnap-gpu-suspend.c"), "-ldl", "-lpthread")
        cls.env = dict(os.environ, LD_LIBRARY_PATH=t, IBDIR=cls.ib)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def probe(self, case, shim="libnvsnap_ibv.so", prog="probe", **env):
        p = subprocess.run([os.path.join(self.tmp, prog), case, os.path.join(self.tmp, shim)],
                           capture_output=True, text=True, timeout=60, env=dict(self.env, **env))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        return dict(kv.split("=", 1) for kv in p.stdout.split())

    def test_nccls_own_are_left_to_release(self):
        out = self.probe("owned")
        self.assertEqual((out["pause_open"], out["pause_mapped"]), ("0", "0"), out)
        self.assertEqual((out["mx_pause_ok"], out["quiesce_ok"]), ("1", "1"), out)

    def test_quiesce_refuses_rdma_a_dump_cannot_save(self):
        for case, kw in (("foreignfd", {}), ("foreignev", {}), ("foreignmap", {}), ("noshim", {}),
                         ("owned", {"FLIP": "1"})):
            out = self.probe(case, **kw)
            self.assertEqual(out["quiesce_ok"], "0", "%s %s: quiesce passed with RDMA left: %s" % (case, kw, out))

    def test_unreadable_files_fail_the_pause_and_quiesce(self):
        out = self.probe("owned", prog="probe-noproc")
        self.assertEqual(out["scanned"], "0", out)
        self.assertEqual(out["mx_pause_ok"], "0", "a pause passed without looking at the process's files")
        self.assertEqual(out["quiesce_ok"], "0", "a quiesce passed without looking at the process's files")

    def test_a_file_that_cannot_be_looked_at_fails_the_pause_and_quiesce(self):
        out = self.probe("owned", prog="probe-badproc")
        self.assertEqual(out["scanned"], "0", "a file the check could not look at was taken for no RDMA: %s" % out)
        self.assertEqual(out["mx_pause_ok"], "0", out)
        self.assertEqual(out["quiesce_ok"], "0", out)
        out = self.probe("owned", prog="probe-fakeproc")
        self.assertEqual((out["scanned"], out["pause_open"], out["pause_mapped"]), ("1", "0", "0"), out)

    def test_running_again_without_a_release_lets_nccls_verbs_run(self):
        out = self.probe("owned")
        self.assertEqual(out["quiesce_ok"], "1", out)
        self.assertEqual(out["opens_at_hold"], "1", "a hold after a quiesce left NCCL's verbs calls held")
        self.assertEqual(out["opens_at_open"], "1", "opening the gates after a quiesce left NCCL's verbs calls held")

    def test_release_refused_after_the_fence_never_runs_again(self):
        out = self.probe("refused", REFUSE="1")
        self.assertEqual((out["quiesce_ok"], out["release_ok"]), ("1", "0"), out)
        self.assertEqual(out["refused"], "7", "a command let a process run again after its NCCL release refused: %s" % out)
        self.assertEqual((out["launch_closed"], out["memory_closed"], out["verbs_opens"]), ("1", "1", "0"), out)

    def test_release_on_a_busy_lock_after_the_fence_never_runs_again(self):
        # The state lock is busy when "release" comes after the quiesce
        # fenced NCCL's verbs: the peers may release, so this process must
        # not run again, with or without the library's nvsnap_ibv_doom_if_fenced.
        for shim, dooms in (("libnvsnap_ibv.so", "1"), ("nodoom/libnvsnap_ibv.so", "-1")):
            out = self.probe("refused", shim=shim, BUSY="1")
            self.assertEqual((out["quiesce_ok"], out["release_reply"], out["release_ok"]), ("1", "busy", "0"), out)
            self.assertEqual(out["refused"], "7", "%s: a process ran again after its release found the lock busy: %s" % (shim, out))
            self.assertEqual((out["launch_closed"], out["memory_closed"], out["verbs_opens"]), ("1", "1", "0"), out)
            self.assertEqual(out["doom_calls"], dooms, "%s: the verbs library was not told: %s" % (shim, out))

    def test_release_on_a_busy_lock_without_a_fence_runs_again(self):
        # No quiesce, so nothing was fenced: the busy release changes
        # nothing, and the process may run again.
        out = self.probe("refused", BUSY="1", UNFENCED="1")
        self.assertEqual((out["release_reply"], out["refused"], out["launch_closed"]), ("busy", "0", "0"), out)
        self.assertEqual(out["doom_calls"], "0", out)

    def test_release_refused_by_an_older_verbs_library_rolls_back(self):
        # No nvsnap_ibv_doomed: the refusal is taken as before, and the
        # process runs again.
        out = self.probe("refused", shim="nodoom/libnvsnap_ibv.so", REFUSE="1")
        self.assertEqual((out["quiesce_ok"], out["release_ok"], out["refused"]), ("1", "0", "0"), out)
        self.assertEqual((out["launch_closed"], out["memory_closed"]), ("0", "0"), out)

    def test_another_file_on_nccls_device_counts(self):
        out = self.probe("foreignfd")
        self.assertEqual((out["pause_open"], out["pause_mapped"]), ("1", "0"), out)

    def test_another_event_file_counts(self):
        out = self.probe("foreignev")
        self.assertEqual((out["pause_open"], out["pause_mapped"]), ("1", "0"), out)

    def test_a_mapping_of_another_device_counts(self):
        out = self.probe("foreignmap")
        self.assertEqual((out["pause_open"], out["pause_mapped"]), ("0", "1"), out)

    def test_a_list_that_changes_during_the_check_leaves_nothing_out(self):
        out = self.probe("owned", FLIP="1")
        self.assertEqual((out["pause_open"], out["pause_mapped"]), ("2", "1"), out)

    def test_a_mapping_of_nccls_device_without_its_file_is_not_seen(self):
        # rdma-core unmaps at close, so another user leaves none; if one is
        # left, the dump fails on it and the capture rolls back.
        out = self.probe("samemap")
        self.assertEqual((out["pause_open"], out["pause_mapped"]), ("0", "0"), out)

    def test_library_without_fds_leaves_nothing_out(self):
        out = self.probe("owned", shim="old/libnvsnap_ibv.so")
        self.assertEqual((out["pause_open"], out["pause_mapped"]), ("2", "1"), out)

    def test_without_the_library_everything_counts(self):
        out = self.probe("noshim")
        self.assertEqual((out["pause_open"], out["pause_mapped"]), ("1", "0"), out)

    def worker(self, foreign):
        log = os.path.join(self.tmp, "log.%s" % self._testMethodName)
        env = dict(self.env, LD_PRELOAD=os.path.join(self.tmp, "libnvsnap_gpushare.so"),
                   SHIM=os.path.join(self.tmp, "libnvsnap_ibv.so"))
        p = subprocess.Popen([sys.executable, "-I", "-c", WORKER, log, "1" if foreign else "0"], env=env,
                             stdout=subprocess.PIPE, text=True)
        self.addCleanup(p.stdout.close)
        self.addCleanup(p.wait)
        self.addCleanup(p.kill)
        # Bounded: a worker stuck before "ready" fails the test, and the
        # cleanups above kill and reap it.
        sel = selectors.DefaultSelector()
        sel.register(p.stdout, selectors.EVENT_READ)
        line = p.stdout.readline().strip() if sel.select(30) else "<nothing in 30 s>"
        sel.close()
        if line != "ready":
            self.fail("worker did not start: %r" % line)
        return p.pid

    def tool(self, pid, *args):
        return subprocess.run([os.path.join(self.tmp, "nvsnap-gpu-suspend"), *args, str(pid)],
                              env=self.env, capture_output=True, text=True, timeout=60)

    def test_mx_pause_with_nccl_rdma(self):
        pid = self.worker(False)
        p = self.tool(pid, "mx-pause")
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("ok paused 1 ModelExpress worker(s)", p.stdout)
        self.assertIn("0 RDMA device file(s) open, 0 mapped", p.stdout)
        self.assertEqual(self.tool(pid, "mx-resume").returncode, 0)

    def test_mx_pause_refuses_other_rdma(self):
        pid = self.worker(True)
        p = self.tool(pid, "mx-pause")
        self.assertNotEqual(p.returncode, 0, p.stdout)
        self.assertIn("1 RDMA device file(s) still open, 0 mapped", p.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
