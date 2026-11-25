#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""The launch gate and GPU drain checks in libnvsnap_gpushare.

No GPU: a probe program includes gpushare.c and calls its gate and sync
functions directly, against a stub driver whose cuCtxSynchronize takes
SYNC_MS and counts its calls. The test checks that

  * a "step" permit a held launch has not taken yet counts as busy, and
    quiesce takes it back, so that launch cannot pass the gate afterwards;
  * a permit taken between the drain check's reads still counts as busy;
  * a drain check ("poll") uses a synchronization that finished between
    two polls, instead of discarding it and reporting busy forever;
  * quiesce never uses a synchronization that finished, or started, before
    it was called;
  * a quiesce that finds the GPU busy while launches are held keeps them
    held (the ranks are catching up), and opens the gates otherwise;
  * a held resume with a lease ("resume hold lease") keeps the gates
    closed while its sender lives, and opens them once it exits, also when
    it was gone before the library took the lease;
  * when the process that sent "hold" exits, the gates open again, also
    when the state lock is busy at that moment (retried until it is free),
    and also after a "resume" from it that failed, or an "open" that found
    nothing to open;
  * a batched copy (CUDA 12 and 13 entry points) waits at the memory gate,
    and a lookup through cuGetProcAddress gets the wrapper of the version
    and stream it asked for, even at an address no exported name has.

Run: python3 -I tests/gpushare/host/test_gate_sync.py
(CUDA_INC names the directory with cuda.h; default /usr/local/cuda/include.)
"""

import os
import shutil
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.normpath(os.path.join(HERE, "../../../docker/agent/gpushare"))
CUDA_INC = os.environ.get("CUDA_INC", "/usr/local/cuda/include")

STUB = r"""
#include <stdlib.h>
#include <unistd.h>
typedef int CUresult;
int stub_syncs;
CUresult cuInit(unsigned f) { (void)f; return 0; }
CUresult cuGetErrorName(CUresult r, const char **s) { (void)r; *s = "stub"; return 0; }
CUresult cuDeviceGetCount(int *n) { *n = 1; return 0; }
CUresult cuDevicePrimaryCtxGetState(int d, unsigned *f, int *a) { (void)d; *f = 0; *a = 1; return 0; }
CUresult cuDevicePrimaryCtxRetain(void **c, int d) { (void)d; *c = (void *)1; return 0; }
CUresult cuDevicePrimaryCtxRelease(int d) { (void)d; return 0; }
CUresult cuCtxPushCurrent(void *c) { (void)c; return 0; }
CUresult cuCtxPopCurrent(void **c) { *c = (void *)1; return 0; }
int stub_batches;
CUresult cuMemcpyBatchAsync(void) { __atomic_add_fetch(&stub_batches, 1, __ATOMIC_SEQ_CST); return 0; }
CUresult cuMemcpyBatchAsync_v2(void) { __atomic_add_fetch(&stub_batches, 1, __ATOMIC_SEQ_CST); return 0; }
/* An entry point at an address no exported name has, as a driver may
 * return from cuGetProcAddress. */
static CUresult unexported(void) { __atomic_add_fetch(&stub_batches, 1, __ATOMIC_SEQ_CST); return 0; }
CUresult cuGetProcAddress_v2(const char *s, void **p, int v, unsigned long long f, void *q)
{ (void)s; (void)v; (void)f; (void)q; *p = (void *)unexported; return 0; }
CUresult cuCtxSynchronize(void)
{
    const char *e = getenv("SYNC_MS");
    usleep((e ? atoi(e) : 0) * 1000);
    __atomic_add_fetch(&stub_syncs, 1, __ATOMIC_SEQ_CST);
    return 0;
}
"""

PROBE = r"""
#define _GNU_SOURCE  /* as gpushare.c: its headers come after this one */
#include <pthread.h>
/* Runs once, on the next lock of gate_mu: what another thread did just
 * before the lock was taken. */
static void (*on_gate_lock)(void);
static int probe_lock(pthread_mutex_t *m);
#define pthread_mutex_lock(m) probe_lock(m)
/* FAIL_PIDFD: pidfd_open fails (EMFILE), so no lease can be taken. */
#include <errno.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>
#ifndef SYS_pidfd_open
#define SYS_pidfd_open 434
#endif
static int probe_pidfd_open(pid_t pid)
{
    if (getenv("FAIL_PIDFD")) { errno = EMFILE; return -1; }
    return (int)syscall(SYS_pidfd_open, pid, 0);
}
#define GPUSHARE_PIDFD_OPEN(pid) probe_pidfd_open(pid)
#include "gpushare.c"
#undef pthread_mutex_lock
#include <sys/wait.h>

static int probe_lock(pthread_mutex_t *m)
{
    void (*f)(void) = m == &gate_mu ? on_gate_lock : NULL;
    if (f) on_gate_lock = NULL, f();
    return pthread_mutex_lock(m);
}
/* What gate_enter does for a held launch let through by "step". */
static void take_permit(void) { lgate.pass--; __atomic_add_fetch(&lgate.inflight, 1, __ATOMIC_SEQ_CST); }
static int closed(struct gate *g) { return __atomic_load_n(&g->closed, __ATOMIC_SEQ_CST); }
/* Wait until n threads wait at gate g; none within 5 s fails the probe:
 * a check of the closed gate that ran before its waiter got there would
 * pass without testing it. */
static void wait_waiting(struct gate *g, int n)
{
    for (int i = 0; i < 5000; i++) {
        pthread_mutex_lock(&gate_mu);
        int w = g->waiting;
        pthread_mutex_unlock(&gate_mu);
        if (w >= n) return;
        usleep(1000);
    }
    printf("no_waiter=1\n");
    exit(9);
}

extern int stub_syncs, stub_batches;
static void *waiter(void *arg) { gate_enter(&lgate); gate_exit(&lgate); *(int *)arg = 1; return NULL; }
static CUresult (*batch_fn)(CUdeviceptr *, CUdeviceptr *, size_t *, size_t, CUmemcpyAttributes *, size_t *,
                            size_t, size_t *, CUstream);
static void *batcher(void *arg) { (void)arg; batch_fn(NULL, NULL, NULL, 0, NULL, NULL, 0, NULL, NULL); return NULL; }

int main(int argc, char **argv)
{
    CUresult r;
    char rep[256];
    if (argc > 1 && !strcmp(argv[1], "permit")) {
        /* A "step" permit not taken yet (the waiter has not run). */
        lgate.closed = 1;
        lgate.pass = 1;
        printf("drained=%d\n", drained(50, &r));
        do_quiesce(rep, sizeof(rep));
        printf("quiesce=%s pass=%d\n", rep[0] ? rep : "ok", lgate.pass);
        int passed = 0;
        pthread_t t;
        pthread_create(&t, NULL, waiter, &passed);
        wait_waiting(&lgate, 1);
        usleep(50000);
        printf("passed_after_quiesce=%d\n", __atomic_load_n(&passed, __ATOMIC_SEQ_CST));
        gate_open();
        pthread_join(t, NULL);
        printf("passed_after_open=%d\n", passed);
        return 0;
    }
    if (argc > 1 && !strcmp(argv[1], "permitrace")) {
        /* The held launch takes a "step" permit between the drain check's
         * read of the launches in flight and its read of the permit. */
        lgate.closed = 1;
        lgate.pass = 1;
        on_gate_lock = take_permit;
        int d = drained(50, &r);
        printf("drained=%d inflight=%d\n", d, lgate.inflight);
        return 0;
    }
    if (argc > 1 && !strcmp(argv[1], "between")) {
        /* Polls 2 ms long, 10 ms apart; each sync takes SYNC_MS (5). */
        lgate.closed = 1;
        int idle = 0;
        for (int i = 0; i < 10; i++) {
            idle += drained(2, &r) == 1;
            usleep(10000);
        }
        printf("idle_polls=%d\n", idle);
        return 0;
    }
    if (argc > 1 && !strcmp(argv[1], "fresh")) {
        /* A poll's sync finishes and is not consumed; quiesce must sync
         * again after closing each gate. */
        lgate.closed = 1;
        drained(0, &r);
        usleep(20000);
        int before = __atomic_load_n(&stub_syncs, __ATOMIC_SEQ_CST);
        do_quiesce(rep, sizeof(rep));
        printf("quiesce=%s syncs=%d\n", rep[0] ? rep : "ok",
               __atomic_load_n(&stub_syncs, __ATOMIC_SEQ_CST) - before);
        return 0;
    }
    if (argc > 1 && !strcmp(argv[1], "busy")) {
        /* Each sync takes SYNC_MS (2500), longer than quiesce waits. */
        do_quiesce(rep, sizeof(rep));
        printf("unheld=%s:%d:%d\n", rep[0] == 'e' ? "busy" : "ok", lgate.closed, mgate.closed);
        do_hold(rep, sizeof(rep));
        do_quiesce(rep, sizeof(rep));
        printf("held=%s:%d:%d\n", rep[0] == 'e' ? "busy" : "ok", lgate.closed, mgate.closed);
        return 0;
    }
    if (argc > 1 && !strcmp(argv[1], "leasenotrelease")) {
        /* A child sends "hold", then a command that only starts like
         * "release" (unknown here), then exits: its lease must stand. */
        start_ctl();
        int up[2], down[2];
        if (pipe(up) || pipe(down)) return 3;
        pid_t c = fork();
        if (!c) {
            char b, r2[256] = "";
            int ok = request(getppid(), "hold", -1, rep, sizeof(rep), NULL) == 0 && !strcmp(rep, "ok held");
            request(getppid(), "release-status", -1, r2, sizeof(r2), NULL);
            ok = ok && !strcmp(r2, "err unknown command");
            if (write(up[1], ok ? "1" : "0", 1) != 1 || read(down[0], &b, 1) != 1) _exit(1);
            _exit(0);
        }
        char b = 0;
        if (read(up[0], &b, 1) != 1) return 4;
        printf("hold=%c closed_while_alive=%d\n", b, closed(&lgate));
        if (write(down[1], "x", 1) != 1) return 5;
        waitpid(c, NULL, 0);
        for (int i = 0; i < 300 && closed(&lgate); i++) usleep(10000);
        printf("closed_after_exit=%d\n", closed(&lgate));
        return 0;
    }
    if (argc > 1 && (!strcmp(argv[1], "lease") || !strcmp(argv[1], "leaseopen"))) {
        /* A child sends "hold" over the control socket, then exits.
         * leaseopen: it sends "open" before it exits, which finds nothing
         * a restore held ("ok not held") and opens nothing. */
        int open_ = !strcmp(argv[1], "leaseopen");
        start_ctl();
        int up[2], down[2];
        if (pipe(up) || pipe(down)) return 3;
        pid_t c = fork();
        if (!c) {
            char b;
            int ok = request(getppid(), "hold", -1, rep, sizeof(rep), NULL) == 0 && !strcmp(rep, "ok held");
            if (ok && open_) ok = request(getppid(), "open", -1, rep, sizeof(rep), NULL) == 0 && !strcmp(rep, "ok not held");
            if (write(up[1], ok ? "1" : "0", 1) != 1 || read(down[0], &b, 1) != 1) _exit(1);
            _exit(0);
        }
        char b = 0;
        if (read(up[0], &b, 1) != 1) return 4;
        usleep(300000);
        printf("hold=%c closed_while_alive=%d\n", b, closed(&lgate));
        if (write(down[1], "x", 1) != 1) return 5;
        waitpid(c, NULL, 0);
        for (int i = 0; i < 100 && closed(&lgate); i++) usleep(10000);
        printf("closed_after_exit=%d\n", closed(&lgate));
        return 0;
    }
    if (argc > 1 && (!strcmp(argv[1], "resumelease") || !strcmp(argv[1], "resumeleaseopen"))) {
        /* As after "resume hold" (--sync-open): the launches are held for an
         * "open" and no lease is left (release ended it). A child sends
         * "lease", then exits. resumeleaseopen: it sends "open" first, which
         * opens the gates and ends the lease; the gates are closed again
         * before it exits, and must stay closed. */
        int open_ = !strcmp(argv[1], "resumeleaseopen");
        start_ctl();
        hold_launches();
        held = 1;
        int up[2], down[2];
        if (pipe(up) || pipe(down)) return 3;
        pid_t c = fork();
        if (!c) {
            char b;
            int ok = request(getppid(), "lease", -1, rep, sizeof(rep), NULL) == 0 && !strcmp(rep, "ok leased");
            if (ok && open_) ok = request(getppid(), "open", -1, rep, sizeof(rep), NULL) == 0 && !strcmp(rep, "ok opened");
            if (write(up[1], ok ? "1" : "0", 1) != 1 || read(down[0], &b, 1) != 1) _exit(1);
            _exit(0);
        }
        char b = 0;
        if (read(up[0], &b, 1) != 1) return 4;
        if (open_) hold_launches();  /* a later hold of some other caller's */
        usleep(300000);
        printf("lease=%c closed_while_alive=%d\n", b, closed(&lgate));
        if (write(down[1], "x", 1) != 1) return 5;
        waitpid(c, NULL, 0);
        for (int i = 0; i < 100 && closed(&lgate); i++) usleep(10000);
        printf("closed_after_exit=%d\n", closed(&lgate));
        return 0;
    }
    if (argc > 1 && (!strcmp(argv[1], "nolease_resume") || !strcmp(argv[1], "nolease_lease") ||
                     !strcmp(argv[1], "nolease_hold"))) {
        /* No lease can be taken (FAIL_PIDFD). A sender asks for one with a
         * held resume, after one ("lease"), or with "hold". Whatever it
         * gets, the launches must not stay held with no lease. */
        const char *cmd = !strcmp(argv[1], "nolease_resume") ? "resume hold lease"
                        : !strcmp(argv[1], "nolease_lease") ? "lease" : "hold";
        start_ctl();
        if (strcmp(cmd, "hold")) hold_launches();
        if (!strcmp(cmd, "lease")) held = 1;  /* as after "resume hold" */
        int up[2], down[2];
        if (pipe(up) || pipe(down)) return 3;
        pid_t c = fork();
        if (!c) {
            char b, r[256] = "";
            request(getppid(), cmd, -1, r, sizeof(r), NULL);
            /* n: ok, but "no lease" (the launches went through); o: ok; e: err */
            char k = r[0] != 'o' ? 'e' : strstr(r, "no lease") ? 'n' : 'o';
            if (write(up[1], &k, 1) != 1 || read(down[0], &b, 1) != 1) _exit(1);
            _exit(0);
        }
        char k = 0;
        if (read(up[0], &k, 1) != 1) return 4;
        for (int i = 0; i < 300 && closed(&lgate); i++) usleep(10000);
        printf("reply=%c closed_while_alive=%d\n", k, closed(&lgate));
        if (write(down[1], "x", 1) != 1) return 5;
        waitpid(c, NULL, 0);
        return 0;
    }
    if (argc > 1 && (!strcmp(argv[1], "holdlease") || !strcmp(argv[1], "holdleasegone"))) {
        /* --sync-open's resume: one request resumes with the launches held
         * and takes the sender's lease. holdlease: the sender lives on
         * after the reply, then exits. holdleasegone: the sender is gone
         * (exited and reaped) before the library gets to its request (the
         * state lock is busy until then). Each way the gates open once
         * the sender is gone, and not before. */
        int gone = !strcmp(argv[1], "holdleasegone");
        start_ctl();
        hold_launches();
        if (gone) pthread_mutex_lock(&mu);
        int up[2], down[2];
        if (pipe(up) || pipe(down)) return 3;
        pid_t c = fork();
        if (!c) {
            char b;
            if (gone) {
                alarm(1);  /* dies waiting for the reply */
                request(getppid(), "resume hold lease", -1, rep, sizeof(rep), NULL);
                _exit(1);
            }
            int ok = request(getppid(), "resume hold lease", -1, rep, sizeof(rep), NULL) == 0 && !strncmp(rep, "ok resumed", 10);
            if (write(up[1], ok ? "1" : "0", 1) != 1 || read(down[0], &b, 1) != 1) _exit(1);
            _exit(0);
        }
        if (gone) {
            waitpid(c, NULL, 0);
            printf("closed_while_locked=%d\n", closed(&lgate));
            pthread_mutex_unlock(&mu);
        } else {
            char b = 0;
            if (read(up[0], &b, 1) != 1) return 4;
            usleep(300000);
            printf("resume=%c closed_while_alive=%d\n", b, closed(&lgate));
            if (write(down[1], "x", 1) != 1) return 5;
            waitpid(c, NULL, 0);
        }
        for (int i = 0; i < 300 && closed(&lgate); i++) usleep(10000);
        printf("closed_after_exit=%d\n", closed(&lgate));
        return 0;
    }
    if (argc > 1 && (!strcmp(argv[1], "leasebusy") || !strcmp(argv[1], "resumefail") || !strcmp(argv[1], "releasefail"))) {
        /* leasebusy: the holder exits while the state lock is busy.
         * resumefail: the holder's "resume" fails on the busy state lock,
         * then the holder exits while the lock is still busy. releasefail:
         * the same with a "release" refused on the busy lock (nothing was
         * released). Each way the gates must open once the lock is free. */
        const char *cmd2 = !strcmp(argv[1], "resumefail") ? "resume" : !strcmp(argv[1], "releasefail") ? "release" : NULL;
        int resume = cmd2 != NULL;
        start_ctl();
        int up[2], down[2];
        if (pipe(up) || pipe(down)) return 3;
        pid_t c = fork();
        if (!c) {
            char b, rr[256] = "";
            int ok = request(getppid(), "hold", -1, rep, sizeof(rep), NULL) == 0 && !strcmp(rep, "ok held");
            if (write(up[1], ok ? "1" : "0", 1) != 1 || read(down[0], &b, 1) != 1) _exit(1);
            if (resume) {
                request(getppid(), cmd2, -1, rr, sizeof(rr), NULL);
                if (write(up[1], rr[0] == 'o' ? "o" : "e", 1) != 1) _exit(1);
            }
            _exit(0);
        }
        char b = 0, rb = '-';
        if (read(up[0], &b, 1) != 1) return 4;
        pthread_mutex_lock(&mu);
        if (write(down[1], "x", 1) != 1) return 5;
        if (resume && read(up[0], &rb, 1) != 1) return 6;
        waitpid(c, NULL, 0);
        usleep(2600000);  /* lock_ctl gives up after 2 s */
        printf("hold=%c resume=%c closed_while_locked=%d\n", b, rb, closed(&lgate));
        pthread_mutex_unlock(&mu);
        for (int i = 0; i < 300 && closed(&lgate); i++) usleep(10000);
        printf("closed_after_unlock=%d\n", closed(&lgate));
        return 0;
    }
    if (argc > 1 && !strcmp(argv[1], "batch")) {
        /* cuGetProcAddress("cuMemcpyBatchAsync") returns the CUDA 12
         * function or, for CUDA 13, _v2: each gets its own wrapper. */
        void *v1 = cuda_sym("cuMemcpyBatchAsync"), *v2 = cuda_sym("cuMemcpyBatchAsync_v2");
        printf("v1=%d v2=%d\n", wrapper_for("cuMemcpyBatchAsync", v1, 0, 0) == (void *)w_cuMemcpyBatchAsync,
               wrapper_for("cuMemcpyBatchAsync", v2, 0, 0) == (void *)w_cuMemcpyBatchAsync_v2);
        batch_fn = wrapper_for("cuMemcpyBatchAsync", v1, 0, 0);
        mgate.closed = 1;
        pthread_t t;
        pthread_create(&t, NULL, batcher, NULL);
        wait_waiting(&mgate, 1);
        printf("copied_while_closed=%d\n", __atomic_load_n(&stub_batches, __ATOMIC_SEQ_CST));
        gate_open();
        pthread_join(t, NULL);
        printf("copied_after_open=%d\n", stub_batches);
        return 0;
    }
    if (argc > 1 && !strcmp(argv[1], "lookup")) {
        /* cuGetProcAddress returns a function no exported name has: the
         * wrapper is the one of the version and stream asked for. CUDA 13
         * has another cuMemcpyBatchAsync (no failIdx); CUDA 12.2 another
         * cuMemPrefetchAsync. */
        enum { PT = CU_GET_PROC_ADDRESS_PER_THREAD_DEFAULT_STREAM };
        struct { const char *name; int ver, flags; void *want; } c[] = {
            { "cuMemcpyBatchAsync", 13000, PT, (void *)w_cuMemcpyBatchAsync_v2_ptsz },
            { "cuMemcpyBatchAsync", 13000, 0, (void *)w_cuMemcpyBatchAsync_v2 },
            { "cuMemcpyBatchAsync", 12080, PT, (void *)w_cuMemcpyBatchAsync_ptsz },
            { "cuMemcpyBatchAsync", 12080, 0, (void *)w_cuMemcpyBatchAsync },
            { "cuMemPrefetchAsync", 12020, PT, (void *)w_cuMemPrefetchAsync_v2_ptsz },
            { "cuMemPrefetchAsync", 12000, 0, (void *)w_cuMemPrefetchAsync },
            { "cuLaunchKernel", 13000, PT, (void *)w_cuLaunchKernel_ptsz },
            { "cuMemcpy", 13000, PT, (void *)w_cuMemcpy_ptds },
        };
        for (size_t i = 0; i < sizeof(c) / sizeof(c[0]); i++) {
            void *p = NULL;
            w_gpa2(c[i].name, &p, c[i].ver, c[i].flags, NULL);
            printf("%s@%d/%d=%d\n", c[i].name, c[i].ver, c[i].flags, p == c[i].want);
        }
        /* The wrapper calls the function the lookup returned, with the
         * CUDA 13 arguments. */
        void *p = NULL;
        w_gpa2("cuMemcpyBatchAsync", &p, 13000, PT, NULL);
        int before = stub_batches;
        ((CUresult (*)(CUdeviceptr *, CUdeviceptr *, size_t *, size_t, CUmemcpyAttributes *, size_t *, size_t,
                       CUstream))p)(NULL, NULL, NULL, 0, NULL, NULL, 0, NULL);
        printf("called=%d\n", stub_batches - before);
        return 0;
    }
    return 2;
}
"""


class GateSync(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not shutil.which("gcc") or not os.path.exists(os.path.join(CUDA_INC, "cuda.h")):
            raise unittest.SkipTest("needs gcc and cuda.h (CUDA_INC)")
        cls.tmp = tempfile.mkdtemp()
        for name, text in (("stub.c", STUB), ("probe.c", PROBE)):
            with open(os.path.join(cls.tmp, name), "w") as f:
                f.write(text)
        subprocess.run(["gcc", "-shared", "-fPIC", "-o", os.path.join(cls.tmp, "libcuda.so.1"),
                        os.path.join(cls.tmp, "stub.c")], check=True)
        subprocess.run(["gcc", "-O2", "-I", SRC, "-I", CUDA_INC, "-o", os.path.join(cls.tmp, "probe"),
                        os.path.join(cls.tmp, "probe.c"), os.path.join(cls.tmp, "libcuda.so.1"),
                        "-Wl,-rpath," + cls.tmp, "-ldl", "-lpthread"], check=True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def probe(self, case, sync_ms=0, **env):
        p = subprocess.run([os.path.join(self.tmp, "probe"), case], capture_output=True, text=True, timeout=60,
                           env=dict(os.environ, SYNC_MS=str(sync_ms), **env))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        return dict(kv.split("=", 1) for line in p.stdout.split("\n") for kv in line.split() if "=" in kv)

    def test_untaken_permit_is_busy_and_revoked(self):
        out = self.probe("permit")
        self.assertEqual(out["drained"], "0", out)
        self.assertEqual(out["quiesce"], "ok", out)
        self.assertEqual(out["pass"], "0", out)
        self.assertEqual(out["passed_after_quiesce"], "0", "a launch passed the gate after quiesce")
        self.assertEqual(out["passed_after_open"], "1", out)

    def test_permit_taken_during_the_drain_check_is_busy(self):
        out = self.probe("permitrace")
        self.assertEqual(out["inflight"], "1", out)
        self.assertEqual(out["drained"], "0", "a launch let through during the drain check was reported drained")

    def test_sync_finished_between_polls_is_used(self):
        out = self.probe("between", sync_ms=5)
        self.assertGreater(int(out["idle_polls"]), 0, "every poll reported busy on a drained GPU")

    def test_quiesce_syncs_again(self):
        out = self.probe("fresh", sync_ms=1)
        self.assertEqual(out["quiesce"], "ok", out)
        self.assertGreaterEqual(int(out["syncs"]), 2, out)

    def test_busy_quiesce_keeps_held_launches_held(self):
        out = self.probe("busy", sync_ms=2500)
        self.assertEqual(out["unheld"], "busy:0:0", "without a hold, a busy quiesce opens the gates")
        self.assertEqual(out["held"], "busy:1:0", "a busy quiesce let held launches run ahead of their peers")

    def test_gates_open_when_the_holder_exits(self):
        out = self.probe("lease")
        self.assertEqual(out["hold"], "1", out)
        self.assertEqual(out["closed_while_alive"], "1", out)
        self.assertEqual(out["closed_after_exit"], "0", "launches stayed held after the tool that held them exited")

    def test_open_that_opened_nothing_keeps_the_lease(self):
        out = self.probe("leaseopen")
        self.assertEqual(out["hold"], "1", out)
        self.assertEqual(out["closed_while_alive"], "1", out)
        self.assertEqual(out["closed_after_exit"], "0", "an open that opened nothing ended the lease: launches stayed held")

    def test_gates_open_when_the_tool_that_leased_a_resume_hold_exits(self):
        out = self.probe("resumelease")
        self.assertEqual(out["lease"], "1", out)
        self.assertEqual(out["closed_while_alive"], "1", out)
        self.assertEqual(out["closed_after_exit"], "0",
                         "launches held by \"resume hold\" stayed held after the tool that would open them exited")

    def test_held_resume_takes_the_lease_of_its_sender(self):
        out = self.probe("holdlease")
        self.assertEqual(out["resume"], "1", out)
        self.assertEqual(out["closed_while_alive"], "1", "the held resume let the launches through")
        self.assertEqual(out["closed_after_exit"], "0", "the tool that sent the held resume exited: launches stayed held")

    def test_command_that_is_not_release_keeps_the_lease(self):
        out = self.probe("leasenotrelease")
        self.assertEqual((out["hold"], out["closed_while_alive"]), ("1", "1"), out)
        self.assertEqual(out["closed_after_exit"], "0", "an unknown command starting with release ended the lease")

    def test_no_lease_never_leaves_launches_held(self):
        # A held resume that cannot take its lease resumes without the hold,
        # and says so ("no lease"); "lease" and "hold" refuse. Each way the
        # launches run while the sender lives.
        for case, reply in (("nolease_resume", "n"), ("nolease_lease", "e"), ("nolease_hold", "e")):
            out = self.probe(case, FAIL_PIDFD="1")
            self.assertEqual((out["reply"], out["closed_while_alive"]), (reply, "0"),
                             "%s: launches held with no lease (reply %s): %s" % (case, out["reply"], out))

    def test_held_resume_from_a_sender_already_gone_opens(self):
        out = self.probe("holdleasegone")
        self.assertEqual(out["closed_while_locked"], "1", out)
        self.assertEqual(out["closed_after_exit"], "0", "a held resume whose sender was gone left the launches held")

    def test_open_ends_the_lease(self):
        out = self.probe("resumeleaseopen")
        self.assertEqual(out["lease"], "1", out)
        self.assertEqual(out["closed_after_exit"], "1", "a lease outlived the open that ended it and opened a later hold")

    def test_gates_open_when_the_holder_exits_on_a_busy_lock(self):
        out = self.probe("leasebusy")
        self.assertEqual(out["hold"], "1", out)
        self.assertEqual(out["closed_while_locked"], "1", out)
        self.assertEqual(out["closed_after_unlock"], "0", "launches stayed held after the state lock was free again")

    def test_failed_resume_keeps_the_lease(self):
        out = self.probe("resumefail")
        self.assertEqual(out["hold"], "1", out)
        self.assertEqual(out["resume"], "e", "resume was expected to fail on the busy state lock: %s" % out)
        self.assertEqual(out["closed_after_unlock"], "0", "a failed resume ended the lease: launches stayed held")

    def test_refused_release_keeps_the_lease(self):
        out = self.probe("releasefail")
        self.assertEqual(out["hold"], "1", out)
        self.assertEqual(out["resume"], "e", "release was expected to be refused on the busy state lock: %s" % out)
        self.assertEqual(out["closed_after_unlock"], "0", "a refused release ended the lease: launches stayed held")

    def test_batched_copy_waits_at_the_memory_gate(self):
        out = self.probe("batch")
        self.assertEqual((out["v1"], out["v2"]), ("1", "1"), out)
        self.assertEqual(out["copied_while_closed"], "0", "a batched copy passed the closed memory gate")
        self.assertEqual(out["copied_after_open"], "1", out)

    def test_lookup_gets_the_wrapper_of_the_version_asked_for(self):
        out = self.probe("lookup")
        lookups = {k: v for k, v in out.items() if "@" in k}
        self.assertEqual(len(lookups), 8, out)
        self.assertEqual([k for k, v in lookups.items() if v != "1"], [], out)
        self.assertEqual(out["called"], "1", out)


if __name__ == "__main__":
    unittest.main(verbosity=2)
