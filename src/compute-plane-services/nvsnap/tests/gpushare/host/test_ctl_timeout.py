#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""A control socket that does not accept stops neither nvsnap-gpu-suspend
nor libnvsnap_gpushare for good.

Each process's control thread serves one request at a time. While it is busy
(waiting on another process's control thread, say), connections to it queue,
and once its accept queue is full connect() waits for room: forever, unless
the socket has a send timeout. A control thread stuck there never serves the
"resume" that would reopen its gate.

No GPU: the library runs on a stub libcuda, and a fake peer binds a control
socket name, listens with no room for a second pending connection and never
accepts. The test fills the queue, then checks that

  * nvsnap-gpu-suspend "open" (10 s reply timeout) gives up on the peer;
  * the library's peer lookup in cuMemImportFromShareableHandle gives up on it
    (30 s) and the import returns.

Run: python3 -I tests/gpushare/host/test_ctl_timeout.py
(CUDA_INC names the directory with cuda.h; default /usr/local/cuda/include.)
"""

import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.normpath(os.path.join(HERE, "../../../docker/agent/gpushare"))
CUDA_INC = os.environ.get("CUDA_INC", "/usr/local/cuda/include")

STUB = r"""
typedef int CUresult;
CUresult cuInit(unsigned f) { (void)f; return 0; }
CUresult cuGetErrorName(CUresult r, const char **s) { (void)r; *s = "stub"; return 0; }
CUresult cuGetErrorString(CUresult r, const char **s) { (void)r; *s = "stub"; return 0; }
CUresult cuMemImportFromShareableHandle(unsigned long long *h, void *os, int type)
{
    (void)os; (void)type;
    *h = 1;
    return 0;
}
#define STUB_FN(n) CUresult n(void) { return 0; }
STUB_FN(cuCheckpointProcessLock)
STUB_FN(cuCheckpointProcessCheckpoint)
STUB_FN(cuCheckpointProcessRestore)
STUB_FN(cuCheckpointProcessUnlock)
STUB_FN(cuCheckpointProcessGetState)
STUB_FN(cuCheckpointProcessGetRestoreThreadId)
"""

# Imports a file descriptor the way NCCL and vLLM do: the library then asks
# every process with a control socket whose export it is.
IMPORTER = r"""
#include <cuda.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
int main(void)
{
    int fd = dup(0);
    CUmemGenericAllocationHandle h;
    CUresult r = cuMemImportFromShareableHandle(&h, (void *)(uintptr_t)fd,
                                                CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR);
    printf("import returned %d\n", (int)r);
    return r != CUDA_SUCCESS;
}
"""

# Binds pid's control socket name, leaves room for one pending connection
# and never accepts.
PEER = r"""
import os, socket, time
s = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
s.bind("\0nvsnap-gpushare.%d" % os.getpid())
s.listen(0)
print("ready", flush=True)
time.sleep(3600)
"""


def build(tmp):
    def cc(*args):
        subprocess.run(["gcc", "-O2", "-Wall", "-Wextra", "-Werror", "-I", SRC, "-I", CUDA_INC, *args], check=True)
    with open(os.path.join(tmp, "stub.c"), "w") as f:
        f.write(STUB)
    with open(os.path.join(tmp, "importer.c"), "w") as f:
        f.write(IMPORTER)
    cc("-shared", "-fPIC", "-o", os.path.join(tmp, "libcuda.so.1"), os.path.join(tmp, "stub.c"))
    os.symlink("libcuda.so.1", os.path.join(tmp, "libcuda.so"))
    cc("-fpatchable-function-entry=4", "-shared", "-fPIC", "-o", os.path.join(tmp, "libnvsnap_gpushare.so"),
       os.path.join(SRC, "gpushare.c"), "-ldl", "-lpthread")
    cc("-o", os.path.join(tmp, "nvsnap-gpu-suspend"), os.path.join(SRC, "nvsnap-gpu-suspend.c"), "-ldl", "-lpthread")
    cc("-o", os.path.join(tmp, "importer"), os.path.join(tmp, "importer.c"), "-L", tmp, "-lcuda")


class ControlSocketNotAccepting(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not shutil.which("gcc") or not os.path.exists(os.path.join(CUDA_INC, "cuda.h")):
            raise unittest.SkipTest("needs gcc and cuda.h (CUDA_INC)")
        cls.tmp = tempfile.mkdtemp()
        build(cls.tmp)
        cls.env = dict(os.environ, LD_LIBRARY_PATH=cls.tmp)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def setUp(self):
        p = subprocess.Popen([sys.executable, "-I", "-c", PEER], stdout=subprocess.PIPE, text=True)
        self.addCleanup(p.stdout.close)
        self.addCleanup(p.wait)
        self.addCleanup(p.kill)
        if p.stdout.readline().strip() != "ready":
            self.fail("fake peer did not start")
        self.pid = p.pid
        # Fill the accept queue: the next connect() has to wait for room.
        self.filler = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        self.addCleanup(self.filler.close)
        self.filler.connect("\0nvsnap-gpushare.%d" % self.pid)
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        probe.setblocking(False)
        with self.assertRaises(BlockingIOError, msg="the accept queue is not full"):
            probe.connect("\0nvsnap-gpushare.%d" % self.pid)
        probe.close()

    def test_tool_gives_up_on_a_full_accept_queue(self):
        t0 = time.monotonic()
        p = subprocess.run([os.path.join(self.tmp, "nvsnap-gpu-suspend"), "open", str(self.pid)],
                           env=self.env, capture_output=True, text=True, timeout=60)
        took = time.monotonic() - t0
        self.assertNotEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("control socket not accepting", p.stdout, p.stdout + p.stderr)
        self.assertLess(took, 20, "open waited %.1f s" % took)

    def test_library_gives_up_on_a_full_accept_queue(self):
        t0 = time.monotonic()
        with open(os.devnull) as null:
            p = subprocess.run([os.path.join(self.tmp, "importer")], stdin=null, capture_output=True, text=True,
                               env=dict(self.env, LD_PRELOAD=os.path.join(self.tmp, "libnvsnap_gpushare.so")),
                               timeout=90)
        took = time.monotonic() - t0
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("import returned 0", p.stdout)
        self.assertLess(took, 45, "the import waited %.1f s" % took)


if __name__ == "__main__":
    unittest.main(verbosity=2)
