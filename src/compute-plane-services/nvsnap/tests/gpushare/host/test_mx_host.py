#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""mx-resume --host makes ModelExpress advertise the given address.

ModelExpress publishes MX_WORKER_HOST, else the address its sockets route
from, as the address peers pull the weights from. A process restored into
another pod keeps the captured pod's address, so without --host it would
publish an address no other pod can reach.

No GPU and no ModelExpress: a Python process runs the library on a stub
libcuda, with a fake ModelExpress loader whose resume_serving records the
MX_WORKER_HOST it sees. The test checks that

  * mx-resume --host H sets MX_WORKER_HOST to H before resume_serving;
  * mx-resume without --host leaves it as it was;
  * a host that is not an address or DNS name is refused, and never reaches
    Python;
  * --host is refused with mx-pause.

Run: python3 -I tests/gpushare/host/test_mx_host.py
(CUDA_INC names the directory with cuda.h; default /usr/local/cuda/include.)
"""

import os
import shutil
import subprocess
import sys
import tempfile
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
CUresult cuGetProcAddress_v2(const char *s, void **p, int v, unsigned long long f, void *q)
{
    (void)s; (void)v; (void)f; (void)q;
    *p = (void *)cuInit;
    return 0;
}
"""

# A worker with a fake ModelExpress 0.7 loader (one paused context). An
# import starts the library's control thread; the worker then waits for
# mx commands, and resume_serving appends the MX_WORKER_HOST it sees to LOG.
WORKER = r"""
import ctypes, os, sys, time, types
LOG = sys.argv[1]

class Ctx:
    p2p_enabled = True
    nixl_manager = None
    mx_client = None

def pause_serving(ctx):
    ctx.nixl_manager = ctx.mx_client = None

def resume_serving(ctx, model):
    with open(LOG, "a") as f:
        f.write(os.environ.get("MX_WORKER_HOST", "<unset>") + "\n")
    ctx.nixl_manager = object()

lifecycle = types.ModuleType("modelexpress.lifecycle")
lifecycle.pause_serving, lifecycle.resume_serving = pause_serving, resume_serving
loader = types.ModuleType("modelexpress.engines.vllm.loader")
loader._loader_registry = {0: types.SimpleNamespace(_ctx=Ctx())}
loader._nixl_managers = {}
mx = types.ModuleType("modelexpress")
mx.lifecycle = lifecycle
sys.modules.update({"modelexpress": mx, "modelexpress.lifecycle": lifecycle,
                    "modelexpress.engines.vllm.loader": loader})

h = ctypes.c_ulonglong()
fd = os.open(os.devnull, os.O_RDONLY)
# As a CUDA application finds it: dlsym on libcuda, which the library wraps.
imp = ctypes.CDLL("libcuda.so.1").cuMemImportFromShareableHandle
r = imp(ctypes.byref(h), ctypes.c_void_p(fd), 1)  # CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR
print("ready" if r == 0 else "import failed: %d" % r, flush=True)
while True:
    time.sleep(0.1)  # releases the GIL for the control thread
"""


def build(tmp, *defines):
    def cc(*args):
        subprocess.run(["gcc", "-O2", "-Wall", "-Wextra", "-Werror", "-I", SRC, "-I", CUDA_INC, *defines, *args],
                       check=True)
    with open(os.path.join(tmp, "stub.c"), "w") as f:
        f.write(STUB)
    cc("-shared", "-fPIC", "-o", os.path.join(tmp, "libcuda.so.1"), os.path.join(tmp, "stub.c"))
    cc("-fpatchable-function-entry=4", "-shared", "-fPIC", "-o", os.path.join(tmp, "libnvsnap_gpushare.so"),
       os.path.join(SRC, "gpushare.c"), "-ldl", "-lpthread")
    cc("-o", os.path.join(tmp, "nvsnap-gpu-suspend"), os.path.join(SRC, "nvsnap-gpu-suspend.c"), "-ldl", "-lpthread")


class MxResumeHost(unittest.TestCase):
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
        self.log = os.path.join(self.tmp, "hosts.%s" % self._testMethodName)
        env = dict(self.env, LD_PRELOAD=os.path.join(self.tmp, "libnvsnap_gpushare.so"))
        env.pop("MX_WORKER_HOST", None)
        p = subprocess.Popen([sys.executable, "-I", "-c", WORKER, self.log], env=env,
                             stdout=subprocess.PIPE, text=True)
        self.addCleanup(p.stdout.close)
        self.addCleanup(p.wait)
        self.addCleanup(p.kill)
        line = p.stdout.readline().strip()
        if line != "ready":
            self.fail("worker did not start: %r" % line)
        self.pid = p.pid

    def tool(self, *args):
        return subprocess.run([os.path.join(self.tmp, "nvsnap-gpu-suspend"), *args, str(self.pid)],
                              env=self.env, capture_output=True, text=True, timeout=60)

    def hosts(self):
        try:
            with open(self.log) as f:
                return f.read().split()
        except FileNotFoundError:
            return []

    def test_host_reaches_modelexpress(self):
        for args in (["mx-pause"], ["--host", "10.1.2.3", "mx-resume"],
                     ["mx-pause"], ["--host", "fd00::7", "mx-resume"],
                     ["mx-pause"], ["mx-resume"]):
            p = self.tool(*args)
            self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
            self.assertIn(" ok ", p.stdout, p.stdout)
        # The last resume has no --host: the previous one stays.
        self.assertEqual(self.hosts(), ["10.1.2.3", "fd00::7", "fd00::7"])

    def test_without_host_leaves_it_unset(self):
        for action in ("mx-pause", "mx-resume"):
            p = self.tool(action)
            self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertEqual(self.hosts(), ["<unset>"])

    def test_bad_host_never_reaches_python(self):
        self.assertEqual(self.tool("mx-pause").returncode, 0)
        for bad in ("10.0.0.1'); import os; os._exit(3)  #", "a b", "x" * 201):
            p = self.tool("--host", bad, "mx-resume")
            self.assertNotEqual(p.returncode, 0, p.stdout)
            self.assertIn("err unknown command", p.stdout, p.stdout + p.stderr)
        self.assertEqual(self.hosts(), [])
        self.assertEqual(self.tool("mx-resume").returncode, 0, "the worker died")

    def test_host_only_with_resume(self):
        p = self.tool("--host", "10.1.2.3", "mx-pause")
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("--host goes with mx-resume", p.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
