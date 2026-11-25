#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""dlsym(RTLD_NEXT) still finds the next definition after its caller.

The library exports dlsym. glibc resolves RTLD_NEXT relative to the object
that called it, so a wrapper that calls the real dlsym and then returns
makes every RTLD_NEXT lookup relative to this library. A library preloaded
after it that interposes a function and looks up the next one gets its own
function back, and loops.

No GPU: a stub libcuda, this library, then an interposer preloaded after
it. The test checks that the interposer's dlsym(RTLD_NEXT, ...) returns the
next definition, not its own, for

  * a libc function (setenv);
  * a CUDA function this library wraps (cuMemFree_v2): the stub's, unwrapped
    (its address is in libcuda.so.1, under that name; none found fails).

Run: python3 -I tests/gpushare/host/test_dlsym_next.py
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
typedef int CUresult;
CUresult cuInit(unsigned f) { (void)f; return 0; }
CUresult cuMemFree_v2(unsigned long long p) { (void)p; return 0; }
"""

# Preloaded after the library: what RTLD_NEXT gives it for each name.
NEXT = r"""
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int setenv(const char *n, const char *v, int o)
{
    int (*next)(const char *, const char *, int) = (int (*)(const char *, const char *, int))dlsym(RTLD_NEXT, "setenv");
    if (!next || next == setenv) { printf("setenv: %s\n", next ? "self" : "none"); return -1; }
    return next(n, v, o);
}

int cuMemFree_v2(unsigned long long p)
{
    (void)p;
    return 0;
}

/* "next" only for the driver's own definition: cuMemFree_v2 in
 * libcuda.so.1, told by dladdr (dlsym on the driver's handle would return
 * the library's wrapper when it is preloaded). */
void report_cuda(void)
{
    void *next = dlsym(RTLD_NEXT, "cuMemFree_v2");
    Dl_info di;
    const char *f = next && dladdr(next, &di) && di.dli_fname ? di.dli_fname : NULL, *b = f ? strrchr(f, '/') : NULL;
    b = b ? b + 1 : f;
    int driver = b && !strcmp(b, "libcuda.so.1") && di.dli_sname && !strcmp(di.dli_sname, "cuMemFree_v2");
    printf("cuMemFree_v2: %s\n", !next ? "none" : next == (void *)cuMemFree_v2 ? "self" : driver ? "next" : b ? b : "unknown");
}
"""

MAIN = r"""
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>

int main(void)
{
    void (*report_cuda)(void) = (void (*)(void))dlsym(RTLD_DEFAULT, "report_cuda");
    if (setenv("NVSNAP_TEST", "1", 1) == 0 && getenv("NVSNAP_TEST")) printf("setenv: next\n");
    report_cuda();
    return 0;
}
"""


class DlsymNext(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not shutil.which("gcc") or not os.path.exists(os.path.join(CUDA_INC, "cuda.h")):
            raise unittest.SkipTest("needs gcc and cuda.h (CUDA_INC)")
        cls.tmp = tempfile.mkdtemp()

        def cc(*args):
            subprocess.run(["gcc", "-O2", "-Wall", "-Wextra", "-Werror", "-I", SRC, "-I", CUDA_INC, *args], check=True)
        for name, text in (("stub.c", STUB), ("next.c", NEXT), ("main.c", MAIN)):
            with open(os.path.join(cls.tmp, name), "w") as f:
                f.write(text)
        cc("-shared", "-fPIC", "-o", os.path.join(cls.tmp, "libcuda.so.1"), os.path.join(cls.tmp, "stub.c"))
        cc("-fpatchable-function-entry=4", "-shared", "-fPIC", "-o", os.path.join(cls.tmp, "libnvsnap_gpushare.so"),
           os.path.join(SRC, "gpushare.c"), "-ldl", "-lpthread")
        # Kept as a dependency (no --as-needed drop: next.c calls no driver
        # function), so the driver is loaded after it for RTLD_NEXT to find.
        cc("-shared", "-fPIC", "-o", os.path.join(cls.tmp, "libnext.so"), os.path.join(cls.tmp, "next.c"),
           "-L", cls.tmp, "-Wl,--no-as-needed", "-l:libcuda.so.1", "-Wl,--as-needed", "-ldl")
        cc("-o", os.path.join(cls.tmp, "main"), os.path.join(cls.tmp, "main.c"), "-ldl")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def run_main(self, *preload):
        env = dict(os.environ, LD_LIBRARY_PATH=self.tmp,
                   LD_PRELOAD=":".join(os.path.join(self.tmp, p) for p in preload))
        p = subprocess.run([os.path.join(self.tmp, "main")], env=env, capture_output=True, text=True, timeout=30)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        return p.stdout.split("\n")

    def test_without_the_library(self):
        self.assertEqual(self.run_main("libnext.so")[:2], ["setenv: next", "cuMemFree_v2: next"])

    def test_preloaded_after_the_library(self):
        self.assertEqual(self.run_main("libnvsnap_gpushare.so", "libnext.so")[:2],
                         ["setenv: next", "cuMemFree_v2: next"])


if __name__ == "__main__":
    unittest.main()
