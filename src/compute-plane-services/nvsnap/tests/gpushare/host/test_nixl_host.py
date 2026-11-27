#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""nixl-pause and nixl-resume drive vLLM's NIXL KV connector around a capture.

No GPU and no vLLM: a Python process runs the library on a stub libcuda,
with fake vLLM modules shaped like vLLM 0.31's (an engine core whose busy
loop runs on the main thread, one in-process worker, the NixlPullConnector
on both sides). Each fake logs what vLLM would do. The test checks that

  * nixl-pause stops new requests, waits for KV transfers in flight, stops
    the handshake listener and shuts down the worker connector;
  * nixl-resume --host re-creates it under a new engine id, restarts the
    listener on that host and takes requests again;
  * a request held at a paused decode engine past a third of the KV lease
    is prefilled locally;
  * a failed pause leaves the engine serving as before, and the tool exits
    non-zero;
  * a pause whose rollback failed too is not taken for a finished one: the
    next pause is refused until a resume reconciles it;
  * a release the engine started before the pause's deadline is waited
    for, not reported as a failure while it still runs;
  * a process without vLLM answers ok, and a bad host never reaches Python;
  * nixl-pause names the process's UCX, and refuses a UCX other than 1.21 or 1.22
    (restored into another pod, it would keep the old pod's routes);
  * given a GPU process without a control socket (a vLLM API server), the
    tool prints nothing for it and exits 0.

Run: python3 -I tests/gpushare/host/test_nixl_host.py
(CUDA_INC names the directory with cuda.h; default /usr/local/cuda/include.)
"""

import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from test_mx_host import CUDA_INC, build  # noqa: E402
from test_rdma_nccl import SHIM, rdma_defines  # noqa: E402

# argv: LOG MODE. MODE "none": no vLLM; "busy": a KV send is in flight for
# 1 s after the first pause request; "fail": the connector shutdown raises;
# "fail1": it raises on TP worker 1 only; "failresume": the first re-created
# connector fails to register; "stuck": the listener ignores its stop event;
# "partial": TP worker 1's first shutdown raises and the first three
# re-created connectors fail to register; "late": the engine reaches its
# first idle point 59 s after the pause asks, and the shutdown takes 2 s.
# Env: WORKERS (TP workers, all in this process), LEASE, HELD, UCX_LIB (a
# stub libucp to load, as NIXL loads UCX; its version is 1.<UCX_MINOR>.0).
WORKER = r"""
import ctypes, enum, os, queue, sys, threading, time, types
LOG, MODE = sys.argv[1], sys.argv[2]
def log(*a):
    with open(LOG, "a") as f:
        f.write(" ".join(str(x) for x in a) + "\n")

def mod(name, **kw):
    m = types.ModuleType(name)
    m.__dict__.update(kw)
    sys.modules[name] = m
    return m

class PauseState(enum.Enum):
    UNPAUSED = 0
    PAUSED_NEW = 1
    PAUSED_ALL = 2
class RequestStatus(enum.Enum):
    WAITING = 0
    RUNNING = 1
class EngineCoreRequestType(enum.Enum):
    ADD = 0
    WAKEUP = 1

class Executor:
    def shutdown(self, wait):
        pass

FAILED_REGISTER, FAILED_SHUTDOWN = [], []

class Worker:
    def __init__(self, vllm_config, engine_id, kv_cache_config):
        self.idx = vllm_config.idx
        self.engine_id, self.use_host_buffer = engine_id, False
        self._recving_transfers, self._reqs_to_send, self._handshake_futures = {}, {}, {}
        self._handshake_initiation_executor = Executor()
        log("worker", engine_id)
    def register_kv_caches(self, kv):
        if self.engine_id != "e0" and len(FAILED_REGISTER) < {"failresume": 1, "partial": 3}.get(MODE, 0):
            FAILED_REGISTER.append(1)
            raise RuntimeError("register failed")
        self.device_kv_caches = kv
        log("register", kv)
    def shutdown(self):  # vLLM before 0.31, which has no _finish_shutdown
        log("old-shutdown", self.engine_id)
        self._finish_shutdown()
    def _finish_shutdown(self):
        if MODE == "late":
            time.sleep(2)
        if MODE == "fail" or (MODE == "fail1" and self.idx == 1):
            raise RuntimeError("shutdown failed")
        if MODE == "partial" and self.idx == 1 and not FAILED_SHUTDOWN:
            FAILED_SHUTDOWN.append(1)
            raise RuntimeError("shutdown failed")
        log("shutdown", self.engine_id)

if os.environ.get("OLD_VLLM"):
    class OldWorker(Worker):
        def __getattribute__(self, name):
            if name == "_finish_shutdown" and sys._getframe(1).f_code.co_name != "shutdown":
                raise AttributeError(name)
            return object.__getattribute__(self, name)
    Worker = OldWorker

class NixlBaseConnector:
    pass
class NixlPullConnector(NixlBaseConnector):
    pass

class Sched:
    def __init__(self):
        self.engine_id, self.side_channel_host, self.side_channel_port = "e0", "10.0.0.1", 5600
        self._kv_lease_duration = float(os.environ.get("LEASE", "30"))
        self._stop_event, self._nixl_handshake_listener_t = threading.Event(), None
    def listen(self):
        stuck = MODE == "stuck" and self._nixl_handshake_listener_t is None
        t = threading.Thread(target=(lambda: time.sleep(3600)) if stuck else self._stop_event.wait,
                             daemon=True)
        t.start()
        self._nixl_handshake_listener_t = t
        log("listen", self.side_channel_host, self.engine_id)

if MODE != "none":
    cfg = types.SimpleNamespace(engine_id="e0")
    WC = []  # worker side, one per TP worker, as get_kv_transfer_group() returns it
    for i in range(int(os.environ.get("WORKERS", "1"))):
        wc = NixlPullConnector()
        wc.kv_transfer_config, wc.engine_id = types.SimpleNamespace(engine_id="e0"), "e0"
        wc._vllm_config, wc.kv_cache_config = types.SimpleNamespace(idx=i), object()
        wc.connector_worker = Worker(wc._vllm_config, "e0", None)
        wc.connector_worker.register_kv_caches("kv0")
        WC.append(wc)
    CUR = [WC[0]]
    kc = NixlPullConnector()  # scheduler side
    kc.engine_id, kc.connector_scheduler = "e0", Sched()
    def set_meta(meta):
        log("meta", sorted(meta.values()))
        kc.connector_scheduler.listen()
    kc.set_xfer_handshake_metadata_pp_aware = set_meta
    kc.connector_scheduler.listen()

    class Scheduler:
        def __init__(self):
            self._pause_state, self.requests = PauseState.UNPAUSED, {}
        @property
        def pause_state(self):
            return self._pause_state
        def set_pause_state(self, s):
            self._pause_state = s
            log("pause_state", s.name)
        def get_kv_connector(self):
            return kc

    class ModelExecutor:
        def collective_rpc(self, fn, args=()):
            out, err = [], None
            for w in WC:  # like vLLM: every worker runs it; the first error is raised
                CUR[0] = w
                try:
                    out.append(fn(None, *args))
                except Exception as x:
                    err = err or x
            CUR[0] = WC[0]
            if err:
                raise err
            return out
        def get_kv_connector_handshake_metadata(self):
            return [{i: w.connector_worker.engine_id} for i, w in enumerate(WC)]

    class EngineCoreProc:
        def __init__(self):
            self.scheduler, self.model_executor = Scheduler(), ModelExecutor()
            self.vllm_config = types.SimpleNamespace(kv_transfer_config=cfg)
            self._idle_state_callbacks, self.input_queue = [], queue.Queue()
        def run_busy_loop(self):
            print("ready", flush=True)  # the pause finds the engine from here on
            while True:
                try:
                    kind, req = self.input_queue.get(timeout=0.05)
                except queue.Empty:
                    kind = None
                if kind == EngineCoreRequestType.ADD:
                    self.scheduler.requests[req.request_id] = req
                if MODE == "busy" and self._idle_state_callbacks and not hasattr(self, "busy"):
                    self.busy = WC[0].connector_worker._reqs_to_send
                    self.busy["r1"] = 1
                    threading.Timer(1.0, lambda: (log("drained"), self.busy.clear())).start()
                if MODE == "late" and self._idle_state_callbacks and not hasattr(self, "late"):
                    self.late = True
                    time.sleep(59)  # the pause waits 60 s for an idle point
                cbs, self._idle_state_callbacks = self._idle_state_callbacks, []
                for cb in cbs:
                    cb(self)

    st = mod("vllm.distributed.kv_transfer.kv_transfer_state",
             has_kv_transfer_group=lambda: True, get_kv_transfer_group=lambda: CUR[0])
    mod("vllm.distributed.kv_transfer", get_kv_transfer_group=lambda: CUR[0])
    mod("vllm.distributed.kv_transfer.kv_connector.v1.nixl.connector",
        NixlBaseConnector=NixlBaseConnector, NixlPullConnector=NixlPullConnector)
    mod("vllm.v1.engine.core", EngineCoreProc=EngineCoreProc)
    mod("vllm.v1.engine", EngineCoreRequestType=EngineCoreRequestType)
    mod("vllm.v1.core.sched.interface", PauseState=PauseState)
    mod("vllm.v1.request", RequestStatus=RequestStatus)

if os.environ.get("UCX_LIB"):
    ctypes.CDLL(os.environ["UCX_LIB"])
if os.environ.get("SHIM"):  # NCCL's verbs over RDMA, loaded as NCCL loads them
    ctypes.CDLL(os.environ["SHIM"], mode=os.RTLD_LOCAL)
if os.environ.get("FOREIGN"):  # a device file NIXL's shutdown left open
    keep = os.open(os.path.join(os.environ["IBDIR"], "uverbs0"), os.O_RDWR)
h = ctypes.c_ulonglong()
fd = os.open(os.devnull, os.O_RDONLY)
imp = ctypes.CDLL("libcuda.so.1").cuMemImportFromShareableHandle
r = imp(ctypes.byref(h), ctypes.c_void_p(fd), 1)  # starts the library's control thread
if r != 0 or MODE == "none":
    print("ready" if r == 0 else "import failed: %d" % r, flush=True)
if MODE == "none":
    while True:
        time.sleep(0.1)
e = EngineCoreProc()
if os.environ.get("HELD"):  # a decode request waiting to read its KV from prefill
    e.input_queue.put((EngineCoreRequestType.ADD, types.SimpleNamespace(
        request_id="held", status=RequestStatus.WAITING, kv_transfer_params={"do_remote_prefill": True})))
e.run_busy_loop()
"""


class NixlPauseResume(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not shutil.which("gcc") or not os.path.exists(os.path.join(CUDA_INC, "cuda.h")):
            raise unittest.SkipTest("needs gcc and cuda.h (CUDA_INC)")
        cls.tmp = tempfile.mkdtemp()
        # Files here stand for /dev/infiniband's (see test_rdma_nccl.py).
        cls.ib = os.path.join(cls.tmp, "ib")
        os.mkdir(cls.ib)
        with open(os.path.join(cls.ib, "uverbs0"), "wb") as f:
            f.write(b"\0" * 4096)
        build(cls.tmp, *rdma_defines(cls.ib))
        cls.env = dict(os.environ, LD_LIBRARY_PATH=cls.tmp, IBDIR=cls.ib)
        with open(os.path.join(cls.tmp, "shim.c"), "w") as f:
            f.write(SHIM)
        cls.shim = os.path.join(cls.tmp, "libnvsnap_ibv.so")
        subprocess.run(["gcc", "-O2", "-Wall", "-Werror", "-shared", "-fPIC", "-Wl,-soname,libnvsnap_ibv.so",
                        "-o", cls.shim, os.path.join(cls.tmp, "shim.c")], check=True)
        # Named as NIXL bundles it.
        cls.ucp = os.path.join(cls.tmp, "libucp-1234abcd.so.0")
        with open(cls.ucp + ".c", "w") as f:
            f.write("#include <stdlib.h>\n"
                    "void ucp_get_version(unsigned *a, unsigned *b, unsigned *c)\n"
                    "{ *a = 1; *b = atoi(getenv(\"UCX_MINOR\")); *c = 0; }\n")
        subprocess.run(["gcc", "-O2", "-Wall", "-Werror", "-shared", "-fPIC", "-o", cls.ucp, cls.ucp + ".c"],
                       check=True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def start(self, mode="ok", **env):
        self.log = os.path.join(self.tmp, "log.%s" % self._testMethodName)
        env = dict(self.env, LD_PRELOAD=os.path.join(self.tmp, "libnvsnap_gpushare.so"), **env)
        env.pop("VLLM_NIXL_SIDE_CHANNEL_HOST", None)
        p = subprocess.Popen([sys.executable, "-I", "-c", WORKER, self.log, mode], env=env,
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
                              env=self.env, capture_output=True, text=True, timeout=150)

    def events(self):
        with open(self.log) as f:
            return [line.strip() for line in f]

    def ok(self, *args):
        p = self.tool(*args)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn(" ok ", p.stdout, p.stdout)
        return p.stdout

    def test_pause_leaves_nccls_rdma_to_release(self):
        # A TP worker whose NCCL runs over RDMA through libnvsnap_ibv: its
        # device file is still open after the pause, and "release" closes it.
        self.start(SHIM=self.shim)
        self.assertIn("released on 1 worker(s)", self.ok("nixl-pause"))
        self.ok("nixl-resume")

    def test_pause_counts_everything_when_nccls_list_changes(self):
        self.start(SHIM=self.shim, FLIP="1")
        p = self.tool("nixl-pause")
        self.assertNotEqual(p.returncode, 0, p.stdout)
        self.assertIn(", 2, 1)]", p.stdout + p.stderr)

    def test_pause_refuses_other_rdma_beside_nccls(self):
        self.start(SHIM=self.shim, FOREIGN="1")
        p = self.tool("nixl-pause")
        self.assertNotEqual(p.returncode, 0, p.stdout)
        self.assertIn("RDMA device files still open after the release", p.stdout + p.stderr)
        self.assertIn(", 1, 0)]", p.stdout + p.stderr)

    def test_pause_resume_new_engine_id_and_host(self):
        self.start()
        out = self.ok("nixl-pause")
        self.assertIn("released on 1 worker(s)", out)
        ev = self.events()
        self.assertEqual(ev[-2:], ["pause_state PAUSED_NEW", "shutdown e0"])
        out = self.ok("--host", "10.9.8.7", "nixl-resume")
        self.assertIn("side channel 10.9.8.7:5600", out)
        ev = self.events()[len(ev):]
        self.assertTrue(ev[0].startswith("worker ") and ev[0] != "worker e0", ev)
        new_id = ev[0].split()[1]
        self.assertEqual(ev[1:], ["register kv0", "meta ['%s']" % new_id, "listen 10.9.8.7 %s" % new_id,
                                  "pause_state UNPAUSED"])
        self.assertIn("engine %s" % new_id, out)
        # Resume again: nothing to do. Pause and resume without --host: the host stays.
        self.assertIn("not paused", self.ok("nixl-resume"))
        self.ok("nixl-pause")
        self.assertIn("side channel 10.9.8.7:5600", self.ok("nixl-resume"))

    def test_pause_waits_for_transfers(self):
        self.start("busy")
        self.ok("nixl-pause")
        ev = self.events()
        self.assertLess(ev.index("drained"), ev.index("shutdown e0"), ev)

    def test_failed_pause_keeps_serving(self):
        self.start("fail")
        p = self.tool("nixl-pause")
        self.assertNotEqual(p.returncode, 0, p.stdout)
        self.assertIn("shutdown failed", p.stdout)
        self.assertIn("resuming every pid", p.stderr)
        ev = self.events()
        self.assertEqual(ev[-1], "pause_state UNPAUSED", ev)
        self.assertTrue(ev[-2].startswith("listen 10.0.0.1 ") and ev[-2] != "listen 10.0.0.1 e0", ev)

    def test_partial_release_leaves_one_engine_id(self):
        # TP worker 1 fails to shut down: after the rollback both workers must
        # come back under the same new engine id.
        self.start("fail1", WORKERS="2")
        p = self.tool("nixl-pause")
        self.assertNotEqual(p.returncode, 0, p.stdout)
        ev = self.events()
        ids = [e.split()[1] for e in ev[ev.index("pause_state PAUSED_NEW"):] if e.startswith("worker ")]
        self.assertEqual(len(ids), 2, ev)
        self.assertEqual(ids[0], ids[1], ev)
        self.assertNotEqual(ids[0], "e0", ev)
        self.assertEqual(ev[-1], "pause_state UNPAUSED", ev)

    def test_unfinished_pause_is_not_taken_for_a_pause(self):
        # The pause fails on worker 1, and its rollback, and the tool's
        # resume after it, fail to re-create worker 0: the engine is neither
        # paused nor serving. Another pause must not answer "already paused".
        self.start("partial", WORKERS="2")
        p = self.tool("nixl-pause")
        self.assertNotEqual(p.returncode, 0, p.stdout)
        p = self.tool("nixl-pause")
        self.assertNotEqual(p.returncode, 0, "a pause that never finished was taken for one:\n" + p.stdout)
        self.assertIn("did not finish", p.stdout)
        # The tool's resume after that refusal reconciles it.
        self.assertIn("not paused", self.ok("nixl-resume"))
        self.assertIn("released on 2 worker(s)", self.ok("nixl-pause"))
        self.assertIn("resumed as engine", self.ok("nixl-resume"))

    def test_release_started_before_the_deadline_is_waited_for(self):
        # The engine starts the release just before the 60 s deadline and
        # finishes it after: the pause reports what the release did, and no
        # rollback re-creates the connector behind it.
        self.start("late")
        t0 = time.monotonic()
        out = self.ok("nixl-pause")
        self.assertIn("released on 1 worker(s)", out)
        self.assertGreater(time.monotonic() - t0, 60, out)
        ev = self.events()
        self.assertEqual(ev[-2:], ["pause_state PAUSED_NEW", "shutdown e0"], ev)
        self.assertIn("resumed as engine", self.ok("nixl-resume"))

    def test_failed_resume_can_be_retried(self):
        self.start("failresume")
        self.ok("nixl-pause")
        p = self.tool("nixl-resume")
        self.assertNotEqual(p.returncode, 0, p.stdout)
        self.assertIn("register failed", p.stdout)
        self.assertIn("resumed as engine", self.ok("nixl-resume"))
        self.assertEqual(self.events()[-1], "pause_state UNPAUSED")

    def test_lease_counts_the_drain(self):
        # The send in flight holds the pause for 1 s: that counts toward the
        # lease (1.5 s / 3), so the held request is prefilled locally.
        self.start("busy", LEASE="1.5", HELD="1")
        self.ok("nixl-pause")
        self.assertIn("1 request(s) prefilled here", self.ok("nixl-resume"))

    def test_stuck_listener_keeps_serving(self):
        self.start("stuck")
        p = self.tool("nixl-pause")
        self.assertNotEqual(p.returncode, 0, p.stdout)
        self.assertIn("listener did not stop", p.stdout)
        ev = self.events()
        self.assertEqual(ev[-1], "pause_state UNPAUSED", ev)
        self.assertFalse([e for e in ev if e.startswith("shutdown")], ev)

    def test_held_request_prefilled_locally(self):
        self.start(LEASE="0.3", HELD="1")
        self.ok("nixl-pause")
        time.sleep(0.5)  # longer than a third of the lease
        out = self.ok("nixl-resume")
        self.assertIn("1 request(s) prefilled here", out)

    def test_short_pause_keeps_remote_prefill(self):
        self.start(HELD="1")
        self.ok("nixl-pause")
        self.assertIn("0 request(s) prefilled here", self.ok("nixl-resume"))

    def test_pause_names_ucx(self):
        self.start(UCX_LIB=self.ucp, UCX_MINOR="22")
        self.assertIn("released on 1 worker(s), UCX 1.22.0;", self.ok("nixl-pause"))
        self.assertIn("resumed as engine", self.ok("nixl-resume"))

    def test_vllm_without_finish_shutdown(self):
        # vLLM 0.28 (Dynamo 1.5.1): the worker has shutdown() only.
        self.start(OLD_VLLM="1")
        self.ok("nixl-pause")
        ev = self.events()
        self.assertIn("old-shutdown e0", ev)
        self.assertIn("shutdown e0", ev)
        self.assertIn("resumed as engine", self.ok("nixl-resume"))

    def test_pause_accepts_ucx_121(self):
        self.start(UCX_LIB=self.ucp, UCX_MINOR="21")
        self.assertIn("released on 1 worker(s), UCX 1.21.0;", self.ok("nixl-pause"))
        self.assertIn("resumed as engine", self.ok("nixl-resume"))

    def test_pause_refuses_other_ucx(self):
        self.start(UCX_LIB=self.ucp, UCX_MINOR="20")
        p = self.tool("nixl-pause")
        self.assertNotEqual(p.returncode, 0, p.stdout)
        self.assertIn("UCX 1.20.0 is not 1.21 or 1.22", p.stdout)
        self.assertNotIn("pause_state PAUSED_NEW", self.events())  # refused before pausing

    def test_without_vllm(self):
        self.start("none")
        self.assertIn("ok no NIXL KV connector", self.ok("nixl-pause"))
        self.assertIn("ok no NIXL KV connector", self.ok("nixl-resume"))

    def test_bad_host_never_reaches_python(self):
        self.start()
        for bad in ("10.0.0.1'); import os; os._exit(3)  #", "a b", "x" * 201):
            p = self.tool("--host", bad, "nixl-resume")
            self.assertNotEqual(p.returncode, 0, p.stdout)
            self.assertIn("err unknown command", p.stdout, p.stdout + p.stderr)
        p = self.tool("--host", "10.1.2.3", "nixl-pause")
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("--host goes with nixl-resume", p.stderr)
        self.assertIn("not paused", self.ok("nixl-resume"), "the worker died")


# Looks up one driver entry point and allocates nothing, like vLLM's engine
# core at TP > 1, then waits.
LOOKUP = r"""
import ctypes, time
lib = ctypes.CDLL("libcuda.so.1")
p = ctypes.c_void_p()
r = lib.cuGetProcAddress_v2(b"cuInit", ctypes.byref(p), 12000, ctypes.c_ulonglong(0), None)
print("ready" if r == 0 else "lookup failed: %d" % r, flush=True)
while True:
    time.sleep(0.1)
"""


class ControlSocketOnCudaUse(unittest.TestCase):
    """The control socket starts on the first entry-point lookup only with
    NVSNAP_GPUSHARE_CTL_ON_CUDA_USE=1; otherwise nothing changes."""

    @classmethod
    def setUpClass(cls):
        NixlPauseResume.setUpClass.__func__(cls)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def has_socket(self, **extra):
        env = dict(self.env, LD_PRELOAD=os.path.join(self.tmp, "libnvsnap_gpushare.so"))
        env.pop("NVSNAP_GPUSHARE_CTL_ON_CUDA_USE", None)
        env.update(extra)
        p = subprocess.Popen([sys.executable, "-I", "-c", LOOKUP], env=env, stdout=subprocess.PIPE, text=True)
        self.addCleanup(p.stdout.close)
        self.addCleanup(p.wait)
        self.addCleanup(p.kill)
        line = p.stdout.readline().strip()
        if line != "ready":
            self.fail("process did not start: %r" % line)
        time.sleep(0.3)
        with open("/proc/net/unix") as f:
            return "@nvsnap-gpushare.%d" % p.pid in f.read()

    def test_off_by_default(self):
        self.assertFalse(self.has_socket())

    def test_on_with_env(self):
        self.assertTrue(self.has_socket(NVSNAP_GPUSHARE_CTL_ON_CUDA_USE="1"))


class GPUProcessWithoutControlSocket(unittest.TestCase):
    """A vLLM pod's API server maps the library and holds GPU device files
    but never looks up a CUDA entry point, so it has no control socket.
    Given both pids, the tool reaches only the engine: it prints the
    engine's reply, nothing for the API server, and exits 0. The agent
    passes the tool only the pids with a control socket and reads its
    replies by pid (gpushare_nixl.go, controlSocketPIDs)."""

    @classmethod
    def setUpClass(cls):
        NixlPauseResume.setUpClass.__func__(cls)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def test_tool_skips_a_pid_without_a_socket(self):
        engine = NixlPauseResume("test_without_vllm")
        engine.tmp, engine.env = self.tmp, self.env
        engine._testMethodName = self._testMethodName
        self.addCleanup(engine.doCleanups)
        engine.start()
        env = dict(self.env, LD_PRELOAD=os.path.join(self.tmp, "libnvsnap_gpushare.so"))
        env.pop("NVSNAP_GPUSHARE_CTL_ON_CUDA_USE", None)
        api = subprocess.Popen([sys.executable, "-I", "-c", LOOKUP], env=env, stdout=subprocess.PIPE, text=True)
        self.addCleanup(api.stdout.close)
        self.addCleanup(api.wait)
        self.addCleanup(api.kill)
        self.assertEqual(api.stdout.readline().strip(), "ready")
        with open("/proc/net/unix") as f:
            sockets = f.read()
        self.assertIn("@nvsnap-gpushare.%d" % engine.pid, sockets)
        self.assertNotIn("@nvsnap-gpushare.%d" % api.pid, sockets)
        tool = os.path.join(self.tmp, "nvsnap-gpu-suspend")
        for verb, args in (("pause", []), ("resume", ["--host", "10.9.8.7"])):
            p = subprocess.run([tool, *args, "nixl-" + verb, str(engine.pid), str(api.pid)], env=self.env,
                               capture_output=True, text=True, timeout=150)
            self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
            self.assertIn("pid=%d nixl %s" % (engine.pid, verb), p.stdout)
            self.assertNotIn("pid=%d " % api.pid, p.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
