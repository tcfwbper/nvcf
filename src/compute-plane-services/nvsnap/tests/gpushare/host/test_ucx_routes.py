#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""UCX route checks ask the kernel once a process changes network namespace.

UCX reads the routing table once per process; a process restored into
another pod keeps the old pod's routes. No GPU and no UCX: stub libraries
named like NIXL's bundled UCX (libucs, libucp, and a transport that calls
libucs's route functions, as libuct_ib does) are loaded RTLD_LOCAL by a
probe under the library. The probe calls the route functions, moves into a
new network namespace (as a restore into another pod does) and calls them
again. The test checks that

  * in the namespace of the first call, UCX's own functions answer;
  * in a new one, the routes are read again, from every table (loopback is
    local, a TEST-NET address has no route, and one with a route only in
    table 200, as the RoCE interfaces in a pod have, has one), for UCX 1.22;
  * any other UCX version keeps UCX's answers;
  * a route the kernel lists after more than 1,024 others still counts;
  * a dump the kernel marks interrupted (the tables changed during it) is
    read again, and is never used: a partial one could miss the longest
    prefix (RouteDumpInterrupted, no root needed).

Needs root, or passwordless sudo, for the network namespace.
Run: python3 -I tests/gpushare/host/test_ucx_routes.py
(CUDA_INC names the directory with cuda.h; default /usr/local/cuda/include.)
"""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from test_mx_host import CUDA_INC, SRC, build  # noqa: E402

# UCX's answers: route length 99 and local interface 42, which the kernel never gives.
UCS = r"""
struct sockaddr;
int ucs_netlink_route_exists(int i, const struct sockaddr *s, int *len) { (void)i; (void)s; if (len) *len = 99; return 1; }
int ucs_netlink_is_best_route(int i, const struct sockaddr *s) { (void)i; (void)s; return 1; }
int ucs_netlink_get_local_route_ndev_index(const struct sockaddr *s) { (void)s; return 42; }
"""

UCP = r"""
#include <stdlib.h>
void ucp_get_version(unsigned *a, unsigned *b, unsigned *c)
{
    *a = 1; *b = atoi(getenv("UCX_MINOR")); *c = 0;
}
"""

UCT = r"""
#include <arpa/inet.h>
#include <stdio.h>
int ucs_netlink_route_exists(int, const struct sockaddr *, int *);
int ucs_netlink_is_best_route(int, const struct sockaddr *);
int ucs_netlink_get_local_route_ndev_index(const struct sockaddr *);
void probe(const char *tag)
{
    struct sockaddr_in lo = { .sin_family = AF_INET }, far = lo, tbl = lo;
    int len = -1, tlen = -1;
    inet_pton(AF_INET, "127.0.0.1", &lo.sin_addr);
    inet_pton(AF_INET, "192.0.2.1", &far.sin_addr);
    inet_pton(AF_INET, "198.51.100.7", &tbl.sin_addr);
    int ok = ucs_netlink_route_exists(1, (struct sockaddr *)&lo, &len);
    int tok = ucs_netlink_route_exists(1, (struct sockaddr *)&tbl, &tlen);
    printf("%s_lo=%d %s_lo_len=%d %s_far=%d %s_best=%d %s_local=%d %s_tbl=%d %s_tbl_len=%d\n", tag, ok, tag, len,
           tag, ucs_netlink_route_exists(1, (struct sockaddr *)&far, NULL), tag,
           ucs_netlink_is_best_route(1, (struct sockaddr *)&lo), tag,
           ucs_netlink_get_local_route_ndev_index((struct sockaddr *)&lo), tag, tok, tag, tlen);
}
"""

PROBE = r"""
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <dlfcn.h>
#include <linux/rtnetlink.h>
#include <net/if.h>
#include <sched.h>
#include <stdio.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <unistd.h>
/* DST/24 dev lo, in table 200 only. */
static int add_table_route(const char *dst_s)
{
    struct { struct nlmsghdr h; struct rtmsg r; char a[64]; } q = {
        { NLMSG_LENGTH(sizeof(struct rtmsg)), RTM_NEWROUTE, NLM_F_REQUEST | NLM_F_CREATE | NLM_F_ACK, 1, 0 },
        { .rtm_family = AF_INET, .rtm_dst_len = 24, .rtm_table = 200, .rtm_protocol = RTPROT_STATIC,
          .rtm_scope = RT_SCOPE_LINK, .rtm_type = RTN_UNICAST }, { 0 } };
    struct in_addr dst;
    int oif = 1, s = socket(AF_NETLINK, SOCK_RAW, NETLINK_ROUTE);
    char ack[256];
    inet_pton(AF_INET, dst_s, &dst);
    struct rtattr *a = (struct rtattr *)((char *)&q + NLMSG_ALIGN(q.h.nlmsg_len));
    a->rta_type = RTA_DST; a->rta_len = RTA_LENGTH(4); memcpy(RTA_DATA(a), &dst, 4);
    q.h.nlmsg_len = NLMSG_ALIGN(q.h.nlmsg_len) + RTA_ALIGN(a->rta_len);
    a = (struct rtattr *)((char *)&q + q.h.nlmsg_len);
    a->rta_type = RTA_OIF; a->rta_len = RTA_LENGTH(4); memcpy(RTA_DATA(a), &oif, 4);
    q.h.nlmsg_len += RTA_ALIGN(a->rta_len);
    if (s < 0 || send(s, &q, q.h.nlmsg_len, 0) < 0 || recv(s, ack, sizeof(ack), 0) < 0) return -1;
    close(s);
    return ((struct nlmsgerr *)NLMSG_DATA((struct nlmsghdr *)ack))->error;
}

int main(int argc, char **argv)
{
    void *ucp = dlopen(argv[1], RTLD_NOW | RTLD_LOCAL), *uct = dlopen(argv[2], RTLD_NOW | RTLD_LOCAL);
    void (*probe)(const char *) = uct ? (void (*)(const char *))dlsym(uct, "probe") : NULL;
    if (!ucp || !probe) { fprintf(stderr, "%s\n", dlerror()); return 2; }
    probe("first");
    if (argc > 3) {
        struct ifreq ifr;
        int s;
        if (unshare(CLONE_NEWNET)) { perror("unshare"); return 2; }
        s = socket(AF_INET, SOCK_DGRAM, 0);
        memset(&ifr, 0, sizeof(ifr));
        strcpy(ifr.ifr_name, "lo");
        ioctl(s, SIOCGIFFLAGS, &ifr);
        ifr.ifr_flags |= IFF_UP;
        if (ioctl(s, SIOCSIFFLAGS, &ifr)) { perror("lo up"); return 2; }
        close(s);
        /* "many": 1,100 routes listed before the one the probe looks up. */
        for (int i = 0; argc > 4 && i < 1100; i++) {
            char d[32];
            snprintf(d, sizeof(d), "10.%d.%d.0", i / 256, i % 256);
            if (add_table_route(d)) { fprintf(stderr, "route %s not added\n", d); return 2; }
        }
        if (add_table_route("198.51.100.0")) { fprintf(stderr, "route in table 200 not added\n"); return 2; }
    }
    probe("then");
    fflush(stdout);
    return 0;
}
"""


class UcxRoutes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not shutil.which("gcc") or not os.path.exists(os.path.join(CUDA_INC, "cuda.h")):
            raise unittest.SkipTest("needs gcc and cuda.h (CUDA_INC)")
        if os.geteuid() == 0:
            cls.root = []
        elif shutil.which("sudo") and subprocess.run(["sudo", "-n", "true"], capture_output=True).returncode == 0:
            cls.root = ["sudo", "-n"]
        else:
            raise unittest.SkipTest("needs root or passwordless sudo (network namespace)")
        cls.tmp = tempfile.mkdtemp()
        build(cls.tmp)

        def lib(name, src, *extra):
            c = os.path.join(cls.tmp, name + ".c")
            with open(c, "w") as f:
                f.write(src)
            out = os.path.join(cls.tmp, name)
            subprocess.run(["gcc", "-O2", "-Wall", "-Werror", "-shared", "-fPIC", "-o", out, c, *extra], check=True)
            return out

        # Named as NIXL bundles them.
        cls.ucs = lib("libucs-1234abcd.so.0", UCS)
        cls.ucp = lib("libucp-1234abcd.so.0", UCP)
        cls.uct = lib("libuct_ib.so.0", UCT, cls.ucs, "-Wl,-rpath," + cls.tmp)
        with open(os.path.join(cls.tmp, "probe.c"), "w") as f:
            f.write(PROBE)
        cls.probe_bin = os.path.join(cls.tmp, "probe")
        subprocess.run(["gcc", "-O2", "-Wall", "-Werror", "-o", cls.probe_bin, os.path.join(cls.tmp, "probe.c"),
                        "-ldl"], check=True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def run_probe(self, minor, new_netns, many=False):
        cmd = [*self.root, "env", "LD_PRELOAD=" + os.path.join(self.tmp, "libnvsnap_gpushare.so"),
               "LD_LIBRARY_PATH=" + self.tmp, "UCX_MINOR=%d" % minor,
               self.probe_bin, self.ucp, self.uct] + (["newns"] if new_netns else []) + (["many"] if many else [])
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        out = dict(kv.split("=", 1) for kv in p.stdout.split())
        return out, p.stderr

    def assert_ucx(self, out, tag):
        self.assertEqual((out[tag + "_lo"], out[tag + "_lo_len"], out[tag + "_local"]), ("1", "99", "42"), out)

    def test_same_namespace_ucx_answers(self):
        out, _ = self.run_probe(22, False)
        self.assert_ucx(out, "first")
        self.assert_ucx(out, "then")

    def test_new_namespace_reads_routes_again(self):
        out, err = self.run_probe(22, True)
        self.assert_ucx(out, "first")
        self.assertEqual(out["then_lo"], "1", out)
        self.assertEqual(out["then_lo_len"], "32", out)
        self.assertEqual(out["then_far"], "0", out)
        self.assertEqual(out["then_best"], "1", out)
        self.assertEqual(out["then_local"], "1", out)  # lo
        self.assertEqual((out["then_tbl"], out["then_tbl_len"]), ("1", "24"), out)
        self.assertIn("UCX route checks read the routes again", err)

    def test_route_after_many_others_counts(self):
        out, _ = self.run_probe(22, True, many=True)
        self.assertEqual((out["then_tbl"], out["then_tbl_len"]), ("1", "24"), out)
        self.assertEqual((out["then_lo"], out["then_lo_len"], out["then_local"]), ("1", "32", "1"), out)

    def test_other_ucx_version_keeps_ucx_answers(self):
        out, err = self.run_probe(21, True)
        self.assert_ucx(out, "then")
        self.assertIn("is not 1.22", err)


# Calls the route lookup in the library's own code, with recv wrapped: the
# first N dumps (argv[1]) come back marked interrupted, as the kernel marks
# a dump the routing tables changed under.
INTR_PROBE = r"""
#define _GNU_SOURCE
#include <sys/types.h>
#include <sys/socket.h>
static ssize_t probe_recv(int fd, void *b, size_t n, int f);
#define recv probe_recv
#include "gpushare.c"
#undef recv

static int intr_left;
static ssize_t probe_recv(int fd, void *b, size_t n, int f)
{
    ssize_t r = recv(fd, b, n, f);
    int dom = 0;
    socklen_t l = sizeof(dom);
    if (r <= 0 || !intr_left || getsockopt(fd, SOL_SOCKET, SO_DOMAIN, &dom, &l) || dom != AF_NETLINK) return r;
    int left = (int)r;
    for (struct nlmsghdr *h = b; NLMSG_OK(h, left); h = NLMSG_NEXT(h, left)) {
        h->nlmsg_flags |= NLM_F_DUMP_INTR;
        if (h->nlmsg_type == NLMSG_DONE) intr_left--;
    }
    return r;
}

int main(int argc, char **argv)
{
    intr_left = argc > 1 ? atoi(argv[1]) : 0;
    struct sockaddr_in lo = { .sin_family = AF_INET, .sin_addr = { htonl(INADDR_LOOPBACK) } };
    int dev = -1, len = rt_len((struct sockaddr *)&lo, -1, RTN_LOCAL, &dev);
    printf("len=%d dev=%d intr_left=%d\n", len, dev, intr_left);
    return 0;
}
"""


class RouteDumpInterrupted(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not shutil.which("gcc") or not os.path.exists(os.path.join(CUDA_INC, "cuda.h")):
            raise unittest.SkipTest("needs gcc and cuda.h (CUDA_INC)")
        cls.tmp = tempfile.mkdtemp()
        build(cls.tmp)
        with open(os.path.join(cls.tmp, "intr.c"), "w") as f:
            f.write(INTR_PROBE)
        cls.probe_bin = os.path.join(cls.tmp, "intr")
        subprocess.run(["gcc", "-O2", "-Wall", "-Werror", "-Wno-unused-function", "-I", SRC, "-I", CUDA_INC,
                        "-o", cls.probe_bin, os.path.join(cls.tmp, "intr.c"), os.path.join(cls.tmp, "libcuda.so.1"),
                        "-Wl,-rpath," + cls.tmp, "-ldl", "-lpthread"], check=True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def lookup(self, interrupted):
        p = subprocess.run([self.probe_bin, str(interrupted)], capture_output=True, text=True, timeout=60)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        return dict(kv.split("=", 1) for kv in p.stdout.split())

    def test_interrupted_dump_is_read_again(self):
        self.assertEqual(self.lookup(0), {"len": "32", "dev": "1", "intr_left": "0"})  # 127.0.0.1, local, on lo
        self.assertEqual(self.lookup(1), {"len": "32", "dev": "1", "intr_left": "0"})

    def test_dump_interrupted_every_time_is_not_used(self):
        out = self.lookup(100)
        self.assertEqual(out["len"], "-1", "an interrupted dump's partial result was used: %s" % out)
        self.assertEqual(out["intr_left"], "97", "the dump was not read 3 times: %s" % out)


if __name__ == "__main__":
    unittest.main(verbosity=2)
