#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""libnvsnap_ibv: NCCL's verbs, destroyed before a checkpoint and created
again after it, on other NICs.

No NIC: a fake libibverbs moves bytes between the queue pairs of one
process, and checks every queue pair number, GID, LID and key it is given
against the live objects. fake_move() makes it another node: a new GID and
LID, and new numbers for every queue pair and registration. A program that
uses verbs as NCCL does (dlopen, dlvsym, the shadow context's ops) writes
with immediate data from one queue pair to the other, then the test

  * suspends: every device is closed, and a completion NCCL had not polled
    yet, and a receive it had posted, survive;
  * moves to the "other node", restores, exchanges the new numbers through
    a fabmap-style file and reconnects;
  * writes again with the numbers and keys NCCL had from before: the bytes
    land, the receive posted before the suspend completes, and the
    completion names NCCL's queue pair number.

A second program (ROLLBACK) runs under libnvsnap_gpushare with the same
library, and nvsnap-gpu-suspend suspends it: the release cannot destroy
one queue pair, and the tool's own rollback (undo, remap_all) must leave
that queue pair reaching its re-created peer. TOOL_SRC names another
nvsnap-gpu-suspend.c to build instead (negative controls).

Run: python3 -I tests/gpushare/host/test_ibv.py
(IBV_INC names the directory with infiniband/verbs.h; default /usr/include.
CUDA_INC names the one with cuda.h; default /usr/local/cuda/include.)
"""

import os
import selectors
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.normpath(os.path.join(HERE, "../../../docker/agent/gpushare"))
IBV_INC = os.environ.get("IBV_INC", "/usr/include")
CUDA_INC = os.environ.get("CUDA_INC", "/usr/local/cuda/include")
sys.path.insert(0, HERE)
from test_rdma_nccl import STUB as CUDA_STUB  # noqa: E402  (libcuda for gpushare's quiesce)

FAKE = r"""
#include <errno.h>
#include <fcntl.h>
#include <infiniband/verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* Two devices that number their keys and queue pairs alike, as NICs do. */
static int epoch, counter[2], open_ctxs;
static struct ibv_device dev0 = { .node_type = IBV_NODE_CA, .transport_type = IBV_TRANSPORT_IB, .name = "fake0" };
static struct ibv_device dev1 = { .node_type = IBV_NODE_CA, .transport_type = IBV_TRANSPORT_IB, .name = "fake1" };
static struct ibv_device *devs[3] = { &dev0, &dev1, NULL };
static int di(struct ibv_context *c) { return c->device == &dev1; }

struct fmr { struct ibv_mr m; int live; };
struct fwc { struct ibv_wc w; };
struct fcq { struct ibv_cq c; struct ibv_wc q[256]; int n; int live; };
struct frecv { uint64_t wr_id; struct ibv_sge sg; };
struct fqp { struct ibv_qp q; uint32_t dest; union ibv_gid dgid; uint16_t dlid; int global; int sgid;
             struct frecv r[64]; int nr; int live; int unacked; };
static struct fmr *mrs[256]; static int n_mrs;
static struct fqp *qps[256]; static int n_qps;

static void gid_of(int e, int d, union ibv_gid *g)
{
    memset(g, 0, sizeof(*g));
    g->raw[10] = g->raw[11] = 0xff;
    g->raw[12] = 10; g->raw[14] = d; g->raw[15] = 1 + e;
}

int fake_open_contexts(void) { return open_ctxs; }
/* The GID table of every port: an index below 16 set here holds the IPv4
 * address (gid_of) with RoCE type kind - 1; every other index below 16
 * holds a link-local address, type RoCE v1; indices from 16 are empty (a
 * zero GID, ENODATA from the type query). As a RoCE NIC: index 3, IPv4
 * RoCE v2. */
static int gkind[16] = { [3] = 1 + IBV_GID_TYPE_ROCE_V2 }, gid_ex_fail;
static int ib_link;  /* the ports' link layer is InfiniBand: every GID is type IB */
/* As the kernel's sysfs, under $FAKE_SYSFS (directories made by the test):
 * <dev>/ports/1/gid_attrs/types/<index> for the 16 entries in use. */
static void sysfs_sync(void)
{
    const char *root = getenv("FAKE_SYSFS");
    for (int d = 0; root && d < 2; d++)
        for (int i = 0; i < 16; i++) {
            char p[512];
            snprintf(p, sizeof(p), "%s/fake%d/ports/1/gid_attrs/types/%d", root, d, i);
            FILE *f = fopen(p, "w");
            if (!f) continue;
            fputs(!ib_link && gkind[i] == 1 + IBV_GID_TYPE_ROCE_V2 ? "RoCE v2\n" : "IB/RoCE v1\n", f);
            fclose(f);
        }
}
void fake_gid(int i, int kind) { gkind[i] = kind; sysfs_sync(); }
void fake_gid_ex_fail(int on) { gid_ex_fail = on; }  /* the GID type query fails */
void fake_ib(int on) { ib_link = on; sysfs_sync(); }
static int gid_queries;
int fake_gid_queries(void) { return gid_queries; }  /* GID table reads, either call */
/* After a move the two NICs number apart: a number looked up under the
 * wrong device is one the right device rejects. */
void fake_move(void) { epoch++; counter[0] = 0; counter[1] = 50; }

/* Knobs for the white-box cases (test_ibv.py PROBE). */
static int port_block, port_in, fail_marker, fail_init, fail_destroy, err_opcode, async_calls, live_pds;
static uint32_t next_lkey;
static struct fqp *event_qp;
/* on: a port query stays in the provider until it is turned off. */
void fake_port_block(int on) { __atomic_store_n(&port_block, on, __ATOMIC_SEQ_CST); }
int fake_port_in(void) { return __atomic_load_n(&port_in, __ATOMIC_SEQ_CST); }  /* port queries in the provider */
void fake_fail_marker(int nth) { fail_marker = nth; }   /* the nth fence write's post fails */
void fake_fail_init(int n) { fail_init = n; }           /* the next n moves to INIT fail */
void fake_fail_destroy(int n) { fail_destroy = n; }     /* the next n queue pair destroys fail */
void fake_err_opcode(int on) { err_opcode = on; }       /* failed completions carry a receive's opcode */
void fake_next_lkey(uint32_t k) { next_lkey = k; }
void fake_raise_qp_event(struct ibv_qp *q) { event_qp = (struct fqp *)q; }
int fake_async_calls(void) { return async_calls; }
int fake_live_pds(void) { return live_pds; }
int fake_qp_state(struct ibv_qp *q) { return q->state; }
int fake_qp_receives(struct ibv_qp *q) { return ((struct fqp *)q)->nr; }
int fake_qp_sgid(struct ibv_qp *q) { return ((struct fqp *)q)->sgid; }

static void push(struct fcq *c, struct ibv_wc w) { if (c->n < 256) c->q[c->n++] = w; }

static int poll_cq(struct ibv_cq *cq, int n, struct ibv_wc *wc)
{
    struct fcq *c = (struct fcq *)cq;
    int k = c->n < n ? c->n : n;
    memcpy(wc, c->q, k * sizeof(*wc));
    memmove(c->q, c->q + k, (c->n - k) * sizeof(*wc));
    c->n -= k;
    return k;
}

/* A key of device c's (queue pairs here connect within one device). */
static struct fmr *mr_by(uint32_t key, int remote, struct ibv_context *c)
{
    for (int i = 0; i < n_mrs; i++)
        if (mrs[i]->live && mrs[i]->m.context->device == c->device &&
            (remote ? mrs[i]->m.rkey : mrs[i]->m.lkey) == key) return mrs[i];
    return NULL;
}

static int post_send(struct ibv_qp *qp, struct ibv_send_wr *wr, struct ibv_send_wr **bad)
{
    struct fqp *s = (struct fqp *)qp;
    union ibv_gid me;
    gid_of(epoch, di(qp->context), &me);
    for (; wr; wr = wr->next) {
        if (wr->wr_id == 0x6e76736e61706962ULL && fail_marker && !--fail_marker) { *bad = wr; return ENOMEM; }
        int status = IBV_WC_SUCCESS;
        struct fqp *d = NULL;
        for (int i = 0; i < n_qps; i++)
            if (qps[i]->live && qps[i]->q.context->device == qp->context->device && qps[i]->q.qp_num == s->dest) d = qps[i];
        if (s->q.state != IBV_QPS_RTS || !d || (s->global ? memcmp(s->dgid.raw, me.raw, 16) : s->dlid != 1 + epoch))
            status = IBV_WC_RETRY_EXC_ERR;
        size_t len = 0;
        for (int j = 0; j < wr->num_sge; j++) len += wr->sg_list[j].length;
        if (!status && len) {
            struct fmr *rm = mr_by(wr->wr.rdma.rkey, 1, qp->context);
            if (!rm || wr->wr.rdma.remote_addr < (uintptr_t)rm->m.addr ||
                wr->wr.rdma.remote_addr + len > (uintptr_t)rm->m.addr + rm->m.length)
                status = IBV_WC_REM_ACCESS_ERR;
            char *to = (char *)(uintptr_t)wr->wr.rdma.remote_addr;
            for (int j = 0; !status && j < wr->num_sge; j++) {
                if (!mr_by(wr->sg_list[j].lkey, 0, qp->context)) { status = IBV_WC_LOC_PROT_ERR; break; }
                memcpy(to, (void *)(uintptr_t)wr->sg_list[j].addr, wr->sg_list[j].length);
                to += wr->sg_list[j].length;
            }
        }
        if (!status && wr->opcode == IBV_WR_RDMA_WRITE_WITH_IMM) {
            if (!d->nr) status = IBV_WC_RNR_RETRY_EXC_ERR;
            else {
                struct ibv_wc w = { .wr_id = d->r[0].wr_id, .status = IBV_WC_SUCCESS, .opcode = IBV_WC_RECV_RDMA_WITH_IMM,
                                    .byte_len = len, .imm_data = wr->imm_data, .qp_num = d->q.qp_num,
                                    .wc_flags = IBV_WC_WITH_IMM };
                memmove(d->r, d->r + 1, --d->nr * sizeof(d->r[0]));
                push((struct fcq *)d->q.recv_cq, w);
            }
        }
        if (status || (wr->send_flags & IBV_SEND_SIGNALED)) {
            struct ibv_wc w = { .wr_id = wr->wr_id, .status = status, .qp_num = s->q.qp_num,
                                .opcode = status && err_opcode ? IBV_WC_RECV : IBV_WC_RDMA_WRITE };
            push((struct fcq *)s->q.send_cq, w);
        }
        if (status) { *bad = wr; s->q.state = IBV_QPS_ERR; return 0; }
    }
    return 0;
}

static int post_recv(struct ibv_qp *qp, struct ibv_recv_wr *wr, struct ibv_recv_wr **bad)
{
    struct fqp *q = (struct fqp *)qp;
    for (; wr; wr = wr->next) {
        if (q->nr == 64 || (wr->num_sge && !mr_by(wr->sg_list[0].lkey, 0, qp->context))) { *bad = wr; return EINVAL; }
        q->r[q->nr].wr_id = wr->wr_id;
        if (wr->num_sge) q->r[q->nr].sg = wr->sg_list[0];
        q->nr++;
    }
    return 0;
}

struct ibv_device **(ibv_get_device_list)(int *n) { if (n) *n = 2; return devs; }
void ibv_free_device_list(struct ibv_device **l) { (void)l; }
const char *ibv_get_device_name(struct ibv_device *d) { return d->name; }
struct ibv_context *ibv_open_device(struct ibv_device *d)
{
    struct ibv_context *c = calloc(1, sizeof(*c));
    /* A device file of its own, as rdma-core's (/dev/null here). */
    c->device = d; c->cmd_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
    c->async_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
    c->ops.poll_cq = poll_cq; c->ops.post_send = post_send; c->ops.post_recv = post_recv;
    open_ctxs++;
    sysfs_sync();
    return c;
}
/* As the kernel: what is left of the device's objects goes with it. The
 * context is not freed: a queue pair left over may still point at it. */
int ibv_close_device(struct ibv_context *c)
{
    for (int i = 0; i < n_qps; i++) if (qps[i]->q.context == c) qps[i]->live = 0;
    for (int i = 0; i < n_mrs; i++) if (mrs[i]->m.context == c) mrs[i]->live = 0;
    close(c->cmd_fd); close(c->async_fd); open_ctxs--;
    return 0;
}
int ibv_get_async_event(struct ibv_context *c, struct ibv_async_event *e)
{
    async_calls++;
    if (!event_qp || event_qp->q.context != c) { errno = EAGAIN; return -1; }
    e->event_type = IBV_EVENT_QP_FATAL;
    e->element.qp = &event_qp->q;
    __atomic_add_fetch(&event_qp->unacked, 1, __ATOMIC_SEQ_CST);
    event_qp = NULL;
    return 0;
}
void ibv_ack_async_event(struct ibv_async_event *e)
{
    if (e->event_type == IBV_EVENT_QP_FATAL) __atomic_sub_fetch(&((struct fqp *)e->element.qp)->unacked, 1, __ATOMIC_SEQ_CST);
}
int ibv_query_device(struct ibv_context *c, struct ibv_device_attr *a) { (void)c; memset(a, 0, sizeof(*a)); return 0; }
int (ibv_query_port)(struct ibv_context *c, uint8_t p, struct _compat_ibv_port_attr *a)
{
    (void)c; (void)p;
    __atomic_add_fetch(&port_in, 1, __ATOMIC_SEQ_CST);
    while (__atomic_load_n(&port_block, __ATOMIC_SEQ_CST)) usleep(1000);
    __atomic_sub_fetch(&port_in, 1, __ATOMIC_SEQ_CST);
    struct ibv_port_attr *x = (struct ibv_port_attr *)a;
    memset(x, 0, sizeof(*x));
    x->state = IBV_PORT_ACTIVE; x->lid = 1 + epoch; x->link_layer = ib_link ? IBV_LINK_LAYER_INFINIBAND : IBV_LINK_LAYER_ETHERNET;
    return 0;
}
/* static: the probe exports its own ibv_query_gid (the library's). */
static void gid_at(struct ibv_context *c, int i, union ibv_gid *g)
{
    memset(g, 0, sizeof(*g));
    if (i < 0 || i >= 16) return;
    if (!gkind[i]) { g->raw[0] = 0xfe; g->raw[1] = 0x80; g->raw[14] = di(c); g->raw[15] = i; return; }
    gid_of(epoch, di(c), g);
}
int ibv_query_gid(struct ibv_context *c, uint8_t p, int i, union ibv_gid *g) { (void)p; gid_queries++; gid_at(c, i, g); return 0; }
/* ibv_query_gid_ex (rdma-core's inline calls this): the entry and its type.
 * Built with NO_GID_EX: a libibverbs older than rdma-core 32, without it. */
#ifndef NO_GID_EX
int _ibv_query_gid_ex(struct ibv_context *c, uint32_t p, uint32_t i, struct ibv_gid_entry *e, uint32_t flags, size_t size)
{
    gid_queries++;
    if (flags || size < sizeof(*e)) return EINVAL;
    if (gid_ex_fail) return EOPNOTSUPP;
    memset(e, 0, sizeof(*e));
    gid_at(c, i, &e->gid);
    if (!memcmp(e->gid.raw, (uint8_t[16]){ 0 }, 16)) return ENODATA;
    e->gid_index = i; e->port_num = p;
    e->gid_type = ib_link ? IBV_GID_TYPE_IB : i < 16 && gkind[i] ? gkind[i] - 1 : IBV_GID_TYPE_ROCE_V1;
    return 0;
}
#endif
int ibv_query_pkey(struct ibv_context *c, uint8_t p, int i, __be16 *k) { (void)c; (void)p; (void)i; *k = 0xffff; return 0; }
int ibv_query_qp(struct ibv_qp *q, struct ibv_qp_attr *a, int m, struct ibv_qp_init_attr *ia)
{ (void)q; (void)a; (void)m; (void)ia; return 0; }
struct ibv_pd *ibv_alloc_pd(struct ibv_context *c) { struct ibv_pd *p = calloc(1, sizeof(*p)); p->context = c; live_pds++; return p; }
int ibv_dealloc_pd(struct ibv_pd *p) { free(p); live_pds--; return 0; }
struct ibv_mr *(ibv_reg_mr)(struct ibv_pd *pd, void *addr, size_t len, int access)
{
    (void)access;
    struct fmr *m = calloc(1, sizeof(*m));
    m->m.pd = pd; m->m.context = pd->context; m->m.addr = addr; m->m.length = len;
    int d = di(pd->context);
    m->m.lkey = 5000 + 1000 * epoch + counter[d]; m->m.rkey = 7000 + 1000 * epoch + counter[d]++;
    if (next_lkey) m->m.lkey = next_lkey, next_lkey = 0;
    m->live = 1;
    mrs[n_mrs++] = m;
    return &m->m;
}
int ibv_dereg_mr(struct ibv_mr *m) { ((struct fmr *)m)->live = 0; return 0; }
struct ibv_cq *ibv_create_cq(struct ibv_context *c, int cqe, void *x, struct ibv_comp_channel *ch, int v)
{
    (void)ch; (void)v;
    struct fcq *q = calloc(1, sizeof(*q));
    q->c.context = c; q->c.cqe = cqe; q->c.cq_context = x; q->live = 1;
    return &q->c;
}
int ibv_destroy_cq(struct ibv_cq *c) { ((struct fcq *)c)->live = 0; return 0; }
struct ibv_qp *ibv_create_qp(struct ibv_pd *pd, struct ibv_qp_init_attr *a)
{
    struct fqp *q = calloc(1, sizeof(*q));
    q->q.context = pd->context; q->q.pd = pd; q->q.send_cq = a->send_cq; q->q.recv_cq = a->recv_cq;
    q->q.qp_num = 100 + 1000 * epoch + counter[di(pd->context)]++; q->q.state = IBV_QPS_RESET; q->q.qp_type = a->qp_type;
    q->live = 1;
    qps[n_qps++] = q;
    return &q->q;
}
int ibv_modify_qp(struct ibv_qp *qp, struct ibv_qp_attr *a, int mask)
{
    struct fqp *q = (struct fqp *)qp;
    if ((mask & IBV_QP_STATE) && a->qp_state == IBV_QPS_INIT && fail_init) { fail_init--; return EINVAL; }
    /* The state machine's order: RTR only from INIT, RTS only from RTR. */
    if ((mask & IBV_QP_STATE) && ((a->qp_state == IBV_QPS_RTR && q->q.state != IBV_QPS_INIT) ||
                                  (a->qp_state == IBV_QPS_RTS && q->q.state != IBV_QPS_RTR)))
        return EINVAL;
    if (mask & IBV_QP_DEST_QPN) q->dest = a->dest_qp_num;
    if (mask & IBV_QP_AV)
        q->dgid = a->ah_attr.grh.dgid, q->dlid = a->ah_attr.dlid, q->global = a->ah_attr.is_global, q->sgid = a->ah_attr.grh.sgid_index;
    if (mask & IBV_QP_STATE) q->q.state = a->qp_state;
    return 0;
}
/* As rdma-core: waits until every event of the queue pair is acknowledged. */
int ibv_destroy_qp(struct ibv_qp *q)
{
    struct fqp *f = (struct fqp *)q;
    for (int i = 0; i < 3000 && __atomic_load_n(&f->unacked, __ATOMIC_SEQ_CST); i++) usleep(1000);
    if (__atomic_load_n(&f->unacked, __ATOMIC_SEQ_CST)) return EBUSY;
    if (fail_destroy && fail_destroy--) return EIO;
    f->live = 0;
    return 0;
}
int ibv_fork_init(void) { return 0; }
const char *ibv_event_type_str(enum ibv_event_type e) { (void)e; return "event"; }
"""

FAKE_MAP = r"""
IBVERBS_1.1 {
  global:
    ibv_get_device_list; ibv_free_device_list; ibv_get_device_name; ibv_open_device;
    ibv_close_device; ibv_get_async_event; ibv_ack_async_event; ibv_query_device;
    ibv_query_port; ibv_query_gid; ibv_query_pkey; ibv_query_qp; ibv_alloc_pd;
    ibv_dealloc_pd; ibv_reg_mr; ibv_dereg_mr; ibv_create_cq; ibv_destroy_cq;
    ibv_create_qp; ibv_modify_qp; ibv_destroy_qp; ibv_fork_init; ibv_event_type_str;
  local: *;
};
IBVERBS_1.11 { global: _ibv_query_gid_ex; } IBVERBS_1.1;
FAKE_1 { global: fake_*; };
"""

# Uses verbs as NCCL does: dlopen the library NCCL_IBVERBS_LIB names,
# dlvsym every function at IBVERBS_1.1, and post and poll through the
# context's ops.
APP = r"""
#define _GNU_SOURCE
#include <dlfcn.h>
#include <infiniband/verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define S(f) __typeof__(&f) p_##f = dlvsym(h, #f, "IBVERBS_1.1")

int main(int argc, char **argv)
{
    void *h = dlopen(argv[1], RTLD_NOW);
    if (!h) { printf("dlopen %s\n", dlerror()); return 2; }
    int (*fence)(char *, size_t) = dlsym(h, "nvsnap_ibv_fence");
    int (*suspend)(char *, size_t) = dlsym(h, "nvsnap_ibv_suspend");
    int (*restore)(char *, size_t) = dlsym(h, "nvsnap_ibv_restore");
    int (*map)(FILE *) = dlsym(h, "nvsnap_ibv_map");
    int (*connect)(const char *, char *, size_t) = dlsym(h, "nvsnap_ibv_connect");
    void (*open_)(void) = dlsym(h, "nvsnap_ibv_open");
    int (*fds)(int *, int) = dlsym(h, "nvsnap_ibv_fds");
    if (!fds) { printf("no nvsnap_ibv_fds\n"); return 2; }
    struct ibv_device **(*getl)(int *) = dlvsym(h, "ibv_get_device_list", "IBVERBS_1.1");
    S(ibv_open_device); S(ibv_alloc_pd); S(ibv_create_cq); S(ibv_create_qp);
    S(ibv_modify_qp); S(ibv_query_gid);
    struct ibv_mr *(*regmr)(struct ibv_pd *, void *, size_t, int) = dlvsym(h, "ibv_reg_mr", "IBVERBS_1.1");
    struct ibv_port_attr pa;
    int (*qport)(struct ibv_context *, uint8_t, struct ibv_port_attr *) = dlvsym(h, "ibv_query_port", "IBVERBS_1.1");

    int nd;
    struct ibv_device **l = getl(&nd);
    void *fake = dlopen("libibverbs.so.1", RTLD_NOW | RTLD_NOLOAD);  /* loaded by the library */
    if (!fake) { printf("the library did not load the real verbs\n"); return 2; }
    int (*open_ctxs)(void) = dlsym(fake, "fake_open_contexts");
    void (*move)(void) = dlsym(fake, "fake_move");
    /* The same objects on both devices: their numbers and keys collide. */
    enum { D = 2 };
    struct ibv_qp *a[D], *b[D];
    struct ibv_cq *cq[D];
    uint32_t qa[D], qb[D], rkey[D], lkey[D];
    static char src[D][64], dst[D][64];
    for (int d = 0; d < D; d++) {
        struct ibv_context *c = p_ibv_open_device(l[d]);
        struct ibv_pd *pd = p_ibv_alloc_pd(c);
        struct ibv_mr *ms = regmr(pd, src[d], sizeof(src[d]), 0), *md = regmr(pd, dst[d], sizeof(dst[d]), 7);
        cq[d] = p_ibv_create_cq(c, 64, NULL, NULL, 0);
        struct ibv_qp_init_attr ia = { .send_cq = cq[d], .recv_cq = cq[d], .qp_type = IBV_QPT_RC,
                                       .cap = { .max_send_wr = 16, .max_recv_wr = 16, .max_send_sge = 1, .max_recv_sge = 1 } };
        a[d] = p_ibv_create_qp(pd, &ia), b[d] = p_ibv_create_qp(pd, &ia);
        union ibv_gid gid;
        p_ibv_query_gid(c, 1, 3, &gid);
        qport(c, 1, &pa);
        /* What NCCL keeps and sends its peer: numbers, keys, GID and LID. */
        qa[d] = a[d]->qp_num, qb[d] = b[d]->qp_num, rkey[d] = md->rkey, lkey[d] = ms->lkey;
        struct ibv_qp *pair[2][2] = { { a[d], b[d] }, { b[d], a[d] } };
        for (int i = 0; i < 2; i++) {
            struct ibv_qp_attr x = { .qp_state = IBV_QPS_INIT, .port_num = 1 };
            p_ibv_modify_qp(pair[i][0], &x, IBV_QP_STATE | IBV_QP_PORT);
            struct ibv_qp_attr y = { .qp_state = IBV_QPS_RTR, .dest_qp_num = pair[i][1]->qp_num,
                                     .ah_attr = { .is_global = 1, .dlid = pa.lid, .port_num = 1,
                                                  .grh = { .dgid = gid, .sgid_index = 3 } } };
            p_ibv_modify_qp(pair[i][0], &y, IBV_QP_STATE | IBV_QP_AV | IBV_QP_DEST_QPN);
            struct ibv_qp_attr z = { .qp_state = IBV_QPS_RTS };
            p_ibv_modify_qp(pair[i][0], &z, IBV_QP_STATE);
        }
    }
    printf("same_numbers=%d\n", qa[0] == qa[1] && lkey[0] == lkey[1] && rkey[0] == rkey[1]);
    struct ibv_wc wc[8];
    int got;
    struct ibv_send_wr *wb;
    for (int round = 0; round < 2; round++) {
        for (int d = 0; d < D; d++) {
            /* A receive on b, posted now, completed by the write after the suspend. */
            struct ibv_recv_wr rw = { .wr_id = 40 + round }, *rb;
            if (b[d]->context->ops.post_recv(b[d], &rw, &rb)) { printf("post_recv failed\n"); return 1; }
            /* A signaled write from a nobody polls before the suspend. */
            struct ibv_sge sg0 = { (uintptr_t)src[d], 4, lkey[d] };
            struct ibv_send_wr w0 = { .wr_id = 7, .sg_list = &sg0, .num_sge = 1, .opcode = IBV_WR_RDMA_WRITE,
                                      .send_flags = IBV_SEND_SIGNALED, .wr = { .rdma = { (uintptr_t)dst[d] + 32, rkey[d] } } };
            if (a[d]->context->ops.post_send(a[d], &w0, &wb)) { printf("post_send failed\n"); return 1; }
        }
        char rep[256];
        /* Round 1 as gpushare does it: fence at "quiesce", then "release". */
        if (round == 1 && fence(rep, sizeof(rep))) { printf("fence: %s\n", rep); return 1; }
        /* NCCL's device files, which the pause checks leave to the suspend. */
        int fv[8], one, nf = fds(fv, 8), cut = fds(&one, 1), dn = 0;
        for (int i = 0; i < nf && i < 8; i++) {
            char p[64], t[64] = "";
            snprintf(p, sizeof(p), "/proc/self/fd/%d", fv[i]);
            ssize_t k = readlink(p, t, sizeof(t) - 1);
            dn += k > 0 && !strncmp(t, "/dev/null", 9);
        }
        if (suspend(rep, sizeof(rep))) { printf("suspend: %s\n", rep); return 1; }
        printf("round=%d fds=%d fds_cut=%d fds_devnull=%d fds_after_suspend=%d\n", round, nf, cut, dn, fds(fv, 8));
        printf("round=%d suspended=%s open_after_suspend=%d\n", round, rep, open_ctxs());
        move();
        if (restore(rep, sizeof(rep))) { printf("restore: %s\n", rep); return 1; }
        char path[] = "/tmp/ibvmapXXXXXX";
        int fd = mkstemp(path);
        FILE *f = fdopen(fd, "w");
        int lines = map(f);
        fclose(f);
        if (connect(path, rep, sizeof(rep))) { printf("connect: %s\n", rep); return 1; }
        unlink(path);
        open_();
        printf("lines=%d\n", lines);
        for (int d = 0; d < D; d++) {
            /* NCCL's numbers and keys, from before. */
            snprintf(src[d], sizeof(src[d]), "hello%d%d", d, round);
            struct ibv_sge sg = { (uintptr_t)src[d], 8, lkey[d] };
            struct ibv_send_wr w = { .wr_id = 9, .sg_list = &sg, .num_sge = 1, .opcode = IBV_WR_RDMA_WRITE_WITH_IMM,
                                     .send_flags = IBV_SEND_SIGNALED, .imm_data = 77,
                                     .wr = { .rdma = { (uintptr_t)dst[d], rkey[d] } } };
            if (a[d]->context->ops.post_send(a[d], &w, &wb)) { printf("post_send after restore failed\n"); return 1; }
            int held = 0, wrote = 0, recvd = 0, bad = 0;
            for (int t = 0; t < 4 && (got = cq[d]->context->ops.poll_cq(cq[d], 8, wc)) >= 0; t++)
                for (int i = 0; i < got; i++) {
                    if (wc[i].status) { bad++; printf("wc status %d wr_id %lu\n", wc[i].status, (unsigned long)wc[i].wr_id); }
                    else if (wc[i].wr_id == 7 && wc[i].qp_num == qa[d]) held++;
                    else if (wc[i].wr_id == 9 && wc[i].qp_num == qa[d]) wrote++;
                    else if (wc[i].wr_id == (uint64_t)(40 + round) && wc[i].qp_num == qb[d] && wc[i].imm_data == 77) recvd++;
                    else { bad++; printf("unexpected wc wr_id %lu qp %u\n", (unsigned long)wc[i].wr_id, wc[i].qp_num); }
                }
            printf("held=%d wrote=%d recvd=%d bad=%d data=%s\n", held, wrote, recvd, bad,
                   strncmp(dst[d], src[d], 8) ? "wrong" : "ok");
        }
    }
    return 0;
}
"""

# White-box cases: the library's source compiled in, over the same fake
# verbs; each prints key=value words (see the test_ methods below).
PROBE = r"""
#define _GNU_SOURCE
#include <poll.h>
#include <pthread.h>
#include <sys/resource.h>
/* asyncgen: the first poll of an async event wait runs a whole suspend,
 * restore and resume before it returns that the old descriptor is ready. */
static int probe_poll(struct pollfd *p, nfds_t n, int t);
#define poll(p, n, t) probe_poll(p, n, t)
/* openrace: nvsnap_ibv_open stops after its check of the doomed state
 * (NVSNAP_IBV_OPEN_HOOK) until the probe lets it go. */
static int open_pause, open_paused, open_go;
#define NVSNAP_IBV_OPEN_HOOK() do { \
        if (__atomic_load_n(&open_pause, __ATOMIC_SEQ_CST)) { \
            __atomic_store_n(&open_paused, 1, __ATOMIC_SEQ_CST); \
            while (!__atomic_load_n(&open_go, __ATOMIC_SEQ_CST)) usleep(1000); \
        } \
    } while (0)
/* Set once stop_calls is in its wait for verbs calls in progress. */
static int in_drain_wait;
#define NVSNAP_IBV_WAITING_HOOK() __atomic_store_n(&in_drain_wait, 1, __ATOMIC_SEQ_CST)
/* sysfs: the fake's tree, or none. */
#define NVSNAP_IBV_SYSFS (getenv("FAKE_SYSFS") ? getenv("FAKE_SYSFS") : "/nonexistent/sysfs")
/* Counts the broadcasts that let NCCL's held verbs calls go (reopen). */
static int probe_broadcast(pthread_cond_t *c);
#define pthread_cond_broadcast(c) probe_broadcast(c)
#include "nvsnap_ibv.c"
#undef poll
#undef pthread_cond_broadcast
static int gate_opens;
static int probe_broadcast(pthread_cond_t *c)
{
    if (c == &gcv) __atomic_add_fetch(&gate_opens, 1, __ATOMIC_SEQ_CST);
    return pthread_cond_broadcast(c);
}

#define FK(ret, name, ...) static ret (*name)(__VA_ARGS__)
FK(void, fake_move, void); FK(void, fake_port_block, int); FK(void, fake_fail_marker, int);
FK(void, fake_fail_init, int); FK(void, fake_fail_destroy, int); FK(void, fake_err_opcode, int); FK(void, fake_next_lkey, uint32_t);
FK(void, fake_raise_qp_event, struct ibv_qp *); FK(int, fake_async_calls, void); FK(int, fake_live_pds, void);
FK(int, fake_qp_state, struct ibv_qp *); FK(int, fake_qp_receives, struct ibv_qp *); FK(int, fake_port_in, void);
FK(void, fake_gid, int, int); FK(void, fake_gid_ex_fail, int); FK(int, fake_qp_sgid, struct ibv_qp *); FK(void, fake_ib, int); FK(int, fake_gid_queries, void);
static void fakes(void)
{
#define G(name) *(void **)&name = dlsym(real_lib, #name); if (!name) { printf("no %s\n", #name); exit(2); }
    G(fake_move); G(fake_port_block); G(fake_fail_marker); G(fake_fail_init); G(fake_fail_destroy); G(fake_err_opcode);
    G(fake_next_lkey); G(fake_raise_qp_event); G(fake_async_calls); G(fake_live_pds); G(fake_qp_state);
    G(fake_qp_receives); G(fake_port_in); G(fake_gid); G(fake_gid_ex_fail); G(fake_qp_sgid); G(fake_ib); G(fake_gid_queries);
#undef G
}

static char rep[256];
static struct ibv_context *ctx;
static struct ibv_pd *pd;
static struct ibv_cq *cq;
static struct ibv_qp *qa, *qb;
static struct ibv_mr *mr;
static char buf[64];

static void up(int qps)
{
    int nd;
    struct ibv_device **l = ibv_get_device_list(&nd);
    ctx = ibv_open_device(l[0]);
    fakes();
    if (!qps) return;
    pd = ibv_alloc_pd(ctx);
    mr = ibv_reg_mr(pd, buf, sizeof(buf), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    cq = ibv_create_cq(ctx, 64, NULL, NULL, 0);
    struct ibv_qp_init_attr ia = { .send_cq = cq, .recv_cq = cq, .qp_type = IBV_QPT_RC,
                                   .cap = { .max_send_wr = 16, .max_recv_wr = 16, .max_send_sge = 1, .max_recv_sge = 8 } };
    qa = ibv_create_qp(pd, &ia), qb = ibv_create_qp(pd, &ia);
    union ibv_gid gid;
    ibv_query_gid(ctx, 1, 3, &gid);
    struct ibv_port_attr pa;
    (ibv_query_port)(ctx, 1, (struct _compat_ibv_port_attr *)&pa);
    struct ibv_qp *pair[2][2] = { { qa, qb }, { qb, qa } };
    for (int i = 0; i < 2; i++) {
        struct ibv_qp_attr x = { .qp_state = IBV_QPS_INIT, .port_num = 1, .qp_access_flags = IBV_ACCESS_REMOTE_WRITE };
        ibv_modify_qp(pair[i][0], &x, IBV_QP_STATE | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
        struct ibv_qp_attr y = { .qp_state = IBV_QPS_RTR, .dest_qp_num = pair[i][1]->qp_num,
                                 .ah_attr = { .is_global = 1, .dlid = pa.lid, .port_num = 1,
                                              .grh = { .dgid = gid, .sgid_index = 3 } } };
        ibv_modify_qp(pair[i][0], &y, IBV_QP_STATE | IBV_QP_AV | IBV_QP_DEST_QPN);
        struct ibv_qp_attr z = { .qp_state = IBV_QPS_RTS };
        ibv_modify_qp(pair[i][0], &z, IBV_QP_STATE);
    }
}

/* fabmap / remap: this process's own lines, as the tool merges them */
static int reconnect(void)
{
    char path[] = "/tmp/ibvmapXXXXXX";
    FILE *f = fdopen(mkstemp(path), "w");
    nvsnap_ibv_map(f);
    fclose(f);
    int r = nvsnap_ibv_connect(path, rep, sizeof(rep));
    unlink(path);
    if (r) printf("connect: %s\n", rep);
    return r;
}

/* suspend, "move to another node", restore, exchange, reconnect, resume */
static int cycle(void)
{
    if (nvsnap_ibv_suspend(rep, sizeof(rep))) return printf("suspend: %s\n", rep), -1;
    fake_move();
    if (nvsnap_ibv_restore(rep, sizeof(rep))) return printf("restore: %s\n", rep), -1;
    if (reconnect()) return -1;
    nvsnap_ibv_open();
    return 0;
}

static int poll_calls, repolled;
static int probe_poll(struct pollfd *p, nfds_t n, int t)
{
    if (!getenv("ASYNCGEN")) return poll(p, n, t);
    if (poll_calls++) { repolled = 1; errno = EBADF; return -1; }
    int r = cycle();
    p[0].revents = POLLIN, p[1].revents = 0;
    return r ? -1 : 1;
}

static void *query_port(void *a)
{
    struct ibv_port_attr pa;
    (void)a;
    (ibv_query_port)(ctx, 1, (struct _compat_ibv_port_attr *)&pa);
    return NULL;
}
static void *alloc_pd(void *a) { (void)a; return ibv_alloc_pd(ctx); }
static int sus_rc;
static void *suspend_t(void *a) { (void)a; sus_rc = nvsnap_ibv_suspend(rep, sizeof(rep)); return NULL; }
static void *open_t(void *a) { (void)a; nvsnap_ibv_open(); return NULL; }
static int fence_rc;
static void *fence_t(void *a) { (void)a; __atomic_store_n(&fence_rc, nvsnap_ibv_fence(rep, sizeof(rep)), __ATOMIC_SEQ_CST); return NULL; }
static struct ibv_async_event ev;
static int got_event;
static void *event_t(void *a)
{
    if (ibv_get_async_event(ctx, &ev)) return NULL;
    __atomic_store_n(&got_event, 1, __ATOMIC_SEQ_CST);
    if (!a) return NULL;
    usleep(300000);
    ibv_ack_async_event(&ev);
    return NULL;
}
static int sent;
static void *send_t(void *a)
{
    (void)a;
    struct ibv_sge sg = { (uintptr_t)buf, 4, mr->lkey };
    struct ibv_send_wr w = { .wr_id = 11, .sg_list = &sg, .num_sge = 1, .opcode = IBV_WR_RDMA_WRITE,
                             .send_flags = IBV_SEND_SIGNALED, .wr = { .rdma = { (uintptr_t)buf + 32, mr->rkey } } }, *b;
    qa->context->ops.post_send(qa, &w, &b);
    __atomic_store_n(&sent, 1, __ATOMIC_SEQ_CST);
    return NULL;
}
static double now(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return t.tv_sec + t.tv_nsec / 1e9; }

/* maprace: a reader looks up a key that is always there while the table grows. */
static struct u32map *cur;
static int stop_reader;
static long misses, reads;
static void *reader(void *a)
{
    (void)a;
    while (!__atomic_load_n(&stop_reader, __ATOMIC_SEQ_CST)) {
        struct u32map *m = __atomic_load_n(&cur, __ATOMIC_SEQ_CST);
        uint32_t v;
        if (m) { reads++; if (map_get(m, 1, &v) || v != 1) misses++; }
    }
    return NULL;
}

int main(int argc, char **argv)
{
    const char *c = argc > 1 ? argv[1] : "";
    pthread_t t, t2;
    if (!strcmp(c, "maprace")) {
        pthread_create(&t, NULL, reader, NULL);
        for (int it = 0; it < 4000; it++) {
            struct u32map *m = calloc(1, sizeof(*m));
            map_put(m, 1, 1);
            __atomic_store_n(&cur, m, __ATOMIC_SEQ_CST);
            for (uint32_t k = 2; k < 2000; k++) map_put(m, k, k);
        }
        __atomic_store_n(&stop_reader, 1, __ATOMIC_SEQ_CST);
        pthread_join(t, NULL);
        printf("misses=%ld reads_ok=%d\n", misses, reads > 0);
        return 0;
    }
    if (!strcmp(c, "portdrain")) {
        /* A port query in the provider when the fence starts. The query
         * stays there until the fence is in its wait for calls in progress
         * (NVSNAP_IBV_WAITING_HOOK); the fence must not have returned
         * then, and must pass once the query returns. A fence that never
         * waits never signals, and the query is released only after 5 s. */
        up(0);
        fake_port_block(1);
        pthread_create(&t, NULL, query_port, NULL);
        int in = 0;
        for (int i = 0; i < 5000 && !(in = fake_port_in()); i++) usleep(1000);
        if (!in) { printf("query_never_entered=1\n"); return 9; }
        fence_rc = 99;
        pthread_create(&t2, NULL, fence_t, NULL);
        int waiting = 0;
        for (int i = 0; i < 5000 && !(waiting = __atomic_load_n(&in_drain_wait, __ATOMIC_SEQ_CST)); i++) usleep(1000);
        int returned_early = __atomic_load_n(&fence_rc, __ATOMIC_SEQ_CST) != 99;
        int inflight_seen = __atomic_load_n(&inflight, __ATOMIC_SEQ_CST);
        fake_port_block(0);
        pthread_join(t2, NULL);
        pthread_join(t, NULL);
        printf("fence=%d fence_waiting=%d returned_before_query=%d inflight_seen=%d\n", fence_rc, waiting,
               returned_early, inflight_seen);
        nvsnap_ibv_open();
        return 0;
    }
    if (!strcmp(c, "pdgate")) {
        /* A protection domain created while its registration waits. */
        up(0);
        pthread_mutex_lock(&omu);
        pthread_create(&t, NULL, alloc_pd, NULL);
        usleep(200000);
        printf("inflight_while_registering=%d\n", __atomic_load_n(&inflight, __ATOMIC_SEQ_CST));
        pthread_create(&t2, NULL, suspend_t, NULL);
        usleep(100000);
        pthread_mutex_unlock(&omu);
        pthread_join(t, NULL);
        pthread_join(t2, NULL);
        printf("suspend=%d live_pds=%d\n", sus_rc, fake_live_pds());
        return 0;
    }
    if (!strcmp(c, "markers")) {
        /* The second fence write cannot be posted: the first one's
         * completion must not reach NCCL. */
        up(1);
        fake_fail_marker(2);
        int r = nvsnap_ibv_fence(rep, sizeof(rep));
        struct ibv_wc wc[16];
        int k = 0, n = cq->context->ops.poll_cq(cq, 16, wc);
        for (int i = 0; i < n; i++) k += wc[i].wr_id == MARKER;
        int again = nvsnap_ibv_fence(rep, sizeof(rep));
        nvsnap_ibv_open();
        n = cq->context->ops.poll_cq(cq, 16, wc);
        for (int i = 0; i < n; i++) k += wc[i].wr_id == MARKER;
        printf("fence=%d fence_again=%d markers_to_nccl=%d\n", r, again, k);
        return 0;
    }
    if (!strcmp(c, "failedwc")) {
        /* A send fails; its completion's opcode is a receive's, and its
         * wr_id that of a receive still outstanding on the queue pair. */
        up(1);
        fake_err_opcode(1);
        struct ibv_recv_wr rw = { .wr_id = 7 }, *rb;
        qa->context->ops.post_recv(qa, &rw, &rb);
        struct ibv_sge sg = { (uintptr_t)buf, 4, mr->lkey };
        struct ibv_send_wr w = { .wr_id = 7, .sg_list = &sg, .num_sge = 1, .opcode = IBV_WR_RDMA_WRITE,
                                 .send_flags = IBV_SEND_SIGNALED, .wr = { .rdma = { 1, 12345 } } }, *b;
        qa->context->ops.post_send(qa, &w, &b);
        struct ibv_wc wc[4];
        int n = cq->context->ops.poll_cq(cq, 4, wc);
        int fr = nvsnap_ibv_fence(rep, sizeof(rep));
        printf("polled=%d status_failed=%d receives_kept=%d fence=%d\n", n, n == 1 && wc[0].status != 0,
               ((struct vqp *)qa)->n_recvs, fr);
        return 0;
    }
    if (!strcmp(c, "events") || !strcmp(c, "events_noack")) {
        /* NCCL holds an event of a queue pair when the suspend starts. */
        int ack = !strcmp(c, "events");
        up(1);
        fake_raise_qp_event(((struct vqp *)qa)->real);
        pthread_create(&t, NULL, event_t, ack ? (void *)1 : NULL);
        for (int i = 0; i < 2000 && !__atomic_load_n(&got_event, __ATOMIC_SEQ_CST); i++) usleep(1000);
        double t0 = now();
        int r = nvsnap_ibv_suspend(rep, sizeof(rep));
        printf("event_shadow=%d suspend=%d took_ms=%d qps_destroyed=%d\n", ev.element.qp == qa, r,
               (int)((now() - t0) * 1000), ((struct vqp *)qa)->real == NULL);
        if (!ack) printf("reason=%s\n", strstr(rep, "acknowledged") ? "unacked" : "other");
        pthread_join(t, NULL);
        return 0;
    }
    if (!strcmp(c, "asyncgen")) {
        /* The wait outlives a suspend, restore and resume between its poll
         * and its read. */
        up(0);
        struct ibv_async_event e;
        int r = ibv_get_async_event(ctx, &e);
        printf("get=%d provider_reads=%d repolled=%d\n", r, fake_async_calls(), repolled);
        return 0;
    }
    if (!strcmp(c, "newkey")) {
        /* After a restore, a new registration's real lkey is the visible
         * lkey of an older one. */
        up(1);
        uint32_t old_visible = mr->lkey;
        if (cycle()) return 1;
        fake_next_lkey(old_visible);
        static char b2[64];
        struct ibv_mr *m2 = ibv_reg_mr(pd, b2, sizeof(b2), IBV_ACCESS_LOCAL_WRITE);
        struct vmr *v1 = (struct vmr *)mr, *v2 = (struct vmr *)m2;
        struct vctx *vc = (struct vctx *)ctx;
        printf("real_collides=%d old_ok=%d new_ok=%d distinct=%d\n", v2->real->lkey == old_visible,
               lkey_of(vc, mr->lkey) == v1->real->lkey, lkey_of(vc, m2->lkey) == v2->real->lkey, m2->lkey != mr->lkey);
        return 0;
    }
    if (!strcmp(c, "retry")) {
        /* The first restore fails moving a queue pair to INIT; the retry
         * must set it up again. */
        up(1);
        struct ibv_recv_wr rw = { .wr_id = 5 }, *rb;
        qa->context->ops.post_recv(qa, &rw, &rb);
        if (nvsnap_ibv_suspend(rep, sizeof(rep))) return printf("suspend: %s\n", rep), 1;
        fake_move();
        fake_fail_init(1);  /* the first queue pair's */
        int r1 = nvsnap_ibv_restore(rep, sizeof(rep));
        int r2 = nvsnap_ibv_restore(rep, sizeof(rep));
        struct vqp *b = (struct vqp *)qa;
        printf("first=%d retry=%d state_init=%d receives=%d\n", r1, r2,
               b->real && fake_qp_state(b->real) == IBV_QPS_INIT, b->real ? fake_qp_receives(b->real) : -1);
        return 0;
    }
    if (!strcmp(c, "remapretry")) {
        /* "connect" fails moving a queue pair to RTS; the retry goes on
         * from RTR instead of asking for RTR again. */
        up(1);
        if (nvsnap_ibv_suspend(rep, sizeof(rep))) return 1;
        fake_move();
        if (nvsnap_ibv_restore(rep, sizeof(rep))) return 1;
        struct vqp *a = (struct vqp *)qa;
        a->attr[2].qp_state = IBV_QPS_RTR;  /* fails: RTR from RTR */
        int r1 = reconnect();
        a->attr[2].qp_state = IBV_QPS_RTS;
        int r2 = reconnect();
        printf("first=%d retry=%d state_rts=%d\n", r1, r2, fake_qp_state(a->real) == IBV_QPS_RTS);
        return 0;
    }
    if (!strcmp(c, "fenceheld")) {
        /* After the fence, a send waits for the restore instead of going
         * out on a queue pair "release" destroys. */
        up(1);
        if (nvsnap_ibv_fence(rep, sizeof(rep))) return printf("fence: %s\n", rep), 1;
        pthread_create(&t, NULL, send_t, NULL);
        usleep(200000);
        int early = __atomic_load_n(&sent, __ATOMIC_SEQ_CST);
        if (cycle()) return 1;
        pthread_join(t, NULL);
        struct ibv_wc wc[8];
        int ok = 0, n = cq->context->ops.poll_cq(cq, 8, wc);
        for (int i = 0; i < n; i++) ok += wc[i].wr_id == 11 && wc[i].status == IBV_WC_SUCCESS && wc[i].qp_num == qa->qp_num;
        printf("sent_before_release=%d sent_after_restore=%d completed=%d\n", early, sent, ok);
        /* A fence, then the process runs on without a release (the vote
         * said retry). */
        nvsnap_ibv_fence(rep, sizeof(rep));
        sent = 0;
        pthread_create(&t, NULL, send_t, NULL);
        nvsnap_ibv_open();
        pthread_join(t, NULL);
        printf("sent_after_open=%d\n", sent);
        /* After a release, opening does not let calls reach destroyed objects. */
        nvsnap_ibv_suspend(rep, sizeof(rep));
        nvsnap_ibv_open();
        printf("held_after_release=%d\n", !!__atomic_load_n(&suspended, __ATOMIC_SEQ_CST));
        return 0;
    }
    if (!strcmp(c, "sge5")) {
        up(1);
        struct ibv_sge sg[5] = { { (uintptr_t)buf, 4, mr->lkey } };
        for (int i = 1; i < 5; i++) sg[i] = sg[0];
        struct ibv_recv_wr rw = { .wr_id = 9, .sg_list = sg, .num_sge = 5 }, *rb;
        qb->context->ops.post_recv(qb, &rw, &rb);
        printf("fence=%d\n", nvsnap_ibv_fence(rep, sizeof(rep)));
        return 0;
    }
    if (!strcmp(c, "openrace")) {
        /* An open that has checked the doomed state, and not yet let the
         * calls go, when the release (state lock busy) dooms the process:
         * the calls must stay held for good, and a send must not go. */
        up(1);
        int f = nvsnap_ibv_fence(rep, sizeof(rep));
        __atomic_store_n(&open_pause, 1, __ATOMIC_SEQ_CST);
        pthread_create(&t, NULL, open_t, NULL);
        for (int i = 0; i < 5000 && !__atomic_load_n(&open_paused, __ATOMIC_SEQ_CST); i++) usleep(1000);
        int paused = __atomic_load_n(&open_paused, __ATOMIC_SEQ_CST);
        int (*doom)(void) = (int (*)(void))dlsym(RTLD_DEFAULT, "nvsnap_ibv_doom_if_fenced");
        int d = doom ? doom() : -1;
        __atomic_store_n(&open_go, 1, __ATOMIC_SEQ_CST);
        pthread_join(t, NULL);
        pthread_create(&t2, NULL, send_t, NULL);
        usleep(200000);  /* held: the send stays blocked */
        printf("fence=%d paused=%d doom=%d doomed=%d held=%d sent=%d\n", f, paused, d, nvsnap_ibv_doomed(),
               !!__atomic_load_n(&suspended, __ATOMIC_SEQ_CST), __atomic_load_n(&sent, __ATOMIC_SEQ_CST));
        return 0;  /* a held send stays blocked */
    }
    if (!strcmp(c, "doomiffenced") || !strcmp(c, "doomunfenced")) {
        /* gpushare's release could not run (its state lock busy): after a
         * fence NCCL's calls stay held for good; without one nothing
         * changes. */
        up(1);
        int f = !strcmp(c, "doomiffenced") ? nvsnap_ibv_fence(rep, sizeof(rep)) : 0;
        int (*doom)(void) = (int (*)(void))dlsym(RTLD_DEFAULT, "nvsnap_ibv_doom_if_fenced");
        int d = doom ? doom() : -1;
        nvsnap_ibv_open();
        printf("fence=%d doom=%d doomed=%d held_after_open=%d\n", f, d, nvsnap_ibv_doomed(),
               !!__atomic_load_n(&suspended, __ATOMIC_SEQ_CST));
        return 0;
    }
    if (!strcmp(c, "refusetimeout")) {
        /* After a fence, a thread caught between counting itself in and
         * seeing the gate closed (enter) keeps inflight up: the release's
         * wait for calls in progress times out. NCCL's calls must not be
         * let go even for a moment: a peer may be releasing. */
        up(1);
        int f = nvsnap_ibv_fence(rep, sizeof(rep));
        __atomic_add_fetch(&inflight, 1, __ATOMIC_SEQ_CST);
        int before = __atomic_load_n(&gate_opens, __ATOMIC_SEQ_CST);
        int s = nvsnap_ibv_suspend(rep, sizeof(rep));
        printf("fence=%d suspend=%d opens=%d held=%d timeout=%d\n", f, s, __atomic_load_n(&gate_opens, __ATOMIC_SEQ_CST) - before,
               !!__atomic_load_n(&suspended, __ATOMIC_SEQ_CST), strstr(rep, "did not return") != NULL);
        return 0;
    }
    if (!strcmp(c, "refusefenced") || !strcmp(c, "refuseunfenced")) {
        /* The release refuses (a queue pair with a failed completion),
         * after a fence that succeeded (its peers may be releasing now) or
         * with none before it. Then whatever would let the process run
         * again opens NCCL's verbs. */
        up(1);
        int f = !strcmp(c, "refusefenced") ? nvsnap_ibv_fence(rep, sizeof(rep)) : 0;
        ((struct vqp *)qa)->failed = 1;
        int s = nvsnap_ibv_suspend(rep, sizeof(rep));
        int after_refusal = !!__atomic_load_n(&suspended, __ATOMIC_SEQ_CST);
        nvsnap_ibv_open();
        int after_open = !!__atomic_load_n(&suspended, __ATOMIC_SEQ_CST);
        pthread_create(&t, NULL, send_t, NULL);
        /* Open: the send goes out, so wait for it (no time decides it).
         * Held: it must stay blocked; give it 200 ms to show otherwise. */
        if (!after_open) pthread_join(t, NULL);
        else usleep(200000);
        int (*doomed_q)(void) = (int (*)(void))dlsym(RTLD_DEFAULT, "nvsnap_ibv_doomed");
        printf("fence=%d suspend=%d held_after_refusal=%d held_after_open=%d sent=%d doomed=%d\n", f, s,
               after_refusal, after_open, __atomic_load_n(&sent, __ATOMIC_SEQ_CST), doomed_q ? doomed_q() : -1);
        return 0;  /* a held send stays blocked */
    }
    if (!strcmp(c, "destroyfail")) {
        /* The release cannot destroy one queue pair (qa); its peer (qb) is
         * destroyed. After the restore, qa must reach the new qb. */
        up(1);
        fake_fail_destroy(1);
        int s = nvsnap_ibv_suspend(rep, sizeof(rep));
        fake_move();
        int r = nvsnap_ibv_restore(rep, sizeof(rep));
        int k = r ? -1 : reconnect();
        nvsnap_ibv_open();
        int ok = 0;
        if (!k) {
            send_t(NULL);
            struct ibv_wc wc[8];
            int n = cq->context->ops.poll_cq(cq, 8, wc);
            for (int i = 0; i < n; i++) ok += wc[i].wr_id == 11 && wc[i].status == IBV_WC_SUCCESS;
        }
        printf("suspend=%d restore=%d connect=%d completed=%d\n", s, r, k, ok);
        return 0;
    }
    if (!strcmp(c, "sge5drain")) {
        /* The receive is already in a verbs call (past the gate) when the
         * fence checks, and is recorded only while the fence waits for
         * that call to return. */
        up(1);
        enter();
        pthread_create(&t, NULL, fence_t, NULL);
        while (!__atomic_load_n(&suspended, __ATOMIC_SEQ_CST)) usleep(1000);
        struct ibv_sge sg[5] = { { (uintptr_t)buf, 4, mr->lkey } };
        for (int i = 1; i < 5; i++) sg[i] = sg[0];
        struct ibv_recv_wr rw = { .wr_id = 9, .sg_list = sg, .num_sge = 5 };
        recv_add((struct vqp *)qb, &rw);
        leave();
        pthread_join(t, NULL);
        printf("fence=%d\n", fence_rc);
        return 0;
    }
    if (!strncmp(c, "roce_", 5)) {
        /* NCCL connected through index 3, IPv4 RoCE v2. On the node of the
         * restore, index 3 holds the same IPv4 address as RoCE v1, and
         * index 4 (roce_type) holds it as RoCE v2; roce_none has no RoCE v2
         * entry. roce_unknown: the provider could not tell the GID's type
         * when NCCL read it. roce_ib: an InfiniBand port, its table as it
         * was. PROBE_GIDEX_FAIL: the type query fails throughout;
         * PROBE_IB: the ports are InfiniBand. */
        int nd;
        ibv_get_device_list(&nd);
        fakes();
        if (getenv("PROBE_IB")) fake_ib(1);
        if (!strcmp(c, "roce_unknown") || getenv("PROBE_GIDEX_FAIL")) fake_gid_ex_fail(1);
        up(1);
        if (!getenv("PROBE_GIDEX_FAIL")) fake_gid_ex_fail(0);
        /* NCCL reads the port's table to choose an entry, empty ones too. */
        for (int i = 0; i < 24; i++) { union ibv_gid x; ibv_query_gid(ctx, 1, i, &x); }
        fake_gid_ex_fail(0);  /* the new node's provider reports types */
        if (strcmp(c, "roce_ib")) fake_gid(3, 1 + IBV_GID_TYPE_ROCE_V1);
        if (!strcmp(c, "roce_type")) fake_gid(4, 1 + IBV_GID_TYPE_ROCE_V2);
        if (nvsnap_ibv_suspend(rep, sizeof(rep))) return printf("suspend: %s\n", rep), 1;
        fake_move();
        int q0 = fake_gid_queries();
        int r = nvsnap_ibv_restore(rep, sizeof(rep));
        printf("restore=%d restore_gid_queries=%d\n", r, fake_gid_queries() - q0);
        if (r) {
            printf("restore_error=%s\n", strstr(rep, "did not report") ? "unknown" : strstr(rep, "no GID of") ? "nomatch" : "other");
            fprintf(stderr, "%s\n", rep);
            return 0;
        }
        if (reconnect()) return 1;
        nvsnap_ibv_open();
        send_t(NULL);
        struct ibv_wc wc[8];
        int n = cq->context->ops.poll_cq(cq, 8, wc), ok = 0;
        for (int i = 0; i < n; i++) ok += wc[i].wr_id == 11 && wc[i].status == IBV_WC_SUCCESS;
        printf("sgid_a=%d sgid_b=%d completed=%d\n", fake_qp_sgid(((struct vqp *)qa)->real),
               fake_qp_sgid(((struct vqp *)qb)->real), ok);
        return 0;
    }
    if (!strcmp(c, "noslot")) {
        up(0);
        pd = ibv_alloc_pd(ctx);
        cq = ibv_create_cq(ctx, 64, NULL, NULL, 0);
        n_qslots = 64 * QCHUNK;  /* every slot used */
        struct ibv_qp_init_attr ia = { .send_cq = cq, .recv_cq = cq, .qp_type = IBV_QPT_RC };
        qa = ibv_create_qp(pd, &ia);
        printf("created=%d fence=%d\n", qa != NULL, nvsnap_ibv_fence(rep, sizeof(rep)));
        return 0;
    }
    if (!strcmp(c, "noeventfd")) {
        /* No descriptor left for the wake: the open fails, and leaves no
         * device open. */
        int nd;
        struct ibv_device **l = ibv_get_device_list(&nd);
        int f = dup(0);
        close(f);
        struct rlimit rl;
        getrlimit(RLIMIT_NOFILE, &rl);
        rlim_t cur_lim = rl.rlim_cur;
        rl.rlim_cur = f;
        setrlimit(RLIMIT_NOFILE, &rl);
        errno = 0;
        struct ibv_context *x = ibv_open_device(l[0]);
        int e = errno;
        rl.rlim_cur = cur_lim;
        setrlimit(RLIMIT_NOFILE, &rl);
        printf("opened=%d errno_set=%d contexts=%d\n", x != NULL, e != 0, nvsnap_ibv_count());
        return 0;
    }
    return 2;
}
"""


# A worker for the GPU tool's rollback: libnvsnap_gpushare preloaded (its
# control thread started by an import), libnvsnap_ibv loaded as NCCL loads
# it over the fake verbs, two of NCCL's queue pairs connected, and the
# first queue pair destroy set to fail. It prints "ready", lets
# nvsnap-gpu-suspend suspend it (the release fails and the tool rolls
# back: load, the verbs exchange, remap, resume), then, on a line on stdin,
# writes from the queue pair whose destroy failed to its peer, which the
# rollback re-created with a new number.
ROLLBACK = r"""
#define _GNU_SOURCE
#include <dlfcn.h>
#include <infiniband/verbs.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define S(f) __typeof__(&f) p_##f = dlvsym(h, #f, "IBVERBS_1.1")

int main(void)
{
    /* A driver lookup starts the library's control thread
     * (NVSNAP_GPUSHARE_CTL_ON_CUDA_USE=1): a process with NCCL's verbs and
     * no GPU memory of its own still takes part in the suspend. */
    void *cuda = dlopen("libcuda.so.1", RTLD_NOW);
    int (*gpa)(const char *, void **, int, unsigned long long, void *) = cuda ? dlsym(cuda, "cuGetProcAddress_v2") : NULL;
    void *fn = NULL;
    if (!gpa || gpa("cuInit", &fn, 12000, 0, NULL)) { printf("no driver\n"); return 2; }
    void *h = dlopen("libnvsnap_ibv.so", RTLD_NOW | RTLD_LOCAL);
    if (!h) { printf("dlopen %s\n", dlerror()); return 2; }
    struct ibv_device **(*getl)(int *) = dlvsym(h, "ibv_get_device_list", "IBVERBS_1.1");
    S(ibv_open_device); S(ibv_alloc_pd); S(ibv_create_cq); S(ibv_create_qp); S(ibv_modify_qp); S(ibv_query_gid);
    struct ibv_mr *(*regmr)(struct ibv_pd *, void *, size_t, int) = dlvsym(h, "ibv_reg_mr", "IBVERBS_1.1");
    int (*qport)(struct ibv_context *, uint8_t, struct ibv_port_attr *) = dlvsym(h, "ibv_query_port", "IBVERBS_1.1");
    int nd;
    struct ibv_device **l = getl(&nd);
    void *fake = dlopen("libibverbs.so.1", RTLD_NOW | RTLD_NOLOAD);  /* loaded by the library */
    void (*fail_destroy)(int) = fake ? (void (*)(int))dlsym(fake, "fake_fail_destroy") : NULL;
    if (!fail_destroy) { printf("no fake verbs\n"); return 2; }
    struct ibv_context *c = p_ibv_open_device(l[0]);
    struct ibv_pd *pd = p_ibv_alloc_pd(c);
    static char src[64], dst[64];
    struct ibv_mr *ms = regmr(pd, src, sizeof(src), 0), *md = regmr(pd, dst, sizeof(dst), 7);
    struct ibv_cq *cq = p_ibv_create_cq(c, 64, NULL, NULL, 0);
    struct ibv_qp_init_attr ia = { .send_cq = cq, .recv_cq = cq, .qp_type = IBV_QPT_RC,
                                   .cap = { .max_send_wr = 16, .max_recv_wr = 16, .max_send_sge = 1, .max_recv_sge = 1 } };
    struct ibv_qp *a = p_ibv_create_qp(pd, &ia), *b = p_ibv_create_qp(pd, &ia);
    union ibv_gid gid;
    struct ibv_port_attr pa;
    p_ibv_query_gid(c, 1, 3, &gid);
    qport(c, 1, &pa);
    struct ibv_qp *pair[2][2] = { { a, b }, { b, a } };
    for (int i = 0; i < 2; i++) {
        struct ibv_qp_attr x = { .qp_state = IBV_QPS_INIT, .port_num = 1, .qp_access_flags = IBV_ACCESS_REMOTE_WRITE };
        p_ibv_modify_qp(pair[i][0], &x, IBV_QP_STATE | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
        struct ibv_qp_attr y = { .qp_state = IBV_QPS_RTR, .dest_qp_num = pair[i][1]->qp_num,
                                 .ah_attr = { .is_global = 1, .dlid = pa.lid, .port_num = 1,
                                              .grh = { .dgid = gid, .sgid_index = 3 } } };
        p_ibv_modify_qp(pair[i][0], &y, IBV_QP_STATE | IBV_QP_AV | IBV_QP_DEST_QPN);
        struct ibv_qp_attr z = { .qp_state = IBV_QPS_RTS };
        p_ibv_modify_qp(pair[i][0], &z, IBV_QP_STATE);
    }
    fail_destroy(1);  /* a's */
    printf("ready\n");
    fflush(stdout);
    char line[16];
    if (!fgets(line, sizeof(line), stdin)) return 3;
    /* NCCL writes from a to b with the numbers and keys it had. */
    snprintf(src, sizeof(src), "rolled");
    struct ibv_sge sg = { (uintptr_t)src, 6, ms->lkey };
    struct ibv_send_wr w = { .wr_id = 5, .sg_list = &sg, .num_sge = 1, .opcode = IBV_WR_RDMA_WRITE,
                             .send_flags = IBV_SEND_SIGNALED, .wr = { .rdma = { (uintptr_t)dst, md->rkey } } }, *wb;
    int posted = !a->context->ops.post_send(a, &w, &wb), ok = 0;
    struct ibv_wc wc[8];
    int got = posted ? cq->context->ops.poll_cq(cq, 8, wc) : 0;
    for (int i = 0; i < got; i++) ok += wc[i].wr_id == 5 && wc[i].status == IBV_WC_SUCCESS;
    printf("written=%d data=%s\n", ok, strncmp(dst, "rolled", 6) ? "wrong" : "ok");
    return 0;
}
"""


class Ibv(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not shutil.which("gcc") or not os.path.exists(os.path.join(IBV_INC, "infiniband/verbs.h")) or \
                not os.path.exists(os.path.join(CUDA_INC, "cuda.h")):
            raise unittest.SkipTest("needs gcc, infiniband/verbs.h (IBV_INC) and cuda.h (CUDA_INC)")
        cls.tmp = tempfile.mkdtemp()
        t = cls.tmp
        for name, text in (("fake.c", FAKE), ("fake.map", FAKE_MAP), ("app.c", APP)):
            with open(os.path.join(t, name), "w") as f:
                f.write(text)
        inc = ["-I", IBV_INC]
        cls.fake = os.path.join(t, "libibverbs.so.1")
        subprocess.run(["gcc", "-shared", "-fPIC", *inc, "-Wl,--version-script=" + os.path.join(t, "fake.map"),
                        "-o", cls.fake, os.path.join(t, "fake.c")], check=True)
        # The same verbs without _ibv_query_gid_ex (libibverbs before rdma-core 32).
        os.mkdir(os.path.join(t, "old"))
        with open(os.path.join(t, "old", "fake.map"), "w") as f:
            f.write("\n".join(l for l in FAKE_MAP.splitlines() if "IBVERBS_1.11" not in l))
        cls.fake_old = os.path.join(t, "old", "libibverbs.so.1")
        subprocess.run(["gcc", "-shared", "-fPIC", "-DNO_GID_EX", *inc,
                        "-Wl,--version-script=" + os.path.join(t, "old", "fake.map"),
                        "-o", cls.fake_old, os.path.join(t, "fake.c")], check=True)
        subprocess.run(["gcc", "-O2", "-Wall", "-Wextra", "-Werror", "-fPIC", "-shared", "-fvisibility=hidden", *inc,
                        "-I", CUDA_INC, "-Wl,--version-script=" + os.path.join(SRC, "nvsnap_ibv.map"),
                        "-o", os.path.join(t, "libnvsnap_ibv.so"), os.path.join(SRC, "nvsnap_ibv.c"),
                        "-ldl", "-lpthread"], check=True)
        subprocess.run(["gcc", "-O2", "-Wall", "-Werror", *inc, "-o", os.path.join(t, "app"), os.path.join(t, "app.c"), "-ldl"], check=True)
        with open(os.path.join(t, "probe.c"), "w") as f:
            f.write(PROBE)
        with open(os.path.join(t, "cuda.c"), "w") as f:
            f.write(CUDA_STUB)
        with open(os.path.join(t, "rollback.c"), "w") as f:
            f.write(ROLLBACK)
        subprocess.run(["gcc", "-shared", "-fPIC", "-I", CUDA_INC, "-o", os.path.join(t, "libcuda.so.1"),
                        os.path.join(t, "cuda.c")], check=True)
        subprocess.run(["gcc", "-O2", "-Wall", "-Werror", *inc, "-o", os.path.join(t, "rollback"),
                        os.path.join(t, "rollback.c"), "-ldl"],
                       check=True)
        # The library and the GPU tool, as the agent image builds them.
        subprocess.run(["gcc", "-O2", "-Wall", "-Wextra", "-Werror", "-fpatchable-function-entry=4", "-fPIC", "-shared",
                        "-I", SRC, "-I", CUDA_INC, "-o", os.path.join(t, "libnvsnap_gpushare.so"),
                        os.path.join(SRC, "gpushare.c"), "-ldl", "-lpthread"], check=True)
        cls.tool = os.path.join(t, "nvsnap-gpu-suspend")
        subprocess.run(["gcc", "-O2", "-Wall", "-Wextra", "-Werror", "-I", SRC, "-I", CUDA_INC, "-o", cls.tool,
                        os.environ.get("TOOL_SRC", os.path.join(SRC, "nvsnap-gpu-suspend.c")), "-ldl", "-lpthread"],
                       check=True)
        subprocess.run(["gcc", "-O2", "-Wall", "-Wextra", "-Werror", "-Wno-unused-function", *inc, "-I", CUDA_INC,
                        "-I", SRC, "-rdynamic", "-o", os.path.join(t, "probe"), os.path.join(t, "probe.c"), "-ldl",
                        "-lpthread"],
                       check=True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def test_restore_on_another_nic(self):
        # The fake verbs, whatever NVSNAP_IBV_REAL_LIB the caller has.
        p = subprocess.run([os.path.join(self.tmp, "app"), os.path.join(self.tmp, "libnvsnap_ibv.so")],
                           capture_output=True, text=True, timeout=60,
                           env=dict(os.environ, LD_LIBRARY_PATH=self.tmp, NVSNAP_IBV_REAL_LIB=self.fake))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        rounds = [dict(kv.split("=", 1) for kv in l.split() if "=" in kv) for l in p.stdout.splitlines()]
        for r in (0, 1):  # two checkpoints in a row
            sus = [x for x in rounds if x.get("round") == str(r) and "open_after_suspend" in x][0]
            self.assertEqual(sus["open_after_suspend"], "0", p.stdout)
            # Each open device's command and event files, all of them even
            # past max; none once closed.
            fd = [x for x in rounds if x.get("round") == str(r) and "fds" in x][0]
            self.assertEqual((fd["fds"], fd["fds_cut"], fd["fds_devnull"], fd["fds_after_suspend"]),
                             ("4", "4", "4", "0"), p.stdout)
        self.assertIn("same_numbers=1", p.stdout)  # two NICs, the same keys and queue pair numbers
        done = [x for x in rounds if "held" in x]
        self.assertEqual(len(done), 4, p.stdout + p.stderr)  # 2 rounds x 2 devices
        for d in done:
            self.assertEqual(d, {"held": "1", "wrote": "1", "recvd": "1", "bad": "0", "data": "ok"}, p.stdout + p.stderr)
        moved = [x for x in rounds if "lines" in x]
        self.assertGreaterEqual(int(moved[0]["lines"]), 10, p.stdout)  # per device: a port, 2 keys, 2 queue pairs

    def probe(self, case, **env):
        e = dict(os.environ, LD_LIBRARY_PATH=self.tmp, NVSNAP_IBV_REAL_LIB=self.fake)
        e.update(env)
        p = subprocess.run([os.path.join(self.tmp, "probe"), case], capture_output=True, text=True, timeout=60, env=e)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        return dict(kv.split("=", 1) for line in p.stdout.splitlines() for kv in line.split() if "=" in kv)

    def test_lookup_sees_one_table_while_it_grows(self):
        out = self.probe("maprace")
        self.assertEqual(out["reads_ok"], "1", out)
        self.assertEqual(out["misses"], "0", "a lookup missed a key while the table grew")

    def test_call_in_the_provider_finishes_during_suspend(self):
        out = self.probe("portdrain")
        self.assertEqual((out["fence_waiting"], out["inflight_seen"]), ("1", "1"), out)
        self.assertEqual(out["returned_before_query"], "0", "the fence returned with a query still in the provider: %s" % out)
        self.assertEqual(out["fence"], "0", "a port query that finished during the suspend made it fail: %s" % out)

    def test_creation_counts_until_registered(self):
        out = self.probe("pdgate")
        self.assertEqual(out["inflight_while_registering"], "1", "a created object left the gate before its shadow")
        self.assertEqual((out["suspend"], out["live_pds"]), ("0", "0"), out)

    def test_fence_writes_never_reach_nccl(self):
        out = self.probe("markers")
        self.assertNotEqual(out["fence"], "0", out)
        self.assertEqual(out["fence_again"], "0", out)
        self.assertEqual(out["markers_to_nccl"], "0", "a fence write's completion went to NCCL")

    def test_failed_completion_refuses_the_checkpoint(self):
        out = self.probe("failedwc")
        self.assertEqual((out["polled"], out["status_failed"]), ("1", "1"), out)
        self.assertEqual(out["receives_kept"], "1", "a failed send's completion retired a receive")
        self.assertNotEqual(out["fence"], "0", "a queue pair with a failed completion was checkpointed")

    def test_suspend_lets_event_acknowledgements_finish(self):
        out = self.probe("events")
        self.assertEqual(out["event_shadow"], "1", out)
        self.assertEqual((out["suspend"], out["qps_destroyed"]), ("0", "1"), out)

    def test_suspend_refuses_unacknowledged_events(self):
        out = self.probe("events_noack")
        self.assertNotEqual(out["suspend"], "0", out)
        self.assertEqual((out["qps_destroyed"], out["reason"]), ("0", "unacked"), out)
        self.assertLess(int(out["took_ms"]), 2900, out)

    def test_async_wait_that_outlived_a_restore_polls_again(self):
        out = self.probe("asyncgen", ASYNCGEN="1")
        self.assertEqual(out["provider_reads"], "0", "read the replaced context's events without polling it")
        self.assertEqual(out["repolled"], "1", out)

    def test_new_registration_after_restore_gets_its_own_key(self):
        out = self.probe("newkey")
        self.assertEqual(out["real_collides"], "1", out)
        self.assertEqual((out["old_ok"], out["new_ok"], out["distinct"]), ("1", "1", "1"), out)

    def test_retried_restore_sets_up_a_failed_queue_pair_again(self):
        out = self.probe("retry")
        self.assertNotEqual(out["first"], "0", out)
        self.assertEqual((out["retry"], out["state_init"], out["receives"]), ("0", "1", "1"), out)

    def test_retried_connect_goes_on_from_rtr(self):
        out = self.probe("remapretry")
        self.assertNotEqual(out["first"], "0", out)
        self.assertEqual((out["retry"], out["state_rts"]), ("0", "1"), out)

    def test_sends_after_the_fence_wait_for_the_restore(self):
        out = self.probe("fenceheld")
        self.assertEqual(out["sent_before_release"], "0", "a send went out after the fence, before release")
        self.assertEqual((out["sent_after_restore"], out["completed"]), ("1", "1"), out)
        self.assertEqual(out["sent_after_open"], "1", out)
        self.assertEqual(out["held_after_release"], "1", "calls ran again with the real objects destroyed")

    def test_receive_with_too_many_entries_refuses_the_checkpoint(self):
        self.assertNotEqual(self.probe("sge5")["fence"], "0")

    def test_release_refused_after_the_fence_keeps_nccl_held(self):
        out = self.probe("refusefenced")
        self.assertEqual(out["fence"], "0", out)
        self.assertNotEqual(out["suspend"], "0", out)
        self.assertEqual((out["held_after_refusal"], out["held_after_open"], out["sent"]), ("1", "1", "0"),
                         "a rank whose peers may be releasing let NCCL post again: %s" % out)
        self.assertEqual(out["doomed"], "1", out)

    def test_doom_while_an_open_is_deciding_still_wins(self):
        out = self.probe("openrace")
        self.assertEqual((out["fence"], out["paused"], out["doom"]), ("0", "1", "1"), out)
        self.assertEqual((out["doomed"], out["held"], out["sent"]), ("1", "1", "0"),
                         "an open decided before the doom let NCCL's calls go after it: %s" % out)

    def test_release_that_could_not_run_after_the_fence_keeps_nccl_held(self):
        out = self.probe("doomiffenced")
        self.assertEqual(out, {"fence": "0", "doom": "1", "doomed": "1", "held_after_open": "1"}, out)
        out = self.probe("doomunfenced")
        self.assertEqual(out, {"fence": "0", "doom": "0", "doomed": "0", "held_after_open": "0"}, out)

    def test_release_timing_out_after_the_fence_never_lets_nccl_go(self):
        out = self.probe("refusetimeout")
        self.assertEqual((out["fence"], out["timeout"]), ("0", "1"), out)
        self.assertNotEqual(out["suspend"], "0", out)
        self.assertEqual((out["opens"], out["held"]), ("0", "1"),
                         "the release's timeout let NCCL's held calls go after the fence: %s" % out)

    def test_release_refused_without_a_fence_lets_nccl_run(self):
        out = self.probe("refuseunfenced")
        self.assertNotEqual(out["suspend"], "0", out)
        self.assertEqual((out["held_after_refusal"], out["held_after_open"], out["sent"]), ("0", "0", "1"), out)
        self.assertEqual(out["doomed"], "0", out)

    def test_tool_rollback_of_a_failed_destroy_reaches_the_new_peer(self):
        env = dict(os.environ, LD_LIBRARY_PATH=self.tmp, NVSNAP_IBV_REAL_LIB=self.fake, NVSNAP_GPUSHARE_CTL_ON_CUDA_USE="1",
                   LD_PRELOAD=os.path.join(self.tmp, "libnvsnap_gpushare.so"))
        w = subprocess.Popen([os.path.join(self.tmp, "rollback")], env=env, stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, text=True)
        self.addCleanup(w.wait)
        self.addCleanup(w.kill)
        sel = selectors.DefaultSelector()
        sel.register(w.stdout, selectors.EVENT_READ)
        line = w.stdout.readline().strip() if sel.select(30) else "<nothing in 30 s>"
        sel.close()
        self.assertEqual(line, "ready", line)
        # The suspend's release fails (one queue pair cannot be destroyed),
        # and the tool rolls the process back.
        s = subprocess.run([self.tool, "--timeout-ms", "10000", "suspend", str(w.pid)], capture_output=True, text=True,
                           timeout=120, env=dict(os.environ, LD_LIBRARY_PATH=self.tmp))
        out = s.stdout + s.stderr
        self.assertNotEqual(s.returncode, 0, out)
        self.assertIn("could not be destroyed", out)
        self.assertIn("suspend failed; rolled back, the workload runs as before", out)
        res, _ = w.communicate("go\n", timeout=60)
        self.assertIn("written=1 data=ok", res,
                      "after the tool's rollback, the queue pair whose destroy failed does not reach its peer:\n" + out + res)

    def test_queue_pair_left_by_a_failed_destroy_reaches_its_new_peer(self):
        out = self.probe("destroyfail")
        self.assertNotEqual(out["suspend"], "0", out)
        self.assertEqual((out["restore"], out["connect"]), ("0", "0"), out)
        self.assertEqual(out["completed"], "1", "a queue pair left by the release still targets its peer's old number")

    def test_receive_recorded_while_the_fence_waits_refuses_the_checkpoint(self):
        self.assertNotEqual(self.probe("sge5drain")["fence"], "0",
                            "a receive cut short while the fence waited for its call was checkpointed")

    def test_restore_picks_a_gid_of_the_same_roce_type(self):
        out = self.probe("roce_type")
        self.assertEqual(out["restore"], "0", out)
        self.assertEqual((out["sgid_a"], out["sgid_b"]), ("4", "4"),
                         "the restore took a GID of the same address family but another RoCE type: %s" % out)
        self.assertEqual(out["completed"], "1", out)

    def test_restore_reads_only_the_gids_queue_pairs_use(self):
        # NCCL read 24 entries; one is in use. The others are not searched
        # for on the new port (up to 256 reads each).
        out = self.probe("roce_type")
        self.assertEqual(out["restore"], "0", out)
        self.assertLess(int(out["restore_gid_queries"]), 16, out)

    def test_restore_without_a_gid_of_the_same_roce_type_is_refused(self):
        out = self.probe("roce_none")
        self.assertNotEqual(out["restore"], "0", "a restore with no GID of the captured RoCE type went on: %s" % out)
        self.assertEqual(out["restore_error"], "nomatch", out)

    def test_restore_without_the_captured_gid_type_is_refused(self):
        out = self.probe("roce_unknown")
        self.assertNotEqual(out["restore"], "0", "a restore whose captured GID type is unknown went on: %s" % out)
        self.assertEqual(out["restore_error"], "unknown", out)

    def sysfs(self):
        root = tempfile.mkdtemp(dir=self.tmp)
        for d in ("fake0", "fake1"):
            os.makedirs(os.path.join(root, d, "ports/1/gid_attrs/types"))
        return root

    def test_restore_reads_the_gid_type_from_sysfs_without_the_query(self):
        # A libibverbs without _ibv_query_gid_ex: the types NCCL reads.
        out = self.probe("roce_type", NVSNAP_IBV_REAL_LIB=self.fake_old, FAKE_SYSFS=self.sysfs())
        self.assertEqual(tuple(out.get(k) for k in ("restore", "sgid_a", "sgid_b", "completed")), ("0", "4", "4", "1"), out)
        out = self.probe("roce_none", NVSNAP_IBV_REAL_LIB=self.fake_old, FAKE_SYSFS=self.sysfs())
        self.assertEqual(out.get("restore_error"), "nomatch", out)

    def test_restore_reads_the_gid_type_from_sysfs_when_the_query_fails(self):
        out = self.probe("roce_type", PROBE_GIDEX_FAIL="1", FAKE_SYSFS=self.sysfs())
        self.assertEqual(tuple(out.get(k) for k in ("restore", "sgid_a", "sgid_b", "completed")), ("0", "4", "4", "1"), out)

    def test_restore_on_an_infiniband_port_needs_no_type_query(self):
        out = self.probe("roce_ib", NVSNAP_IBV_REAL_LIB=self.fake_old, PROBE_IB="1")
        self.assertEqual(tuple(out.get(k) for k in ("restore", "sgid_a", "sgid_b", "completed")), ("0", "3", "3", "1"), out)

    def test_restore_without_the_query_or_sysfs_is_refused(self):
        out = self.probe("roce_type", NVSNAP_IBV_REAL_LIB=self.fake_old)
        self.assertNotEqual(out["restore"], "0", out)
        self.assertEqual(out["restore_error"], "unknown", out)

    def test_untracked_queue_pair_refuses_the_checkpoint(self):
        out = self.probe("noslot")
        self.assertEqual(out["created"], "1", out)
        self.assertNotEqual(out["fence"], "0", out)

    def test_open_without_a_wake_fails(self):
        out = self.probe("noeventfd")
        self.assertEqual((out["opened"], out["errno_set"], out["contexts"]), ("0", "1", "0"), out)


if __name__ == "__main__":
    unittest.main(verbosity=2)
