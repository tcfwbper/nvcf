/* SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * libnvsnap_ibv.so: NCCL's verbs, made checkpointable.
 *
 * NCCL keeps its RDMA devices open, with queue pairs, memory registrations
 * and completion queues, until the process exits, and CRIU cannot dump
 * any of them. NCCL loads its verbs from the library NCCL_IBVERBS_LIB
 * names; pointed at this one, it gets shadow objects whose fields it
 * reads (queue pair numbers, memory keys, port LID and GIDs) keep their
 * values for the life of the process, while the real objects underneath
 * are destroyed before a checkpoint and created again after the restore,
 * on whatever NICs the process then has. The keys and queue pair numbers
 * NCCL sent its peers stay valid: the library translates them, on every
 * post, to the real ones, which the processes of the job exchange when
 * they are restored (gpushare's "fabmap" / "remap" lists).
 *
 * Only NCCL loads this library (dlopen, local), so other users of verbs in
 * the process (UCX, NIXL) are untouched. gpushare drives it from its
 * control thread: nvsnap_ibv_suspend at "release", nvsnap_ibv_restore at
 * "load", nvsnap_ibv_map at "fabmap", nvsnap_ibv_connect at "remap" and
 * nvsnap_ibv_open at "resume".
 *
 * Handled: reliable-connected queue pairs, completion queues without a
 * completion channel, memory registered with ibv_reg_mr, ibv_reg_mr_iova2
 * or ibv_reg_dmabuf_mr (GPU memory through a CUDA dma-buf), RoCE and IB.
 * A shared receive queue, an address handle or another queue pair type, a
 * receive with more than RECV_SGE scatter entries, a queue pair with a
 * failed completion, or an async event NCCL does not acknowledge makes the
 * suspend refuse. A refusal at the fence ("quiesce"), or at a release with no
 * fence before it, lets the workload run on. One at a release after a fence
 * (every rank fenced, its peers may be releasing) keeps NCCL's calls held
 * for good, and the process must be replaced (nvsnap_ibv_doomed).
 */
#define _GNU_SOURCE
#include <cuda.h>
#include <dlfcn.h>
#include <errno.h>
#include <infiniband/verbs.h>
#include <poll.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <time.h>
#include <unistd.h>

#define V11 "IBVERBS_1.1"

static void logf_(const char *fmt, ...)
{
    char b[512];
    va_list ap;
    va_start(ap, fmt);
    int n = snprintf(b, sizeof(b), "[nvsnap-ibv %d] ", getpid());
    vsnprintf(b + n, sizeof(b) - n - 1, fmt, ap);
    va_end(ap);
    strcat(b, "\n");
    ssize_t __attribute__((unused)) w = write(2, b, strlen(b));
}

/* ── the real verbs ───────────────────────────────────────────────────── */

static void *real_lib;
static struct {
    struct ibv_device **(*get_device_list)(int *);
    void (*free_device_list)(struct ibv_device **);
    const char *(*get_device_name)(struct ibv_device *);
    struct ibv_context *(*open_device)(struct ibv_device *);
    int (*close_device)(struct ibv_context *);
    int (*get_async_event)(struct ibv_context *, struct ibv_async_event *);
    void (*ack_async_event)(struct ibv_async_event *);
    int (*query_device)(struct ibv_context *, struct ibv_device_attr *);
    int (*query_port)(struct ibv_context *, uint8_t, struct _compat_ibv_port_attr *);
    int (*query_gid)(struct ibv_context *, uint8_t, int, union ibv_gid *);
    int (*query_pkey)(struct ibv_context *, uint8_t, int, __be16 *);
    int (*query_qp)(struct ibv_qp *, struct ibv_qp_attr *, int, struct ibv_qp_init_attr *);
    struct ibv_pd *(*alloc_pd)(struct ibv_context *);
    int (*dealloc_pd)(struct ibv_pd *);
    struct ibv_mr *(*reg_mr)(struct ibv_pd *, void *, size_t, int);
    struct ibv_mr *(*reg_mr_iova2)(struct ibv_pd *, void *, size_t, uint64_t, unsigned);
    struct ibv_mr *(*reg_dmabuf_mr)(struct ibv_pd *, uint64_t, size_t, uint64_t, int, int);
    int (*dereg_mr)(struct ibv_mr *);
    struct ibv_cq *(*create_cq)(struct ibv_context *, int, void *, struct ibv_comp_channel *, int);
    int (*destroy_cq)(struct ibv_cq *);
    struct ibv_qp *(*create_qp)(struct ibv_pd *, struct ibv_qp_init_attr *);
    int (*modify_qp)(struct ibv_qp *, struct ibv_qp_attr *, int);
    int (*destroy_qp)(struct ibv_qp *);
    int (*fork_init)(void);
    const char *(*event_type_str)(enum ibv_event_type);
    int (*query_ece)(struct ibv_qp *, struct ibv_ece *);
    int (*set_ece)(struct ibv_qp *, struct ibv_ece *);
    int (*query_port_speed)(struct ibv_context *, uint8_t, uint64_t *);
} R;

static pthread_once_t load_once = PTHREAD_ONCE_INIT;

static void load_real(void)
{
    const char *p = getenv("NVSNAP_IBV_REAL_LIB");
    real_lib = dlopen(p && *p ? p : "libibverbs.so.1", RTLD_NOW | RTLD_LOCAL);
    if (!real_lib) { logf_("cannot load the real verbs: %s", dlerror()); return; }
#define L(f, v) (*(void **)&R.f = dlvsym(real_lib, "ibv_" #f, v))
    L(get_device_list, V11); L(free_device_list, V11); L(get_device_name, V11);
    L(open_device, V11); L(close_device, V11); L(get_async_event, V11); L(ack_async_event, V11);
    L(query_device, V11); L(query_port, V11); L(query_gid, V11); L(query_pkey, V11);
    L(query_qp, V11); L(alloc_pd, V11); L(dealloc_pd, V11); L(reg_mr, V11);
    L(reg_mr_iova2, "IBVERBS_1.8"); L(reg_dmabuf_mr, "IBVERBS_1.12"); L(dereg_mr, V11);
    L(create_cq, V11); L(destroy_cq, V11); L(create_qp, V11); L(modify_qp, V11);
    L(destroy_qp, V11); L(fork_init, V11); L(event_type_str, V11);
    L(query_ece, "IBVERBS_1.10"); L(set_ece, "IBVERBS_1.10"); L(query_port_speed, "IBVERBS_1.16");
#undef L
}

static int real_ok(void)
{
    pthread_once(&load_once, load_real);
    return real_lib && R.open_device;
}

/* ── shadow objects ───────────────────────────────────────────────────── */

/* Each shadow starts with the struct NCCL sees. */
struct vctx;
/* One generation of a translation table: readers load the pointer once. */
struct u32tab { int cap, used; uint32_t *k, *v; };
struct u32map { struct u32tab *t; };
struct vgid { uint8_t port; int index; union ibv_gid old, cur; int cur_index; };
struct vport { uint8_t port; uint16_t old_lid, cur_lid; int queried; };
struct vctx {
    struct ibv_context v;
    struct ibv_device dev;          /* a copy: NCCL reads its name */
    struct ibv_context *real;
    unsigned gen;                   /* changes whenever real does */
    int wake;                       /* eventfd: wakes the async event wait */
    struct vgid *gids; int n_gids;
    struct vport *ports; int n_ports;
    /* Keys and queue pair numbers are unique on one device only. */
    struct u32map lkeys;    /* NCCL's lkey -> real lkey */
    struct u32map qpn_v;    /* real qpn -> NCCL's qpn */
    struct u32map qpn_slot; /* real qpn -> slot in qslots, which never move (read without a lock) */
};
struct vpd { struct ibv_pd v; struct vctx *c; struct ibv_pd *real; };
enum { MR_PLAIN, MR_IOVA2, MR_DMABUF };
struct vmr {
    struct ibv_mr v; struct vpd *pd; struct ibv_mr *real;
    int kind, access; uint64_t iova, dmabuf_off; size_t dmabuf_size;
};
struct vcq {
    struct ibv_cq v; struct vctx *c; struct ibv_cq *real; int comp_vector;
    struct ibv_wc *held; int n_held, cap_held;  /* completions polled at suspend, for NCCL */
};
#define RECV_SGE 4  /* a receive with more makes the process uncheckpointable */
struct recv_rec { uint64_t wr_id; int n; struct ibv_sge sg[RECV_SGE]; };
struct vqp {
    struct ibv_qp v; struct vpd *pd; struct ibv_qp *real;
    struct ibv_qp_init_attr init;
    struct ibv_qp_attr attr[3]; int mask[3];  /* INIT, RTR, RTS, as NCCL set them */
    struct ibv_ece ece; int has_ece;
    struct recv_rec *recvs; int n_recvs, cap_recvs;  /* posted, not completed */
    pthread_mutex_t rmu;
    int remote;                                     /* index into rports after connect */
    int markers;     /* fence writes posted and not completed yet (omu, or the gate closed) */
    int fence_wait;  /* the current fence waits for this one's markers */
    int failed;      /* a completion failed: which receives finished is unknown */
    int slot;
    int fresh;  /* re-created by a restore: 1 needs RTR and RTS, 2 RTS */
};

#define PUSHP(arr, n, x) do { \
        void *t_ = realloc(arr, ((n) + 1) * sizeof(*(arr))); \
        if (!t_) abort(); \
        (arr) = t_; \
        (arr)[(n)++] = (x); \
    } while (0)

static pthread_mutex_t omu = PTHREAD_MUTEX_INITIALIZER;  /* object lists */
static struct vctx **ctxs; static int n_ctxs;
static struct vpd **pds; static int n_pds;
static struct vmr **mrs; static int n_mrs;
static struct vcq **cqs; static int n_cqs;
static struct vqp **qps; static int n_qps;
static int unsupported;  /* an object this library cannot re-create */

static void drop(void **arr, int *n, void *x)
{
    for (int i = 0; i < *n; i++)
        if (arr[i] == x) { arr[i] = arr[--*n]; return; }
}

/* ── the data path gate ───────────────────────────────────────────────── */

/* suspended: the real objects are gone (or going), or a fence holds them
 * until release; every call that uses a real object waits while it is not
 * 0. It is one word of two bits, changed only atomically: G_HELD, and
 * G_DOOM (held for good, see doom()), which reopen() never clears: a doom
 * and an open racing each other always end held. Each such call
 * stays in (inflight) until its shadow is up to date, so a suspend that
 * has waited for inflight to reach 0 sees every object there is.
 * translated: the real keys and queue pair numbers differ from NCCL's.
 * released: the real objects are gone until a restore brings them back. */
static int suspended, translated, inflight, released;
enum { G_HELD = 1, G_DOOM = 2 };
static pthread_mutex_t gmu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t gcv = PTHREAD_COND_INITIALIZER;

static void enter(void)
{
    for (;;) {
        __atomic_add_fetch(&inflight, 1, __ATOMIC_SEQ_CST);
        if (!__atomic_load_n(&suspended, __ATOMIC_SEQ_CST)) return;
        __atomic_sub_fetch(&inflight, 1, __ATOMIC_SEQ_CST);
        pthread_mutex_lock(&gmu);
        while (__atomic_load_n(&suspended, __ATOMIC_SEQ_CST)) pthread_cond_wait(&gcv, &gmu);
        pthread_mutex_unlock(&gmu);
    }
}
static void leave(void) { __atomic_sub_fetch(&inflight, 1, __ATOMIC_SEQ_CST); }

/* ── key and queue pair number translation ────────────────────────────── */

static uint32_t hash32(uint32_t x) { x ^= x >> 16; x *= 0x7feb352d; x ^= x >> 15; x *= 0x846ca68b; return x ^ (x >> 16); }

/* Writers hold omu; readers on the data path take no lock. A table that
 * grows is a new generation (capacity and both arrays), published with one
 * pointer that a reader loads once; the old one is kept, never freed (they
 * are small: one entry per queue pair or registration), so a reader still
 * on it finishes there. Values are written before keys. */
static struct u32tab *tab_new(int cap)
{
    struct u32tab *t = calloc(1, sizeof(*t));
    if (!t || !(t->k = malloc(cap * 4)) || !(t->v = calloc(cap, 4))) abort();
    memset(t->k, 0xff, cap * 4);
    t->cap = cap;
    return t;
}

static void tab_put(struct u32tab *t, uint32_t k, uint32_t v)
{
    for (uint32_t i = hash32(k);; i++) {
        uint32_t s = i & (t->cap - 1);
        if (t->k[s] == 0xffffffff || t->k[s] == k) {
            if (t->k[s] == 0xffffffff) t->used++;
            __atomic_store_n(&t->v[s], v, __ATOMIC_SEQ_CST);
            __atomic_store_n(&t->k[s], k, __ATOMIC_SEQ_CST);
            return;
        }
    }
}

static void map_put(struct u32map *m, uint32_t k, uint32_t v)
{
    struct u32tab *t = m->t;
    if (!t || 2 * (t->used + 1) > t->cap) {
        struct u32tab *b = tab_new(t ? t->cap * 2 : 64);
        for (int i = 0; t && i < t->cap; i++) if (t->k[i] != 0xffffffff) tab_put(b, t->k[i], t->v[i]);
        __atomic_store_n(&m->t, b, __ATOMIC_SEQ_CST);
        t = b;
    }
    tab_put(t, k, v);
}

static void map_clear(struct u32map *m)
{
    if (m->t) memset(m->t->k, 0xff, m->t->cap * 4), m->t->used = 0;
}

static int map_get(const struct u32map *m, uint32_t k, uint32_t *v)
{
    const struct u32tab *t = __atomic_load_n(&m->t, __ATOMIC_SEQ_CST);
    if (!t) return -1;
    for (uint32_t i = hash32(k), n = 0; n < (uint32_t)t->cap; i++, n++) {
        uint32_t s = i & (t->cap - 1);
        uint32_t x = __atomic_load_n(&t->k[s], __ATOMIC_SEQ_CST);
        if (x == k) { *v = __atomic_load_n(&t->v[s], __ATOMIC_SEQ_CST); return 0; }
        if (x == 0xffffffff) return -1;
    }
    return -1;
}

/* A remote port, by the GID (and LID) its process had before the restore:
 * its new GID and LID, and its keys and queue pair numbers old -> new. */
struct rport { union ibv_gid old, cur; uint16_t old_lid, cur_lid; struct u32map keys, qpns; };
static struct rport *rports; static int n_rports;
#define QCHUNK 1024
static struct vqp **qslots[64]; static int n_qslots;

static int qslot_add(struct vqp *q)
{
    if (n_qslots >= 64 * QCHUNK) return -1;
    if (!qslots[n_qslots / QCHUNK] && !(qslots[n_qslots / QCHUNK] = calloc(QCHUNK, sizeof(struct vqp *)))) abort();
    qslots[n_qslots / QCHUNK][n_qslots % QCHUNK] = q;
    return n_qslots++;
}


static int rport_find(const union ibv_gid *gid, int lid)
{
    for (int i = 0; i < n_rports; i++)
        if (gid ? !memcmp(rports[i].old.raw, gid->raw, 16) : rports[i].old_lid == lid) return i;
    return -1;
}

/* ── verbs NCCL calls ─────────────────────────────────────────────────── */

#define EXPORT __attribute__((visibility("default")))

EXPORT struct ibv_device **(ibv_get_device_list)(int *n)
{
    if (!real_ok()) { if (n) *n = 0; errno = ENOSYS; return NULL; }
    return R.get_device_list(n);
}
EXPORT void ibv_free_device_list(struct ibv_device **l) { R.free_device_list(l); }
EXPORT const char *ibv_get_device_name(struct ibv_device *d) { return R.get_device_name(d); }
EXPORT int ibv_fork_init(void) { return real_ok() ? R.fork_init() : ENOSYS; }
EXPORT const char *ibv_event_type_str(enum ibv_event_type e) { return real_ok() ? R.event_type_str(e) : "?"; }

static int w_poll_cq(struct ibv_cq *cq, int n, struct ibv_wc *wc);
static int w_req_notify_cq(struct ibv_cq *cq, int solicited) { (void)cq; (void)solicited; return EOPNOTSUPP; }
static int w_post_send(struct ibv_qp *qp, struct ibv_send_wr *wr, struct ibv_send_wr **bad);
static int w_post_recv(struct ibv_qp *qp, struct ibv_recv_wr *wr, struct ibv_recv_wr **bad);
static int w_post_srq_recv(struct ibv_srq *s, struct ibv_recv_wr *wr, struct ibv_recv_wr **bad)
{ (void)s; (void)wr; *bad = wr; return EOPNOTSUPP; }

EXPORT struct ibv_context *ibv_open_device(struct ibv_device *dev)
{
    if (!real_ok()) { errno = ENOSYS; return NULL; }
    /* Without its wake, a suspend could not stop a thread waiting for its
     * async events before closing the real context. */
    int wake = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (wake < 0) return NULL;
    enter();
    struct ibv_context *r = R.open_device(dev);
    struct vctx *c = r ? calloc(1, sizeof(*c)) : NULL;
    if (!c) {
        int e = errno;
        if (r) R.close_device(r), e = ENOMEM;
        leave();
        close(wake);
        errno = e;
        return NULL;
    }
    c->real = r;
    c->dev = *dev;
    c->v.device = &c->dev;
    c->v.cmd_fd = -1;
    c->v.async_fd = -1;
    c->v.num_comp_vectors = r->num_comp_vectors;
    pthread_mutex_init(&c->v.mutex, NULL);
    c->v.ops.poll_cq = w_poll_cq;
    c->v.ops.req_notify_cq = w_req_notify_cq;
    c->v.ops.post_send = w_post_send;
    c->v.ops.post_recv = w_post_recv;
    c->v.ops.post_srq_recv = w_post_srq_recv;
    c->wake = wake;
    pthread_mutex_lock(&omu);
    PUSHP(ctxs, n_ctxs, c);
    pthread_mutex_unlock(&omu);
    leave();
    return &c->v;
}

/* Objects are created and destroyed inside the gate, the registry update
 * included (see suspended). */
EXPORT int ibv_close_device(struct ibv_context *ctx)
{
    struct vctx *c = (struct vctx *)ctx;
    enter();
    pthread_mutex_lock(&omu);
    drop((void **)ctxs, &n_ctxs, c);
    pthread_mutex_unlock(&omu);
    int r = c->real ? R.close_device(c->real) : 0;
    leave();
    if (c->wake >= 0) close(c->wake);
    free(c->gids); free(c->ports); free(c);
    return r;
}

EXPORT int ibv_query_device(struct ibv_context *ctx, struct ibv_device_attr *a)
{
    enter(); int r = R.query_device(((struct vctx *)ctx)->real, a); leave(); return r;
}

static struct vport *port_of(struct vctx *c, uint8_t port)
{
    for (int i = 0; i < c->n_ports; i++) if (c->ports[i].port == port) return &c->ports[i];
    struct vport p = { .port = port };
    PUSHP(c->ports, c->n_ports, p);
    return &c->ports[c->n_ports - 1];
}

/* NCCL sends its peers the LID and GID it reads once: keep showing those. */
EXPORT int (ibv_query_port)(struct ibv_context *ctx, uint8_t port, struct _compat_ibv_port_attr *attr)
{
    struct vctx *c = (struct vctx *)ctx;
    enter();
    int r = R.query_port(c->real, port, attr);
    if (!r) {
        struct ibv_port_attr *a = (struct ibv_port_attr *)attr;
        pthread_mutex_lock(&omu);
        struct vport *p = port_of(c, port);
        if (!p->queried) p->queried = 1, p->old_lid = p->cur_lid = a->lid;
        a->lid = p->old_lid;
        pthread_mutex_unlock(&omu);
    }
    leave();
    return r;
}

EXPORT int ibv_query_gid(struct ibv_context *ctx, uint8_t port, int index, union ibv_gid *gid)
{
    struct vctx *c = (struct vctx *)ctx;
    pthread_mutex_lock(&omu);
    for (int i = 0; i < c->n_gids; i++)
        if (c->gids[i].port == port && c->gids[i].index == index) {
            *gid = c->gids[i].old;
            pthread_mutex_unlock(&omu);
            return 0;
        }
    pthread_mutex_unlock(&omu);
    enter();
    int r = R.query_gid(c->real, port, index, gid);
    leave();
    if (!r) {
        struct vgid g = { .port = port, .index = index, .old = *gid, .cur = *gid, .cur_index = index };
        pthread_mutex_lock(&omu);
        PUSHP(c->gids, c->n_gids, g);
        pthread_mutex_unlock(&omu);
    }
    return r;
}

EXPORT int ibv_query_pkey(struct ibv_context *ctx, uint8_t port, int index, __be16 *pkey)
{
    enter(); int r = R.query_pkey(((struct vctx *)ctx)->real, port, index, pkey); leave(); return r;
}

int ibv_query_port_speed(struct ibv_context *ctx, uint8_t port, uint64_t *speed);
EXPORT int ibv_query_port_speed(struct ibv_context *ctx, uint8_t port, uint64_t *speed)
{
    if (!R.query_port_speed) return EOPNOTSUPP;
    enter(); int r = R.query_port_speed(((struct vctx *)ctx)->real, port, speed); leave(); return r;
}

EXPORT struct ibv_pd *ibv_alloc_pd(struct ibv_context *ctx)
{
    struct vctx *c = (struct vctx *)ctx;
    enter();
    struct ibv_pd *r = R.alloc_pd(c->real);
    struct vpd *p = r ? calloc(1, sizeof(*p)) : NULL;
    if (p) {
        p->c = c; p->real = r; p->v.context = ctx; p->v.handle = r->handle;
        pthread_mutex_lock(&omu);
        PUSHP(pds, n_pds, p);
        pthread_mutex_unlock(&omu);
    } else if (r) {
        R.dealloc_pd(r), errno = ENOMEM;
    }
    leave();
    return p ? &p->v : NULL;
}

EXPORT int ibv_dealloc_pd(struct ibv_pd *pd)
{
    struct vpd *p = (struct vpd *)pd;
    enter();
    pthread_mutex_lock(&omu);
    drop((void **)pds, &n_pds, p);
    pthread_mutex_unlock(&omu);
    int r = p->real ? R.dealloc_pd(p->real) : 0;
    leave();
    free(p);
    return r;
}

static int key_used(struct u32map *m, uint32_t k)
{
    uint32_t v;
    return !map_get(m, k, &v);
}

/* Called inside the gate (the caller registered r there). Once a restore
 * has translated keys, a new registration's real lkey may be a key NCCL
 * still uses for an older one (whose real key is now another): it then
 * gets a visible lkey of its own. An lkey never leaves the process, so any
 * unused value does. Every lkey has its entry from then on, so a visible
 * key is never taken for a real one. (An rkey goes to peers as it is.) */
static struct ibv_mr *new_mr(struct vpd *p, struct ibv_mr *r, int kind, int access, uint64_t iova,
                             uint64_t off, size_t dsize, void *addr, size_t len)
{
    if (!r) return NULL;
    struct vmr *m = calloc(1, sizeof(*m));
    if (!m) { R.dereg_mr(r); errno = ENOMEM; return NULL; }
    m->pd = p; m->real = r; m->kind = kind; m->access = access; m->iova = iova;
    m->dmabuf_off = off; m->dmabuf_size = dsize;
    m->v = *r;
    m->v.context = &p->c->v; m->v.pd = &p->v; m->v.addr = addr; m->v.length = len;
    pthread_mutex_lock(&omu);
    struct u32map *lk = &p->c->lkeys;
    if (lk->t) {
        static uint32_t next = 0x80000000u;
        if (key_used(lk, m->v.lkey) && __atomic_load_n(&translated, __ATOMIC_SEQ_CST))
            do m->v.lkey = next++; while (m->v.lkey == 0xffffffff || key_used(lk, m->v.lkey));
        map_put(lk, m->v.lkey, r->lkey);
    }
    PUSHP(mrs, n_mrs, m);
    pthread_mutex_unlock(&omu);
    return &m->v;
}

EXPORT struct ibv_mr *(ibv_reg_mr)(struct ibv_pd *pd, void *addr, size_t len, int access)
{
    struct vpd *p = (struct vpd *)pd;
    enter();
    struct ibv_mr *r = new_mr(p, R.reg_mr(p->real, addr, len, access), MR_PLAIN, access, (uintptr_t)addr, 0, 0, addr, len);
    leave();
    return r;
}

EXPORT struct ibv_mr *ibv_reg_mr_iova2(struct ibv_pd *pd, void *addr, size_t len, uint64_t iova, unsigned access)
{
    struct vpd *p = (struct vpd *)pd;
    if (!R.reg_mr_iova2) { errno = EOPNOTSUPP; return NULL; }
    enter();
    struct ibv_mr *r = new_mr(p, R.reg_mr_iova2(p->real, addr, len, iova, access), MR_IOVA2, access, iova, 0, 0, addr, len);
    leave();
    return r;
}

/* GPU memory: NCCL registers [iova, iova + len) through a dma-buf of the
 * range that starts offset bytes before iova, then closes the dma-buf. The
 * restore gets a new dma-buf of the same range from CUDA. */
EXPORT struct ibv_mr *ibv_reg_dmabuf_mr(struct ibv_pd *pd, uint64_t off, size_t len, uint64_t iova, int fd, int access)
{
    struct vpd *p = (struct vpd *)pd;
    if (!R.reg_dmabuf_mr) { errno = EOPNOTSUPP; return NULL; }
    off_t size = lseek(fd, 0, SEEK_END);
    enter();
    struct ibv_mr *r = new_mr(p, R.reg_dmabuf_mr(p->real, off, len, iova, fd, access), MR_DMABUF, access, iova, off,
                              size > 0 ? (size_t)size : off + len, (void *)(uintptr_t)iova, len);
    leave();
    return r;
}

EXPORT int ibv_dereg_mr(struct ibv_mr *mr)
{
    struct vmr *m = (struct vmr *)mr;
    enter();
    pthread_mutex_lock(&omu);
    drop((void **)mrs, &n_mrs, m);
    pthread_mutex_unlock(&omu);
    int r = m->real ? R.dereg_mr(m->real) : 0;
    leave();
    free(m);
    return r;
}

EXPORT struct ibv_cq *ibv_create_cq(struct ibv_context *ctx, int cqe, void *cq_context,
                                    struct ibv_comp_channel *ch, int vec)
{
    struct vctx *c = (struct vctx *)ctx;
    if (ch) { errno = EOPNOTSUPP; return NULL; }  /* not re-creatable here: NCCL polls */
    enter();
    struct ibv_cq *r = R.create_cq(c->real, cqe, cq_context, NULL, vec);
    struct vcq *q = r ? calloc(1, sizeof(*q)) : NULL;
    if (q) {
        q->c = c; q->real = r; q->comp_vector = vec;
        q->v.context = ctx; q->v.cq_context = cq_context; q->v.cqe = r->cqe; q->v.handle = r->handle;
        pthread_mutex_init(&q->v.mutex, NULL);
        pthread_cond_init(&q->v.cond, NULL);
        pthread_mutex_lock(&omu);
        PUSHP(cqs, n_cqs, q);
        pthread_mutex_unlock(&omu);
    } else if (r) {
        R.destroy_cq(r), errno = ENOMEM;
    }
    leave();
    return q ? &q->v : NULL;
}

EXPORT int ibv_destroy_cq(struct ibv_cq *cq)
{
    struct vcq *q = (struct vcq *)cq;
    enter();
    pthread_mutex_lock(&omu);
    drop((void **)cqs, &n_cqs, q);
    pthread_mutex_unlock(&omu);
    int r = q->real ? R.destroy_cq(q->real) : 0;
    leave();
    free(q->held); free(q);
    return r;
}

EXPORT struct ibv_qp *ibv_create_qp(struct ibv_pd *pd, struct ibv_qp_init_attr *ia)
{
    struct vpd *p = (struct vpd *)pd;
    if (ia->srq || ia->qp_type != IBV_QPT_RC) {
        __atomic_store_n(&unsupported, 1, __ATOMIC_SEQ_CST);
        logf_("queue pair type %d%s: this process cannot be checkpointed", ia->qp_type, ia->srq ? " with an SRQ" : "");
    }
    struct ibv_qp_init_attr a = *ia;
    enter();
    a.send_cq = ((struct vcq *)ia->send_cq)->real;
    a.recv_cq = ((struct vcq *)ia->recv_cq)->real;
    struct ibv_qp *r = R.create_qp(p->real, &a);
    struct vqp *q = r ? calloc(1, sizeof(*q)) : NULL;
    if (!q) {
        if (r) R.destroy_qp(r), errno = ENOMEM;
        leave();
        return NULL;
    }
    ia->cap = a.cap;
    q->pd = p; q->real = r; q->init = *ia; q->remote = -1;
    pthread_mutex_init(&q->rmu, NULL);
    q->v = *r;
    q->v.context = pd->context; q->v.pd = pd; q->v.send_cq = ia->send_cq; q->v.recv_cq = ia->recv_cq;
    q->v.qp_context = ia->qp_context;
    pthread_mutex_init(&q->v.mutex, NULL);
    pthread_cond_init(&q->v.cond, NULL);
    pthread_mutex_lock(&omu);
    PUSHP(qps, n_qps, q);
    q->slot = qslot_add(q);
    if (q->slot < 0) logf_("queue pair %u: no slot left to track its receives: this process cannot be checkpointed", r->qp_num);
    map_put(&q->pd->c->qpn_v, r->qp_num, r->qp_num);
    if (q->slot >= 0) map_put(&q->pd->c->qpn_slot, r->qp_num, q->slot);
    pthread_mutex_unlock(&omu);
    leave();
    return &q->v;
}

EXPORT int ibv_modify_qp(struct ibv_qp *qp, struct ibv_qp_attr *a, int mask)
{
    struct vqp *q = (struct vqp *)qp;
    int k = !(mask & IBV_QP_STATE) ? -1 : a->qp_state == IBV_QPS_INIT ? 0 : a->qp_state == IBV_QPS_RTR ? 1
            : a->qp_state == IBV_QPS_RTS ? 2 : -1;
    enter();
    int r = R.modify_qp(q->real, a, mask);
    if (!r && k >= 0) q->attr[k] = *a, q->mask[k] = mask;
    if (!r && (mask & IBV_QP_STATE)) q->v.state = a->qp_state;
    leave();
    if (!r && k == 1) {  /* the port's GID identifies this queue pair to its peer's exchange */
        struct vctx *c = q->pd->c;
        int port = q->mask[0] & IBV_QP_PORT ? q->attr[0].port_num : 1;
        int idx = a->ah_attr.is_global ? a->ah_attr.grh.sgid_index : 0;
        union ibv_gid g;
        ibv_query_gid(&c->v, port, idx, &g);
        struct ibv_port_attr pa;
        (ibv_query_port)(&c->v, port, (struct _compat_ibv_port_attr *)&pa);
    }
    return r;
}

EXPORT int ibv_query_qp(struct ibv_qp *qp, struct ibv_qp_attr *a, int mask, struct ibv_qp_init_attr *ia)
{
    struct vqp *q = (struct vqp *)qp;
    enter(); int r = R.query_qp(q->real, a, mask, ia); leave();
    if (!r && ia) ia->send_cq = q->v.send_cq, ia->recv_cq = q->v.recv_cq, ia->srq = NULL;
    if (!r && (mask & IBV_QP_DEST_QPN) && q->mask[1] & IBV_QP_DEST_QPN) a->dest_qp_num = q->attr[1].dest_qp_num;
    return r;
}

EXPORT int ibv_destroy_qp(struct ibv_qp *qp)
{
    struct vqp *q = (struct vqp *)qp;
    enter();
    pthread_mutex_lock(&omu);
    drop((void **)qps, &n_qps, q);
    if (q->slot >= 0) __atomic_store_n(&qslots[q->slot / QCHUNK][q->slot % QCHUNK], NULL, __ATOMIC_SEQ_CST);
    pthread_mutex_unlock(&omu);
    int r = q->real ? R.destroy_qp(q->real) : 0;
    leave();
    pthread_mutex_lock(&q->rmu);
    free(q->recvs);  /* the shadow itself is kept: a poll may still be reading it */
    q->recvs = NULL, q->n_recvs = q->cap_recvs = 0;
    pthread_mutex_unlock(&q->rmu);
    return r;
}

EXPORT int ibv_query_ece(struct ibv_qp *qp, struct ibv_ece *ece)
{
    if (!R.query_ece) return EOPNOTSUPP;
    enter(); int r = R.query_ece(((struct vqp *)qp)->real, ece); leave(); return r;
}

EXPORT int ibv_set_ece(struct ibv_qp *qp, struct ibv_ece *ece)
{
    struct vqp *q = (struct vqp *)qp;
    if (!R.set_ece) return EOPNOTSUPP;
    enter();
    int r = R.set_ece(q->real, ece);
    if (!r) q->ece = *ece, q->has_ece = 1;
    leave();
    return r;
}

/* Events NCCL got for a queue pair or completion queue and has not
 * acknowledged yet, with the real object the acknowledgement goes to. The
 * provider's destroy of that object waits for it, so a suspend destroys
 * nothing while any is out, and an acknowledgement needs only amu, never
 * omu (which a suspend holds while it destroys). */
struct unacked { void *shadow, *real; };
static struct unacked *unacked; static int n_unacked;
static pthread_mutex_t amu = PTHREAD_MUTEX_INITIALIZER;

static void **event_obj(struct ibv_async_event *e, int *cq)
{
    switch (e->event_type) {
    case IBV_EVENT_CQ_ERR:
        *cq = 1;
        return (void **)&e->element.cq;
    case IBV_EVENT_QP_FATAL: case IBV_EVENT_QP_REQ_ERR: case IBV_EVENT_QP_ACCESS_ERR:
    case IBV_EVENT_COMM_EST: case IBV_EVENT_SQ_DRAINED: case IBV_EVENT_PATH_MIG:
    case IBV_EVENT_PATH_MIG_ERR: case IBV_EVENT_QP_LAST_WQE_REACHED:
        *cq = 0;
        return (void **)&e->element.qp;
    default:
        return NULL;
    }
}

static int unacked_count(void)
{
    pthread_mutex_lock(&amu);
    int n = n_unacked;
    pthread_mutex_unlock(&amu);
    return n;
}

/* Async events: wait on the real context's events and on this library's
 * wake, so that a suspend never closes a descriptor a thread waits on. The
 * context's generation, taken with the descriptor, tells a wait that
 * outlived a whole suspend and restore (the descriptor it saw ready is
 * gone, and the new context may have no event: its read would block while
 * holding the gate) from one that can read. */
EXPORT int ibv_get_async_event(struct ibv_context *ctx, struct ibv_async_event *ev)
{
    struct vctx *c = (struct vctx *)ctx;
    for (;;) {
        enter();
        unsigned gen = __atomic_load_n(&c->gen, __ATOMIC_SEQ_CST);
        struct pollfd p[2] = { { c->real->async_fd, POLLIN, 0 }, { c->wake, POLLIN, 0 } };
        leave();
        if (poll(p, 2, -1) < 0 && errno != EINTR) return -1;
        if (p[1].revents) { uint64_t x; ssize_t __attribute__((unused)) n = read(c->wake, &x, sizeof(x)); continue; }
        if (!(p[0].revents & POLLIN)) continue;  /* also a descriptor a suspend closed */
        enter();
        if (__atomic_load_n(&suspended, __ATOMIC_SEQ_CST) || __atomic_load_n(&c->gen, __ATOMIC_SEQ_CST) != gen) {
            leave();
            continue;
        }
        int r = R.get_async_event(c->real, ev);
        int cq;
        void **obj = r ? NULL : event_obj(ev, &cq);
        if (obj) {
            void *real = *obj;
            pthread_mutex_lock(&omu);
            for (int i = 0; cq && i < n_cqs; i++) if (cqs[i]->real == real) { *obj = &cqs[i]->v; break; }
            for (int i = 0; !cq && i < n_qps; i++) if (qps[i]->real == real) { *obj = &qps[i]->v; break; }
            pthread_mutex_unlock(&omu);
            if (*obj != real) {
                struct unacked u = { *obj, real };
                pthread_mutex_lock(&amu);
                PUSHP(unacked, n_unacked, u);
                pthread_mutex_unlock(&amu);
            }
        }
        leave();
        return r;
    }
}

EXPORT void ibv_ack_async_event(struct ibv_async_event *ev)
{
    struct ibv_async_event e = *ev;
    int cq;
    void **obj = event_obj(&e, &cq);
    if (obj) {
        pthread_mutex_lock(&amu);
        for (int i = 0; i < n_unacked; i++)
            if (unacked[i].shadow == *obj) { *obj = unacked[i].real; unacked[i] = unacked[--n_unacked]; break; }
        pthread_mutex_unlock(&amu);
    }
    R.ack_async_event(&e);
}

/* ── data path ────────────────────────────────────────────────────────── */

#define MARKER 0x6e76736e61706962ULL  /* wr_id of the suspend's fence writes */

static void recv_add(struct vqp *q, const struct ibv_recv_wr *w)
{
    if (w->num_sge > RECV_SGE && !__atomic_exchange_n(&unsupported, 1, __ATOMIC_SEQ_CST))
        logf_("queue pair %u: a receive with %d scatter entries (kept: %d): this process cannot be checkpointed",
              q->v.qp_num, w->num_sge, RECV_SGE);
    struct recv_rec r = { .wr_id = w->wr_id, .n = w->num_sge > RECV_SGE ? RECV_SGE : w->num_sge };
    memcpy(r.sg, w->sg_list, r.n * sizeof(r.sg[0]));
    pthread_mutex_lock(&q->rmu);
    if (q->n_recvs == q->cap_recvs) {
        q->cap_recvs = q->cap_recvs ? 2 * q->cap_recvs : 16;
        q->recvs = realloc(q->recvs, q->cap_recvs * sizeof(*q->recvs));
    }
    q->recvs[q->n_recvs++] = r;
    pthread_mutex_unlock(&q->rmu);
}

static void recv_done(struct vqp *q, uint64_t wr_id)
{
    pthread_mutex_lock(&q->rmu);
    for (int i = 0; i < q->n_recvs; i++)
        if (q->recvs[i].wr_id == wr_id) { memmove(&q->recvs[i], &q->recvs[i + 1], (--q->n_recvs - i) * sizeof(*q->recvs)); break; }
    pthread_mutex_unlock(&q->rmu);
}

static struct vqp *qp_by_real_num(struct vctx *c, uint32_t qpn)
{
    uint32_t s;
    if (map_get(&c->qpn_slot, qpn, &s)) return NULL;
    return __atomic_load_n(&qslots[s / QCHUNK][s % QCHUNK], __ATOMIC_SEQ_CST);
}

static uint32_t lkey_of(struct vctx *c, uint32_t k)
{
    uint32_t v;
    return map_get(&c->lkeys, k, &v) ? k : v;
}

static int w_post_send(struct ibv_qp *qp, struct ibv_send_wr *wr, struct ibv_send_wr **bad)
{
    struct vqp *q = (struct vqp *)qp;
    enter();
    int r;
    if (!__atomic_load_n(&translated, __ATOMIC_RELAXED)) {
        r = q->real->context->ops.post_send(q->real, wr, bad);
    } else {
        /* Copy the chain with real keys; NCCL's stays as it wrote it. */
        int nw = 0, ns = 0;
        for (struct ibv_send_wr *w = wr; w; w = w->next) nw++, ns += w->num_sge;
        struct ibv_send_wr *cw = alloca(nw * sizeof(*cw));
        struct ibv_sge *cs = alloca((ns ? ns : 1) * sizeof(*cs));
        struct rport *rp = q->remote >= 0 ? &rports[q->remote] : NULL;
        int i = 0, s = 0;
        for (struct ibv_send_wr *w = wr; w; w = w->next, i++) {
            cw[i] = *w;
            cw[i].next = w->next ? &cw[i + 1] : NULL;
            cw[i].sg_list = &cs[s];
            for (int j = 0; j < w->num_sge; j++, s++) {
                cs[s] = w->sg_list[j];
                if (!(w->send_flags & IBV_SEND_INLINE)) cs[s].lkey = lkey_of(q->pd->c, cs[s].lkey);
            }
            uint32_t k;
            if (rp && (w->opcode == IBV_WR_RDMA_WRITE || w->opcode == IBV_WR_RDMA_WRITE_WITH_IMM ||
                       w->opcode == IBV_WR_RDMA_READ) && !map_get(&rp->keys, w->wr.rdma.rkey, &k))
                cw[i].wr.rdma.rkey = k;
            else if (rp && (w->opcode == IBV_WR_ATOMIC_CMP_AND_SWP || w->opcode == IBV_WR_ATOMIC_FETCH_AND_ADD) &&
                     !map_get(&rp->keys, w->wr.atomic.rkey, &k))
                cw[i].wr.atomic.rkey = k;
        }
        struct ibv_send_wr *cb = NULL;
        r = q->real->context->ops.post_send(q->real, cw, &cb);
        if (r && cb) { struct ibv_send_wr *w = wr; for (struct ibv_send_wr *x = cw; x != cb; x = x->next) w = w->next; *bad = w; }
    }
    leave();
    return r;
}

static int w_post_recv(struct ibv_qp *qp, struct ibv_recv_wr *wr, struct ibv_recv_wr **bad)
{
    struct vqp *q = (struct vqp *)qp;
    enter();
    /* Recorded before posting (the completion may come first): receives a
     * suspend finds outstanding are posted again after it. */
    for (struct ibv_recv_wr *w = wr; w; w = w->next) recv_add(q, w);
    int r;
    if (!__atomic_load_n(&translated, __ATOMIC_RELAXED)) {
        r = q->real->context->ops.post_recv(q->real, wr, bad);
    } else {
        int nw = 0, ns = 0;
        for (struct ibv_recv_wr *w = wr; w; w = w->next) nw++, ns += w->num_sge;
        struct ibv_recv_wr *cw = alloca(nw * sizeof(*cw));
        struct ibv_sge *cs = alloca((ns ? ns : 1) * sizeof(*cs));
        int i = 0, s = 0;
        for (struct ibv_recv_wr *w = wr; w; w = w->next, i++) {
            cw[i] = *w;
            cw[i].next = w->next ? &cw[i + 1] : NULL;
            cw[i].sg_list = &cs[s];
            for (int j = 0; j < w->num_sge; j++, s++) cs[s] = w->sg_list[j], cs[s].lkey = lkey_of(q->pd->c, cs[s].lkey);
        }
        struct ibv_recv_wr *cb = NULL;
        r = q->real->context->ops.post_recv(q->real, cw, &cb);
        if (r && cb) { struct ibv_recv_wr *w = wr; for (struct ibv_recv_wr *x = cw; x != cb; x = x->next) w = w->next; *bad = w; }
    }
    if (r) for (struct ibv_recv_wr *w = *bad; w; w = w->next) recv_done(q, w->wr_id);
    leave();
    return r;
}

/* A completion polled from a real queue: 1 if it is a fence write of this
 * library's (counted off its queue pair; never NCCL's), else 0 after the
 * receive it completes is retired. A failed completion's opcode is not
 * defined (ibv_poll_cq(3)): which receive, if any, it ends is unknown, so
 * its queue pair is marked and the process is not checkpointed while it
 * lives (NCCL tears such a connection down). */
static int seen_wc(struct vcq *cq, const struct ibv_wc *w)
{
    struct vqp *q = qp_by_real_num(cq->c, w->qp_num);
    if (w->wr_id == MARKER && q && __atomic_load_n(&q->markers, __ATOMIC_SEQ_CST) > 0) {
        if (w->status != IBV_WC_SUCCESS) __atomic_store_n(&q->failed, 1, __ATOMIC_SEQ_CST);
        __atomic_sub_fetch(&q->markers, 1, __ATOMIC_SEQ_CST);
        return 1;
    }
    if (w->status != IBV_WC_SUCCESS) {
        if (q && !__atomic_exchange_n(&q->failed, 1, __ATOMIC_SEQ_CST))
            logf_("queue pair %u: completion status %d: this process cannot be checkpointed while it lives",
                  q->v.qp_num, w->status);
    } else if ((w->opcode & IBV_WC_RECV) && q) {
        recv_done(q, w->wr_id);
    }
    return 0;
}

/* Completions for NCCL: first those a suspend polled, then the real ones,
 * with NCCL's queue pair numbers, less the fence writes. */
static int w_poll_cq(struct ibv_cq *cq, int n, struct ibv_wc *wc)
{
    struct vcq *q = (struct vcq *)cq;
    enter();
    int got = 0;
    if (q->n_held) {
        got = q->n_held < n ? q->n_held : n;
        memcpy(wc, q->held, got * sizeof(*wc));
        memmove(q->held, q->held + got, (q->n_held - got) * sizeof(*wc));
        q->n_held -= got;
    }
    while (got < n) {
        int r = q->real->context->ops.poll_cq(q->real, n - got, wc + got);
        if (r < 0) { leave(); return got ? got : r; }
        if (!r) break;
        int k = got;
        for (int i = got; i < got + r; i++) {
            uint32_t v;
            if (seen_wc(q, &wc[i])) continue;
            if (!map_get(&q->c->qpn_v, wc[i].qp_num, &v)) wc[i].qp_num = v;
            wc[k++] = wc[i];
        }
        if (k == got + r) { got = k; break; }
        got = k;  /* fence writes left out: poll again for the room they took */
    }
    leave();
    return got;
}

/* ── suspend and restore ──────────────────────────────────────────────── */

static double now_s(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec + t.tv_nsec / 1e9;
}

static void hold_wc(struct vcq *q, const struct ibv_wc *w)
{
    if (q->n_held == q->cap_held) {
        q->cap_held = q->cap_held ? 2 * q->cap_held : 16;
        q->held = realloc(q->held, q->cap_held * sizeof(*q->held));
    }
    q->held[q->n_held++] = *w;
}

/* Every send posted before the suspend completes before the queue pair is
 * destroyed: a signaled zero-length write behind them (mark) completes
 * after them. Completions polled here are kept for NCCL. A fence write is
 * counted on its queue pair from its post to its completion, here or in a
 * later poll (after a fence that failed part way): NCCL never sees one. */
static int fence(char *reply, size_t n, int mark)
{
    int want = 0;
    for (int i = 0; i < n_qps; i++) qps[i]->fence_wait = 0;
    for (int i = 0; mark && i < n_qps; i++) {
        struct vqp *q = qps[i];
        if (getenv("NVSNAP_IBV_DEBUG")) {
            struct ibv_qp_attr qa;
            struct ibv_qp_init_attr qi;
            int qr = R.query_qp(q->real, &qa, IBV_QP_STATE | IBV_QP_CAP, &qi);
            logf_("fence: qp %u (real %u) state seen %d real %d (rc %d) max_send_wr %u access %#x dest %u",
                  q->v.qp_num, q->real->qp_num, q->v.state, qr ? -1 : (int)qa.qp_state, qr,
                  q->init.cap.max_send_wr, q->attr[0].qp_access_flags, q->attr[1].dest_qp_num);
        }
        if (q->v.state != IBV_QPS_RTS) continue;
        /* The peer must allow the fence's operation: NCCL's data queue
         * pairs allow remote writes, its GPU flush queue pair (connected
         * to itself) only remote reads. A zero-length one touches no
         * memory and needs no key. */
        int acc = q->mask[0] & IBV_QP_ACCESS_FLAGS ? q->attr[0].qp_access_flags : 0;
        if (!(acc & (IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ))) continue;
        struct ibv_send_wr w = { .wr_id = MARKER, .send_flags = IBV_SEND_SIGNALED,
                                 .opcode = acc & IBV_ACCESS_REMOTE_WRITE ? IBV_WR_RDMA_WRITE : IBV_WR_RDMA_READ }, *b;
        int e = q->real->context->ops.post_send(q->real, &w, &b);
        if (e) {
            snprintf(reply, n, "err fence write on queue pair %u: %s", q->v.qp_num, strerror(e > 0 ? e : errno));
            return -1;
        }
        __atomic_add_fetch(&q->markers, 1, __ATOMIC_SEQ_CST);
        q->fence_wait = 1;
        want++;
    }
    /* Then until no completion queue has anything left: a completion still
     * in a queue when it is destroyed never reaches NCCL. */
    double t0 = now_s();
    for (int last = 0; !last;) {
        last = want == 0;  /* the fences are in: passes until one finds nothing */
        for (int i = 0; i < n_cqs; i++) {
            struct ibv_wc wc[16];
            int r = cqs[i]->real->context->ops.poll_cq(cqs[i]->real, 16, wc);
            if (r != 0) last = 0;
            for (int j = 0; j < r; j++) {
                struct vqp *q = qp_by_real_num(cqs[i]->c, wc[j].qp_num);
                if (wc[j].status != IBV_WC_SUCCESS && getenv("NVSNAP_IBV_DEBUG"))
                    logf_("fence: completion wr_id %#llx qp %u status %d vendor_err %#x opcode %d",
                          (unsigned long long)wc[j].wr_id, wc[j].qp_num, wc[j].status, wc[j].vendor_err, wc[j].opcode);
                if (seen_wc(cqs[i], &wc[j])) {
                    if (wc[j].status != IBV_WC_SUCCESS) {
                        snprintf(reply, n, "err fence on queue pair %u: completion status %d", q->v.qp_num,
                                 wc[j].status);
                        return -1;
                    }
                    if (q->fence_wait && !__atomic_load_n(&q->markers, __ATOMIC_SEQ_CST)) q->fence_wait = 0, want--;
                    continue;
                }
                uint32_t v;
                if (!map_get(&cqs[i]->c->qpn_v, wc[j].qp_num, &v)) wc[j].qp_num = v;
                hold_wc(cqs[i], &wc[j]);
            }
        }
        if (now_s() - t0 > 5) {
            snprintf(reply, n, "err %d queue pair(s) did not finish their sends in 5 s%s", want,
                     want ? "" : " (completions kept coming)");
            return -1;
        }
    }
    return 0;
}

/* Let NCCL's calls go, unless they are held for good (G_DOOM): decided and
 * done in one compare-and-swap, so a doom that comes at any point before
 * it wins, and one that comes after holds the calls again. */
static void reopen(void)
{
    pthread_mutex_lock(&gmu);
    int s = __atomic_load_n(&suspended, __ATOMIC_SEQ_CST);
    while (s && !(s & G_DOOM) &&
           !__atomic_compare_exchange_n(&suspended, &s, 0, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {}
    pthread_cond_broadcast(&gcv);
    pthread_mutex_unlock(&gmu);
}

/* Set by nvsnap_ibv_fence, cleared when NCCL's verbs open again. */
static int fenced;

/* Hold NCCL's calls for good (G_DOOM), when a release refuses after a
 * fence that succeeded. Every rank fenced, so the others may be destroying
 * their queue pairs now, and a rollback does not re-create this rank's (it
 * released nothing): its queue pairs would keep targeting peers that are
 * gone. NCCL's verbs calls then stay held, whatever asks to open them, and
 * the process must be replaced (gpushare refuses to let it run again:
 * nvsnap_ibv_doomed). Takes no lock. 1 the first time. */
static int doom(void)
{
    return !(__atomic_fetch_or(&suspended, G_HELD | G_DOOM, __ATOMIC_SEQ_CST) & G_DOOM);
}

static int doomed(void) { return __atomic_load_n(&suspended, __ATOMIC_SEQ_CST) & G_DOOM; }

/* A release refused: let NCCL's calls run again, unless it had been fenced. */
static void release_refused(void)
{
    if (!fenced) {
        reopen();
        return;
    }
    if (doom())
        logf_("release refused after the fence: NCCL's verbs calls stay held for good (its peers may be releasing); "
              "the process cannot run again");
}

/* Test hook: runs in nvsnap_ibv_open between its check of the doomed state
 * and its decision to let NCCL's calls go (the host tests define it before
 * including this file). Nothing in a build of the library. */
#ifndef NVSNAP_IBV_OPEN_HOOK
#define NVSNAP_IBV_OPEN_HOOK() do {} while (0)
#endif

/* Test hook: runs in each round of stop_calls' wait for calls in progress
 * (the host tests define it before including this file). Nothing in a
 * build of the library. */
#ifndef NVSNAP_IBV_WAITING_HOOK
#define NVSNAP_IBV_WAITING_HOOK() do {} while (0)
#endif

/* Stop NCCL's verbs calls and wait for those in progress, without omu: a
 * call in progress may need it to finish (see suspended). */
static int stop_calls(char *reply, size_t n)
{
    if (__atomic_load_n(&unsupported, __ATOMIC_SEQ_CST)) {
        snprintf(reply, n, "err NCCL uses a verbs object this library cannot re-create (see the log)");
        return -1;
    }
    __atomic_fetch_or(&suspended, G_HELD, __ATOMIC_SEQ_CST);
    pthread_mutex_lock(&omu);
    for (int i = 0; i < n_ctxs; i++) { uint64_t one = 1; ssize_t __attribute__((unused)) w = write(ctxs[i]->wake, &one, sizeof(one)); }
    pthread_mutex_unlock(&omu);
    double t0 = now_s();
    while (__atomic_load_n(&inflight, __ATOMIC_SEQ_CST)) {
        NVSNAP_IBV_WAITING_HOOK();
        if (now_s() - t0 > 2) {
            /* After a fence the calls stay held: the caller decides
             * (release_refused), and a peer may be releasing. */
            if (!fenced) reopen();
            snprintf(reply, n, "err a verbs call did not return in 2 s");
            return -1;
        }
        usleep(1000);
    }
    return 0;
}

/* Queue pairs whose state this library cannot save (omu held, after
 * stop_calls): one with a failed completion (see seen_wc), or one it could
 * not track; or an object it cannot re-create, recorded by a call that was
 * already in progress when stop_calls checked (a receive with too many
 * scatter entries, kept cut short). */
static int untracked(char *reply, size_t n)
{
    if (__atomic_load_n(&unsupported, __ATOMIC_SEQ_CST)) {
        snprintf(reply, n, "err NCCL uses a verbs object this library cannot re-create (see the log)");
        return -1;
    }
    for (int i = 0; i < n_qps; i++)
        if (__atomic_load_n(&qps[i]->failed, __ATOMIC_SEQ_CST) || qps[i]->slot < 0) {
            snprintf(reply, n, "err queue pair %u %s: its outstanding receives are not known", qps[i]->v.qp_num,
                     qps[i]->slot < 0 ? "is not tracked (too many created)" : "had a failed completion");
            return -1;
        }
    return 0;
}

/* gpushare "quiesce": once the GPU work has drained, every send NCCL
 * posted completes (see fence), while every peer, on every node, still has
 * its queue pairs: no node releases before all have quiesced. NCCL's
 * verbs calls stay held until "release": a send posted after the fence
 * might not complete before release destroys its queue pair (NCCL's proxy
 * can post the last write of a send whose kernel has finished). Held, it
 * is posted after the restore, on the new queue pair. Whatever lets the
 * process run again without a release calls nvsnap_ibv_open. */
EXPORT int nvsnap_ibv_fence(char *reply, size_t n)
{
    if (doomed()) {
        snprintf(reply, n, "err a release refused after the fence: NCCL's verbs calls stay held");
        return -1;
    }
    int r = stop_calls(reply, n);
    if (r < 0) return r;
    pthread_mutex_lock(&omu);
    r = untracked(reply, n);
    if (r == 0) r = fence(reply, n, 1);
    __atomic_store_n(&fenced, r == 0, __ATOMIC_SEQ_CST);
    pthread_mutex_unlock(&omu);
    if (r < 0) reopen();
    return r;
}

/* Destroy every real object (gpushare "release", after quiesce). After
 * nvsnap_ibv_fence only the completions that arrived since are collected:
 * a fence now could wait on a peer another node has already released, and
 * NCCL has posted nothing since (its calls stayed held). */
EXPORT int nvsnap_ibv_suspend(char *reply, size_t n)
{
    if (stop_calls(reply, n) < 0) {
        release_refused();
        return -1;
    }
    /* The provider's destroy waits for every event of the object to be
     * acknowledged: give NCCL's event thread the time it takes. */
    double t0 = now_s();
    int ua;
    while ((ua = unacked_count()) && now_s() - t0 < 2) usleep(1000);
    if (ua) {
        release_refused();
        snprintf(reply, n, "err NCCL has not acknowledged %d async event(s) of a queue pair or completion queue", ua);
        return -1;
    }
    pthread_mutex_lock(&omu);
    if (untracked(reply, n) < 0 || fence(reply, n, !fenced) < 0) {
        pthread_mutex_unlock(&omu);
        release_refused();
        return -1;
    }
    __atomic_store_n(&fenced, 0, __ATOMIC_SEQ_CST);
    __atomic_store_n(&released, 1, __ATOMIC_SEQ_CST);
    int bad = 0;
    for (int i = 0; i < n_qps; i++) if (R.destroy_qp(qps[i]->real)) bad++; else qps[i]->real = NULL;
    for (int i = 0; i < n_cqs; i++) if (R.destroy_cq(cqs[i]->real)) bad++; else cqs[i]->real = NULL;
    for (int i = 0; i < n_mrs; i++) if (R.dereg_mr(mrs[i]->real)) bad++; else mrs[i]->real = NULL;
    for (int i = 0; i < n_pds; i++) if (R.dealloc_pd(pds[i]->real)) bad++; else pds[i]->real = NULL;
    for (int i = 0; i < n_ctxs; i++)
        if (R.close_device(ctxs[i]->real)) bad++;
        else ctxs[i]->real = NULL, __atomic_add_fetch(&ctxs[i]->gen, 1, __ATOMIC_SEQ_CST);
    /* An object whose destroy failed went with its device when the device
     * closed (the kernel destroys what is left of a closed device): its
     * handle is stale. Forget it, so the restore creates it again with the
     * rest. Kept, a queue pair would be skipped by the restore and by
     * "connect", and go on targeting its peer's old number while the peer
     * is created again. The provider's memory for it is leaked. */
    for (int i = 0; bad && i < n_qps; i++) if (qps[i]->real && !qps[i]->pd->c->real) qps[i]->real = NULL;
    for (int i = 0; bad && i < n_cqs; i++) if (cqs[i]->real && !cqs[i]->c->real) cqs[i]->real = NULL;
    for (int i = 0; bad && i < n_mrs; i++) if (mrs[i]->real && !mrs[i]->pd->c->real) mrs[i]->real = NULL;
    for (int i = 0; bad && i < n_pds; i++) if (pds[i]->real && !pds[i]->c->real) pds[i]->real = NULL;
    int nq = n_qps, nm = n_mrs, nc = n_ctxs;
    pthread_mutex_unlock(&omu);
    if (bad) { snprintf(reply, n, "err %d verbs object(s) could not be destroyed", bad); return -1; }
    snprintf(reply, n, "ok released %d device(s), %d queue pair(s), %d memory registration(s)", nc, nq, nm);
    return 0;
}

/* The CUDA calls a dma-buf registration needs, from the process's libcuda. */
static CUresult (*cu_get_handle)(void *, CUdeviceptr, size_t, CUmemRangeHandleType, unsigned long long);
static CUresult (*cu_ptr_attr)(void *, CUpointer_attribute, CUdeviceptr);
static CUresult (*cu_push)(CUcontext), (*cu_pop)(CUcontext *);
static CUresult (*cu_dev_get)(CUdevice *, int), (*cu_retain)(CUcontext *, CUdevice), (*cu_release)(CUdevice);

static int dmabuf_fd(uint64_t base, size_t size)
{
    if (!cu_get_handle) {
        void *cu = dlopen("libcuda.so.1", RTLD_NOW | RTLD_NOLOAD);
        if (!cu) return -1;
        *(void **)&cu_get_handle = dlsym(cu, "cuMemGetHandleForAddressRange");
        *(void **)&cu_ptr_attr = dlsym(cu, "cuPointerGetAttribute");
        *(void **)&cu_push = dlsym(cu, "cuCtxPushCurrent_v2");
        *(void **)&cu_pop = dlsym(cu, "cuCtxPopCurrent_v2");
        *(void **)&cu_dev_get = dlsym(cu, "cuDeviceGet");
        *(void **)&cu_retain = dlsym(cu, "cuDevicePrimaryCtxRetain");
        *(void **)&cu_release = dlsym(cu, "cuDevicePrimaryCtxRelease_v2");
        if (!cu_get_handle || !cu_ptr_attr || !cu_push || !cu_pop || !cu_dev_get || !cu_retain || !cu_release) return -1;
    }
    /* The device's primary context: memory NCCL maps with cuMemMap
     * belongs to no context (the pointer's context attribute is NULL). */
    CUcontext ctx = NULL, old;
    CUdevice dev;
    int fd = -1, ord = -1;
    CUresult r = cu_ptr_attr(&ord, CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL, base);
    if (r == CUDA_SUCCESS && (r = cu_dev_get(&dev, ord)) == CUDA_SUCCESS && (r = cu_retain(&ctx, dev)) == CUDA_SUCCESS) {
        if ((r = cu_push(ctx)) == CUDA_SUCCESS) {
            if ((r = cu_get_handle(&fd, base, size, CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD, 0)) != CUDA_SUCCESS) fd = -1;
            cu_pop(&old);
        }
        cu_release(dev);
    }
    if (fd < 0) {
        static CUresult (*range)(CUdeviceptr *, size_t *, CUdeviceptr);
        void *cu = dlopen("libcuda.so.1", RTLD_NOW | RTLD_NOLOAD);
        if (!range && cu) *(void **)&range = dlsym(cu, "cuMemGetAddressRange_v2");
        CUdeviceptr b = 0;
        size_t sz = 0;
        CUresult rr = range ? range(&b, &sz, base) : (CUresult)-1;
        logf_("dma-buf of %#llx+%zu: CUDA error %d (device %d; allocation %d: %#llx+%zu)",
              (unsigned long long)base, size, r, ord, rr, (unsigned long long)b, sz);
    }
    return fd;
}

/* Does a queue pair of c use GID g (its port, and its RTR source index)? */
static int gid_used(struct vctx *c, const struct vgid *g)
{
    for (int i = 0; i < n_qps; i++) {
        struct vqp *q = qps[i];
        if (q->pd->c != c || !(q->mask[1] & IBV_QP_AV)) continue;
        int port = q->mask[0] & IBV_QP_PORT ? q->attr[0].port_num : 1;
        int idx = q->attr[1].ah_attr.is_global ? q->attr[1].ah_attr.grh.sgid_index : 0;
        if (port == g->port && idx == g->index) return 1;
    }
    return 0;
}

static struct ibv_device *find_dev(struct ibv_device **l, int nd, const char *name)
{
    for (int i = 0; i < nd; i++) if (!strcmp(R.get_device_name(l[i]), name)) return l[i];
    return NULL;
}

/* After the restore the same GID index may hold another kind of address:
 * use the index on the new port that holds the same kind (RoCE v2 IPv4,
 * IPv6, or IB) as the old one did, the old index first. */
static int gid_kind(const union ibv_gid *g)
{
    static const uint8_t v4[12] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff };
    if (!memcmp(g->raw, v4, 12)) return 1;
    if (g->raw[0] == 0xfe && g->raw[1] == 0x80) return 2;
    return 3;
}

static int pick_gid(struct vctx *c, struct vgid *g)
{
    union ibv_gid x;
    if (!R.query_gid(c->real, g->port, g->index, &x) && gid_kind(&x) == gid_kind(&g->old) &&
        memcmp(x.raw, (uint8_t[16]){ 0 }, 16)) {
        g->cur = x, g->cur_index = g->index;
        return 0;
    }
    for (int i = 0; i < 256; i++)
        if (!R.query_gid(c->real, g->port, i, &x) && gid_kind(&x) == gid_kind(&g->old) && memcmp(x.raw, (uint8_t[16]){ 0 }, 16)) {
            g->cur = x, g->cur_index = i;
            return 0;
        }
    return -1;
}

/* Create every object again (gpushare "load", after GPU memory is back):
 * devices by name, then protection domains, registrations, completion
 * queues and queue pairs, which go to INIT and get NCCL's outstanding
 * receives again. Peers' new numbers come with "connect". An object whose
 * setup fails part way is destroyed again, so that a retry ("load" again)
 * sets it up from the start instead of taking it for done. */
EXPORT int nvsnap_ibv_restore(char *reply, size_t n)
{
    pthread_mutex_lock(&omu);
    int nd = 0, bad = 0;
    struct ibv_device **l = n_ctxs ? R.get_device_list(&nd) : NULL;
    for (int i = 0; i < n_ctxs && !bad; i++) {
        struct vctx *c = ctxs[i];
        if (c->real) continue;
        struct ibv_device *d = l ? find_dev(l, nd, c->dev.name) : NULL;
        if (!d || !(c->real = R.open_device(d))) {
            snprintf(reply, n, "err RDMA device %s is not here", c->dev.name);
            bad = 1;
            break;
        }
        __atomic_add_fetch(&c->gen, 1, __ATOMIC_SEQ_CST);
        /* NCCL reads many GID entries to choose one (some empty); only the
         * ones a queue pair uses must exist again. A device that fails the
         * check is closed again, so a retry checks it again. */
        for (int k = 0; k < c->n_gids && !bad; k++)
            if (pick_gid(c, &c->gids[k]) && gid_used(c, &c->gids[k])) {
                snprintf(reply, n, "err %s port %d: no GID like index %d had", c->dev.name, c->gids[k].port, c->gids[k].index);
                bad = 1;
            }
        if (bad) {
            R.close_device(c->real);
            c->real = NULL;
            __atomic_add_fetch(&c->gen, 1, __ATOMIC_SEQ_CST);
            break;
        }
        for (int k = 0; k < c->n_ports && !bad; k++) {
            struct ibv_port_attr pa;
            if (!R.query_port(c->real, c->ports[k].port, (struct _compat_ibv_port_attr *)&pa)) c->ports[k].cur_lid = pa.lid;
        }
    }
    if (l) R.free_device_list(l);
    for (int i = 0; i < n_pds && !bad; i++)
        if (!pds[i]->real && !(pds[i]->real = R.alloc_pd(pds[i]->c->real))) {
            snprintf(reply, n, "err alloc_pd: %s", strerror(errno));
            bad = 1;
        }
    for (int i = 0; i < n_ctxs; i++) map_clear(&ctxs[i]->lkeys);
    for (int i = 0; i < n_mrs && !bad; i++) {
        struct vmr *m = mrs[i];
        if (!m->real) {
            if (m->kind == MR_DMABUF) {
                int fd = dmabuf_fd(m->iova - m->dmabuf_off, m->dmabuf_size);
                m->real = fd < 0 ? NULL : R.reg_dmabuf_mr(m->pd->real, m->dmabuf_off, m->v.length, m->iova, fd, m->access);
                if (fd >= 0) close(fd);
            } else if (m->kind == MR_IOVA2) {
                m->real = R.reg_mr_iova2(m->pd->real, m->v.addr, m->v.length, m->iova, m->access);
            } else {
                m->real = R.reg_mr(m->pd->real, m->v.addr, m->v.length, m->access);
            }
            if (!m->real) {
                logf_("register %p+%zu again (kind %d, dma-buf offset %llu of %zu): %s", m->v.addr, m->v.length, m->kind,
                      (unsigned long long)m->dmabuf_off, m->dmabuf_size, strerror(errno));
                snprintf(reply, n, "err register %p+%zu again: %s", m->v.addr, m->v.length, strerror(errno));
                bad = 1;
                break;
            }
        }
        map_put(&m->pd->c->lkeys, m->v.lkey, m->real->lkey);
    }
    for (int i = 0; i < n_cqs && !bad; i++) {
        struct vcq *q = cqs[i];
        if (!q->real && !(q->real = R.create_cq(q->c->real, q->v.cqe, q->v.cq_context, NULL, q->comp_vector))) {
            snprintf(reply, n, "err create_cq: %s", strerror(errno));
            bad = 1;
        }
    }
    for (int i = 0; i < n_ctxs; i++) map_clear(&ctxs[i]->qpn_v), map_clear(&ctxs[i]->qpn_slot);
    for (int i = 0; i < n_qps && !bad; i++) {
        struct vqp *q = qps[i];
        if (!q->real) {
            struct ibv_qp_init_attr a = q->init;
            a.send_cq = ((struct vcq *)q->init.send_cq)->real;
            a.recv_cq = ((struct vcq *)q->init.recv_cq)->real;
            q->fresh = 1;
            if (!(q->real = R.create_qp(q->pd->real, &a))) {
                snprintf(reply, n, "err create_qp: %s", strerror(errno));
                bad = 1;
                break;
            }
            if (q->mask[0] && (errno = R.modify_qp(q->real, &q->attr[0], q->mask[0]))) {
                snprintf(reply, n, "err queue pair %u to INIT: %s", q->v.qp_num, strerror(errno));
                bad = 1;
            }
            /* NCCL's receives still outstanding, with the real keys. */
            for (int k = 0; k < q->n_recvs && !bad; k++) {
                struct recv_rec *rr = &q->recvs[k];
                struct ibv_sge sg[RECV_SGE];
                for (int j = 0; j < rr->n; j++) sg[j] = rr->sg[j], sg[j].lkey = lkey_of(q->pd->c, sg[j].lkey);
                struct ibv_recv_wr w = { .wr_id = rr->wr_id, .sg_list = sg, .num_sge = rr->n }, *b;
                if (q->real->context->ops.post_recv(q->real, &w, &b)) {
                    snprintf(reply, n, "err post a receive again on queue pair %u", q->v.qp_num);
                    bad = 1;
                }
            }
            if (bad) {
                R.destroy_qp(q->real);
                q->real = NULL;
                break;
            }
        }
        map_put(&q->pd->c->qpn_v, q->real->qp_num, q->v.qp_num);
        if (q->slot >= 0) map_put(&q->pd->c->qpn_slot, q->real->qp_num, q->slot);
    }
    int nq = n_qps;
    if (!bad) __atomic_store_n(&released, 0, __ATOMIC_SEQ_CST);
    pthread_mutex_unlock(&omu);
    if (bad) return -1;
    snprintf(reply, n, "ok re-created %d queue pair(s)", nq);
    return 0;
}

static void hex16(const union ibv_gid *g, char *o)
{
    for (int i = 0; i < 16; i++) sprintf(o + 2 * i, "%02x", g->raw[i]);
}

static int unhex16(const char *s, union ibv_gid *g)
{
    for (int i = 0; i < 16; i++) {
        unsigned x;
        if (sscanf(s + 2 * i, "%2x", &x) != 1) return -1;
        g->raw[i] = x;
    }
    return 0;
}

/* The port a queue pair uses: its GID, as NCCL saw it, and as it is now. */
static struct vgid *qp_gid(struct vqp *q)
{
    struct vctx *c = q->pd->c;
    int port = q->mask[0] & IBV_QP_PORT ? q->attr[0].port_num : 1;
    int idx = q->mask[1] & IBV_QP_AV && q->attr[1].ah_attr.is_global ? q->attr[1].ah_attr.grh.sgid_index : 0;
    for (int i = 0; i < c->n_gids; i++) if (c->gids[i].port == port && c->gids[i].index == idx) return &c->gids[i];
    return NULL;
}

static struct vport *qp_port(struct vqp *q)
{
    struct vctx *c = q->pd->c;
    int port = q->mask[0] & IBV_QP_PORT ? q->attr[0].port_num : 1;
    for (int i = 0; i < c->n_ports; i++) if (c->ports[i].port == port) return &c->ports[i];
    return NULL;
}

/* Lines for gpushare's exchange (two words each, as fabric handles):
 *   ibvp:<old GID>:<old LID> <new GID>:<new LID>   a port
 *   ibvq:<old GID>:<old qpn> <new qpn>              a queue pair on it
 *   ibvk:<old GID>:<old rkey> <new rkey>            a registration on its device */
EXPORT int nvsnap_ibv_map(FILE *fp)
{
    char a[33], b[33];
    int lines = 0;
    pthread_mutex_lock(&omu);
    for (int i = 0; i < n_qps; i++) {
        struct vqp *q = qps[i];
        struct vgid *g = qp_gid(q);
        struct vport *p = qp_port(q);
        if (!g || !q->real) continue;
        hex16(&g->old, a);
        hex16(&g->cur, b);
        int seen = 0;
        for (int k = 0; k < i && !seen; k++) seen = qp_gid(qps[k]) == g;
        if (!seen) {
            fprintf(fp, "ibvp:%s:%u %s:%u\n", a, p ? p->old_lid : 0, b, p ? p->cur_lid : 0);
            lines++;
            for (int k = 0; k < n_mrs; k++)
                if (mrs[k]->pd->c == q->pd->c && mrs[k]->real)
                    fprintf(fp, "ibvk:%s:%u %u\n", a, mrs[k]->v.rkey, mrs[k]->real->rkey), lines++;
        }
        fprintf(fp, "ibvq:%s:%u %u\n", a, q->v.qp_num, q->real->qp_num);
        lines++;
    }
    pthread_mutex_unlock(&omu);
    return lines;
}

static struct rport *rport_get(const union ibv_gid *old)
{
    int i = rport_find(old, 0);
    if (i >= 0) return &rports[i];
    struct rport r = { .old = *old };
    PUSHP(rports, n_rports, r);
    return &rports[n_rports - 1];
}

/* Read every process's lines, then bring each queue pair to RTR and RTS
 * as NCCL did, with the peer's new GID, LID and queue pair number. */
EXPORT int nvsnap_ibv_connect(const char *path, char *reply, size_t n)
{
    FILE *fp = path ? fopen(path, "re") : NULL;
    char w1[160], w2[160];
    pthread_mutex_lock(&omu);
    while (fp && fscanf(fp, "%159s %159s", w1, w2) == 2) {
        union ibv_gid g, ng;
        unsigned x, y;
        if (!strncmp(w1, "ibvp:", 5) && strlen(w1) > 38 && !unhex16(w1 + 5, &g) && sscanf(w1 + 38, "%u", &x) == 1 &&
            !unhex16(w2, &ng) && sscanf(w2 + 33, "%u", &y) == 1) {
            struct rport *r = rport_get(&g);
            r->cur = ng, r->old_lid = x, r->cur_lid = y;
        } else if (!strncmp(w1, "ibvq:", 5) && strlen(w1) > 38 && !unhex16(w1 + 5, &g) && sscanf(w1 + 38, "%u", &x) == 1 &&
                   sscanf(w2, "%u", &y) == 1) {
            map_put(&rport_get(&g)->qpns, x, y);
        } else if (!strncmp(w1, "ibvk:", 5) && strlen(w1) > 38 && !unhex16(w1 + 5, &g) && sscanf(w1 + 38, "%u", &x) == 1 &&
                   sscanf(w2, "%u", &y) == 1) {
            map_put(&rport_get(&g)->keys, x, y);
        }
    }
    if (fp) fclose(fp);
    int bad = 0, nq = 0;
    for (int i = 0; i < n_qps && !bad; i++) {
        struct vqp *q = qps[i];
        if (!q->fresh || !(q->mask[1] & IBV_QP_STATE)) continue;  /* not re-created, or never connected */
        struct ibv_qp_attr a = q->attr[1];
        int ri = a.ah_attr.is_global ? rport_find(&a.ah_attr.grh.dgid, 0) : rport_find(NULL, a.ah_attr.dlid);
        uint32_t qpn;
        if (ri < 0 || map_get(&rports[ri].qpns, a.dest_qp_num, &qpn)) {
            snprintf(reply, n, "err no new number for the peer of queue pair %u (peer qpn %u)", q->v.qp_num, a.dest_qp_num);
            bad = 1;
            break;
        }
        struct vgid *g = qp_gid(q);
        a.dest_qp_num = qpn;
        if (a.ah_attr.is_global) {
            a.ah_attr.grh.dgid = rports[ri].cur;
            if (g) a.ah_attr.grh.sgid_index = g->cur_index;
        }
        a.ah_attr.dlid = rports[ri].cur_lid;
        /* A retry after a failure past RTR goes on from RTR. */
        int e = 0;
        if (q->fresh == 1) {
            if (q->has_ece && R.set_ece) R.set_ece(q->real, &q->ece);
            if (!(e = R.modify_qp(q->real, &a, q->mask[1]))) q->fresh = 2;
        }
        if (!e && q->mask[2]) e = R.modify_qp(q->real, &q->attr[2], q->mask[2]);
        if (e) {
            snprintf(reply, n, "err reconnect queue pair %u to %s: %s", q->v.qp_num, q->fresh == 1 ? "RTR" : "RTS",
                     strerror(e > 0 ? e : errno));
            bad = 1;
            break;
        }
        q->remote = ri;
        q->fresh = 0;
        nq++;
    }
    pthread_mutex_unlock(&omu);
    if (bad) return -1;
    if (nq) __atomic_store_n(&translated, 1, __ATOMIC_SEQ_CST);
    snprintf(reply, n, "ok reconnected %d queue pair(s)", nq);
    return 0;
}

/* Let NCCL's verbs calls run again (gpushare "resume", or anything that
 * lets the process run again after a fence without a release). Not while
 * the real objects are gone: a call would use a destroyed one. */
EXPORT void nvsnap_ibv_open(void)
{
    if (doomed()) {
        static int said;
        if (!__atomic_exchange_n(&said, 1, __ATOMIC_SEQ_CST))
            logf_("open: a release refused after the fence: NCCL's verbs calls stay held");
        return;
    }
    NVSNAP_IBV_OPEN_HOOK();
    pthread_mutex_lock(&omu);
    int gone = __atomic_load_n(&released, __ATOMIC_SEQ_CST);
    if (!gone) __atomic_store_n(&fenced, 0, __ATOMIC_SEQ_CST);
    pthread_mutex_unlock(&omu);
    if (gone) { logf_("open: NCCL's verbs objects are not re-created: its calls stay held"); return; }
    reopen();  /* not if a doom came after the check above */
}

/* 1 once a release refused after a fence (see doomed): the process must
 * not run again. */
EXPORT int nvsnap_ibv_doomed(void) { return !!doomed(); }

/* gpushare's "release" could not run at all (its state lock was busy)
 * after a fence: as a refused release, keep NCCL's calls held for good.
 * 1 when fenced (now doomed), else 0 and nothing changes. Takes no lock. */
EXPORT int nvsnap_ibv_doom_if_fenced(void)
{
    if (!__atomic_load_n(&fenced, __ATOMIC_SEQ_CST)) return 0;
    if (doom())
        logf_("release did not run after the fence: NCCL's verbs calls stay held for good; the process cannot run again");
    return 1;
}

/* The files NCCL's open devices hold (each real context's command and
 * event file), at most max of them in fds; returns how many there are.
 * gpushare's "mx" and "nixl" pauses leave these to nvsnap_ibv_suspend,
 * which closes them, instead of failing on them. */
EXPORT int nvsnap_ibv_fds(int *fds, int max)
{
    int k = 0;
    pthread_mutex_lock(&omu);
    for (int i = 0; i < n_ctxs; i++) {
        struct ibv_context *r = ctxs[i]->real;
        if (!r) continue;
        if (r->cmd_fd >= 0) { if (k < max) fds[k] = r->cmd_fd; k++; }
        if (r->async_fd >= 0) { if (k < max) fds[k] = r->async_fd; k++; }
    }
    pthread_mutex_unlock(&omu);
    return k;
}

/* How many RDMA devices NCCL has open (it opens them even when it
 * connects no queue pair: one node, NVLink only). */
EXPORT int nvsnap_ibv_count(void)
{
    pthread_mutex_lock(&omu);
    int r = n_ctxs;
    pthread_mutex_unlock(&omu);
    return r;
}
