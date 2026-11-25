# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# libnvsnap_gpushare.so: LD_PRELOAD shim that releases and re-maps GPU memory
# shared between processes around a CUDA checkpoint (see gpushare.h).
# nvsnap-gpu-suspend: drives it and the CUDA checkpoint API.
# libnvsnap_ibv.so: NCCL's verbs (NCCL_IBVERBS_LIB), destroyed before a
# checkpoint and created again after it (nvsnap_ibv.c).
# Needs CUDA >= 13 headers (cuda.h: CUcheckpointGpuPair for --gpu-map) and
# the rdma-core headers (infiniband/verbs.h).

CC ?= gcc
CUDA_INC ?= /usr/local/cuda/include
IBV_INC ?= /usr/include
CFLAGS ?= -O2 -Wall -Wextra

all: libnvsnap_gpushare.so nvsnap-gpu-suspend libnvsnap_ibv.so

# Every function starts with no-ops (-fpatchable-function-entry): the library
# hands its wrappers out for the driver's entry points, and a program that
# hooks those by rewriting their first instructions (UCX's memory hooks,
# "bistro") must not relocate our code. UCX's arm64 relocation of ADRP drops
# the page alignment (src/ucm/bistro/bistro_aarch64.c, still in 1.20), so a
# wrapper whose first instructions load a global jumped to a wrong address:
# NIXL's UCX backend crashed in w_alloc. A no-op is not in the set UCX
# relocates; it then hooks our entries another way. GCC 8 and later.
GPUSHARE_ENTRY ?= -fpatchable-function-entry=4

libnvsnap_gpushare.so: gpushare.c gpushare.h
	$(CC) $(CFLAGS) $(GPUSHARE_ENTRY) -fPIC -shared -I. -I$(CUDA_INC) -o $@ gpushare.c -ldl -lpthread

# Only NCCL loads it (dlopen, local): its verbs symbols, at the versions
# NCCL asks for, never reach the process's other users of verbs.
libnvsnap_ibv.so: nvsnap_ibv.c nvsnap_ibv.map
	$(CC) $(CFLAGS) $(GPUSHARE_ENTRY) -fPIC -shared -fvisibility=hidden -I$(IBV_INC) -I$(CUDA_INC) \
		-Wl,--version-script=nvsnap_ibv.map -Wl,-soname,libnvsnap_ibv.so -o $@ nvsnap_ibv.c -ldl -lpthread

nvsnap-gpu-suspend: nvsnap-gpu-suspend.c gpushare.h
	$(CC) $(CFLAGS) -I. -I$(CUDA_INC) -o $@ nvsnap-gpu-suspend.c -ldl -lpthread

clean:
	rm -f libnvsnap_gpushare.so nvsnap-gpu-suspend libnvsnap_ibv.so

.PHONY: all clean
