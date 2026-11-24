<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->
# Node model cache: download and harvest times, November 2026

The record behind the download, harvest and load times that
[node-model-cache.md](../design/node-model-cache.md) and
[model-cache-single-flight.md](../design/model-cache-single-flight.md) cite. The observations moved here unchanged
from those design notes.

- Hardware: a 4-pod multi-node instance on GB300 nodes. GPUs per node and driver: not recorded.
- Date: November 2026. The day is not recorded.
- nvsnap build: not recorded. The node model cache was on.
- Source: the download inits' start and end times, and the agent's harvest log lines. The raw logs are not kept in
  this repository.

| What was measured | Result |
|---|---|
| Model size, per pod | 1.56 TB |
| Download init, start to end, each of the 4 pods | 30 to 32 minutes |
| Download rate per pod, from the size and the init's duration | about 870 MB/s |
| Harvest of one pod's download volume into the node's cache | about 5.5 minutes |
| Engine load, after the harvest ended | about 13 minutes more |
