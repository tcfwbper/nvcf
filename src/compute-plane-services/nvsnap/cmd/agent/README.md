<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# cmd/agent

`nvsnap-agent`, the binary of the node DaemonSet. With no subcommand it parses
flags into an `agent.Config` and runs the agent
([`../../internal/agent/`](../../internal/agent/)), which also serves the
mutating admission webhook when `--webhook` is set.

## Used by

- The agent DaemonSet in [`deploy/helm/nvsnap`](../../deploy/helm/nvsnap/);
  the chart's values become the flags.
- Built by [`docker/agent/Dockerfile.app`](../../docker/agent/Dockerfile.app), the
  Makefile `agent` target and the root `BUILD.bazel` `nvsnap_agent_image`.
- Entry point: `main.go:main`. Subcommands the agent runs as helper
  processes (`main.go` dispatches on `os.Args[1]`):
  - `capture-write` / `capture-copy`: compatibility entry points for
    writer Jobs started by older agents; they run a capture plan from
    `NVSNAP_CAPTURE_PLAN` (`capture_write.go:runCaptureWrite`). Current
    agents copy in-process instead
    (`checkpointstore.PerCapturePVCBackend.runMountHolderCopy`).
  - `pidns-dump-exec`, `pidns-restore-exec`: run CRIU inside a pod's pid
    namespace (`pidns_exec.go`).
  - `placeholder-files`: from a restore placeholder's mount namespace, pass
    its mount tree and unix listeners to the agent (`pidns_exec.go`).

## Key files

- `main.go`: flag definitions, subcommand dispatch, `agent.New` and `Run`.
- `capture_write.go`: the `capture-write` compatibility subcommand.
- `pidns_exec.go`: the pid-namespace CRIU and placeholder helpers.

## Configuration

The chart's `agent-daemonset.yaml` template sets most flags from
[`values.yaml`](../../deploy/helm/nvsnap/values.yaml); others keep their
default or take it from an environment variable the chart sets (for example
`--node-name` from `NODE_NAME`). Run `nvsnap-agent -h` for the list. Environment variables and routes are
listed in [docs/REFERENCE.md](../../docs/REFERENCE.md).
