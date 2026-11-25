<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# ui/

The nvsnap web UI: a React and TypeScript single-page app built with Vite
and Tailwind. nvsnap-server embeds the built `dist/` tree and serves it at
`/`, and the app calls the server's `/api/v1` REST API and WebSocket.

## Used by

- [`cmd/nvsnap-server/main.go`](../cmd/nvsnap-server/main.go) passes
  `ui.DistFS` (from `embed.go`) to `server.SetUI`.
- [`docker/Dockerfile.server`](../docker/Dockerfile.server) runs the npm build
  in a Node stage and copies its `dist/` into the Go build.
- Bazel (`BUILD.bazel`) embeds the checked-in `dist/` as it is; it does not
  run the npm build.

## Key files

- `embed.go`: `DistFS`, the `//go:embed all:dist` file system.
- `package.json`: `npm run dev` and `build`. It also defines `lint` and
  `test`, but both fail today: `npm run lint` exits 2 because ESLint finds
  no configuration, and `npm test` exits 1 because Vitest finds no test
  files.
- `vite.config.ts`, `tailwind.config.js`, `tsconfig.json`: build set-up.
- `index.html`: the page the app mounts in.
- `dist/`: build output; regenerate with `npm run build`, do not edit.

## Subdirectories

- [`src/`](src/): the app source.
