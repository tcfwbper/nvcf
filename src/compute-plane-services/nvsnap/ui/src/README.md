<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# ui/src

Source of the web UI. `main.tsx` mounts `App`, and `App.tsx` sets up the
router: `/` redirects to `/demo`, and each other route renders one page.

## Key files

- `main.tsx`: entry point.
- `App.tsx`: routes to the pages in [`pages/`](pages/).
- `components/Layout.tsx`: the navigation shell around every page.
- `index.css`: Tailwind styles.

## Subdirectories

- [`api/`](api/): the REST and WebSocket clients and React Query hooks.
- [`pages/`](pages/): one component per route.
