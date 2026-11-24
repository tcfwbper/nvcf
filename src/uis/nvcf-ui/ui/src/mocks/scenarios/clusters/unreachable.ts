/**
 * SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { HttpResponse, http } from "msw";

// The generated handlers hardcode `status: 200`, so error scenarios use raw
// MSW handlers. A bare 502 with no body is what the BFF writes when it can't
// dial the upstream (backend/cmd/server/main.go, newProxy ErrorHandler).
const unreachable = () => new HttpResponse(null, { status: 502 });

export const handlers = [
	http.get("/v1/si/accounts/:ncaId/clusters", unreachable),
	http.get("/v1/si/accounts/:ncaId/clusters/:clusterId", unreachable),
];
