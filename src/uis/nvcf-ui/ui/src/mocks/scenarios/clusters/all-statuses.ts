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

import {
	getGetClusterForNcaIdAndClusterIdMockHandler,
	getGetClusterForNcaIdAndClusterIdResponseMock,
	getGetClustersMockHandler,
} from "~/generated/api/clusters/clusters.msw";
import type { GetClusterResponse } from "~/generated/model/getClusterResponse";
import { GetClusterResponseStatus } from "~/generated/model/getClusterResponseStatus";

// ICMS returns gpuUsage from the stored heartbeat, which only these statuses keep.
const STATUSES_WITH_GPU_USAGE = new Set<string>([
	GetClusterResponseStatus.READY,
	GetClusterResponseStatus.CORDON,
	GetClusterResponseStatus.CORDON_AND_DRAIN,
]);

// The statuses SIS and ICMS send; the spec also lists PAUSED, FAILED, and DELETED.
const STATUSES = [
	GetClusterResponseStatus.READY,
	GetClusterResponseStatus.CORDON,
	GetClusterResponseStatus.CORDON_AND_DRAIN,
	GetClusterResponseStatus.NOT_READY,
	GetClusterResponseStatus.UNHEALTHY,
	GetClusterResponseStatus.ABANDONED,
];

// One cluster per status, with readable ids such as /clusters/unhealthy.
const CLUSTERS: GetClusterResponse[] = STATUSES.map((status) => {
	const cluster = getGetClusterForNcaIdAndClusterIdResponseMock();
	return {
		...cluster,
		clusterId: status.toLowerCase(),
		clusterName: `${status.toLowerCase()}-cluster`,
		status,
		gpuUsage: STATUSES_WITH_GPU_USAGE.has(status)
			? cluster.gpuUsage
			: undefined,
	};
});

export const handlers = [
	getGetClustersMockHandler(CLUSTERS),
	getGetClusterForNcaIdAndClusterIdMockHandler(
		(info) =>
			CLUSTERS.find((c) => c.clusterId === info.params.clusterId) ??
			getGetClusterForNcaIdAndClusterIdResponseMock(),
	),
];
