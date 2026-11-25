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

import { Banner } from "@nvidia/foundations-react-core";
import clsx from "clsx";
import type { GetClusterResponse } from "~/generated/model/getClusterResponse";
import { GetClusterResponseStatus } from "~/generated/model/getClusterResponseStatus";
import { getStatusDisplay } from "./StatusBadge";

type ClusterStatus = NonNullable<GetClusterResponse["status"]>;

const bannerCopy = new Map<ClusterStatus, { heading: string; body: string }>([
	[
		GetClusterResponseStatus.UNHEALTHY,
		{
			heading: "No contact",
			body: "The control plane is not receiving anything from this cluster. Everything shown below is last-known and stale.",
		},
	],
	[
		GetClusterResponseStatus.ABANDONED,
		{
			heading: "Cannot accept workloads",
			body: "This cluster is not able to take work.",
		},
	],
	[
		GetClusterResponseStatus.CORDON,
		{
			heading: "In maintenance",
			body: "New functions and tasks are paused. Running workloads continue.",
		},
	],
	[
		GetClusterResponseStatus.CORDON_AND_DRAIN,
		{
			heading: "In maintenance",
			body: "Cluster is being drained. New work is paused and capacity is intentionally unavailable.",
		},
	],
	[
		GetClusterResponseStatus.NOT_READY,
		{
			heading: "Awaiting handshake",
			body: "Registration attempt in progress, awaiting validation from the control plane. This usually completes in a few minutes.",
		},
	],
]);

export function ClusterHealthBanner({
	status,
}: {
	status?: GetClusterResponse["status"];
}) {
	const copy = status && bannerCopy.get(status);
	if (!status || !copy) return null;
	const { color } = getStatusDisplay(status);

	return (
		<Banner
			className={clsx({
				"[--bg-color:var(--background-color-accent-purple)] [--border-color:var(--border-color-accent-purple)] [--text-color:var(--text-color-accent-purple-strong)]":
					color === "purple",
				"[--bg-color:var(--background-color-accent-gray)] [--border-color:var(--border-color-accent-gray)] [--text-color:var(--text-color-accent-gray)]":
					color === "gray",
			})}
			kind="header"
			slotIcon={null}
			slotSubheading={copy.body}
			status={color === "red" ? "error" : "info"}
		>
			{copy.heading}
		</Banner>
	);
}
