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

import { Breadcrumbs, PageHeader, Panel } from "@nvidia/foundations-react-core";
import { createFileRoute, Link } from "@tanstack/react-router";
import { DetailList } from "~/components/DetailList";
import { getActiveNcaId } from "~/features/accounts/getActiveNcaId";
import { ClusterHealthBanner } from "~/features/clusters/components/ClusterHealthBanner";
import { GpuUtilization } from "~/features/clusters/components/GpuUtilization";
import { InstanceConfigPanel } from "~/features/clusters/components/InstanceConfigPanel";
import { StatusBadge } from "~/features/clusters/components/StatusBadge";
import {
	getGetClusterForNcaIdAndClusterIdQueryOptions,
	useGetClusterForNcaIdAndClusterIdSuspense,
} from "~/generated/api/clusters/clusters";
import { GetClusterResponseStatus } from "~/generated/model/getClusterResponseStatus";
import { DETAIL_REFETCH_INTERVAL } from "~/lib/queryClient";
import { formatDateTime } from "~/utils/formatters";

export const Route = createFileRoute("/clusters/$clusterId")({
	staticData: { account: { redirectOnChange: "/clusters" } },
	loader: async ({ context: { queryClient }, params }) => {
		const ncaId = await getActiveNcaId(queryClient);
		const cluster = await queryClient.ensureQueryData({
			...getGetClusterForNcaIdAndClusterIdQueryOptions(ncaId, params.clusterId),
			revalidateIfStale: true,
		});
		return { ncaId, clusterName: cluster.clusterName };
	},
	head: ({ loaderData }) => ({
		meta: [{ title: `${loaderData?.clusterName ?? "Cluster"} · NVCF` }],
	}),
	component: ClusterDetails,
});

function ClusterDetails() {
	const { ncaId } = Route.useLoaderData();
	const { clusterId } = Route.useParams();

	const { data: cluster } = useGetClusterForNcaIdAndClusterIdSuspense(
		ncaId,
		clusterId,
		{ query: { refetchInterval: DETAIL_REFETCH_INTERVAL } },
	);

	const cfg = cluster.clusterConfigurations ?? {};
	const gpuUsage = Object.entries(cluster.gpuUsage ?? {});

	return (
		<div className="flex flex-col gap-6">
			<PageHeader
				kind="flat"
				slotBreadcrumbs={
					<Breadcrumbs
						items={[
							{
								children: (
									<Link to="/clusters" viewTransition>
										Compute Clusters
									</Link>
								),
							},
							cluster.clusterName ?? clusterId,
						]}
					/>
				}
				slotHeading={cluster.clusterName ?? clusterId}
			/>
			<ClusterHealthBanner status={cluster.status} />
			<div className="grid grid-cols-1 gap-6 lg:grid-cols-2 lg:gap-x-8 lg:items-start">
				<div className="flex flex-col gap-6">
					<Panel elevation="high" slotHeading="Overview">
						<DetailList
							items={[
								{
									label: "Status",
									value: <StatusBadge status={cluster.status} />,
								},
								{
									label: "Cluster Description",
									value: cluster.clusterDescription,
								},
								{
									label: "Cluster Agent Version",
									value: cluster.nvcaVersion,
								},
								{ label: "Cluster Group", value: cluster.clusterGroupName },
								{ label: "Region", value: cluster.region },
								{
									label: "GPU Utilization",
									value:
										gpuUsage.length > 0 ? (
											<div className="flex flex-col gap-6">
												{gpuUsage.map(([name, usage]) => (
													<GpuUtilization
														key={name}
														name={name}
														usage={usage}
													/>
												))}
											</div>
										) : undefined,
								},
								{
									label: "Last Updated",
									value: formatDateTime(cluster.nvcaLastConnected),
								},
							]}
						/>
					</Panel>
					{cluster.status !== GetClusterResponseStatus.NOT_READY && (
						<InstanceConfigPanel
							gpus={cluster.gpus ?? []}
							usage={cluster.gpuUsage || {}}
						/>
					)}
				</div>

				<Panel elevation="high" slotHeading="Configuration">
					<DetailList
						items={[
							{
								label: "Node-selector Label",
								value: cfg["node-selector-label"],
							},
							{ label: "Priority Class", value: cfg["priority-class"] },
							{
								label: "Model Cache Volume Mount Options",
								value: cfg["model-cache-volume-mount-options"],
							},
							{
								label: "Network CIDR Range",
								value: cfg["network-cidr-range"],
							},
							{
								label: "Worker Degradation Period",
								value: cfg["worker-degradation-period"],
							},
							{ label: "Cluster Attributes", value: cluster.attributes },
							{ label: "Cluster Features", value: cluster.capabilities },
						]}
					/>
				</Panel>
			</div>
		</div>
	);
}
