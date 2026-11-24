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

import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { describe, expect, it } from "vitest";
import { getGetClustersMockHandler } from "~/generated/api/clusters/clusters.msw";
import { GetClusterResponseStatus } from "~/generated/model/getClusterResponseStatus";
import { server } from "~/mocks/server";
import { renderWithRouter } from "~/testing/render";

describe("ClustersList", () => {
	it("renders a list of clusters", async () => {
		server.use(
			getGetClustersMockHandler([
				{
					clusterName: "cluster-1",
					status: GetClusterResponseStatus.READY,
					region: "us-east-1",
					gpus: [{ name: "NVIDIA A100" }],
				},
			]),
		);

		renderWithRouter({
			initialLocation: "/clusters",
		});

		expect(await screen.findByText("cluster-1")).toBeInTheDocument();
	});

	it("shows empty state when no clusters are returned", async () => {
		server.use(getGetClustersMockHandler([]));

		renderWithRouter({
			initialLocation: "/clusters",
		});

		expect(await screen.findByText("No data")).toBeInTheDocument();
	});

	describe("status groups", () => {
		const clusters = [
			{ clusterName: "ready-1", status: GetClusterResponseStatus.READY },
			{ clusterName: "cordon-1", status: GetClusterResponseStatus.CORDON },
			{ clusterName: "lost-1", status: GetClusterResponseStatus.UNHEALTHY },
			{ clusterName: "gone-1", status: GetClusterResponseStatus.ABANDONED },
		];

		it("narrows to No Contact and Unavailable clusters under Issues", async () => {
			const user = userEvent.setup();
			server.use(getGetClustersMockHandler(clusters));
			renderWithRouter({ initialLocation: "/clusters" });

			await user.click(
				await screen.findByRole("button", { name: /Issues\s*2/ }),
			);

			expect(await screen.findByText("lost-1")).toBeInTheDocument();
			expect(screen.getByText("gone-1")).toBeInTheDocument();
			expect(screen.queryByText("ready-1")).not.toBeInTheDocument();
			expect(screen.queryByText("cordon-1")).not.toBeInTheDocument();
			expect(
				screen.getByRole("menuitemcheckbox", { name: /^No Contact/ }),
			).toBeInTheDocument();
			expect(
				screen.getByRole("menuitemcheckbox", { name: /^Unavailable/ }),
			).toBeInTheDocument();
			expect(
				screen.queryByRole("menuitemcheckbox", { name: /^Ready/ }),
			).not.toBeInTheDocument();
		});

		it("applies a status filter from a dashboard link", async () => {
			server.use(getGetClustersMockHandler(clusters));
			const filters = encodeURIComponent(
				JSON.stringify([{ id: "status", value: ["Ready"] }]),
			);
			renderWithRouter({ initialLocation: `/clusters?filters=${filters}` });

			expect(await screen.findByText("ready-1")).toBeInTheDocument();
			expect(screen.queryByText("lost-1")).not.toBeInTheDocument();
			expect(screen.getByRole("button", { name: /All/ })).toHaveAttribute(
				"aria-pressed",
				"true",
			);
		});
	});

	it("shows error state when the API fails", async () => {
		server.use(
			http.get(
				"/v1/si/accounts/:ncaId/clusters",
				() => new HttpResponse(null, { status: 500 }),
			),
		);

		renderWithRouter({
			initialLocation: "/clusters",
		});

		expect(
			await screen.findByRole("button", { name: /try again/i }),
		).toBeInTheDocument();
	});
});
