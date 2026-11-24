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

import type { ColumnDef } from "@tanstack/react-table";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { DataTable } from "./index";

interface Cluster {
	name: string;
	status: string;
	region: string;
	gpus: string[];
}

const clusters: Cluster[] = [
	{ name: "alpha", status: "ready", region: "us-east", gpus: ["A100"] },
	{ name: "bravo", status: "ready", region: "us-west", gpus: ["A100", "H100"] },
	{ name: "charlie", status: "unavailable", region: "us-east", gpus: ["H100"] },
];

const columns: ColumnDef<Cluster, unknown>[] = [
	{ accessorKey: "name", header: "Name" },
	{
		accessorKey: "status",
		header: "Status",
		filterFn: "multiSelect",
		meta: {
			filterVariant: "multi-select",
			filterOptions: [
				{ label: "Ready", value: "ready" },
				{ label: "Unavailable", value: "unavailable" },
				{ label: "Pending", value: "pending" },
			],
		},
	},
	{
		accessorKey: "region",
		header: "Region",
		filterFn: "multiSelect",
		meta: { filterVariant: "multi-select" },
	},
	{
		accessorKey: "gpus",
		header: "GPUs",
		filterFn: "multiSelect",
		getUniqueValues: (row) => row.gpus,
		meta: { filterVariant: "multi-select" },
	},
];

function renderTable() {
	return render(
		<DataTable columns={columns} data={clusters}>
			<DataTable.Toolbar>
				<DataTable.Filters />
				<DataTable.Search />
			</DataTable.Toolbar>
			<DataTable.Content />
		</DataTable>,
	);
}

const names = () =>
	screen
		.queryAllByRole("row")
		.slice(1)
		.map((row) => within(row).getAllByRole("cell")[0].textContent);

const option = (name: RegExp) => screen.getByRole("menuitemcheckbox", { name });

describe("DataTable.ColumnFilter", () => {
	it("renders one dropdown per filterable column in column order", () => {
		renderTable();
		expect(
			screen
				.getAllByRole("button", { expanded: false })
				.map((b) => b.textContent),
		).toEqual(["Status", "Region", "GPUs"]);
	});

	it("lists fixed options with counts and disables options with no rows", () => {
		renderTable();
		expect(option(/^Ready\s*2/)).toBeInTheDocument();
		expect(option(/^Unavailable\s*1/)).toBeInTheDocument();
		expect(option(/^Pending\s*0/)).toBeDisabled();
	});

	it("lists data-derived options from the rows", () => {
		renderTable();
		expect(option(/^us-east\s*2/)).toBeInTheDocument();
		expect(option(/^us-west\s*1/)).toBeInTheDocument();
	});

	it("narrows rows and shows the selected count on the trigger", async () => {
		const user = userEvent.setup();
		renderTable();

		await user.click(option(/^Ready/));

		await waitFor(() => expect(names()).toEqual(["alpha", "bravo"]));
		expect(
			screen.getByRole("button", { name: "Status (1)" }),
		).toBeInTheDocument();
	});

	it("updates counts from other filters and keeps the column's own counts", async () => {
		const user = userEvent.setup();
		renderTable();

		await user.click(option(/^us-west/));

		await waitFor(() => expect(names()).toEqual(["bravo"]));
		expect(option(/^Unavailable\s*0/)).toBeDisabled();
		expect(option(/^us-east\s*2/)).toBeInTheDocument();
	});

	it("keeps a selected option enabled when its count drops to 0", async () => {
		const user = userEvent.setup();
		renderTable();

		await user.click(option(/^Unavailable/));
		await user.type(screen.getByRole("textbox"), "bravo");

		await waitFor(() => expect(option(/^Unavailable\s*0/)).toBeInTheDocument());
		expect(option(/^Unavailable\s*0/)).toBeEnabled();
		expect(option(/^Unavailable\s*0/)).toBeChecked();
	});

	it("matches array cells per element", async () => {
		const user = userEvent.setup();
		renderTable();

		expect(option(/^H100\s*2/)).toBeInTheDocument();
		await user.click(option(/^H100/));

		await waitFor(() => expect(names()).toEqual(["bravo", "charlie"]));
	});
});
