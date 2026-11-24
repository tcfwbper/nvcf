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

import type { ColumnDef, ColumnFiltersState } from "@tanstack/react-table";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { DataTable } from "./index";

interface Cluster {
	name: string;
	status: string;
	region: string;
}

const clusters: Cluster[] = [
	{ name: "alpha", status: "ready", region: "us-east" },
	{ name: "bravo", status: "ready", region: "us-west" },
	{ name: "charlie", status: "unavailable", region: "us-east" },
	{ name: "delta", status: "no-contact", region: "us-west" },
];

const columns: ColumnDef<Cluster, unknown>[] = [
	{ accessorKey: "name", header: "Name" },
	{
		accessorKey: "status",
		header: "Status",
		meta: {
			filterVariant: "multi-select",
			filterOptions: [
				{ label: "Ready", value: "ready" },
				{ label: "Unavailable", value: "unavailable" },
				{ label: "No Contact", value: "no-contact" },
			],
			groups: [
				{ id: "all", label: "All" },
				{
					id: "issues",
					label: "Issues",
					values: ["unavailable", "no-contact"],
				},
			],
		},
	},
	{
		accessorKey: "region",
		header: "Region",
		filterFn: "multiSelect",
		meta: { filterVariant: "multi-select" },
	},
];

function renderTable({
	data = clusters,
	columnFilters,
}: {
	data?: Cluster[];
	columnFilters?: ColumnFiltersState;
} = {}) {
	return render(
		<DataTable
			columns={columns}
			data={data}
			reactTableOptions={
				columnFilters ? { initialState: { columnFilters } } : undefined
			}
		>
			<DataTable.Toolbar>
				<DataTable.GroupFilter />
				<DataTable.Filters />
				<DataTable.ItemCount label="cluster" />
			</DataTable.Toolbar>
			<DataTable.ActiveFilters />
			<DataTable.Content />
		</DataTable>,
	);
}

const names = () =>
	screen
		.queryAllByRole("row")
		.slice(1)
		.map((row) => within(row).getAllByRole("cell")[0].textContent);

const pill = (name: RegExp) => screen.getByRole("button", { name });
const option = (name: RegExp) =>
	screen.queryByRole("menuitemcheckbox", { name });

describe("DataTable.GroupFilter", () => {
	it("renders a pill per group with bucket totals, default group active", () => {
		renderTable();
		expect(screen.getByRole("group", { name: "Status" })).toBeInTheDocument();
		expect(pill(/All\s*4/)).toHaveAttribute("aria-pressed", "true");
		expect(pill(/Issues\s*2/)).toHaveAttribute("aria-pressed", "false");
	});

	it("narrows rows and the status options to the selected group", async () => {
		const user = userEvent.setup();
		renderTable();

		await user.click(pill(/Issues/));

		await waitFor(() => expect(names()).toEqual(["charlie", "delta"]));
		expect(pill(/Issues/)).toHaveAttribute("aria-pressed", "true");
		expect(option(/^Ready/)).not.toBeInTheDocument();
		expect(option(/^Unavailable/)).toBeInTheDocument();
		expect(option(/^No Contact/)).toBeInTheDocument();
	});

	it("keeps the group when selecting a value in the grouped column", async () => {
		const user = userEvent.setup();
		renderTable();

		await user.click(pill(/Issues/));
		await user.click(
			screen.getByRole("menuitemcheckbox", { name: /^Unavailable/ }),
		);

		await waitFor(() => expect(names()).toEqual(["charlie"]));
		expect(pill(/Issues/)).toHaveAttribute("aria-pressed", "true");
	});

	it("clears other filters when switching groups", async () => {
		const user = userEvent.setup();
		renderTable();

		await user.click(
			screen.getByRole("menuitemcheckbox", { name: /^us-east/ }),
		);
		await waitFor(() => expect(names()).toEqual(["alpha", "charlie"]));

		await user.click(pill(/Issues/));

		await waitFor(() => expect(names()).toEqual(["charlie", "delta"]));
		expect(
			screen.queryByText((_text, el) => el?.textContent === "Region: us-east"),
		).not.toBeInTheDocument();
	});

	it("keeps totals when other filters change", async () => {
		const user = userEvent.setup();
		renderTable();

		await user.click(
			screen.getByRole("menuitemcheckbox", { name: /^us-east/ }),
		);

		await waitFor(() => expect(names()).toEqual(["alpha", "charlie"]));
		expect(pill(/All\s*4/)).toBeInTheDocument();
		expect(pill(/Issues\s*2/)).toBeInTheDocument();
	});

	it("keeps the group when clearing filters", async () => {
		const user = userEvent.setup();
		renderTable();

		await user.click(pill(/Issues/));
		await user.click(
			screen.getByRole("menuitemcheckbox", { name: /^Unavailable/ }),
		);
		await user.click(screen.getByRole("button", { name: "Clear filters" }));

		await waitFor(() => expect(names()).toEqual(["charlie", "delta"]));
		expect(pill(/Issues/)).toHaveAttribute("aria-pressed", "true");
	});

	it("does not show a chip for the group alone", async () => {
		const user = userEvent.setup();
		renderTable();

		await user.click(pill(/Issues/));

		await waitFor(() => expect(names()).toEqual(["charlie", "delta"]));
		expect(
			screen.queryByRole("button", { name: "Clear filters" }),
		).not.toBeInTheDocument();
	});

	it("counts items against the active group's rows", async () => {
		const user = userEvent.setup();
		renderTable();

		await user.click(pill(/Issues/));
		expect(await screen.findByText("2 clusters")).toBeInTheDocument();

		await user.click(
			screen.getByRole("menuitemcheckbox", { name: /^us-east/ }),
		);
		expect(await screen.findByText("1 of 2 clusters")).toBeInTheDocument();
	});

	it("applies a plain filter value as a selection in the default group", () => {
		renderTable({ columnFilters: [{ id: "status", value: ["ready"] }] });
		expect(names()).toEqual(["alpha", "bravo"]);
		expect(pill(/All/)).toHaveAttribute("aria-pressed", "true");
	});

	it("clears a chip's selection and keeps the group", async () => {
		const user = userEvent.setup();
		renderTable();

		await user.click(pill(/Issues/));
		await user.click(
			screen.getByRole("menuitemcheckbox", { name: /^No Contact/ }),
		);
		await waitFor(() => expect(names()).toEqual(["delta"]));

		await user.click(
			screen.getByRole("button", { name: /^Remove Status filter/ }),
		);

		await waitFor(() => expect(names()).toEqual(["charlie", "delta"]));
		expect(pill(/Issues/)).toHaveAttribute("aria-pressed", "true");
	});

	it("ignores selections outside the active group", () => {
		renderTable({
			columnFilters: [
				{ id: "status", value: { group: "issues", selected: ["ready"] } },
			],
		});
		expect(names()).toEqual(["charlie", "delta"]);
		expect(pill(/Issues/)).toHaveAttribute("aria-pressed", "true");
		expect(option(/^Ready/)).not.toBeInTheDocument();
		expect(
			screen.queryByRole("button", { name: "Clear filters" }),
		).not.toBeInTheDocument();
	});

	it("falls back to the default group for an unknown group id", () => {
		renderTable({ columnFilters: [{ id: "status", value: { group: "x" } }] });
		expect(names()).toEqual(["alpha", "bravo", "charlie", "delta"]);
		expect(pill(/All/)).toHaveAttribute("aria-pressed", "true");
	});

	it("shows the no-results state for an empty group", async () => {
		const user = userEvent.setup();
		renderTable({ data: clusters.filter((c) => c.status === "ready") });

		await user.click(pill(/Issues/));

		expect(await screen.findByText("No results found")).toBeInTheDocument();
	});
});
