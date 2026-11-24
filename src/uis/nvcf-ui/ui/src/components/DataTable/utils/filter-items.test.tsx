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

import type { Column, ColumnMeta, RowData } from "@tanstack/react-table";
import { render } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { buildFilterItems, getColumnOptions } from "./filter-items";

const options = [
	{ label: "Active", value: "active" },
	{ label: "Inactive", value: "inactive" },
];

function mockColumn(
	filterVariant?: ColumnMeta<RowData, unknown>["filterVariant"],
	filterValue?: unknown,
	facets = new Map<unknown, number>([["active", 2]]),
) {
	return {
		id: "status",
		columnDef: { meta: { filterVariant, filterOptions: options } },
		getFilterValue: () => filterValue,
		getFacetedUniqueValues: () => facets,
		setFilterValue: vi.fn(),
	} as unknown as Column<RowData, unknown>;
}

/** The value written by `setFilterValue`, applying an updater to `current`. */
function written(column: Column<RowData, unknown>, current?: unknown) {
	const [update] = vi.mocked(column.setFilterValue).mock.calls[0];
	return typeof update === "function" ? update(current) : update;
}

/** The rendered count slot of an item. */
function countSlot(item: ReturnType<typeof buildFilterItems>[number]) {
	const { container } = render(<>{"slotEnd" in item && item.slotEnd}</>);
	return container.firstElementChild;
}

describe("buildFilterItems", () => {
	it("returns empty array for unknown filter variant", () => {
		expect(buildFilterItems(mockColumn(undefined), "test")).toEqual([]);
	});

	describe("multi-select", () => {
		it("shows faceted counts and disables options at 0", () => {
			const [active, inactive] = buildFilterItems(
				mockColumn("multi-select"),
				"test",
			);
			expect(countSlot(active)).toHaveTextContent("2");
			expect(countSlot(active)).toHaveClass("text-placeholder");
			expect("disabled" in active && active.disabled).toBe(false);
			expect(countSlot(inactive)).toHaveTextContent("0");
			expect(countSlot(inactive)).not.toHaveClass("text-placeholder");
			expect("disabled" in inactive && inactive.disabled).toBe(true);
		});

		it("counts non-string values against their string options", () => {
			const [item] = buildFilterItems(
				mockColumn(
					"multi-select",
					undefined,
					new Map<unknown, number>([[8, 3]]),
				),
				"test",
				[{ label: "8", value: "8" }],
			);
			expect(countSlot(item)).toHaveTextContent("3");
		});

		it("sums counts for a numeric and string key that normalize to the same value", () => {
			const [item] = buildFilterItems(
				mockColumn(
					"multi-select",
					undefined,
					new Map<unknown, number>([
						[8, 3],
						["8", 2],
					]),
				),
				"test",
				[{ label: "8", value: "8" }],
			);
			expect(countSlot(item)).toHaveTextContent("5");
		});

		it("keeps a selected option at 0 enabled", () => {
			const items = buildFilterItems(
				mockColumn("multi-select", ["inactive"]),
				"test",
			);
			expect("disabled" in items[1] && items[1].disabled).toBe(false);
		});

		it("returns one item per option", () => {
			const items = buildFilterItems(mockColumn("multi-select"), "test");
			expect(items).toHaveLength(options.length);
		});

		it("reflects selected state from current filter value", () => {
			const items = buildFilterItems(
				mockColumn("multi-select", ["active"]),
				"test",
			);
			expect(items.map((i) => "checked" in i && i.checked)).toEqual([
				true,
				false,
			]);
		});

		it("adds a value when checked", () => {
			const column = mockColumn("multi-select", ["active"]);
			const item = buildFilterItems(column, "test")[1];
			expect(item.kind).toBe("checkbox");
			if (item.kind === "checkbox") item.onCheckedChange?.(true);
			expect(written(column, ["active"])).toEqual(["active", "inactive"]);
		});

		it("clears filter when last value is unchecked", () => {
			const column = mockColumn("multi-select", ["active"]);
			const item = buildFilterItems(column, "test")[0];
			expect(item.kind).toBe("checkbox");
			if (item.kind === "checkbox") item.onCheckedChange?.(false);
			expect(written(column, ["active"])).toBeUndefined();
		});
	});

	describe("single-select", () => {
		it("returns a single radio group", () => {
			const items = buildFilterItems(mockColumn("single-select"), "test");
			expect(items).toHaveLength(1);
			expect(items[0].kind).toBe("radio");
		});

		it("scopes radio name with prefix and column id", () => {
			const items = buildFilterItems(mockColumn("single-select"), "filter");
			expect("name" in items[0] && items[0].name).toBe("filter-status");
		});

		it("sets filter on value change", () => {
			const column = mockColumn("single-select");
			const item = buildFilterItems(column, "test")[0];
			expect(item.kind).toBe("radio");
			if (item.kind !== "radio") return;
			item.onValueChange?.("inactive");
			expect(written(column)).toBe("inactive");
		});

		it("clears filter when value is empty", () => {
			const column = mockColumn("single-select", "active");
			const item = buildFilterItems(column, "test")[0];
			expect(item.kind).toBe("radio");
			if (item.kind !== "radio") return;
			item.onValueChange?.("");
			expect(written(column, "active")).toBeUndefined();
		});
	});
});

describe("getColumnOptions", () => {
	it("limits derived options on a grouped column to the active group", () => {
		const column = {
			id: "status",
			columnDef: {
				meta: {
					filterVariant: "multi-select",
					groups: [
						{ id: "all", label: "All" },
						{ id: "issues", label: "Issues", values: ["down"] },
					],
				},
			},
			getFilterValue: () => ({ group: "issues" }),
			getFacetedUniqueValues: () =>
				new Map<unknown, number>([
					["up", 2],
					["down", 1],
				]),
		} as unknown as Column<RowData, unknown>;
		expect(getColumnOptions(column).map((o) => o.value)).toEqual(["down"]);
	});
});
