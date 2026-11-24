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

import type {
	DropdownCheckboxItemEntry,
	DropdownRadioGroupEntry,
} from "@nvidia/foundations-react-core";
import type { Column, RowData } from "@tanstack/react-table";
import {
	getActiveGroup,
	getSelectedValues,
	isInGroup,
	withSelected,
} from "./groups";

type Option = { label: string; value: string };

/** Filter menus have a minimum width, and disabled options keep the menu background. */
export const filterMenuClassName =
	"min-w-48 [&_.nv-menu-item--disabled]:bg-transparent";

/** Fixed `filterOptions`, else the faceted values plus any selected, limited to the active group. */
export function getColumnOptions(column: Column<RowData, unknown>): Option[] {
	const values = new Set([
		...[...column.getFacetedUniqueValues().keys()].map(String),
		...getSelectedValues(column),
	]);
	const options =
		column.columnDef.meta?.filterOptions ??
		[...values]
			.filter(Boolean)
			.sort((a, b) => a.localeCompare(b))
			.map((value) => ({ label: value, value }));
	const group = getActiveGroup(column);
	return options.filter((option) => isInGroup(option.value, group));
}

/** Dropdown items with faceted counts. Options at 0 are disabled unless selected. */
export function buildFilterItems(
	column: Column<RowData, unknown>,
	namePrefix: string,
	options = getColumnOptions(column),
): (DropdownCheckboxItemEntry | DropdownRadioGroupEntry)[] {
	const filterVariant = column.columnDef.meta?.filterVariant;
	const counts = new Map<string, number>();
	for (const [value, n] of column.getFacetedUniqueValues()) {
		counts.set(String(value), (counts.get(String(value)) ?? 0) + n);
	}
	const selected = getSelectedValues(column);
	const items = options.map((opt) => {
		const count = counts.get(opt.value) ?? 0;
		const disabled = count === 0 && !selected.includes(opt.value);
		return {
			...opt,
			disabled,
			slotEnd: (
				<span className={disabled ? undefined : "text-placeholder"}>
					{count}
				</span>
			),
		};
	});

	if (filterVariant === "multi-select") {
		return items.map(({ label, value, disabled, slotEnd }) => ({
			kind: "checkbox" as const,
			children: label,
			checked: selected.includes(value),
			disabled,
			slotEnd,
			onCheckedChange: (checked: boolean | "indeterminate") => {
				const next = checked
					? [...selected, value]
					: selected.filter((v) => v !== value);
				column.setFilterValue(withSelected(next.length > 0 ? next : undefined));
			},
		}));
	}

	if (filterVariant === "single-select") {
		return [
			{
				kind: "radio" as const,
				slotHeading: null,
				name: `${namePrefix}-${column.id}`,
				value: selected[0] ?? "",
				onValueChange: (value: string) => {
					column.setFilterValue(withSelected(value || undefined));
				},
				items: items.map(({ label, value, disabled, slotEnd }) => ({
					children: label,
					value,
					disabled,
					slotEnd,
				})),
			},
		];
	}

	return [];
}
