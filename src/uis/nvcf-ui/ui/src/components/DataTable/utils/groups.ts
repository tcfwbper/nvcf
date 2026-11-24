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
	Column,
	ColumnDef,
	FilterFn,
	RowData,
	Table,
} from "@tanstack/react-table";
import { castArray, isPlainObject } from "lodash-es";
import { matchesSelection } from "./filter-fns";

export interface ColumnGroup {
	id: string;
	label: string;
	/** Column values in this group. Omit to match every row. */
	values?: string[];
}

type Selection = string | string[];
type GroupFilterValue = { group?: string; selected?: Selection };
type AnyColumn = Column<RowData, unknown>;

/** The group id and selections. A plain value, such as from a link, is a selection in the default group. */
function parseFilterValue(value: unknown) {
	const { group, selected }: GroupFilterValue = isPlainObject(value)
		? (value as GroupFilterValue)
		: { selected: value as Selection | undefined };
	return { group, selected: castArray(selected ?? []) };
}

/** The active group and the selections within it. Selections outside the group are dropped. */
function readFilterValue(value: unknown, groups: ColumnGroup[]) {
	const { group: id, selected } = parseFilterValue(value);
	const group =
		groups.find((g) => g.id === id) ?? groups.find((g) => !g.values);
	return {
		group,
		selected: selected.filter(
			(v) => !group?.values || group.values.includes(v),
		),
	};
}

export function isInGroup(value: unknown, group?: ColumnGroup): boolean {
	const values = group?.values;
	return !values || castArray(value).some((v) => values.includes(String(v)));
}

function groupFilter<TData extends RowData>(
	groups: ColumnGroup[],
): FilterFn<TData> {
	const fn: FilterFn<TData> = (
		row,
		columnId,
		{ group, selected }: ReturnType<typeof readFilterValue>,
	) => {
		const cell = row.getValue(columnId);
		return (
			isInGroup(cell, group) &&
			(selected.length === 0 || matchesSelection(cell, selected))
		);
	};
	fn.resolveFilterValue = (value) => readFilterValue(value, groups);
	fn.autoRemove = (value) => {
		const { group, selected } = readFilterValue(value, groups);
		return !group?.values && selected.length === 0;
	};
	return fn;
}

/** Sets the group filter function on every column that declares `meta.groups`. */
export function withColumnGroups<TData extends RowData, TValue>(
	columns: ColumnDef<TData, TValue>[],
): ColumnDef<TData, TValue>[] {
	return columns.map((def) => {
		if ("columns" in def && def.columns) {
			return { ...def, columns: withColumnGroups(def.columns) };
		}
		const groups = def.meta?.groups;
		return groups ? { ...def, filterFn: groupFilter(groups) } : def;
	});
}

/** Dropdown selections. On grouped columns, only those in the active group. */
export function getSelectedValues(column: AnyColumn): string[] {
	const value = column.getFilterValue();
	const groups = column.columnDef.meta?.groups;
	return groups
		? readFilterValue(value, groups).selected
		: parseFilterValue(value).selected;
}

/** Updater for `column.setFilterValue` that sets selections and keeps the column's group. */
export const withSelected = (selected?: Selection) => (old: unknown) => {
	const { group } = parseFilterValue(old);
	return group ? { group, selected } : selected;
};

/** The column with `columnId`, else the first column that declares groups. */
export function getGroupedColumn(
	table: Table<RowData>,
	columnId?: string,
): AnyColumn | undefined {
	return table
		.getAllLeafColumns()
		.find((c) => c.columnDef.meta?.groups && (!columnId || c.id === columnId));
}

export function getActiveGroup(column: AnyColumn): ColumnGroup | undefined {
	const groups = column.columnDef.meta?.groups;
	return groups && readFilterValue(column.getFilterValue(), groups).group;
}

/** Switches `column` to group `groupId` and clears every other filter. */
export function setActiveGroup(
	table: Table<RowData>,
	column: AnyColumn,
	groupId: string,
) {
	const group = column.columnDef.meta?.groups?.find((g) => g.id === groupId);
	if (!group) return;
	table.setColumnFilters(
		group.values ? [{ id: column.id, value: { group: group.id } }] : [],
	);
}

/** Clears every dropdown selection and keeps each column's group. */
export function resetSelections(table: Table<RowData>) {
	table.setColumnFilters((filters) =>
		filters.flatMap((filter) => {
			const { group } = parseFilterValue(filter.value);
			return group ? [{ id: filter.id, value: { group } }] : [];
		}),
	);
}
