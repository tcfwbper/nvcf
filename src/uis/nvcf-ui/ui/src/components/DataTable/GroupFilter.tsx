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

import { Tag, Text } from "@nvidia/foundations-react-core";
import { useMemo } from "react";
import { useDataTable } from "./DataTableContext";
import { getColumnLabel } from "./utils";
import {
	getActiveGroup,
	getGroupedColumn,
	isInGroup,
	setActiveGroup,
} from "./utils/groups";

interface GroupFilterProps {
	/** Grouped column to render. Defaults to the first column with `meta.groups`. */
	columnId?: string;
	/** Accessible name for the pills. Defaults to the column label. */
	label?: string;
}

/** Pills for a column's `meta.groups`. Switching groups clears the other filters. */
export function GroupFilter({ columnId, label }: GroupFilterProps) {
	const table = useDataTable("GroupFilter");
	const column = getGroupedColumn(table, columnId);
	const groups = column?.columnDef.meta?.groups;
	const rows = table.getCoreRowModel().rows;

	const counts = useMemo(
		() =>
			column && groups
				? groups.map(
						(group) =>
							rows.filter((row) => isInGroup(row.getValue(column.id), group))
								.length,
					)
				: [],
		[column, groups, rows],
	);

	if (!column || !groups) return null;

	const active = getActiveGroup(column);

	return (
		<fieldset className="flex min-w-0 flex-wrap items-center gap-2">
			<legend className="sr-only">{label ?? getColumnLabel(column)}</legend>
			{groups.map((group, index) => (
				<Tag
					aria-pressed={group === active}
					color="gray"
					key={group.id}
					kind="outline"
					onClick={() => setActiveGroup(table, column, group.id)}
					selected={group === active}
				>
					{group.label}
					<Text kind="body/regular/xs">{counts[index]}</Text>
				</Tag>
			))}
		</fieldset>
	);
}
