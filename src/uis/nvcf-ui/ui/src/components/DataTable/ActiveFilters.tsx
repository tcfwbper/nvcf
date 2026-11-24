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

import { Button, Tag } from "@nvidia/foundations-react-core";
import { X } from "lucide-react";
import { useDataTable } from "./DataTableContext";
import { getColumnLabel } from "./utils";
import {
	getSelectedValues,
	resetSelections,
	withSelected,
} from "./utils/groups";

export function ActiveFilters() {
	const table = useDataTable("ActiveFilters");
	const columnFilters = table.getState().columnFilters;

	if (columnFilters.length === 0) return null;

	const activeFilters = columnFilters.flatMap((columnFilter) => {
		const column = table.getColumn(columnFilter.id);
		if (!column) return [];

		const { filterVariant, filterOptions = [] } = column.columnDef.meta ?? {};
		if (!filterVariant) return [];

		const selected = getSelectedValues(column);
		if (selected.length === 0) return [];

		const resolveLabel = (value: string) =>
			filterOptions.find((o) => o.value === value)?.label ?? value;
		const label = getColumnLabel(column);
		const displayValue = selected.map(resolveLabel).join(", ");

		return [{ column, label, displayValue }];
	});

	if (activeFilters.length === 0) return null;

	return (
		<div className="flex flex-wrap items-center gap-2">
			{activeFilters.map(({ column, label, displayValue }) => (
				<Tag
					aria-label={`Remove ${label} filter: ${displayValue}`}
					className="whitespace-nowrap"
					color="gray"
					key={column.id}
					kind="outline"
					onClick={() => column.setFilterValue(withSelected(undefined))}
				>
					<b>{label}: </b>
					{displayValue}
					<X size="1em" />
				</Tag>
			))}
			<Button
				color="neutral"
				kind="tertiary"
				onClick={() => resetSelections(table)}
				size="small"
			>
				Clear filters
			</Button>
		</div>
	);
}
