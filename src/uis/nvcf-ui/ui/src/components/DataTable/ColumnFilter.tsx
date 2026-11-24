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

import { Dropdown } from "@nvidia/foundations-react-core";
import { useDataTable } from "./DataTableContext";
import {
	buildFilterItems,
	filterMenuClassName,
	getColumnLabel,
	getColumnOptions,
} from "./utils";
import { getSelectedValues } from "./utils/groups";

/** KUI `filters.md`: offer typeahead above 7 options. */
const FILTERABLE_THRESHOLD = 7;

interface ColumnFilterProps {
	columnId: string;
}

export function ColumnFilter({ columnId }: ColumnFilterProps) {
	const table = useDataTable("ColumnFilter");
	const column = table.getAllLeafColumns().find((c) => c.id === columnId);
	if (!column?.columnDef.meta?.filterVariant) return null;

	const options = getColumnOptions(column);
	const selectedCount = getSelectedValues(column).length;

	return (
		<Dropdown
			attributes={{ DropdownContent: { className: filterMenuClassName } }}
			filterable={options.length > FILTERABLE_THRESHOLD}
			items={buildFilterItems(column, "column-filter", options)}
			size="small"
		>
			{getColumnLabel(column)}
			{selectedCount > 0 && ` (${selectedCount})`}
		</Dropdown>
	);
}
