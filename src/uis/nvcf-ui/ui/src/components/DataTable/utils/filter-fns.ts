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

import type { FilterFn, RowData } from "@tanstack/react-table";
import { castArray } from "lodash-es";

const includesValue: FilterFn<RowData> = (
	row,
	columnId,
	filterValue: string,
) => {
	const search = filterValue.toLowerCase();
	const cellValue = row.getValue(columnId);
	if (typeof cellValue === "string") {
		return cellValue.toLowerCase().includes(search);
	}
	if (Array.isArray(cellValue)) {
		return cellValue.some(
			(item) => typeof item === "string" && item.toLowerCase().includes(search),
		);
	}
	if (cellValue != null) {
		return String(cellValue).toLowerCase().includes(search);
	}
	return false;
};

includesValue.autoRemove = (val) =>
	val === undefined || val === null || val === "";

/** True when the cell, or any element of an array cell, is in `selected`. Compares as strings. */
export function matchesSelection(cellValue: unknown, selected: unknown[]) {
	return castArray(cellValue).some((v) => selected.includes(String(v)));
}

const multiSelect: FilterFn<RowData> = (row, columnId, filterValue) =>
	Array.isArray(filterValue) &&
	matchesSelection(row.getValue(columnId), filterValue);

export const filterFns = { includesValue, multiSelect };
