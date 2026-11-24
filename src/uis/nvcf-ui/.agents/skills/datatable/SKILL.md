---
name: datatable
description: >-
  Build and change list tables in the nvcf-ui app with the shared DataTable
  compound component (TanStack Table): columns, toolbars, search, sort, column
  visibility, per-column filter dropdowns, group pills, and URL state. Use when
  adding a list page, adding or changing a table column, adding filters or
  pills to a table, or editing `ui/src/components/DataTable/`.
license: Apache-2.0
---

# DataTable

`ui/src/components/DataTable/` wraps TanStack Table v8. A page passes `data`
and `columns`, then composes tools as children. Every tool reads the table
from context, so tools can sit in any order or layout.

```tsx
const search = Route.useSearch();
const navigate = Route.useNavigate();
const tableUrlState = useTableUrlSync(search, navigate);

<DataTable columns={columns} data={rows} reactTableOptions={tableUrlState}>
  <DataTable.Toolbar>
    <div className="flex items-center gap-6">
      <DataTable.Search className="w-64" placeholder="Search clusters..." />
      <DataTable.GroupFilter />
    </div>
  </DataTable.Toolbar>
  <DataTable.Toolbar>
    <div className="flex items-center gap-4">
      <DataTable.Filters />
    </div>
    <div className="flex items-center gap-4">
      <DataTable.ItemCount label="Cluster" />
      <DataTable.ColumnVisibility />
    </div>
  </DataTable.Toolbar>
  <DataTable.ActiveFilters />
  <DataTable.Content />
  <DataTable.Pagination />
</DataTable>
```

`DataTable.Toolbar` is KUI `TableToolbar`: one 48px row that spreads its
direct children apart (`justify-content: space-between`). Group the tools for
each side of a row in your own flex container, as above. Use one toolbar per
row.

## Where things go

- Columns and toolbar composition live in the route file. They are page
  scaffolding (see `ARCHITECTURE.md`).
- Generic table behavior lives in `components/DataTable/`. Domain logic, such
  as which statuses count as issues, lives in the feature and is passed in
  through column `meta`.
- List pages extend `baseTableSearchSchema` and pass `useTableUrlSync` output
  as `reactTableOptions`, so search, sort, filters, and pagination live in the
  URL.

## Components

| Component | Renders |
| --- | --- |
| `Toolbar` | One toolbar row (see above). |
| `Search` | Global search box. Searches every column. |
| `Sort` | Sort dropdown for sortable columns. `meta.sortType` (`text`, `date`, `number`) or `meta.sortLabels` sets the option labels. |
| `Filters` | One dropdown per `filterVariant` column, in column order. The default. |
| `ColumnFilter columnId` | One dropdown, placed by hand. |
| `FilterMenu` | One dropdown with a submenu per filterable column. |
| `GroupFilter` | Pills for the first column with `groups`, or for `columnId`. |
| `ActiveFilters` | One chip per filtered column, plus Clear filters. |
| `ItemCount` | `N <label>`, or `N of M <label>` while filters or search narrow the rows. M is the active group's rows. `pluralLabel` overrides the plural. |
| `ColumnVisibility` | Show and hide columns. |
| `Content` | The table, or `renderContent` for a custom layout (cards). Takes `emptyMessage`, `noResultsMessage`, and `noColumnsMessage`. |
| `Pagination` | Page controls. `pageSizeOptions` sets the sizes. |
| `Skeleton`, `SkeletonToolbar`, `SkeletonContent` | Loading placeholders for a route's `pendingComponent`. |

## Filters

Declare filters on column `meta`. Components render whatever the columns
declare.

```ts
columnHelper.accessor((c) => getStatusLabel(c.status), {
  id: "status",
  header: "Status",
  meta: {
    filterVariant: "multi-select",          // dropdown
    filterOptions: [{ label: "Ready", value: "Ready" }],  // omit to list values from the data
    groups: [                               // pills
      { id: "all", label: "All" },
      { id: "issues", label: "Issues", values: ["No Contact", "Unavailable"] },
    ],
  },
});
```

- `filterVariant` adds a dropdown. Set `filterFn: "multiSelect"` on columns
  without `groups`.
- `groups` adds pills. `DataTable` sets the filter function for grouped
  columns.
- A column can have either or both.
- Array-valued columns, such as GPU types, also need `getUniqueValues`
  returning the array, so each element is its own option and count.

Rules for filters and groups are in `references/filters.md`. Read it before
changing filter behavior.

## Validate

1. Add or update tests next to the changed component. Use the `testing` skill.
2. Run `task verify`.
