# DataTable filters

How filter dropdowns and group pills behave. Each rule has a test in
`ui/src/components/DataTable/`.

## Dropdowns

Any column with `meta.filterVariant`.

1. Options are the column's fixed `filterOptions`, or the faceted values plus
   any selected value when there are none. A derived value that other filters
   exclude is hidden.
2. Each option shows a count from TanStack faceting: matching rows given the
   other filters and search, but not its own column. Options at 0 are
   disabled. Selected options stay enabled.
3. A selected value missing from fixed `filterOptions`, such as after a
   refetch, still filters and shows a chip until cleared.
4. Triggers show the number of selected values, as in `Status (2)`.
5. Clear filters clears every selection.

## Groups

A group is a named set of one column's values, declared in `meta.groups`,
and rendered as pills. The active group defines the rows the table works
with; dropdowns narrow within it.

1. Groups render in declared order. The group without `values` is the default
   and is active when none is selected. It matches every row.
2. An unknown group id falls back to the default group.
3. Group counts are totals over the unfiltered rows. The item count reads
   `N of M`, where M is the active group's rows, so a group alone is not a
   filter.
4. Switching groups clears every other filter. Search is kept.
5. Clear filters keeps the group. The group alone has no chip.
6. On the grouped column, fixed options are limited to the group's `values`.
   Other columns follow the group through faceting.

### Groups on a column with a dropdown

The pills and the dropdown share the column's TanStack filter value:
`{ group: "<group id>", selected: ["<value>"] }`.

- Read selections with `getSelectedValues` and write them with
  `column.setFilterValue(withSelected(next))`, both from `utils/groups.ts`.
  Never read `getFilterValue` directly. `withSelected` keeps the group intact.
- Selections outside the active group's `values` are ignored: no effect on
  rows, no chip, no trigger count.
- A plain value, such as `filters=[{"id":"<column id>","value":["<value>"]}]`
  from a link, is a selection in the default group.

## Limits

- `getGroupedColumn` and `GroupFilter` default to the first grouped column.
  More than one grouped column works mechanically, but the UI for several
  pill sets is not designed yet.
