-- Add the exact supporting index for the composite assignment -> series-version FK.
-- The former single-column index is redundant because the new composite index has
-- series_version_id as its leftmost column.

set lock_timeout = '5s';
set statement_timeout = '30s';

create index sm_assignments_series_version_series_idx
  on public.sm_assignments (series_version_id, series_id);

drop index public.sm_assignments_series_version_idx;
