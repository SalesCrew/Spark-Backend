-- Remove Supabase default table privileges that the planning runtime does not use.

set lock_timeout = '5s';
set statement_timeout = '30s';

revoke references, trigger on table public.sm_assignment_series from service_role;
revoke references, trigger on table public.sm_assignment_series_versions from service_role;
revoke references, trigger on table public.sm_assignments from service_role;
revoke references, trigger on table public.sm_assignment_events from service_role;
revoke references, trigger on table public.sm_assignment_time_submissions from service_role;
