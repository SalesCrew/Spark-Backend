-- New SM time corrections capture the exact requested start and end stamps.
-- Existing reviewed duration-only requests remain readable as version 0.

set lock_timeout = '5s';
set statement_timeout = '120s';

alter table public.sm_assignment_time_change_requests
  add column timestamp_correction_version integer not null default 0,
  add column original_started_at timestamptz,
  add column original_completed_at timestamptz,
  add column requested_started_at timestamptz,
  add column requested_completed_at timestamptz;

alter table public.sm_assignment_time_change_requests
  alter column timestamp_correction_version set default 1,
  add constraint sm_assignment_time_change_requests_timestamp_version_ck
    check (timestamp_correction_version in (0, 1)),
  add constraint sm_assignment_time_change_requests_timestamps_ck
    check (
      (
        timestamp_correction_version = 0
        and original_started_at is null
        and original_completed_at is null
        and requested_started_at is null
        and requested_completed_at is null
      )
      or (
        timestamp_correction_version = 1
        and (
          (original_started_at is null and original_completed_at is null)
          or (
            original_started_at is not null
            and original_completed_at is not null
            and original_completed_at > original_started_at
          )
        )
        and (
          (
            request_kind = 'deletion'
            and requested_started_at is null
            and requested_completed_at is null
          )
          or (
            request_kind = 'time_change'
            and requested_started_at is not null
            and requested_completed_at is not null
            and requested_completed_at > requested_started_at
            and requested_completed_at <= requested_started_at + interval '24 hours'
            and requested_minutes = greatest(
              1,
              round(extract(epoch from (requested_completed_at - requested_started_at)) / 60.0)::integer
            )
          )
        )
      )
    );

comment on column public.sm_assignment_time_change_requests.timestamp_correction_version is
  '0 = historical duration-only request; 1 = exact start/end timestamp correction.';
comment on column public.sm_assignment_time_change_requests.requested_started_at is
  'Exact visit start requested by the SM; applied only after admin approval.';
comment on column public.sm_assignment_time_change_requests.requested_completed_at is
  'Exact visit end requested by the SM; applied only after admin approval.';
