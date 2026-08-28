-- Exact timestamp corrections may move start and end together without changing
-- the derived duration. Historical duration-only requests keep the old rule.

set lock_timeout = '5s';
set statement_timeout = '120s';

alter table public.sm_assignment_time_change_requests
  drop constraint sm_assignment_time_change_requests_minutes_ck,
  drop constraint sm_assignment_time_change_requests_timestamps_ck;

alter table public.sm_assignment_time_change_requests
  add constraint sm_assignment_time_change_requests_minutes_ck
    check (
      original_minutes between 1 and 1440
      and (
        (
          request_kind = 'time_change'
          and requested_minutes between 1 and 1440
          and (
            timestamp_correction_version = 1
            or requested_minutes <> original_minutes
          )
        )
        or (
          request_kind = 'deletion'
          and requested_minutes is null
        )
      )
    ),
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
          (
            request_kind = 'deletion'
            and (
              (original_started_at is null and original_completed_at is null)
              or (
                original_started_at is not null
                and original_completed_at is not null
                and original_completed_at > original_started_at
              )
            )
            and requested_started_at is null
            and requested_completed_at is null
          )
          or (
            request_kind = 'time_change'
            and original_started_at is not null
            and original_completed_at is not null
            and original_completed_at > original_started_at
            and requested_started_at is not null
            and requested_completed_at is not null
            and requested_completed_at > requested_started_at
            and requested_completed_at <= requested_started_at + interval '24 hours'
            and requested_minutes = greatest(
              1,
              round(extract(epoch from (requested_completed_at - requested_started_at)) / 60.0)::integer
            )
            and (
              requested_started_at <> original_started_at
              or requested_completed_at <> original_completed_at
              or requested_minutes <> original_minutes
            )
          )
        )
      )
    );
