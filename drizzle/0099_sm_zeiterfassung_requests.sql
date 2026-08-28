-- Durable Shelf Merchandiser time-correction workflow. This remains isolated
-- from the GM day-session domain and only references SM assignment records.

set lock_timeout = '5s';
set statement_timeout = '120s';

create unique index sm_assignment_time_submissions_id_assignment_unique
  on public.sm_assignment_time_submissions (id, assignment_id);

create table public.sm_assignment_time_change_requests (
  id uuid primary key default gen_random_uuid(),
  assignment_id uuid not null references public.sm_assignments(id) on delete restrict,
  sm_user_id uuid not null references public.users(id) on delete restrict,
  source_time_submission_id uuid not null,
  request_kind text not null,
  original_minutes integer not null,
  requested_minutes integer,
  request_reason text not null,
  client_request_token text not null,
  status public.sm_change_request_status not null default 'pending',
  reviewed_by_user_id uuid references public.users(id) on delete set null,
  reviewed_at timestamptz,
  admin_note text,
  applied_time_submission_id uuid,
  applied_at timestamptz,
  is_deleted boolean not null default false,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sm_assignment_time_change_requests_source_assignment_fk
    foreign key (source_time_submission_id, assignment_id)
    references public.sm_assignment_time_submissions(id, assignment_id)
    on delete restrict,
  constraint sm_assignment_time_change_requests_applied_assignment_fk
    foreign key (applied_time_submission_id, assignment_id)
    references public.sm_assignment_time_submissions(id, assignment_id)
    on delete restrict,
  constraint sm_assignment_time_change_requests_kind_ck
    check (request_kind in ('time_change', 'deletion')),
  constraint sm_assignment_time_change_requests_minutes_ck
    check (
      original_minutes between 1 and 1440
      and (
        (request_kind = 'time_change' and requested_minutes between 1 and 1440 and requested_minutes <> original_minutes)
        or (request_kind = 'deletion' and requested_minutes is null)
      )
    ),
  constraint sm_assignment_time_change_requests_reason_ck
    check (btrim(request_reason) <> '' and btrim(client_request_token) <> ''),
  constraint sm_assignment_time_change_requests_review_ck
    check (
      (status in ('pending', 'cancelled') and reviewed_by_user_id is null and reviewed_at is null and applied_time_submission_id is null and applied_at is null)
      or (status = 'rejected' and reviewed_by_user_id is not null and reviewed_at is not null and applied_time_submission_id is null and applied_at is null)
      or (
        status = 'approved'
        and reviewed_by_user_id is not null
        and reviewed_at is not null
        and applied_at is not null
        and (
          (request_kind = 'time_change' and applied_time_submission_id is not null)
          or (request_kind = 'deletion' and applied_time_submission_id is null)
        )
      )
    ),
  constraint sm_assignment_time_change_requests_soft_delete_ck
    check ((is_deleted and deleted_at is not null) or (not is_deleted and deleted_at is null))
);

create unique index sm_assignment_time_change_requests_client_token_active_unique
  on public.sm_assignment_time_change_requests (sm_user_id, client_request_token)
  where is_deleted = false;
create unique index sm_assignment_time_change_requests_pending_assignment_unique
  on public.sm_assignment_time_change_requests (assignment_id)
  where is_deleted = false and status = 'pending';
create index sm_assignment_time_change_requests_sm_status_idx
  on public.sm_assignment_time_change_requests (sm_user_id, status, created_at desc)
  where is_deleted = false;
create index sm_assignment_time_change_requests_assignment_idx
  on public.sm_assignment_time_change_requests (assignment_id, created_at desc);
create index sm_assignment_time_change_requests_reviewer_idx
  on public.sm_assignment_time_change_requests (reviewed_by_user_id);
create index sm_assignment_time_change_requests_source_idx
  on public.sm_assignment_time_change_requests (source_time_submission_id);
create index sm_assignment_time_change_requests_applied_idx
  on public.sm_assignment_time_change_requests (applied_time_submission_id);

create trigger sm_assignment_time_change_requests_soft_delete_fields_trg
before insert or update on public.sm_assignment_time_change_requests
for each row execute function public.sm_sync_soft_delete_fields();

create trigger sm_assignment_time_change_requests_reject_hard_delete_trg
before delete on public.sm_assignment_time_change_requests
for each row execute function public.sm_reject_hard_delete();

create trigger sm_assignment_time_change_requests_reject_truncate_trg
before truncate on public.sm_assignment_time_change_requests
for each statement execute function public.sm_reject_hard_delete();

alter table public.sm_assignment_time_change_requests enable row level security;
alter table public.sm_assignment_time_change_requests force row level security;

revoke all privileges on table public.sm_assignment_time_change_requests from public, anon, authenticated;
grant select, insert, update on table public.sm_assignment_time_change_requests to service_role;
revoke delete, truncate, references, trigger on table public.sm_assignment_time_change_requests from service_role;

comment on table public.sm_assignment_time_change_requests is
  'Durable SM requests to correct or remove the current versioned actual time of one completed assignment.';
comment on column public.sm_assignment_time_change_requests.source_time_submission_id is
  'Exact current time revision seen by the SM when the request was created; approval fails safely if it became stale.';
