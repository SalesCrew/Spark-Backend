do $$
begin
  create type "time_entry_change_request_status" as enum ('pending', 'approved', 'rejected', 'cancelled');
exception
  when duplicate_object then null;
end $$;

do $$
begin
  create type "time_entry_change_request_source_kind" as enum ('marktbesuch', 'pause', 'zusatzzeit');
exception
  when duplicate_object then null;
end $$;

create table if not exists "time_entry_change_requests" (
  "id" uuid primary key default gen_random_uuid(),
  "day_session_id" uuid not null references "gm_day_sessions"("id") on delete cascade,
  "gm_user_id" uuid not null references "users"("id") on delete cascade,
  "source_kind" "time_entry_change_request_source_kind" not null,
  "source_id" uuid not null,
  "work_date" date not null,
  "timezone" text not null default 'Europe/Vienna',
  "title_snapshot" text not null default '',
  "subtitle_snapshot" text,
  "original_start_at" timestamptz not null,
  "original_end_at" timestamptz not null,
  "requested_start_at" timestamptz not null,
  "requested_end_at" timestamptz not null,
  "request_note" text,
  "status" "time_entry_change_request_status" not null default 'pending',
  "reviewed_by_user_id" uuid references "users"("id") on delete set null,
  "reviewed_at" timestamptz,
  "applied_at" timestamptz,
  "admin_note" text,
  "is_deleted" boolean not null default false,
  "deleted_at" timestamptz,
  "created_at" timestamptz not null default now(),
  "updated_at" timestamptz not null default now()
);

create unique index if not exists "time_entry_change_requests_pending_source_unique"
  on "time_entry_change_requests" ("gm_user_id", "source_kind", "source_id")
  where "is_deleted" = false and "status" = 'pending';

create index if not exists "time_entry_change_requests_gm_status_idx"
  on "time_entry_change_requests" ("gm_user_id", "status", "created_at");

create index if not exists "time_entry_change_requests_day_session_idx"
  on "time_entry_change_requests" ("day_session_id", "created_at");

create index if not exists "time_entry_change_requests_source_idx"
  on "time_entry_change_requests" ("source_kind", "source_id");

create index if not exists "time_entry_change_requests_work_date_idx"
  on "time_entry_change_requests" ("work_date");

create index if not exists "time_entry_change_requests_deleted_idx"
  on "time_entry_change_requests" ("is_deleted");

revoke all on table public.time_entry_change_requests from public, anon, authenticated;
grant all on table public.time_entry_change_requests to service_role;

alter table public.time_entry_change_requests enable row level security;
alter table public.time_entry_change_requests force row level security;

drop policy if exists time_entry_change_requests_service_role_full on public.time_entry_change_requests;
create policy time_entry_change_requests_service_role_full
  on public.time_entry_change_requests
  for all
  to service_role
  using (true)
  with check (true);
