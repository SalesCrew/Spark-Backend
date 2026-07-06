create table if not exists "visit_session_delete_requests" (
  "id" uuid primary key default gen_random_uuid(),
  "visit_session_id" uuid not null references "visit_sessions"("id") on delete cascade,
  "gm_user_id" uuid not null references "users"("id") on delete cascade,
  "market_id" uuid not null references "markets"("id") on delete cascade,
  "market_name_snapshot" text not null default '',
  "market_address_snapshot" text not null default '',
  "market_postal_code_snapshot" text not null default '',
  "market_city_snapshot" text not null default '',
  "campaign_summary_snapshot" text not null default '',
  "section_summary_snapshot" text not null default '',
  "session_started_at_snapshot" timestamptz,
  "session_submitted_at_snapshot" timestamptz,
  "request_note" text,
  "status" "visit_answer_change_request_status" not null default 'pending',
  "reviewed_by_user_id" uuid references "users"("id") on delete set null,
  "reviewed_at" timestamptz,
  "admin_note" text,
  "is_deleted" boolean not null default false,
  "deleted_at" timestamptz,
  "created_at" timestamptz not null default now(),
  "updated_at" timestamptz not null default now()
);

create unique index if not exists "visit_session_delete_requests_pending_session_unique"
  on "visit_session_delete_requests" ("visit_session_id")
  where "is_deleted" = false and "status" = 'pending';

create index if not exists "visit_session_delete_requests_gm_status_idx"
  on "visit_session_delete_requests" ("gm_user_id", "status", "created_at");

create index if not exists "visit_session_delete_requests_session_idx"
  on "visit_session_delete_requests" ("visit_session_id", "created_at");

create index if not exists "visit_session_delete_requests_market_idx"
  on "visit_session_delete_requests" ("market_id", "created_at");

create index if not exists "visit_session_delete_requests_deleted_idx"
  on "visit_session_delete_requests" ("is_deleted");

revoke all on table public.visit_session_delete_requests from public, anon, authenticated;
grant all on table public.visit_session_delete_requests to service_role;

alter table public.visit_session_delete_requests enable row level security;
alter table public.visit_session_delete_requests force row level security;

drop policy if exists visit_session_delete_requests_service_role_full on public.visit_session_delete_requests;
create policy visit_session_delete_requests_service_role_full
  on public.visit_session_delete_requests
  for all
  to service_role
  using (true)
  with check (true);
