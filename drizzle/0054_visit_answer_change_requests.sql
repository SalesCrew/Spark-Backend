do $$
begin
  create type "visit_answer_change_request_status" as enum ('pending', 'approved', 'rejected', 'cancelled');
exception
  when duplicate_object then null;
end $$;

create table if not exists "visit_answer_change_requests" (
  "id" uuid primary key default gen_random_uuid(),
  "visit_session_id" uuid not null references "visit_sessions"("id") on delete cascade,
  "visit_session_section_id" uuid not null references "visit_session_sections"("id") on delete cascade,
  "visit_session_question_id" uuid not null references "visit_session_questions"("id") on delete cascade,
  "visit_answer_id" uuid references "visit_answers"("id") on delete set null,
  "question_id" uuid not null references "question_bank_shared"("id") on delete restrict,
  "gm_user_id" uuid not null references "users"("id") on delete cascade,
  "market_id" uuid not null references "markets"("id") on delete cascade,
  "question_type" "fragebogen_question_type" not null,
  "question_text_snapshot" text not null default '',
  "current_answer_snapshot" jsonb not null default '{}'::jsonb,
  "requested_answer_payload" jsonb not null default '{}'::jsonb,
  "requested_answer_summary" text not null default '',
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

create unique index if not exists "visit_answer_change_requests_pending_question_unique"
  on "visit_answer_change_requests" ("gm_user_id", "visit_session_question_id")
  where "is_deleted" = false and "status" = 'pending';

create index if not exists "visit_answer_change_requests_gm_status_idx"
  on "visit_answer_change_requests" ("gm_user_id", "status", "created_at");

create index if not exists "visit_answer_change_requests_session_idx"
  on "visit_answer_change_requests" ("visit_session_id", "created_at");

create index if not exists "visit_answer_change_requests_market_idx"
  on "visit_answer_change_requests" ("market_id", "created_at");

create index if not exists "visit_answer_change_requests_deleted_idx"
  on "visit_answer_change_requests" ("is_deleted");

grant select, insert, update, delete on table "visit_answer_change_requests" to authenticated;
grant all on table "visit_answer_change_requests" to service_role;

alter table "visit_answer_change_requests" enable row level security;
alter table "visit_answer_change_requests" force row level security;

drop policy if exists "authenticated_full_access" on "visit_answer_change_requests";
create policy "authenticated_full_access"
  on "visit_answer_change_requests"
  for all
  to authenticated
  using (true)
  with check (true);
