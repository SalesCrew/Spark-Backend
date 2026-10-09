-- SMDurcharbeit monthly campaigns. Additive only: no historical backfill or data mutation.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
create table public.sm_smdurcharbeit_campaigns (
  id uuid primary key default gen_random_uuid(), name text not null check (btrim(name) <> ''),
  status text not null default 'draft' check (status in ('draft','published','paused','archived')),
  start_date date not null, end_date date not null check (end_date >= start_date),
  questionnaire_version_id uuid not null references public.sm_questionnaire_versions(id) on delete restrict,
  roster_draft jsonb not null default '[]'::jsonb check (jsonb_typeof(roster_draft) = 'array'),
  revision integer not null default 1 check (revision > 0),
  created_by_user_id uuid not null references public.users(id) on delete restrict,
  updated_by_user_id uuid not null references public.users(id) on delete restrict,
  created_at timestamptz not null default now(), updated_at timestamptz not null default now()
);
create index sm_smdurcharbeit_campaign_window_idx on public.sm_smdurcharbeit_campaigns(status,start_date,end_date);
create index sm_smdurcharbeit_campaign_version_idx on public.sm_smdurcharbeit_campaigns(questionnaire_version_id);
create table public.sm_smdurcharbeit_campaign_markets (
  id uuid primary key default gen_random_uuid(),
  campaign_id uuid not null references public.sm_smdurcharbeit_campaigns(id) on delete restrict,
  sm_market_id uuid not null references public.sm_smdurcharbeit_markets(sm_market_id) on delete restrict,
  created_at timestamptz not null default now(), unique(campaign_id,sm_market_id), unique(id,campaign_id)
);
create index sm_smdurcharbeit_membership_market_idx on public.sm_smdurcharbeit_campaign_markets(sm_market_id);
create table public.sm_smdurcharbeit_campaign_periods (
  id uuid primary key default gen_random_uuid(),
  campaign_id uuid not null references public.sm_smdurcharbeit_campaigns(id) on delete restrict,
  month date not null check (extract(day from month) = 1),
  questionnaire_version_id uuid not null references public.sm_questionnaire_versions(id) on delete restrict,
  created_at timestamptz not null default now(), unique(campaign_id,month), unique(id,campaign_id)
);
create index sm_smdurcharbeit_period_version_idx on public.sm_smdurcharbeit_campaign_periods(questionnaire_version_id);
create table public.sm_smdurcharbeit_assignment_revisions (
  id uuid primary key default gen_random_uuid(),
  campaign_market_id uuid not null references public.sm_smdurcharbeit_campaign_markets(id) on delete restrict,
  month date not null check (extract(day from month) = 1),
  sm_user_id uuid not null references public.users(id) on delete restrict,
  source_person text, reason text not null check (btrim(reason) <> ''),
  actor_user_id uuid not null references public.users(id) on delete restrict,
  created_at timestamptz not null default now()
);
create index sm_smdurcharbeit_owner_membership_idx on public.sm_smdurcharbeit_assignment_revisions(campaign_market_id,month);
create index sm_smdurcharbeit_owner_user_idx on public.sm_smdurcharbeit_assignment_revisions(sm_user_id);
create table public.sm_smdurcharbeit_month_targets (
  id uuid primary key default gen_random_uuid(),
  campaign_id uuid not null references public.sm_smdurcharbeit_campaigns(id) on delete restrict,
  period_id uuid not null, campaign_market_id uuid not null,
  owner_revision_id uuid not null references public.sm_smdurcharbeit_assignment_revisions(id) on delete restrict,
  market_snapshot jsonb not null,
  eligibility text not null default 'required' check (eligibility in ('required','waived')),
  waiver_reason text, revision integer not null default 1 check (revision > 0),
  latest_submission_id uuid references public.sm_questionnaire_submissions(id) on delete restrict,
  created_at timestamptz not null default now(), updated_at timestamptz not null default now(),
  unique(period_id,campaign_market_id),
  foreign key(period_id,campaign_id) references public.sm_smdurcharbeit_campaign_periods(id,campaign_id) on delete restrict,
  foreign key(campaign_market_id,campaign_id) references public.sm_smdurcharbeit_campaign_markets(id,campaign_id) on delete restrict,
  check (eligibility <> 'waived' or (waiver_reason is not null and btrim(waiver_reason) <> ''))
);
create index sm_smdurcharbeit_target_owner_idx on public.sm_smdurcharbeit_month_targets(owner_revision_id,eligibility);
create index sm_smdurcharbeit_target_membership_idx on public.sm_smdurcharbeit_month_targets(campaign_market_id);
create index sm_smdurcharbeit_target_latest_idx on public.sm_smdurcharbeit_month_targets(latest_submission_id);
create table public.sm_smdurcharbeit_visits (
  id uuid primary key default gen_random_uuid(), target_id uuid not null references public.sm_smdurcharbeit_month_targets(id) on delete restrict,
  owner_revision_id uuid not null references public.sm_smdurcharbeit_assignment_revisions(id) on delete restrict,
  sm_user_id uuid not null references public.users(id) on delete restrict,
  basis_submission_id uuid references public.sm_questionnaire_submissions(id) on delete restrict,
  basis_revision integer not null check (basis_revision > 0),
  created_at timestamptz not null default now(), unique(id,target_id)
);
create index sm_smdurcharbeit_visit_target_idx on public.sm_smdurcharbeit_visits(target_id,created_at);
create index sm_smdurcharbeit_visit_user_idx on public.sm_smdurcharbeit_visits(sm_user_id);
create index sm_smdurcharbeit_visit_owner_idx on public.sm_smdurcharbeit_visits(owner_revision_id);
create index sm_smdurcharbeit_visit_basis_idx on public.sm_smdurcharbeit_visits(basis_submission_id);
alter table public.sm_questionnaire_submissions
  add column smdurcharbeit_visit_id uuid,
  add column smdurcharbeit_target_id uuid,
  add constraint sm_submission_smdurcharbeit_context_ck check (
    (smdurcharbeit_visit_id is null and smdurcharbeit_target_id is null)
    or (smdurcharbeit_visit_id is not null and smdurcharbeit_target_id is not null and assignment_id is null and once_per_market_snapshot = false)),
  add constraint sm_submission_smdurcharbeit_visit_target_fk foreign key(smdurcharbeit_visit_id,smdurcharbeit_target_id)
    references public.sm_smdurcharbeit_visits(id,target_id) on delete restrict;
create unique index sm_submission_smdurcharbeit_visit_unique on public.sm_questionnaire_submissions(smdurcharbeit_visit_id) where smdurcharbeit_visit_id is not null;
create unique index sm_submission_smdurcharbeit_live_draft_unique on public.sm_questionnaire_submissions(smdurcharbeit_target_id)
  where smdurcharbeit_target_id is not null and status = 'draft' and is_current and not is_deleted;
create index sm_submission_smdurcharbeit_target_idx on public.sm_questionnaire_submissions(smdurcharbeit_target_id,submitted_at) where smdurcharbeit_target_id is not null;
create table public.sm_smdurcharbeit_visit_time_revisions (
  id uuid primary key default gen_random_uuid(), visit_id uuid not null references public.sm_smdurcharbeit_visits(id) on delete restrict,
  revision_number integer not null check (revision_number > 0), is_current boolean not null default true,
  started_at timestamptz not null, completed_at timestamptz not null,
  actual_minutes integer not null check (actual_minutes between 1 and 1440), travel_minutes integer not null default 0 check (travel_minutes between 0 and 1440),
  reason text not null, actor_user_id uuid not null references public.users(id) on delete restrict,
  created_at timestamptz not null default now(), check (completed_at > started_at), unique(visit_id,revision_number)
);
create unique index sm_smdurcharbeit_time_current_unique on public.sm_smdurcharbeit_visit_time_revisions(visit_id) where is_current;
create table public.sm_smdurcharbeit_time_change_requests (
  id uuid primary key default gen_random_uuid(), visit_id uuid not null references public.sm_smdurcharbeit_visits(id) on delete restrict,
  sm_user_id uuid not null references public.users(id) on delete restrict, expected_revision integer not null check(expected_revision > 0),
  kind text not null check(kind in ('time_change','deletion')), status text not null default 'pending' check(status in ('pending','approved','rejected','cancelled')),
  started_at timestamptz, completed_at timestamptz, reason text not null check(btrim(reason) <> ''), client_token text not null,
  reviewed_by_user_id uuid references public.users(id) on delete restrict, reviewed_at timestamptz, admin_note text,
  created_at timestamptz not null default now(), unique(sm_user_id,client_token),
  check(kind <> 'time_change' or (started_at is not null and completed_at > started_at))
);
create unique index sm_smdurcharbeit_time_request_pending_unique on public.sm_smdurcharbeit_time_change_requests(visit_id) where status = 'pending';
create index sm_smdurcharbeit_time_request_user_idx on public.sm_smdurcharbeit_time_change_requests(sm_user_id,status);
create table public.sm_smdurcharbeit_answer_provenance (
  answer_id uuid primary key references public.sm_question_answers(id) on delete restrict,
  source_answer_id uuid not null references public.sm_question_answers(id) on delete restrict,
  source_submission_id uuid not null references public.sm_questionnaire_submissions(id) on delete restrict,
  source_revision integer not null, created_at timestamptz not null default now()
);
create index sm_smdurcharbeit_provenance_source_idx on public.sm_smdurcharbeit_answer_provenance(source_answer_id);
create index sm_smdurcharbeit_provenance_submission_idx on public.sm_smdurcharbeit_answer_provenance(source_submission_id);
create table public.sm_smdurcharbeit_answer_file_links (
  answer_id uuid not null references public.sm_question_answers(id) on delete restrict,
  file_id uuid not null references public.sm_question_answer_files(id) on delete restrict,
  is_deleted boolean not null default false, created_at timestamptz not null default now(), primary key(answer_id,file_id)
);
create index sm_smdurcharbeit_file_links_origin_idx on public.sm_smdurcharbeit_answer_file_links(file_id) where not is_deleted;
create table public.sm_smdurcharbeit_events (
  id uuid primary key default gen_random_uuid(), campaign_id uuid not null references public.sm_smdurcharbeit_campaigns(id) on delete restrict,
  target_id uuid references public.sm_smdurcharbeit_month_targets(id) on delete restrict,
  visit_id uuid references public.sm_smdurcharbeit_visits(id) on delete restrict,
  actor_user_id uuid not null references public.users(id) on delete restrict,
  action text not null, reason text not null, before_state jsonb, after_state jsonb,
  created_at timestamptz not null default now()
);
create index sm_smdurcharbeit_events_campaign_idx on public.sm_smdurcharbeit_events(campaign_id,created_at);
create index sm_smdurcharbeit_events_target_idx on public.sm_smdurcharbeit_events(target_id);
create index sm_smdurcharbeit_events_visit_idx on public.sm_smdurcharbeit_events(visit_id);
-- All reads/writes use the role/owner-checked server, never direct browser access.
do $$ declare relation text; begin
  foreach relation in array array['campaigns','campaign_markets','campaign_periods','assignment_revisions','month_targets','visits','visit_time_revisions','time_change_requests','answer_provenance','answer_file_links','events'] loop
    execute format('alter table public.%I enable row level security', 'sm_smdurcharbeit_' || relation);
    execute format('revoke all on public.%I from public, anon, authenticated', 'sm_smdurcharbeit_' || relation);
    execute format('grant select, insert, update on public.%I to service_role', 'sm_smdurcharbeit_' || relation);
  end loop;
end $$;
COMMIT;
