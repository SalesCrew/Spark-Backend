-- Persistent Shelf Merchandising planning domain.
-- Additive only: no GM table or existing SM row is updated or deleted.

set lock_timeout = '5s';
set statement_timeout = '120s';

do $$
begin
  if not exists (
    select 1
    from pg_type t
    join pg_namespace n on n.oid = t.typnamespace
    where n.nspname = 'public' and t.typname = 'sm_assignment_series_status'
  ) then
    create type public.sm_assignment_series_status as enum ('active', 'ended', 'cancelled');
  end if;
  if not exists (
    select 1
    from pg_type t
    join pg_namespace n on n.oid = t.typnamespace
    where n.nspname = 'public' and t.typname = 'sm_assignment_frequency'
  ) then
    create type public.sm_assignment_frequency as enum ('weekly', 'biweekly');
  end if;
  if not exists (
    select 1
    from pg_type t
    join pg_namespace n on n.oid = t.typnamespace
    where n.nspname = 'public' and t.typname = 'sm_assignment_source_type'
  ) then
    create type public.sm_assignment_source_type as enum ('single', 'series');
  end if;
  if not exists (
    select 1
    from pg_type t
    join pg_namespace n on n.oid = t.typnamespace
    where n.nspname = 'public' and t.typname = 'sm_assignment_status'
  ) then
    create type public.sm_assignment_status as enum (
      'planned', 'confirmed', 'open', 'in_progress', 'completed', 'cancelled', 'missed'
    );
  end if;
  if not exists (
    select 1
    from pg_type t
    join pg_namespace n on n.oid = t.typnamespace
    where n.nspname = 'public' and t.typname = 'sm_assignment_event_type'
  ) then
    create type public.sm_assignment_event_type as enum (
      'created',
      'updated',
      'rescheduled',
      'sm_replaced',
      'market_replaced',
      'cancelled',
      'restored',
      'series_future_sm_changed',
      'soft_deleted'
    );
  end if;
end
$$;

create table public.sm_assignment_series (
  id uuid primary key default gen_random_uuid(),
  idempotency_key text not null,
  status public.sm_assignment_series_status not null default 'active',
  timezone text not null default 'Europe/Vienna',
  created_by_user_id uuid not null references public.users(id) on delete restrict,
  is_deleted boolean not null default false,
  deleted_at timestamp with time zone,
  created_at timestamp with time zone not null default now(),
  updated_at timestamp with time zone not null default now(),
  constraint sm_assignment_series_idempotency_ck check (btrim(idempotency_key) <> ''),
  constraint sm_assignment_series_timezone_ck check (timezone = 'Europe/Vienna')
);

create unique index sm_assignment_series_idempotency_active_unique
  on public.sm_assignment_series (idempotency_key)
  where is_deleted = false;
create index sm_assignment_series_created_by_idx on public.sm_assignment_series (created_by_user_id);
create index sm_assignment_series_status_active_idx
  on public.sm_assignment_series (status)
  where is_deleted = false;

create table public.sm_assignment_series_versions (
  id uuid primary key default gen_random_uuid(),
  series_id uuid not null references public.sm_assignment_series(id) on delete restrict,
  version_number integer not null,
  effective_from_date date not null,
  sm_market_id uuid not null references public.sm_markets(id) on delete restrict,
  market_internal_id_snapshot text not null,
  default_sm_user_id uuid not null references public.users(id) on delete restrict,
  planned_minutes integer not null,
  questionnaire_version_id uuid references public.sm_questionnaire_versions(id) on delete restrict,
  flat_rate_cents integer,
  currency text not null default 'EUR',
  frequency public.sm_assignment_frequency not null,
  weekdays integer[] not null,
  valid_from date not null,
  valid_to date not null,
  change_reason text,
  created_by_user_id uuid not null references public.users(id) on delete restrict,
  is_deleted boolean not null default false,
  deleted_at timestamp with time zone,
  created_at timestamp with time zone not null default now(),
  updated_at timestamp with time zone not null default now(),
  constraint sm_assignment_series_versions_number_ck check (version_number >= 1),
  constraint sm_assignment_series_versions_market_snapshot_ck check (btrim(market_internal_id_snapshot) <> ''),
  constraint sm_assignment_series_versions_minutes_ck check (planned_minutes > 0 and planned_minutes <= 1440),
  constraint sm_assignment_series_versions_flat_rate_ck check (flat_rate_cents is null or flat_rate_cents >= 0),
  constraint sm_assignment_series_versions_currency_ck check (currency ~ '^[A-Z]{3}$'),
  constraint sm_assignment_series_versions_weekdays_ck check (
    cardinality(weekdays) between 1 and 7
    and weekdays <@ array[1,2,3,4,5,6,7]
  ),
  constraint sm_assignment_series_versions_range_ck check (valid_to >= valid_from),
  constraint sm_assignment_series_versions_effective_ck check (
    effective_from_date >= valid_from and effective_from_date <= valid_to
  )
);

create unique index sm_assignment_series_versions_number_active_unique
  on public.sm_assignment_series_versions (series_id, version_number)
  where is_deleted = false;
create unique index sm_assignment_series_versions_id_series_unique
  on public.sm_assignment_series_versions (id, series_id);
create index sm_assignment_series_versions_market_idx on public.sm_assignment_series_versions (sm_market_id);
create index sm_assignment_series_versions_sm_idx on public.sm_assignment_series_versions (default_sm_user_id);
create index sm_assignment_series_versions_questionnaire_idx on public.sm_assignment_series_versions (questionnaire_version_id);
create index sm_assignment_series_versions_created_by_idx on public.sm_assignment_series_versions (created_by_user_id);
create index sm_assignment_series_versions_effective_idx
  on public.sm_assignment_series_versions (series_id, effective_from_date desc)
  where is_deleted = false;

create table public.sm_assignments (
  id uuid primary key default gen_random_uuid(),
  source_type public.sm_assignment_source_type not null,
  series_id uuid references public.sm_assignment_series(id) on delete restrict,
  series_version_id uuid references public.sm_assignment_series_versions(id) on delete restrict,
  series_occurrence_key date,
  idempotency_key text not null,

  original_work_date date not null,
  original_sm_user_id uuid not null references public.users(id) on delete restrict,
  original_sm_market_id uuid not null references public.sm_markets(id) on delete restrict,
  original_market_internal_id text not null,
  original_planned_minutes integer not null,

  replacement_work_date date,
  replacement_sm_user_id uuid references public.users(id) on delete restrict,
  replacement_sm_market_id uuid references public.sm_markets(id) on delete restrict,
  replacement_market_internal_id text,
  replacement_planned_minutes integer,

  questionnaire_version_id uuid references public.sm_questionnaire_versions(id) on delete restrict,
  flat_rate_cents integer,
  currency text not null default 'EUR',
  status public.sm_assignment_status not null default 'planned',
  status_before_cancellation public.sm_assignment_status,
  cancelled_at timestamp with time zone,
  cancelled_by_user_id uuid references public.users(id) on delete restrict,
  cancellation_reason text,
  started_at timestamp with time zone,
  completed_at timestamp with time zone,
  created_by_user_id uuid not null references public.users(id) on delete restrict,
  updated_by_user_id uuid not null references public.users(id) on delete restrict,
  is_deleted boolean not null default false,
  deleted_at timestamp with time zone,
  created_at timestamp with time zone not null default now(),
  updated_at timestamp with time zone not null default now(),

  constraint sm_assignments_idempotency_key_ck check (btrim(idempotency_key) <> ''),
  constraint sm_assignments_source_ck check (
    (source_type = 'single' and series_id is null and series_version_id is null and series_occurrence_key is null)
    or
    (source_type = 'series' and series_id is not null and series_version_id is not null and series_occurrence_key is not null)
  ),
  constraint sm_assignments_market_snapshot_ck check (btrim(original_market_internal_id) <> ''),
  constraint sm_assignments_minutes_ck check (original_planned_minutes > 0 and original_planned_minutes <= 1440),
  constraint sm_assignments_replacement_date_ck check (
    replacement_work_date is null or replacement_work_date <> original_work_date
  ),
  constraint sm_assignments_replacement_sm_ck check (
    replacement_sm_user_id is null or replacement_sm_user_id <> original_sm_user_id
  ),
  constraint sm_assignments_replacement_market_ck check (
    (replacement_sm_market_id is null and replacement_market_internal_id is null)
    or
    (
      replacement_sm_market_id is not null
      and replacement_market_internal_id is not null
      and btrim(replacement_market_internal_id) <> ''
      and replacement_sm_market_id <> original_sm_market_id
    )
  ),
  constraint sm_assignments_replacement_minutes_ck check (
    replacement_planned_minutes is null
    or (
      replacement_planned_minutes > 0
      and replacement_planned_minutes <= 1440
      and replacement_planned_minutes <> original_planned_minutes
    )
  ),
  constraint sm_assignments_flat_rate_ck check (flat_rate_cents is null or flat_rate_cents >= 0),
  constraint sm_assignments_currency_ck check (currency ~ '^[A-Z]{3}$'),
  constraint sm_assignments_cancellation_ck check (
    (
      status = 'cancelled'
      and cancelled_at is not null
      and cancelled_by_user_id is not null
      and cancellation_reason is not null
      and btrim(cancellation_reason) <> ''
      and status_before_cancellation is not null
      and status_before_cancellation <> 'cancelled'
    )
    or
    (
      status <> 'cancelled'
      and cancelled_at is null
      and cancelled_by_user_id is null
      and cancellation_reason is null
      and status_before_cancellation is null
    )
  ),
  constraint sm_assignments_execution_time_ck check (
    started_at is null or completed_at is null or completed_at >= started_at
  ),
  constraint sm_assignments_series_version_series_fk
    foreign key (series_version_id, series_id)
    references public.sm_assignment_series_versions(id, series_id)
    on delete restrict
);

create unique index sm_assignments_idempotency_active_unique
  on public.sm_assignments (idempotency_key)
  where is_deleted = false;
create unique index sm_assignments_series_occurrence_active_unique
  on public.sm_assignments (series_id, series_occurrence_key)
  where is_deleted = false and source_type = 'series';
create index sm_assignments_series_idx on public.sm_assignments (series_id);
create index sm_assignments_series_version_series_idx
  on public.sm_assignments (series_version_id, series_id);
create index sm_assignments_original_sm_idx on public.sm_assignments (original_sm_user_id);
create index sm_assignments_replacement_sm_idx on public.sm_assignments (replacement_sm_user_id);
create index sm_assignments_original_market_idx on public.sm_assignments (original_sm_market_id);
create index sm_assignments_replacement_market_idx on public.sm_assignments (replacement_sm_market_id);
create index sm_assignments_questionnaire_idx on public.sm_assignments (questionnaire_version_id);
create index sm_assignments_cancelled_by_idx on public.sm_assignments (cancelled_by_user_id);
create index sm_assignments_created_by_idx on public.sm_assignments (created_by_user_id);
create index sm_assignments_updated_by_idx on public.sm_assignments (updated_by_user_id);
create index sm_assignments_effective_date_active_idx
  on public.sm_assignments ((coalesce(replacement_work_date, original_work_date)), status)
  where is_deleted = false;
create index sm_assignments_effective_sm_date_active_idx
  on public.sm_assignments (
    (coalesce(replacement_sm_user_id, original_sm_user_id)),
    (coalesce(replacement_work_date, original_work_date))
  )
  where is_deleted = false;
create index sm_assignments_effective_market_date_active_idx
  on public.sm_assignments (
    (coalesce(replacement_sm_market_id, original_sm_market_id)),
    (coalesce(replacement_work_date, original_work_date))
  )
  where is_deleted = false;

create table public.sm_assignment_events (
  id uuid primary key default gen_random_uuid(),
  assignment_id uuid not null references public.sm_assignments(id) on delete restrict,
  series_id uuid references public.sm_assignment_series(id) on delete restrict,
  event_type public.sm_assignment_event_type not null,
  actor_user_id uuid not null references public.users(id) on delete restrict,
  reason text,
  before_state jsonb not null default '{}'::jsonb,
  after_state jsonb not null default '{}'::jsonb,
  created_at timestamp with time zone not null default now(),
  constraint sm_assignment_events_before_object_ck check (jsonb_typeof(before_state) = 'object'),
  constraint sm_assignment_events_after_object_ck check (jsonb_typeof(after_state) = 'object')
);

create index sm_assignment_events_assignment_created_idx
  on public.sm_assignment_events (assignment_id, created_at desc);
create index sm_assignment_events_series_created_idx
  on public.sm_assignment_events (series_id, created_at desc)
  where series_id is not null;
create index sm_assignment_events_actor_idx on public.sm_assignment_events (actor_user_id);

create table public.sm_assignment_time_submissions (
  id uuid primary key default gen_random_uuid(),
  assignment_id uuid not null references public.sm_assignments(id) on delete restrict,
  revision_number integer not null,
  actual_minutes integer not null,
  is_current boolean not null default true,
  supersedes_submission_id uuid references public.sm_assignment_time_submissions(id) on delete restrict,
  submitted_by_user_id uuid not null references public.users(id) on delete restrict,
  submitted_at timestamp with time zone not null default now(),
  correction_reason text,
  is_deleted boolean not null default false,
  deleted_at timestamp with time zone,
  created_at timestamp with time zone not null default now(),
  updated_at timestamp with time zone not null default now(),
  constraint sm_assignment_time_submissions_revision_ck check (revision_number >= 1),
  constraint sm_assignment_time_submissions_minutes_ck check (actual_minutes > 0 and actual_minutes <= 1440),
  constraint sm_assignment_time_submissions_supersedes_ck check (
    (revision_number = 1 and supersedes_submission_id is null and correction_reason is null)
    or
    (
      revision_number > 1
      and supersedes_submission_id is not null
      and correction_reason is not null
      and btrim(correction_reason) <> ''
    )
  )
);

create unique index sm_assignment_time_submissions_revision_active_unique
  on public.sm_assignment_time_submissions (assignment_id, revision_number)
  where is_deleted = false;
create unique index sm_assignment_time_submissions_current_active_unique
  on public.sm_assignment_time_submissions (assignment_id)
  where is_deleted = false and is_current = true;
create index sm_assignment_time_submissions_supersedes_idx
  on public.sm_assignment_time_submissions (supersedes_submission_id);
create index sm_assignment_time_submissions_submitted_by_idx
  on public.sm_assignment_time_submissions (submitted_by_user_id);

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'sm_questionnaire_submissions_assignment_fk'
      and conrelid = 'public.sm_questionnaire_submissions'::regclass
  ) then
    alter table public.sm_questionnaire_submissions
      add constraint sm_questionnaire_submissions_assignment_fk
      foreign key (assignment_id)
      references public.sm_assignments(id)
      on delete restrict
      not valid;
    alter table public.sm_questionnaire_submissions
      validate constraint sm_questionnaire_submissions_assignment_fk;
  end if;
end
$$;

create index if not exists sm_questionnaire_submissions_assignment_idx
  on public.sm_questionnaire_submissions (assignment_id)
  where assignment_id is not null;

create function public.sm_guard_assignment_original_fields()
returns trigger
language plpgsql
set search_path = ''
as $$
begin
  if new.source_type is distinct from old.source_type
    or new.series_id is distinct from old.series_id
    or new.series_version_id is distinct from old.series_version_id
    or new.series_occurrence_key is distinct from old.series_occurrence_key
    or new.idempotency_key is distinct from old.idempotency_key
    or new.original_work_date is distinct from old.original_work_date
    or new.original_sm_user_id is distinct from old.original_sm_user_id
    or new.original_sm_market_id is distinct from old.original_sm_market_id
    or new.original_market_internal_id is distinct from old.original_market_internal_id
    or new.original_planned_minutes is distinct from old.original_planned_minutes
    or new.created_by_user_id is distinct from old.created_by_user_id
  then
    raise exception using
      errcode = 'P0001',
      message = 'Original SM assignment fields are immutable';
  end if;
  return new;
end;
$$;

create function public.sm_guard_assignment_series_version_mutation()
returns trigger
language plpgsql
set search_path = ''
as $$
begin
  if new.series_id is distinct from old.series_id
    or new.version_number is distinct from old.version_number
    or new.effective_from_date is distinct from old.effective_from_date
    or new.sm_market_id is distinct from old.sm_market_id
    or new.market_internal_id_snapshot is distinct from old.market_internal_id_snapshot
    or new.default_sm_user_id is distinct from old.default_sm_user_id
    or new.planned_minutes is distinct from old.planned_minutes
    or new.questionnaire_version_id is distinct from old.questionnaire_version_id
    or new.flat_rate_cents is distinct from old.flat_rate_cents
    or new.currency is distinct from old.currency
    or new.frequency is distinct from old.frequency
    or new.weekdays is distinct from old.weekdays
    or new.valid_from is distinct from old.valid_from
    or new.valid_to is distinct from old.valid_to
    or new.change_reason is distinct from old.change_reason
    or new.created_by_user_id is distinct from old.created_by_user_id
  then
    raise exception using
      errcode = 'P0001',
      message = 'SM assignment series versions are immutable';
  end if;
  return new;
end;
$$;

create function public.sm_guard_assignment_series_original_fields()
returns trigger
language plpgsql
set search_path = ''
as $$
begin
  if new.idempotency_key is distinct from old.idempotency_key
    or new.created_by_user_id is distinct from old.created_by_user_id
  then
    raise exception using
      errcode = 'P0001',
      message = 'Original SM assignment series fields are immutable';
  end if;
  return new;
end;
$$;

create function public.sm_reject_assignment_event_mutation()
returns trigger
language plpgsql
set search_path = ''
as $$
begin
  raise exception using
    errcode = 'P0001',
    message = 'SM assignment events are append-only and cannot be updated or deleted';
end;
$$;

create trigger sm_assignments_original_fields_immutable_trg
before update on public.sm_assignments
for each row execute function public.sm_guard_assignment_original_fields();

create trigger sm_assignment_series_versions_immutable_trg
before update on public.sm_assignment_series_versions
for each row execute function public.sm_guard_assignment_series_version_mutation();

create trigger sm_assignment_series_original_fields_immutable_trg
before update on public.sm_assignment_series
for each row execute function public.sm_guard_assignment_series_original_fields();

create trigger sm_assignment_events_append_only_trg
before update or delete on public.sm_assignment_events
for each row execute function public.sm_reject_assignment_event_mutation();

create trigger sm_assignment_events_reject_truncate_trg
before truncate on public.sm_assignment_events
for each statement execute function public.sm_reject_assignment_event_mutation();

do $$
declare
  v_table_name text;
  v_constraint_name text;
begin
  foreach v_table_name in array array[
    'sm_assignment_series',
    'sm_assignment_series_versions',
    'sm_assignments',
    'sm_assignment_time_submissions'
  ]
  loop
    v_constraint_name := v_table_name || '_soft_delete_ck';
    execute format(
      'alter table public.%I add constraint %I check ((is_deleted and deleted_at is not null) or (not is_deleted and deleted_at is null)) not valid',
      v_table_name,
      v_constraint_name
    );
    execute format('alter table public.%I validate constraint %I', v_table_name, v_constraint_name);
    execute format(
      'create trigger %I before insert or update on public.%I for each row execute function public.sm_sync_soft_delete_fields()',
      v_table_name || '_soft_delete_fields_trg',
      v_table_name
    );
    execute format(
      'create trigger %I before delete on public.%I for each row execute function public.sm_reject_hard_delete()',
      v_table_name || '_reject_hard_delete_trg',
      v_table_name
    );
    execute format(
      'create trigger %I before truncate on public.%I for each statement execute function public.sm_reject_hard_delete()',
      v_table_name || '_reject_truncate_trg',
      v_table_name
    );
  end loop;
end
$$;

alter table public.sm_assignment_series enable row level security;
alter table public.sm_assignment_series force row level security;
alter table public.sm_assignment_series_versions enable row level security;
alter table public.sm_assignment_series_versions force row level security;
alter table public.sm_assignments enable row level security;
alter table public.sm_assignments force row level security;
alter table public.sm_assignment_events enable row level security;
alter table public.sm_assignment_events force row level security;
alter table public.sm_assignment_time_submissions enable row level security;
alter table public.sm_assignment_time_submissions force row level security;

revoke all privileges on table public.sm_assignment_series from public, anon, authenticated;
revoke all privileges on table public.sm_assignment_series_versions from public, anon, authenticated;
revoke all privileges on table public.sm_assignments from public, anon, authenticated;
revoke all privileges on table public.sm_assignment_events from public, anon, authenticated;
revoke all privileges on table public.sm_assignment_time_submissions from public, anon, authenticated;

grant select, insert, update on table public.sm_assignment_series to service_role;
grant select, insert, update on table public.sm_assignment_series_versions to service_role;
grant select, insert, update on table public.sm_assignments to service_role;
grant select, insert on table public.sm_assignment_events to service_role;
grant select, insert, update on table public.sm_assignment_time_submissions to service_role;

revoke delete, truncate on table public.sm_assignment_series from service_role;
revoke delete, truncate on table public.sm_assignment_series_versions from service_role;
revoke delete, truncate on table public.sm_assignments from service_role;
revoke update, delete, truncate on table public.sm_assignment_events from service_role;
revoke delete, truncate on table public.sm_assignment_time_submissions from service_role;
revoke references, trigger on table public.sm_assignment_series from service_role;
revoke references, trigger on table public.sm_assignment_series_versions from service_role;
revoke references, trigger on table public.sm_assignments from service_role;
revoke references, trigger on table public.sm_assignment_events from service_role;
revoke references, trigger on table public.sm_assignment_time_submissions from service_role;

revoke all on function public.sm_guard_assignment_original_fields() from public, anon, authenticated;
revoke all on function public.sm_guard_assignment_series_original_fields() from public, anon, authenticated;
revoke all on function public.sm_guard_assignment_series_version_mutation() from public, anon, authenticated;
revoke all on function public.sm_reject_assignment_event_mutation() from public, anon, authenticated;
grant execute on function public.sm_guard_assignment_original_fields() to service_role;
grant execute on function public.sm_guard_assignment_series_original_fields() to service_role;
grant execute on function public.sm_guard_assignment_series_version_mutation() to service_role;
grant execute on function public.sm_reject_assignment_event_mutation() to service_role;

comment on table public.sm_assignment_series is 'Stable identities for recurring Shelf Merchandising assignment series.';
comment on table public.sm_assignment_series_versions is 'Immutable versions of recurring SM planning definitions.';
comment on table public.sm_assignments is 'Materialized one-time and recurring SM assignments with immutable originals and nullable current replacements.';
comment on table public.sm_assignment_events is 'Append-only audit history for SM assignment planning mutations.';
comment on table public.sm_assignment_time_submissions is 'Versioned Ist-Zeit submissions for concrete SM assignments.';
