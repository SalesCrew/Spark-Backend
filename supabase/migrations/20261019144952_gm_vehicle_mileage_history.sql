-- Additive only: no backfill, no change to existing readings, work times or visits.
create table public.gm_mileage_events (
  id uuid primary key default gen_random_uuid(),
  sequence bigserial not null,
  day_session_id uuid not null references public.gm_day_sessions(id) on delete restrict,
  gm_user_id uuid not null references public.users(id) on delete restrict,
  actor_user_id uuid not null references public.users(id) on delete restrict,
  operation_id uuid not null,
  reason text not null check (length(reason) between 2 and 500),
  request_signature text not null,
  before jsonb not null,
  after jsonb not null,
  created_at timestamptz not null default now()
);
create index gm_mileage_events_day_idx on public.gm_mileage_events(day_session_id, sequence desc);
create unique index gm_mileage_events_operation_day_idx on public.gm_mileage_events(operation_id, day_session_id);
alter table public.gm_mileage_events enable row level security;
-- Backend SQL connection owns the table. No Data API access is granted.
do $$ begin
  if exists(select 1 from pg_roles where rolname = 'anon') then
    revoke all on public.gm_mileage_events from anon;
  end if;
  if exists(select 1 from pg_roles where rolname = 'authenticated') then
    revoke all on public.gm_mileage_events from authenticated;
  end if;
end $$;
create schema if not exists private;
create function private.protect_gm_mileage_events() returns trigger language plpgsql as $$
begin raise exception 'Mileage history is append-only'; end;
$$;
create trigger gm_mileage_events_immutable before update or delete on public.gm_mileage_events
for each row execute function private.protect_gm_mileage_events();
-- Legacy editors cannot accidentally overwrite a corrected multi-car day or erase its audit trail.
create function private.check_gm_mileage_reading_change() returns trigger language plpgsql as $$
declare latest jsonb;
begin
  if new.start_km is not distinct from old.start_km and new.end_km is not distinct from old.end_km then return new; end if;
  select e.after into latest from public.gm_mileage_events e where e.day_session_id = old.id order by e.sequence desc limit 1;
  if latest is not null and ((latest->>'startKm')::integer is distinct from new.start_km or (latest->>'endKm')::integer is distinct from new.end_km) then
    raise exception 'Corrected mileage requires an audited vehicle correction' using errcode = '23514';
  end if;
  return new;
end;
$$;
create trigger gm_day_mileage_audit_guard before update of start_km, end_km on public.gm_day_sessions
for each row execute function private.check_gm_mileage_reading_change();
