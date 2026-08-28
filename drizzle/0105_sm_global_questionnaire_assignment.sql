-- One central SM questionnaire selection for all not-yet-started market visits.
-- The selected logical questionnaire template follows its latest effective
-- published version. History is append-only: changing the selection supersedes
-- the current row and inserts a new one.

set lock_timeout = '5s';
set statement_timeout = '60s';

create table public.sm_questionnaire_global_assignments (
  id uuid primary key default gen_random_uuid(),
  questionnaire_template_id uuid not null references public.sm_questionnaire_templates(id) on delete restrict,
  assigned_by_user_id uuid not null references public.users(id) on delete restrict,
  assigned_at timestamp with time zone not null default now(),
  superseded_at timestamp with time zone,
  superseded_by_user_id uuid references public.users(id) on delete restrict,
  is_deleted boolean not null default false,
  deleted_at timestamp with time zone,
  created_at timestamp with time zone not null default now(),
  updated_at timestamp with time zone not null default now(),
  constraint sm_questionnaire_global_assignments_superseded_ck check (
    (superseded_at is null and superseded_by_user_id is null)
    or (superseded_at is not null and superseded_by_user_id is not null and superseded_at >= assigned_at)
  ),
  constraint sm_questionnaire_global_assignments_soft_delete_ck check (
    (is_deleted and deleted_at is not null)
    or (not is_deleted and deleted_at is null)
  )
);

create unique index sm_questionnaire_global_assignments_current_unique
  on public.sm_questionnaire_global_assignments ((true))
  where superseded_at is null and is_deleted = false;
create index sm_questionnaire_global_assignments_template_idx
  on public.sm_questionnaire_global_assignments (questionnaire_template_id);
create index sm_questionnaire_global_assignments_assigned_by_idx
  on public.sm_questionnaire_global_assignments (assigned_by_user_id);
create index sm_questionnaire_global_assignments_superseded_by_idx
  on public.sm_questionnaire_global_assignments (superseded_by_user_id)
  where superseded_by_user_id is not null;

create function public.sm_guard_global_questionnaire_assignment_mutation()
returns trigger
language plpgsql
set search_path = ''
as $$
begin
  if new.questionnaire_template_id is distinct from old.questionnaire_template_id
    or new.assigned_by_user_id is distinct from old.assigned_by_user_id
    or new.assigned_at is distinct from old.assigned_at
    or new.created_at is distinct from old.created_at
  then
    raise exception using
      errcode = 'P0001',
      message = 'SM global questionnaire assignment identity is immutable';
  end if;

  if old.superseded_at is not null and (
    new.superseded_at is distinct from old.superseded_at
    or new.superseded_by_user_id is distinct from old.superseded_by_user_id
  ) then
    raise exception using
      errcode = 'P0001',
      message = 'A superseded SM global questionnaire assignment is immutable';
  end if;

  if old.superseded_at is null and new.superseded_at is not null and new.superseded_by_user_id is null then
    raise exception using
      errcode = 'P0001',
      message = 'Superseding an SM global questionnaire assignment requires an actor';
  end if;

  return new;
end;
$$;

create function public.sm_validate_global_questionnaire_assignment()
returns trigger
language plpgsql
set search_path = ''
as $$
begin
  if not exists (
    select 1
    from public.sm_questionnaire_templates template
    where template.id = new.questionnaire_template_id
      and template.status = 'active'
      and template.is_deleted = false
      and exists (
        select 1
        from public.sm_questionnaire_versions version
        where version.questionnaire_template_id = template.id
          and version.status = 'published'
          and version.is_deleted = false
      )
  ) then
    raise exception using
      errcode = 'P0001',
      message = 'The selected SM questionnaire must be active and have a published version';
  end if;
  return new;
end;
$$;

create function public.sm_protect_globally_assigned_questionnaire()
returns trigger
language plpgsql
set search_path = ''
as $$
begin
  if (new.is_deleted or new.status <> 'active') and exists (
    select 1
    from public.sm_questionnaire_global_assignments assignment
    where assignment.questionnaire_template_id = old.id
      and assignment.superseded_at is null
      and assignment.is_deleted = false
  ) then
    raise exception using
      errcode = 'P0001',
      message = 'The globally assigned SM questionnaire must remain active';
  end if;
  return new;
end;
$$;

create trigger sm_questionnaire_global_assignments_immutable_trg
before update on public.sm_questionnaire_global_assignments
for each row execute function public.sm_guard_global_questionnaire_assignment_mutation();
create trigger sm_questionnaire_global_assignments_validate_trg
before insert on public.sm_questionnaire_global_assignments
for each row execute function public.sm_validate_global_questionnaire_assignment();
create trigger sm_questionnaire_global_assignments_soft_delete_fields_trg
before insert or update on public.sm_questionnaire_global_assignments
for each row execute function public.sm_sync_soft_delete_fields();
create trigger sm_questionnaire_global_assignments_reject_hard_delete_trg
before delete on public.sm_questionnaire_global_assignments
for each row execute function public.sm_reject_hard_delete();
create trigger sm_questionnaire_global_assignments_reject_truncate_trg
before truncate on public.sm_questionnaire_global_assignments
for each statement execute function public.sm_reject_hard_delete();
create trigger sm_questionnaire_templates_protect_global_assignment_trg
before update on public.sm_questionnaire_templates
for each row execute function public.sm_protect_globally_assigned_questionnaire();

alter table public.sm_questionnaire_global_assignments enable row level security;
alter table public.sm_questionnaire_global_assignments force row level security;

revoke all privileges on table public.sm_questionnaire_global_assignments from public, anon, authenticated, service_role;
grant select, insert, update on table public.sm_questionnaire_global_assignments to service_role;

revoke all on function public.sm_guard_global_questionnaire_assignment_mutation() from public, anon, authenticated;
revoke all on function public.sm_validate_global_questionnaire_assignment() from public, anon, authenticated;
revoke all on function public.sm_protect_globally_assigned_questionnaire() from public, anon, authenticated;
grant execute on function public.sm_guard_global_questionnaire_assignment_mutation() to service_role;
grant execute on function public.sm_validate_global_questionnaire_assignment() to service_role;
grant execute on function public.sm_protect_globally_assigned_questionnaire() to service_role;

comment on table public.sm_questionnaire_global_assignments is
  'Append-only history of the one centrally selected logical questionnaire used by every not-yet-started SM market visit.';
comment on column public.sm_questionnaire_global_assignments.questionnaire_template_id is
  'Stable logical questionnaire identity. Each visit resolves and snapshots the latest effective published version when it starts.';
