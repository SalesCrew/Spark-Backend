-- Enforce the SM retention contract at the database boundary.
-- Mutable SM tables are soft-deleted through is_deleted/deleted_at. Direct
-- DELETE statements are rejected, including when the backend connects as the
-- table owner. The answer-event audit trail is strictly append-only.

set lock_timeout = '5s';
set statement_timeout = '120s';

create function public.sm_sync_soft_delete_fields()
returns trigger
language plpgsql
set search_path = ''
as $$
begin
  if new.is_deleted then
    new.deleted_at := coalesce(new.deleted_at, now());
  else
    new.deleted_at := null;
  end if;
  new.updated_at := now();
  return new;
end;
$$;

create function public.sm_reject_hard_delete()
returns trigger
language plpgsql
set search_path = ''
as $$
begin
  raise exception using
    errcode = 'P0001',
    message = format(
      'Hard delete is disabled for public.%I; set is_deleted = true instead',
      tg_table_name
    );
end;
$$;

create function public.sm_reject_answer_event_mutation()
returns trigger
language plpgsql
set search_path = ''
as $$
begin
  raise exception using
    errcode = 'P0001',
    message = 'SM answer events are append-only and cannot be updated or deleted';
end;
$$;

do $$
declare
  v_table_name text;
  v_constraint_name text;
begin
  foreach v_table_name in array array[
    'sm_markets',
    'sm_questions',
    'sm_question_versions',
    'sm_answer_option_versions',
    'sm_question_logic_rules',
    'sm_question_logic_rule_targets',
    'sm_modules',
    'sm_module_versions',
    'sm_module_version_questions',
    'sm_questionnaire_templates',
    'sm_questionnaire_versions',
    'sm_questionnaire_version_modules',
    'sm_questionnaire_submissions',
    'sm_questionnaire_submission_sections',
    'sm_questionnaire_submission_questions',
    'sm_question_answers',
    'sm_question_answer_options',
    'sm_question_answer_matrix_cells',
    'sm_question_answer_files',
    'sm_answer_change_requests',
    'sm_questionnaire_submission_delete_requests'
  ]
  loop
    v_constraint_name := v_table_name || '_soft_delete_ck';

    if not exists (
      select 1
      from pg_constraint
      where conname = v_constraint_name
        and conrelid = format('public.%I', v_table_name)::regclass
    ) then
      execute format(
        'alter table public.%I add constraint %I check ((is_deleted and deleted_at is not null) or (not is_deleted and deleted_at is null)) not valid',
        v_table_name,
        v_constraint_name
      );
      execute format(
        'alter table public.%I validate constraint %I',
        v_table_name,
        v_constraint_name
      );
    end if;

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

    execute format('revoke delete, truncate on table public.%I from service_role', v_table_name);
  end loop;
end;
$$;

create trigger sm_question_answer_events_append_only_trg
before update or delete on public.sm_question_answer_events
for each row execute function public.sm_reject_answer_event_mutation();

create trigger sm_question_answer_events_reject_truncate_trg
before truncate on public.sm_question_answer_events
for each statement execute function public.sm_reject_answer_event_mutation();

revoke update, delete, truncate on table public.sm_question_answer_events from service_role;

revoke all on function public.sm_sync_soft_delete_fields() from public, anon, authenticated;
revoke all on function public.sm_reject_hard_delete() from public, anon, authenticated;
revoke all on function public.sm_reject_answer_event_mutation() from public, anon, authenticated;

grant execute on function public.sm_sync_soft_delete_fields() to service_role;
grant execute on function public.sm_reject_hard_delete() to service_role;
grant execute on function public.sm_reject_answer_event_mutation() to service_role;

comment on function public.sm_sync_soft_delete_fields() is
  'Keeps is_deleted, deleted_at, and updated_at consistent for mutable SM records.';
comment on function public.sm_reject_hard_delete() is
  'Rejects physical deletion from mutable SM tables; callers must soft-delete.';
comment on function public.sm_reject_answer_event_mutation() is
  'Protects the SM answer event audit trail as append-only.';
