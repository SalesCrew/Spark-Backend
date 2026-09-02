-- Conflict-safe and retry-safe questionnaire module saves.
-- Additive only: existing modules start at revision 1.

set lock_timeout = '5s';
set statement_timeout = '60s';

alter table public.module_main
  add column if not exists revision integer not null default 1;
alter table public.module_kuehler
  add column if not exists revision integer not null default 1;
alter table public.module_mhd
  add column if not exists revision integer not null default 1;
alter table public.module_durcharbeit
  add column if not exists revision integer not null default 1;

create table if not exists public.module_save_mutations (
  token uuid primary key,
  scope public.fragebogen_scope not null,
  module_id uuid not null,
  expected_revision integer not null,
  payload_hash text not null,
  status text not null default 'pending',
  result_revision integer,
  error_code text,
  error_message text,
  created_at timestamp with time zone not null default now(),
  updated_at timestamp with time zone not null default now(),
  completed_at timestamp with time zone,
  constraint module_save_mutations_expected_revision_ck check (expected_revision > 0),
  constraint module_save_mutations_status_ck check (status in ('pending', 'completed', 'failed')),
  constraint module_save_mutations_payload_hash_ck check (length(payload_hash) = 64),
  constraint module_save_mutations_terminal_state_ck check (
    (status = 'pending' and result_revision is null and completed_at is null)
    or (status = 'completed' and result_revision is not null and completed_at is not null)
    or (status = 'failed' and result_revision is null and completed_at is not null)
  )
);

create index if not exists module_save_mutations_module_created_idx
  on public.module_save_mutations (scope, module_id, created_at desc);

create index if not exists module_main_question_active_module_order_idx
  on public.module_main_question (module_id, order_index)
  where is_deleted = false;
create index if not exists module_kuehler_question_active_module_order_idx
  on public.module_kuehler_question (module_id, order_index)
  where is_deleted = false;
create index if not exists module_mhd_question_active_module_order_idx
  on public.module_mhd_question (module_id, order_index)
  where is_deleted = false;
create index if not exists module_durcharbeit_question_active_module_order_idx
  on public.module_durcharbeit_question (module_id, order_index)
  where is_deleted = false;
create index if not exists module_question_chains_active_module_idx
  on public.module_question_chains (scope, module_id, question_id)
  where is_deleted = false;
create index if not exists question_rules_active_question_idx
  on public.question_rules (question_id)
  where is_deleted = false;

alter table public.module_save_mutations enable row level security;
alter table public.module_save_mutations force row level security;

revoke all privileges on table public.module_save_mutations from public, anon, authenticated;
grant select, insert, update, delete on table public.module_save_mutations to service_role;

comment on table public.module_save_mutations is
  'Idempotency ledger for conflict-safe admin questionnaire module saves.';
