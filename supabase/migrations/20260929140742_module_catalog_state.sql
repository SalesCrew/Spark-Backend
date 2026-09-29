-- Presentation metadata only. Existing module/question/visit rows are untouched.
create table public.module_catalog_state (
  scope text not null check (scope in ('main','kuehler','mhd','durcharbeit')),
  module_id uuid not null,
  inactive boolean not null default false,
  updated_at timestamptz not null default now(),
  primary key (scope,module_id)
);
alter table public.module_catalog_state enable row level security;
revoke all on public.module_catalog_state from anon, authenticated;
grant select, insert, update, delete on public.module_catalog_state to service_role;
comment on table public.module_catalog_state is 'Admin catalog indicator only; never controls questionnaire or visit execution.';
