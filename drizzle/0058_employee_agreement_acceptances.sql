create table if not exists "employee_agreement_acceptances" (
  "id" uuid primary key default gen_random_uuid(),
  "user_id" uuid not null references "users"("id") on delete cascade,
  "agreement_key" text not null default 'spark_employee_agreement',
  "agreement_version" text not null,
  "agreement_title" text not null,
  "agreement_hash" text not null,
  "accepted_at" timestamptz not null default now(),
  "accepted_ip" text,
  "accepted_user_agent" text,
  "created_at" timestamptz not null default now()
);

create unique index if not exists "employee_agreement_acceptances_user_version_unique"
  on "employee_agreement_acceptances" ("user_id", "agreement_key", "agreement_version");

create index if not exists "employee_agreement_acceptances_user_idx"
  on "employee_agreement_acceptances" ("user_id");

create index if not exists "employee_agreement_acceptances_key_version_idx"
  on "employee_agreement_acceptances" ("agreement_key", "agreement_version");

create index if not exists "employee_agreement_acceptances_accepted_at_idx"
  on "employee_agreement_acceptances" ("accepted_at");

revoke all on table public.employee_agreement_acceptances from public, anon, authenticated;
grant all on table public.employee_agreement_acceptances to service_role;

alter table public.employee_agreement_acceptances enable row level security;
alter table public.employee_agreement_acceptances force row level security;

drop policy if exists employee_agreement_acceptances_service_role_full on public.employee_agreement_acceptances;
create policy employee_agreement_acceptances_service_role_full
  on public.employee_agreement_acceptances
  for all
  to service_role
  using (true)
  with check (true);
