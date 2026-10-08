-- Additive source storage only. Existing market, assignment and answer rows are untouched.
begin;
set local lock_timeout = '5s';
set local statement_timeout = '60s';
alter table public.sm_smdurcharbeit_markets
  add column if not exists smdurcharbeit_source_key text,
  add column if not exists smdurcharbeit_vertriebstyp text,
  add column if not exists smdurcharbeit_firma_betrieb text,
  add column if not exists smdurcharbeit_strasse text,
  add column if not exists smdurcharbeit_plz text,
  add column if not exists smdurcharbeit_ort text,
  add column if not exists smdurcharbeit_em_eh text,
  add column if not exists smdurcharbeit_verplanung text,
  add column if not exists smdurcharbeit_source_file text,
  add column if not exists smdurcharbeit_source_sheet text,
  add column if not exists smdurcharbeit_source_row integer,
  add column if not exists smdurcharbeit_source_values jsonb,
  add column if not exists smdurcharbeit_imported_at timestamptz;
create unique index if not exists sm_smdurcharbeit_source_key_unique
  on public.sm_smdurcharbeit_markets (smdurcharbeit_source_key)
  where smdurcharbeit_source_key is not null;
alter table public.sm_smdurcharbeit_markets enable row level security;
revoke all on public.sm_smdurcharbeit_markets from anon, authenticated;
comment on table public.sm_smdurcharbeit_markets is 'SMDurcharbeit separate market source data; canonical SM identities preserve assignment, answer and photo foreign keys.';
commit;
