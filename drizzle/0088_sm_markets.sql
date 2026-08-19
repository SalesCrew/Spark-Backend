-- Separate Shelf Merchandising market domain.
-- This migration is intentionally additive: it does not read from, update,
-- reference, or otherwise mutate the existing GM `public.markets` table.

create table public.sm_markets (
  id uuid primary key default gen_random_uuid(),
  internal_market_id text,
  flex_number text,
  name text not null,
  db_name text not null default '',
  chain text not null,
  address text not null,
  postal_code text not null,
  city text not null,
  region text not null,
  monday_hours numeric(5, 2),
  tuesday_hours numeric(5, 2),
  wednesday_hours numeric(5, 2),
  thursday_hours numeric(5, 2),
  friday_hours numeric(5, 2),
  service_days_per_week integer generated always as (
    num_nonnulls(monday_hours, tuesday_hours, wednesday_hours, thursday_hours, friday_hours)
  ) stored,
  weekly_hours numeric(6, 2) generated always as (
    coalesce(monday_hours, 0)
    + coalesce(tuesday_hours, 0)
    + coalesce(wednesday_hours, 0)
    + coalesce(thursday_hours, 0)
    + coalesce(friday_hours, 0)
  ) stored,
  shelf_merchandiser_name text not null default '',
  field_service_manager_name text not null default '',
  source_info text not null default '',
  admin_info_note text not null default '',
  is_active boolean not null default true,
  import_source_file_name text not null default '',
  imported_at timestamp with time zone,
  is_deleted boolean not null default false,
  deleted_at timestamp with time zone,
  created_at timestamp with time zone not null default now(),
  updated_at timestamp with time zone not null default now(),
  constraint sm_markets_internal_market_id_normalized_ck check (
    internal_market_id is null
    or (internal_market_id = btrim(internal_market_id) and internal_market_id <> '')
  ),
  constraint sm_markets_flex_number_normalized_ck check (
    flex_number is null
    or (flex_number = btrim(flex_number) and flex_number <> '')
  ),
  constraint sm_markets_name_not_blank_ck check (btrim(name) <> ''),
  constraint sm_markets_chain_not_blank_ck check (btrim(chain) <> ''),
  constraint sm_markets_address_not_blank_ck check (btrim(address) <> ''),
  constraint sm_markets_postal_code_not_blank_ck check (btrim(postal_code) <> ''),
  constraint sm_markets_city_not_blank_ck check (btrim(city) <> ''),
  constraint sm_markets_region_not_blank_ck check (btrim(region) <> ''),
  constraint sm_markets_monday_hours_ck check (monday_hours is null or monday_hours > 0 and monday_hours <= 24),
  constraint sm_markets_tuesday_hours_ck check (tuesday_hours is null or tuesday_hours > 0 and tuesday_hours <= 24),
  constraint sm_markets_wednesday_hours_ck check (wednesday_hours is null or wednesday_hours > 0 and wednesday_hours <= 24),
  constraint sm_markets_thursday_hours_ck check (thursday_hours is null or thursday_hours > 0 and thursday_hours <= 24),
  constraint sm_markets_friday_hours_ck check (friday_hours is null or friday_hours > 0 and friday_hours <= 24)
);

create unique index sm_markets_internal_market_id_active_unique
  on public.sm_markets (internal_market_id)
  where is_deleted = false and internal_market_id is not null;

create unique index sm_markets_flex_number_active_unique
  on public.sm_markets (flex_number)
  where is_deleted = false and flex_number is not null;

create index sm_markets_active_region_chain_idx
  on public.sm_markets (is_deleted, is_active, region, chain);

create index sm_markets_city_idx on public.sm_markets (city);
create index sm_markets_postal_code_idx on public.sm_markets (postal_code);

comment on table public.sm_markets is
  'Shelf Merchandising markets. Operationally separate from the GM public.markets table.';

alter table public.sm_markets enable row level security;
alter table public.sm_markets force row level security;

revoke all privileges on table public.sm_markets from public, anon, authenticated;
grant all privileges on table public.sm_markets to service_role;
