-- Soft-deleted markets must not reserve reusable business identifiers.
-- Active/non-deleted markets remain unique for each identity field.

set lock_timeout = '5s';
set statement_timeout = '60s';

drop index if exists public.markets_standard_market_number_unique;
create unique index markets_standard_market_number_unique
  on public.markets using btree (standard_market_number)
  where is_deleted = false and standard_market_number is not null;

drop index if exists public.markets_coke_master_number_unique;
create unique index markets_coke_master_number_unique
  on public.markets using btree (coke_master_number)
  where is_deleted = false and coke_master_number is not null;

drop index if exists public.markets_flex_number_unique;
create unique index markets_flex_number_unique
  on public.markets using btree (flex_number)
  where is_deleted = false and flex_number is not null;
