create table if not exists "gm_kpi_cache" (
  "id" uuid primary key default gen_random_uuid(),
  "gm_user_id" uuid not null references "users"("id") on delete cascade,
  "ipp_all_time_avg" numeric(12,4) not null default 0,
  "ipp_sample_count" integer not null default 0,
  "bonus_cumulative_eur" numeric(14,2) not null default 0,
  "last_computed_at" timestamptz not null default now(),
  "is_deleted" boolean not null default false,
  "deleted_at" timestamptz,
  "created_at" timestamptz not null default now(),
  "updated_at" timestamptz not null default now()
);

create unique index if not exists "gm_kpi_cache_gm_user_active_unique"
  on "gm_kpi_cache" ("gm_user_id")
  where "is_deleted" = false;
create index if not exists "gm_kpi_cache_gm_user_idx"
  on "gm_kpi_cache" ("gm_user_id");
create index if not exists "gm_kpi_cache_last_computed_idx"
  on "gm_kpi_cache" ("last_computed_at");
create index if not exists "gm_kpi_cache_deleted_idx"
  on "gm_kpi_cache" ("is_deleted");
