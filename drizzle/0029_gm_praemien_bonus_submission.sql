create table if not exists "praemien_gm_wave_totals" (
  "id" uuid primary key default gen_random_uuid(),
  "wave_id" uuid not null references "praemien_waves"("id") on delete cascade,
  "gm_user_id" uuid not null references "users"("id") on delete cascade,
  "total_points" numeric(14,4) not null default 0,
  "current_reward_eur" numeric(12,2) not null default 0,
  "contribution_count" integer not null default 0,
  "last_contribution_at" timestamptz,
  "created_at" timestamptz not null default now(),
  "updated_at" timestamptz not null default now()
);

create unique index if not exists "praemien_gm_wave_totals_wave_gm_unique"
  on "praemien_gm_wave_totals" ("wave_id", "gm_user_id");
create index if not exists "praemien_gm_wave_totals_gm_idx"
  on "praemien_gm_wave_totals" ("gm_user_id", "updated_at");

create table if not exists "praemien_gm_wave_contributions" (
  "id" uuid primary key default gen_random_uuid(),
  "wave_id" uuid not null references "praemien_waves"("id") on delete cascade,
  "gm_user_id" uuid not null references "users"("id") on delete cascade,
  "market_id" uuid not null references "markets"("id") on delete cascade,
  "source_id" uuid not null references "praemien_wave_sources"("id") on delete cascade,
  "pillar_id" uuid not null references "praemien_wave_pillars"("id") on delete cascade,
  "visit_session_id" uuid,
  "red_period_start" date not null,
  "red_period_end" date not null,
  "question_id" uuid not null,
  "score_key" text not null,
  "applied_value" numeric(14,4) not null default 0,
  "submitted_at" timestamptz,
  "created_at" timestamptz not null default now(),
  "updated_at" timestamptz not null default now(),
  constraint "praemien_gm_wave_contributions_red_period_ck"
    check ("red_period_end" >= "red_period_start")
);

create unique index if not exists "praemien_gm_wave_contrib_dedupe_unique"
  on "praemien_gm_wave_contributions" ("wave_id", "gm_user_id", "market_id", "red_period_start", "source_id");
create index if not exists "praemien_gm_wave_contrib_wave_gm_idx"
  on "praemien_gm_wave_contributions" ("wave_id", "gm_user_id", "updated_at");
create index if not exists "praemien_gm_wave_contrib_source_idx"
  on "praemien_gm_wave_contributions" ("source_id", "updated_at");
