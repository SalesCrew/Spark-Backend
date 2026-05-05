alter table "markets"
  add column if not exists "is_active" boolean not null default true;

update "markets"
set "is_active" = true
where "is_active" is distinct from true;

create index if not exists "markets_active_idx"
  on "markets" ("is_active");
