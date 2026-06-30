alter table "users"
  add column if not exists "anonymized_at" timestamptz,
  add column if not exists "anonymized_by_user_id" uuid;

create index if not exists "users_anonymized_at_idx"
  on "users" ("anonymized_at");
