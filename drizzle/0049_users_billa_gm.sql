ALTER TABLE "users"
  ADD COLUMN IF NOT EXISTS "is_billa_gm" boolean DEFAULT false NOT NULL;

CREATE INDEX IF NOT EXISTS "users_billa_gm_idx"
  ON "users" ("is_billa_gm");
