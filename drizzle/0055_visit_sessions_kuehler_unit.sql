ALTER TABLE "visit_sessions"
  ADD COLUMN IF NOT EXISTS "kuehler_unit_id" uuid REFERENCES "market_kuehler_units"("id") ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS "visit_sessions_kuehler_unit_idx"
  ON "visit_sessions" ("kuehler_unit_id");
