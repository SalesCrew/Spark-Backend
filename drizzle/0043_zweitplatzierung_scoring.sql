ALTER TABLE "question_scoring"
  ADD COLUMN IF NOT EXISTS "zweitplatzierung" numeric(8, 2);

CREATE INDEX IF NOT EXISTS "question_scoring_zweitplatzierung_active_idx"
  ON "question_scoring" ("question_id", "zweitplatzierung")
  WHERE "is_deleted" = false AND "zweitplatzierung" IS NOT NULL;
