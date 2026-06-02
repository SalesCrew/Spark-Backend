ALTER TABLE "question_scoring"
  ADD COLUMN IF NOT EXISTS "mitbewerberabfrage" numeric(8, 2);

CREATE INDEX IF NOT EXISTS "question_scoring_mitbewerberabfrage_active_idx"
  ON "question_scoring" ("question_id", "mitbewerberabfrage")
  WHERE "is_deleted" = false AND "mitbewerberabfrage" IS NOT NULL;
