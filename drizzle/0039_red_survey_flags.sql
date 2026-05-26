ALTER TABLE "question_bank_shared"
  ADD COLUMN IF NOT EXISTS "red_survey" boolean;

ALTER TABLE "visit_session_questions"
  ADD COLUMN IF NOT EXISTS "red_survey_snapshot" boolean;

CREATE INDEX IF NOT EXISTS "visit_session_questions_red_survey_snapshot_idx"
  ON "visit_session_questions" ("visit_session_section_id")
  WHERE "is_deleted" = false AND "red_survey_snapshot" = true;

CREATE INDEX IF NOT EXISTS "visit_sessions_gm_market_submitted_idx"
  ON "visit_sessions" ("gm_user_id", "market_id", "submitted_at")
  WHERE "is_deleted" = false AND "status" = 'submitted' AND "submitted_at" IS NOT NULL;

CREATE INDEX IF NOT EXISTS "visit_answers_question_session_active_idx"
  ON "visit_answers" ("question_id", "visit_session_id")
  WHERE "is_deleted" = false;
