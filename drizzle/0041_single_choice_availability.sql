ALTER TABLE "question_bank_shared"
  ADD COLUMN IF NOT EXISTS "single_choice_availability" boolean;

ALTER TABLE "visit_session_questions"
  ADD COLUMN IF NOT EXISTS "single_choice_availability_snapshot" boolean;
