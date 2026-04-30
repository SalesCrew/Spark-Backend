ALTER TABLE visit_session_questions
ADD COLUMN IF NOT EXISTS question_chains_snapshot text[] NOT NULL DEFAULT '{}'::text[];

ALTER TABLE visit_session_questions
ADD COLUMN IF NOT EXISTS applies_to_market_chain_snapshot boolean NOT NULL DEFAULT true;

CREATE INDEX IF NOT EXISTS visit_session_questions_applicability_idx
  ON visit_session_questions(visit_session_section_id, applies_to_market_chain_snapshot, is_deleted);
