-- Additive only. Existing assignments remain default-following (NULL).
-- No backfill, historical rewrite, trigger, grant or RLS change.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
ALTER TABLE public.sm_assignments
  ADD COLUMN smdurcharbeit_questionnaire_override_version_id uuid
  CONSTRAINT sm_assignments_smdurcharbeit_override_version_fk
  REFERENCES public.sm_questionnaire_versions(id) ON DELETE RESTRICT;
CREATE INDEX sm_assignments_smdurcharbeit_override_version_idx
  ON public.sm_assignments(smdurcharbeit_questionnaire_override_version_id)
  WHERE smdurcharbeit_questionnaire_override_version_id IS NOT NULL AND is_deleted = false;
COMMIT;
