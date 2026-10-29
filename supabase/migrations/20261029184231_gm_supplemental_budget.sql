-- Additive only: no existing answers, photos, roles or historical values change.
BEGIN;
SET LOCAL lock_timeout = '3s';
ALTER TABLE public.users
  ADD COLUMN is_supplemental_gm boolean NOT NULL DEFAULT false;
ALTER TABLE public.visit_sessions
  ADD COLUMN is_supplemental_gm_snapshot boolean NOT NULL DEFAULT false;

CREATE SCHEMA IF NOT EXISTS gm_reporting_private;
REVOKE ALL ON SCHEMA gm_reporting_private FROM PUBLIC;

-- Set from the trusted account row, ignoring caller-provided snapshots. The
-- shared row lock serializes stamping with account classification changes.
CREATE FUNCTION gm_reporting_private.stamp_gm_budget() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path = '' AS $function$
BEGIN
  IF TG_OP = 'INSERT' THEN
    SELECT (u.role = 'gm' AND u.is_supplemental_gm)
      INTO NEW.is_supplemental_gm_snapshot
      FROM public.users u WHERE u.id = NEW.gm_user_id FOR SHARE;
    NEW.is_supplemental_gm_snapshot := coalesce(NEW.is_supplemental_gm_snapshot, false);
  ELSE
    NEW.is_supplemental_gm_snapshot := OLD.is_supplemental_gm_snapshot;
  END IF;
  RETURN NEW;
END;
$function$;
REVOKE ALL ON FUNCTION gm_reporting_private.stamp_gm_budget() FROM PUBLIC;
CREATE TRIGGER visit_sessions_stamp_gm_budget
BEFORE INSERT OR UPDATE ON public.visit_sessions
FOR EACH ROW EXECUTE FUNCTION gm_reporting_private.stamp_gm_budget();

-- Existing market/submission and GM/submission indexes support cohort reads.
-- Avoid an index build while holding the short additive DDL lock.
COMMENT ON COLUMN public.users.is_supplemental_gm IS 'Normal GM permissions; new visits use the separate supplemental budget.';
COMMENT ON COLUMN public.visit_sessions.is_supplemental_gm_snapshot IS 'Immutable budget classification at visit creation; retained after account status changes.';
COMMIT;
