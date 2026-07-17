DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ipp_gm_adjustment_event_type') THEN
    CREATE TYPE public.ipp_gm_adjustment_event_type AS ENUM ('set', 'clear');
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.ipp_gm_redmonth_adjustment_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  revision_number bigint GENERATED ALWAYS AS IDENTITY NOT NULL,
  request_id uuid NOT NULL,
  gm_user_id uuid NOT NULL REFERENCES public.users(id) ON DELETE RESTRICT,
  red_period_id uuid NOT NULL REFERENCES public.red_month_periods(id) ON DELETE RESTRICT,
  event_type public.ipp_gm_adjustment_event_type NOT NULL,
  corrected_ipp numeric(12, 4),
  base_calculated_ipp numeric(12, 4) NOT NULL,
  base_sample_count integer NOT NULL,
  base_fingerprint text NOT NULL,
  reason text NOT NULL,
  created_by_user_id uuid NOT NULL REFERENCES public.users(id) ON DELETE RESTRICT,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ipp_gm_redmonth_adjustment_events_base_sample_count_ck CHECK (base_sample_count >= 0),
  CONSTRAINT ipp_gm_redmonth_adjustment_events_value_ck CHECK (
    (event_type = 'set' AND corrected_ipp IS NOT NULL AND corrected_ipp >= 0)
    OR (event_type = 'clear' AND corrected_ipp IS NULL)
  ),
  CONSTRAINT ipp_gm_redmonth_adjustment_events_reason_ck CHECK (char_length(btrim(reason)) >= 8)
);

CREATE UNIQUE INDEX IF NOT EXISTS ipp_gm_redmonth_adjustment_events_revision_unique
  ON public.ipp_gm_redmonth_adjustment_events(revision_number);
CREATE UNIQUE INDEX IF NOT EXISTS ipp_gm_redmonth_adjustment_events_request_unique
  ON public.ipp_gm_redmonth_adjustment_events(request_id);
CREATE INDEX IF NOT EXISTS ipp_gm_redmonth_adjustment_events_latest_idx
  ON public.ipp_gm_redmonth_adjustment_events(gm_user_id, red_period_id, revision_number DESC);
CREATE INDEX IF NOT EXISTS ipp_gm_redmonth_adjustment_events_period_idx
  ON public.ipp_gm_redmonth_adjustment_events(red_period_id, gm_user_id);
CREATE INDEX IF NOT EXISTS ipp_gm_redmonth_adjustment_events_created_by_idx
  ON public.ipp_gm_redmonth_adjustment_events(created_by_user_id);

COMMENT ON TABLE public.ipp_gm_redmonth_adjustment_events IS
  'Immutable admin audit log for effective GL IPP corrections per RED month. Raw answers and calculated market IPP are never changed.';

REVOKE ALL ON TABLE public.ipp_gm_redmonth_adjustment_events FROM anon, authenticated;
GRANT SELECT, INSERT ON TABLE public.ipp_gm_redmonth_adjustment_events TO service_role;
GRANT USAGE, SELECT ON SEQUENCE public.ipp_gm_redmonth_adjustment_events_revision_number_seq TO service_role;

ALTER TABLE public.ipp_gm_redmonth_adjustment_events ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS ipp_gm_redmonth_adjustment_events_service_role_select
  ON public.ipp_gm_redmonth_adjustment_events;
CREATE POLICY ipp_gm_redmonth_adjustment_events_service_role_select
  ON public.ipp_gm_redmonth_adjustment_events
  FOR SELECT TO service_role USING (true);

DROP POLICY IF EXISTS ipp_gm_redmonth_adjustment_events_service_role_insert
  ON public.ipp_gm_redmonth_adjustment_events;
CREATE POLICY ipp_gm_redmonth_adjustment_events_service_role_insert
  ON public.ipp_gm_redmonth_adjustment_events
  FOR INSERT TO service_role WITH CHECK (true);
