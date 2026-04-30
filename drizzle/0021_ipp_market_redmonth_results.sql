CREATE TABLE IF NOT EXISTS ipp_market_redmonth_results (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  market_id uuid NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
  red_period_start date NOT NULL,
  red_period_end date NOT NULL,
  red_period_label text NOT NULL,
  red_period_year integer NOT NULL,
  market_ipp numeric(12, 4) NOT NULL DEFAULT 0,
  source_submission_count integer NOT NULL DEFAULT 0,
  contributing_question_count integer NOT NULL DEFAULT 0,
  is_finalized boolean NOT NULL DEFAULT true,
  computed_at timestamptz NOT NULL DEFAULT now(),
  finalized_at timestamptz NOT NULL DEFAULT now(),
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ipp_market_redmonth_results_period_ck CHECK (red_period_start <= red_period_end)
);

CREATE UNIQUE INDEX IF NOT EXISTS ipp_market_redmonth_results_market_period_active_unique
  ON ipp_market_redmonth_results(market_id, red_period_start)
  WHERE is_deleted = false;

CREATE INDEX IF NOT EXISTS ipp_market_redmonth_results_period_idx
  ON ipp_market_redmonth_results(red_period_start, red_period_end);

CREATE INDEX IF NOT EXISTS ipp_market_redmonth_results_year_idx
  ON ipp_market_redmonth_results(red_period_year, is_deleted);

CREATE INDEX IF NOT EXISTS ipp_market_redmonth_results_market_year_idx
  ON ipp_market_redmonth_results(market_id, red_period_year);

CREATE INDEX IF NOT EXISTS ipp_market_redmonth_results_deleted_idx
  ON ipp_market_redmonth_results(is_deleted);

DO $$
BEGIN
  EXECUTE 'REVOKE ALL ON TABLE public.ipp_market_redmonth_results FROM anon, authenticated';
  EXECUTE 'GRANT ALL ON TABLE public.ipp_market_redmonth_results TO authenticated';
  EXECUTE 'ALTER TABLE public.ipp_market_redmonth_results ENABLE ROW LEVEL SECURITY';
  EXECUTE 'DROP POLICY IF EXISTS auth_full_ipp_market_redmonth_results ON public.ipp_market_redmonth_results';
  EXECUTE 'CREATE POLICY auth_full_ipp_market_redmonth_results ON public.ipp_market_redmonth_results FOR ALL TO authenticated USING (true) WITH CHECK (true)';
  EXECUTE 'DROP POLICY IF EXISTS svc_full_ipp_market_redmonth_results ON public.ipp_market_redmonth_results';
  EXECUTE 'CREATE POLICY svc_full_ipp_market_redmonth_results ON public.ipp_market_redmonth_results FOR ALL TO service_role USING (true) WITH CHECK (true)';
END $$;
