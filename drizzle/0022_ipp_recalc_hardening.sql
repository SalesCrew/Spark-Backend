ALTER TABLE ipp_market_redmonth_results
  ADD COLUMN IF NOT EXISTS question_rows_snapshot jsonb NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE ipp_market_redmonth_results
  ADD COLUMN IF NOT EXISTS snapshot_version integer NOT NULL DEFAULT 1;

CREATE TABLE IF NOT EXISTS ipp_recalc_queue (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  market_id uuid NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
  red_period_start date NOT NULL,
  red_period_end date NOT NULL,
  reason text NOT NULL DEFAULT 'unspecified',
  status text NOT NULL DEFAULT 'pending',
  attempts integer NOT NULL DEFAULT 0,
  last_error text,
  queued_at timestamptz NOT NULL DEFAULT now(),
  started_at timestamptz,
  finished_at timestamptz,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ipp_recalc_queue_status_ck CHECK (status in ('pending','processing','done','failed')),
  CONSTRAINT ipp_recalc_queue_period_ck CHECK (red_period_start <= red_period_end)
);

CREATE UNIQUE INDEX IF NOT EXISTS ipp_recalc_queue_market_period_active_unique
  ON ipp_recalc_queue (market_id, red_period_start)
  WHERE is_deleted = false AND status in ('pending','processing');

CREATE INDEX IF NOT EXISTS ipp_recalc_queue_status_idx
  ON ipp_recalc_queue(status, queued_at);

CREATE INDEX IF NOT EXISTS ipp_recalc_queue_market_period_idx
  ON ipp_recalc_queue(market_id, red_period_start);

CREATE INDEX IF NOT EXISTS ipp_recalc_queue_deleted_idx
  ON ipp_recalc_queue(is_deleted);

DO $$
BEGIN
  EXECUTE 'REVOKE ALL ON TABLE public.ipp_recalc_queue FROM anon, authenticated';
  EXECUTE 'GRANT ALL ON TABLE public.ipp_recalc_queue TO authenticated';
  EXECUTE 'ALTER TABLE public.ipp_recalc_queue ENABLE ROW LEVEL SECURITY';
  EXECUTE 'DROP POLICY IF EXISTS auth_full_ipp_recalc_queue ON public.ipp_recalc_queue';
  EXECUTE 'CREATE POLICY auth_full_ipp_recalc_queue ON public.ipp_recalc_queue FOR ALL TO authenticated USING (true) WITH CHECK (true)';
  EXECUTE 'DROP POLICY IF EXISTS svc_full_ipp_recalc_queue ON public.ipp_recalc_queue';
  EXECUTE 'CREATE POLICY svc_full_ipp_recalc_queue ON public.ipp_recalc_queue FOR ALL TO service_role USING (true) WITH CHECK (true)';
END $$;
