CREATE TABLE IF NOT EXISTS red_month_calendar_config (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  anchor_start date NOT NULL,
  cycle_weeks integer[] NOT NULL,
  timezone text NOT NULL DEFAULT 'Europe/Vienna',
  is_active boolean NOT NULL DEFAULT true,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT red_month_calendar_config_cycle_weeks_not_empty_ck CHECK (cardinality(cycle_weeks) > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS red_month_calendar_config_single_active_unique
  ON red_month_calendar_config (is_active)
  WHERE is_active = true AND is_deleted = false;

CREATE INDEX IF NOT EXISTS red_month_calendar_config_active_idx
  ON red_month_calendar_config (is_active, is_deleted);

INSERT INTO red_month_calendar_config (anchor_start, cycle_weeks, timezone, is_active, is_deleted)
SELECT DATE '2026-01-27', ARRAY[4,4,5], 'Europe/Vienna', true, false
WHERE NOT EXISTS (
  SELECT 1
  FROM red_month_calendar_config
  WHERE is_active = true AND is_deleted = false
);

DO $$
BEGIN
  EXECUTE 'REVOKE ALL ON TABLE public.red_month_calendar_config FROM anon, authenticated';
  EXECUTE 'GRANT ALL ON TABLE public.red_month_calendar_config TO authenticated';
  EXECUTE 'ALTER TABLE public.red_month_calendar_config ENABLE ROW LEVEL SECURITY';
  EXECUTE 'DROP POLICY IF EXISTS auth_full_red_month_calendar_config ON public.red_month_calendar_config';
  EXECUTE 'CREATE POLICY auth_full_red_month_calendar_config ON public.red_month_calendar_config FOR ALL TO authenticated USING (true) WITH CHECK (true)';
  EXECUTE 'DROP POLICY IF EXISTS svc_full_red_month_calendar_config ON public.red_month_calendar_config';
  EXECUTE 'CREATE POLICY svc_full_red_month_calendar_config ON public.red_month_calendar_config FOR ALL TO service_role USING (true) WITH CHECK (true)';
END $$;
