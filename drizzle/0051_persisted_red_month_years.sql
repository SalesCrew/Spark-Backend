DO $$
BEGIN
  CREATE TYPE red_month_year_status AS ENUM ('draft', 'active', 'locked');
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS red_month_years (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  red_year integer NOT NULL,
  anchor_start date NOT NULL,
  cycle_weeks integer[] NOT NULL,
  period_count integer NOT NULL DEFAULT 13,
  timezone text NOT NULL DEFAULT 'Europe/Vienna',
  status red_month_year_status NOT NULL DEFAULT 'draft',
  created_by_user_id uuid REFERENCES users(id) ON DELETE SET NULL,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT red_month_years_cycle_weeks_not_empty_ck CHECK (cardinality(cycle_weeks) > 0),
  CONSTRAINT red_month_years_period_count_positive_ck CHECK (period_count >= 1),
  CONSTRAINT red_month_years_status_ck CHECK (status IN ('draft','active','locked'))
);

CREATE UNIQUE INDEX IF NOT EXISTS red_month_years_year_active_unique
  ON red_month_years(red_year)
  WHERE is_deleted = false;

CREATE INDEX IF NOT EXISTS red_month_years_status_idx
  ON red_month_years(status, is_deleted);

CREATE INDEX IF NOT EXISTS red_month_years_anchor_idx
  ON red_month_years(anchor_start);

CREATE INDEX IF NOT EXISTS red_month_years_created_by_idx
  ON red_month_years(created_by_user_id);

CREATE TABLE IF NOT EXISTS red_month_periods (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  red_month_year_id uuid NOT NULL REFERENCES red_month_years(id) ON DELETE CASCADE,
  red_year integer NOT NULL,
  period_index integer NOT NULL,
  label text NOT NULL,
  start_date date NOT NULL,
  end_date date NOT NULL,
  lookup_end_date date NOT NULL,
  cycle_index integer NOT NULL,
  cycle_weeks integer NOT NULL,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT red_month_periods_period_index_positive_ck CHECK (period_index >= 1),
  CONSTRAINT red_month_periods_cycle_weeks_positive_ck CHECK (cycle_weeks >= 1),
  CONSTRAINT red_month_periods_date_order_ck CHECK (start_date <= end_date AND end_date <= lookup_end_date)
);

CREATE UNIQUE INDEX IF NOT EXISTS red_month_periods_year_period_unique
  ON red_month_periods(red_month_year_id, period_index)
  WHERE is_deleted = false;

CREATE UNIQUE INDEX IF NOT EXISTS red_month_periods_year_label_unique
  ON red_month_periods(red_month_year_id, label)
  WHERE is_deleted = false;

CREATE UNIQUE INDEX IF NOT EXISTS red_month_periods_start_date_unique
  ON red_month_periods(start_date)
  WHERE is_deleted = false;

CREATE INDEX IF NOT EXISTS red_month_periods_lookup_idx
  ON red_month_periods(start_date, lookup_end_date);

CREATE INDEX IF NOT EXISTS red_month_periods_red_year_idx
  ON red_month_periods(red_year, period_index);

CREATE INDEX IF NOT EXISTS red_month_periods_year_id_idx
  ON red_month_periods(red_month_year_id);

WITH active_config AS (
  SELECT
    (
      COALESCE(
        (
          SELECT anchor_start
          FROM red_month_calendar_config
          WHERE is_active = true AND is_deleted = false
          ORDER BY updated_at DESC
          LIMIT 1
        ),
        DATE '2026-01-27'
      ) - ((((EXTRACT(DOW FROM COALESCE(
        (
          SELECT anchor_start
          FROM red_month_calendar_config
          WHERE is_active = true AND is_deleted = false
          ORDER BY updated_at DESC
          LIMIT 1
        ),
        DATE '2026-01-27'
      ))::integer + 6) % 7)) * INTERVAL '1 day')
    )::date AS anchor_start,
    COALESCE(
      (
        SELECT anchor_start
        FROM red_month_calendar_config
        WHERE is_active = true AND is_deleted = false
        ORDER BY updated_at DESC
        LIMIT 1
      ),
      DATE '2026-01-27'
    ) AS configured_anchor_start,
    COALESCE(
      (
        SELECT cycle_weeks
        FROM red_month_calendar_config
        WHERE is_active = true AND is_deleted = false
        ORDER BY updated_at DESC
        LIMIT 1
      ),
      ARRAY[4,4,5]
    ) AS cycle_weeks,
    COALESCE(
      (
        SELECT timezone
        FROM red_month_calendar_config
        WHERE is_active = true AND is_deleted = false
        ORDER BY updated_at DESC
        LIMIT 1
      ),
      'Europe/Vienna'
    ) AS timezone
),
inserted_year AS (
  INSERT INTO red_month_years (red_year, anchor_start, cycle_weeks, period_count, timezone, status, is_deleted)
  SELECT EXTRACT(YEAR FROM anchor_start)::integer, anchor_start, cycle_weeks, 13, timezone, 'active', false
  FROM active_config
  ON CONFLICT DO NOTHING
  RETURNING id
)
SELECT 1;

WITH RECURSIVE target_year AS (
  SELECT *
  FROM red_month_years
  WHERE is_deleted = false
    AND anchor_start = COALESCE(
      (
        (
          SELECT anchor_start
          FROM red_month_calendar_config
          WHERE is_active = true AND is_deleted = false
          ORDER BY updated_at DESC
          LIMIT 1
        ) - ((((EXTRACT(DOW FROM (
          SELECT anchor_start
          FROM red_month_calendar_config
          WHERE is_active = true AND is_deleted = false
          ORDER BY updated_at DESC
          LIMIT 1
        ))::integer + 6) % 7)) * INTERVAL '1 day')
      )::date,
      DATE '2026-01-26'
    )
  ORDER BY created_at DESC
  LIMIT 1
),
generated AS (
  SELECT
    0 AS idx,
    target_year.anchor_start AS start_date,
    0 AS cycle_index,
    target_year.cycle_weeks[1] AS weeks
  FROM target_year

  UNION ALL

  SELECT
    generated.idx + 1 AS idx,
    (generated.start_date + (generated.weeks * 7 * INTERVAL '1 day'))::date AS start_date,
    ((generated.cycle_index + 1) % cardinality(target_year.cycle_weeks)) AS cycle_index,
    target_year.cycle_weeks[((generated.cycle_index + 1) % cardinality(target_year.cycle_weeks)) + 1] AS weeks
  FROM generated
  CROSS JOIN target_year
  WHERE generated.idx + 1 < target_year.period_count
)
INSERT INTO red_month_periods (
  red_month_year_id,
  red_year,
  period_index,
  label,
  start_date,
  end_date,
  lookup_end_date,
  cycle_index,
  cycle_weeks,
  is_deleted
)
SELECT
  target_year.id,
  target_year.red_year,
  generated.idx + 1,
  'RED ' || lpad((generated.idx + 1)::text, 2, '0'),
  generated.start_date,
  (generated.start_date + ((generated.weeks * 7 - 3) * INTERVAL '1 day'))::date,
  (generated.start_date + ((generated.weeks * 7 - 1) * INTERVAL '1 day'))::date,
  generated.cycle_index,
  generated.weeks,
  false
FROM generated
CROSS JOIN target_year
ON CONFLICT DO NOTHING;

ALTER TABLE ipp_market_redmonth_results
  ADD COLUMN IF NOT EXISTS red_period_id uuid;

ALTER TABLE ipp_recalc_queue
  ADD COLUMN IF NOT EXISTS red_period_id uuid;

ALTER TABLE praemien_gm_wave_contributions
  ADD COLUMN IF NOT EXISTS red_period_id uuid;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'ipp_market_redmonth_results_red_period_id_fkey'
  ) THEN
    ALTER TABLE ipp_market_redmonth_results
      ADD CONSTRAINT ipp_market_redmonth_results_red_period_id_fkey
      FOREIGN KEY (red_period_id) REFERENCES red_month_periods(id) ON DELETE SET NULL;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'ipp_recalc_queue_red_period_id_fkey'
  ) THEN
    ALTER TABLE ipp_recalc_queue
      ADD CONSTRAINT ipp_recalc_queue_red_period_id_fkey
      FOREIGN KEY (red_period_id) REFERENCES red_month_periods(id) ON DELETE SET NULL;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'praemien_gm_wave_contributions_red_period_id_fkey'
  ) THEN
    ALTER TABLE praemien_gm_wave_contributions
      ADD CONSTRAINT praemien_gm_wave_contributions_red_period_id_fkey
      FOREIGN KEY (red_period_id) REFERENCES red_month_periods(id) ON DELETE SET NULL;
  END IF;
END $$;

UPDATE ipp_market_redmonth_results target
SET red_period_id = periods.id
FROM red_month_periods periods
WHERE target.red_period_id IS NULL
  AND target.red_period_start = periods.start_date
  AND target.is_deleted = false
  AND periods.is_deleted = false;

UPDATE ipp_recalc_queue target
SET red_period_id = periods.id
FROM red_month_periods periods
WHERE target.red_period_id IS NULL
  AND target.red_period_start = periods.start_date
  AND target.is_deleted = false
  AND periods.is_deleted = false;

UPDATE praemien_gm_wave_contributions target
SET red_period_id = periods.id
FROM red_month_periods periods
WHERE target.red_period_id IS NULL
  AND target.red_period_start = periods.start_date
  AND periods.is_deleted = false;

CREATE INDEX IF NOT EXISTS ipp_market_redmonth_results_red_period_market_idx
  ON ipp_market_redmonth_results(red_period_id, market_id)
  WHERE is_deleted = false;

CREATE INDEX IF NOT EXISTS ipp_recalc_queue_red_period_market_idx
  ON ipp_recalc_queue(red_period_id, market_id)
  WHERE is_deleted = false AND status IN ('pending','processing');

CREATE INDEX IF NOT EXISTS praemien_gm_wave_contrib_red_period_idx
  ON praemien_gm_wave_contributions(red_period_id, gm_user_id);

REVOKE ALL ON TABLE public.red_month_years FROM anon;
REVOKE ALL ON TABLE public.red_month_years FROM authenticated;
GRANT ALL ON TABLE public.red_month_years TO authenticated;

REVOKE ALL ON TABLE public.red_month_periods FROM anon;
REVOKE ALL ON TABLE public.red_month_periods FROM authenticated;
GRANT ALL ON TABLE public.red_month_periods TO authenticated;

ALTER TABLE public.red_month_years ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.red_month_periods ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS auth_full_red_month_years ON public.red_month_years;
CREATE POLICY auth_full_red_month_years
  ON public.red_month_years
  FOR ALL
  TO authenticated
  USING (true)
  WITH CHECK (true);

DROP POLICY IF EXISTS svc_full_red_month_years ON public.red_month_years;
CREATE POLICY svc_full_red_month_years
  ON public.red_month_years
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

DROP POLICY IF EXISTS auth_full_red_month_periods ON public.red_month_periods;
CREATE POLICY auth_full_red_month_periods
  ON public.red_month_periods
  FOR ALL
  TO authenticated
  USING (true)
  WITH CHECK (true);

DROP POLICY IF EXISTS svc_full_red_month_periods ON public.red_month_periods;
CREATE POLICY svc_full_red_month_periods
  ON public.red_month_periods
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);
