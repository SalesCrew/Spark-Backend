DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'gm_day_session_status') THEN
    CREATE TYPE gm_day_session_status AS ENUM (
      'draft',
      'started',
      'ended',
      'submitted',
      'cancelled'
    );
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS gm_day_sessions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  gm_user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  work_date date NOT NULL,
  timezone text NOT NULL DEFAULT 'Europe/Vienna',
  status gm_day_session_status NOT NULL DEFAULT 'draft',
  day_started_at timestamptz,
  day_ended_at timestamptz,
  start_km integer,
  end_km integer,
  start_km_deferred boolean NOT NULL DEFAULT false,
  end_km_deferred boolean NOT NULL DEFAULT false,
  is_start_km_completed boolean NOT NULL DEFAULT false,
  is_end_km_completed boolean NOT NULL DEFAULT false,
  comment text,
  submitted_at timestamptz,
  cancelled_at timestamptz,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT gm_day_sessions_end_after_start_ck CHECK (
    day_ended_at IS NULL OR day_started_at IS NULL OR day_ended_at >= day_started_at
  ),
  CONSTRAINT gm_day_sessions_submit_requires_completion_ck CHECK (
    (status <> 'submitted') OR (
      day_started_at IS NOT NULL
      AND day_ended_at IS NOT NULL
      AND is_start_km_completed = true
      AND is_end_km_completed = true
    )
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS gm_day_sessions_gm_work_date_active_unique
  ON gm_day_sessions(gm_user_id, work_date)
  WHERE is_deleted = false;

CREATE UNIQUE INDEX IF NOT EXISTS gm_day_sessions_one_open_per_gm_unique
  ON gm_day_sessions(gm_user_id)
  WHERE is_deleted = false AND status IN ('started', 'ended');

CREATE INDEX IF NOT EXISTS gm_day_sessions_gm_status_idx
  ON gm_day_sessions(gm_user_id, status, work_date);

CREATE INDEX IF NOT EXISTS gm_day_sessions_work_date_idx
  ON gm_day_sessions(work_date);

CREATE INDEX IF NOT EXISTS gm_day_sessions_deleted_idx
  ON gm_day_sessions(is_deleted);

DO $$
BEGIN
  EXECUTE 'REVOKE ALL ON TABLE public.gm_day_sessions FROM anon, authenticated';
  EXECUTE 'GRANT ALL ON TABLE public.gm_day_sessions TO authenticated';
  EXECUTE 'ALTER TABLE public.gm_day_sessions ENABLE ROW LEVEL SECURITY';
  EXECUTE 'DROP POLICY IF EXISTS auth_full_gm_day_sessions ON public.gm_day_sessions';
  EXECUTE 'CREATE POLICY auth_full_gm_day_sessions ON public.gm_day_sessions FOR ALL TO authenticated USING (true) WITH CHECK (true)';
  EXECUTE 'DROP POLICY IF EXISTS svc_full_gm_day_sessions ON public.gm_day_sessions';
  EXECUTE 'CREATE POLICY svc_full_gm_day_sessions ON public.gm_day_sessions FOR ALL TO service_role USING (true) WITH CHECK (true)';
END $$;
