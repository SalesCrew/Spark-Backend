CREATE TABLE IF NOT EXISTS gm_day_session_pauses (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  day_session_id uuid NOT NULL REFERENCES gm_day_sessions(id) ON DELETE CASCADE,
  gm_user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  pause_started_at timestamptz NOT NULL,
  pause_ended_at timestamptz,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT gm_day_session_pauses_end_after_start_ck CHECK (
    pause_ended_at IS NULL OR pause_ended_at >= pause_started_at
  )
);

CREATE INDEX IF NOT EXISTS gm_day_session_pauses_gm_session_started_idx
  ON gm_day_session_pauses(gm_user_id, day_session_id, pause_started_at);

CREATE INDEX IF NOT EXISTS gm_day_session_pauses_deleted_idx
  ON gm_day_session_pauses(is_deleted);

CREATE UNIQUE INDEX IF NOT EXISTS gm_day_session_pauses_one_open_per_gm_unique
  ON gm_day_session_pauses(gm_user_id)
  WHERE is_deleted = false AND pause_ended_at IS NULL;

DO $$
BEGIN
  EXECUTE 'REVOKE ALL ON TABLE public.gm_day_session_pauses FROM anon, authenticated';
  EXECUTE 'GRANT ALL ON TABLE public.gm_day_session_pauses TO authenticated';
  EXECUTE 'ALTER TABLE public.gm_day_session_pauses ENABLE ROW LEVEL SECURITY';
  EXECUTE 'DROP POLICY IF EXISTS auth_full_gm_day_session_pauses ON public.gm_day_session_pauses';
  EXECUTE 'CREATE POLICY auth_full_gm_day_session_pauses ON public.gm_day_session_pauses FOR ALL TO authenticated USING (true) WITH CHECK (true)';
  EXECUTE 'DROP POLICY IF EXISTS svc_full_gm_day_session_pauses ON public.gm_day_session_pauses';
  EXECUTE 'CREATE POLICY svc_full_gm_day_session_pauses ON public.gm_day_session_pauses FOR ALL TO service_role USING (true) WITH CHECK (true)';
END $$;
