-- A manual value is an explicit administrative override for exactly one
-- premium wave, pillar and GM. Questionnaire answers and calculated
-- contributions remain untouched and become effective again when the
-- override is removed.

CREATE TABLE IF NOT EXISTS public.praemien_wave_pillar_overrides (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  wave_id uuid NOT NULL REFERENCES public.praemien_waves(id) ON DELETE CASCADE,
  pillar_id uuid NOT NULL REFERENCES public.praemien_wave_pillars(id) ON DELETE CASCADE,
  gm_user_id uuid NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  points numeric(14,4) NOT NULL,
  note text,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT praemien_wave_pillar_overrides_points_ck CHECK (points >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS praemien_wave_pillar_overrides_wave_pillar_gm_active_unique
  ON public.praemien_wave_pillar_overrides(wave_id, pillar_id, gm_user_id)
  WHERE is_deleted = false;

CREATE INDEX IF NOT EXISTS praemien_wave_pillar_overrides_wave_idx
  ON public.praemien_wave_pillar_overrides(wave_id, is_deleted);

CREATE INDEX IF NOT EXISTS praemien_wave_pillar_overrides_gm_wave_idx
  ON public.praemien_wave_pillar_overrides(gm_user_id, wave_id);

CREATE INDEX IF NOT EXISTS praemien_wave_pillar_overrides_pillar_gm_idx
  ON public.praemien_wave_pillar_overrides(pillar_id, gm_user_id);

ALTER TABLE public.praemien_wave_pillar_overrides ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE public.praemien_wave_pillar_overrides FROM PUBLIC, anon, authenticated;
GRANT ALL ON TABLE public.praemien_wave_pillar_overrides TO service_role;

DROP POLICY IF EXISTS praemien_wave_pillar_overrides_service_role_full
  ON public.praemien_wave_pillar_overrides;
CREATE POLICY praemien_wave_pillar_overrides_service_role_full
  ON public.praemien_wave_pillar_overrides
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

COMMENT ON TABLE public.praemien_wave_pillar_overrides IS
  'Administrative per-GM pillar values. Active rows take precedence over calculated questionnaire contributions without modifying the source answers.';
