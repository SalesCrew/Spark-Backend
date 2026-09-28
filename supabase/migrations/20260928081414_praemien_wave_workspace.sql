-- Additive extension of existing waves. No historical values are reinterpreted.
CREATE TABLE public.praemien_wave_settings (
  wave_id uuid PRIMARY KEY REFERENCES public.praemien_waves(id) ON DELETE RESTRICT,
  model jsonb NOT NULL,
  revision integer NOT NULL DEFAULT 1 CHECK (revision > 0),
  closed_snapshot jsonb,
  closed_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((closed_snapshot IS NULL) = (closed_at IS NULL))
);
CREATE TABLE public.praemien_metric_entries (
  wave_id uuid NOT NULL REFERENCES public.praemien_wave_settings(wave_id) ON DELETE RESTRICT,
  gm_user_id uuid NOT NULL REFERENCES public.users(id) ON DELETE RESTRICT,
  pillar_key text NOT NULL,
  metric_key text NOT NULL,
  value numeric(16,4),
  target numeric(16,4) CHECK (target > 0),
  note text NOT NULL DEFAULT '',
  actor_id uuid REFERENCES public.users(id) ON DELETE SET NULL,
  actor_name text NOT NULL DEFAULT '',
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (wave_id, gm_user_id, pillar_key, metric_key)
);
CREATE TABLE public.praemien_wave_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  wave_id uuid NOT NULL REFERENCES public.praemien_wave_settings(wave_id) ON DELETE RESTRICT,
  revision integer NOT NULL,
  event_type text NOT NULL CHECK (event_type IN ('rules','values','activate','archive')),
  actor_id uuid REFERENCES public.users(id) ON DELETE SET NULL,
  actor_name text NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX praemien_wave_events_history_idx ON public.praemien_wave_events(wave_id, created_at DESC);
CREATE INDEX praemien_metric_entries_gm_idx ON public.praemien_metric_entries(gm_user_id, wave_id);
ALTER TABLE public.praemien_wave_settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.praemien_metric_entries ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.praemien_wave_events ENABLE ROW LEVEL SECURITY;
-- Authenticated users access these via the permission-checked backend only.
REVOKE ALL ON public.praemien_wave_settings, public.praemien_metric_entries, public.praemien_wave_events FROM anon, authenticated;
GRANT ALL ON public.praemien_wave_settings, public.praemien_metric_entries, public.praemien_wave_events TO service_role;

-- Actual reporting receipt is separate from editable visit start/end stamps.
-- Intentionally no historical backfill: past submission time is unknown.
CREATE TABLE public.praemien_visit_receipts (
  visit_session_id uuid PRIMARY KEY REFERENCES public.visit_sessions(id) ON DELETE CASCADE,
  received_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.praemien_visit_receipts ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.praemien_visit_receipts FROM anon, authenticated;
GRANT SELECT, INSERT ON public.praemien_visit_receipts TO service_role;
CREATE FUNCTION public.praemien_record_visit_receipt() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path = '' AS $$
BEGIN
  IF NEW.status = 'submitted' THEN
    INSERT INTO public.praemien_visit_receipts(visit_session_id)
      VALUES(NEW.id) ON CONFLICT(visit_session_id) DO NOTHING;
  END IF;
  RETURN NEW;
END;
$$;
REVOKE ALL ON FUNCTION public.praemien_record_visit_receipt() FROM PUBLIC, anon, authenticated;
CREATE TRIGGER praemien_visit_receipt AFTER INSERT OR UPDATE OF status ON public.visit_sessions
FOR EACH ROW EXECUTE FUNCTION public.praemien_record_visit_receipt();
