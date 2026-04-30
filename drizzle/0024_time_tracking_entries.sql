DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'time_tracking_activity_type') THEN
    CREATE TYPE time_tracking_activity_type AS ENUM (
      'sonderaufgabe',
      'arztbesuch',
      'werkstatt',
      'homeoffice',
      'schulung',
      'lager',
      'heimfahrt',
      'hoteluebernachtung'
    );
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'time_tracking_entry_status') THEN
    CREATE TYPE time_tracking_entry_status AS ENUM ('draft', 'submitted', 'cancelled');
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'time_tracking_entry_event_type') THEN
    CREATE TYPE time_tracking_entry_event_type AS ENUM (
      'start_set',
      'end_set',
      'comment_set',
      'submitted',
      'cancelled'
    );
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS time_tracking_entries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  gm_user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  market_id uuid REFERENCES markets(id) ON DELETE SET NULL,
  activity_type time_tracking_activity_type NOT NULL,
  client_entry_token text,
  start_at timestamptz,
  end_at timestamptz,
  comment text,
  status time_tracking_entry_status NOT NULL DEFAULT 'draft',
  submitted_at timestamptz,
  cancelled_at timestamptz,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT time_tracking_entries_submitted_complete_ck CHECK (
    (status <> 'submitted') OR (start_at IS NOT NULL AND end_at IS NOT NULL AND end_at >= start_at)
  )
);

CREATE TABLE IF NOT EXISTS time_tracking_entry_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  entry_id uuid NOT NULL REFERENCES time_tracking_entries(id) ON DELETE CASCADE,
  gm_user_id uuid REFERENCES users(id) ON DELETE SET NULL,
  event_type time_tracking_entry_event_type NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS time_tracking_entries_gm_status_start_idx
  ON time_tracking_entries(gm_user_id, status, start_at);

CREATE INDEX IF NOT EXISTS time_tracking_entries_activity_idx
  ON time_tracking_entries(activity_type);

CREATE INDEX IF NOT EXISTS time_tracking_entries_deleted_idx
  ON time_tracking_entries(is_deleted);

CREATE UNIQUE INDEX IF NOT EXISTS time_tracking_entries_draft_token_unique
  ON time_tracking_entries(gm_user_id, activity_type, client_entry_token)
  WHERE is_deleted = false AND status = 'draft' AND client_entry_token IS NOT NULL;

CREATE INDEX IF NOT EXISTS time_tracking_entry_events_entry_idx
  ON time_tracking_entry_events(entry_id, created_at);

CREATE INDEX IF NOT EXISTS time_tracking_entry_events_type_idx
  ON time_tracking_entry_events(event_type);

DO $$
BEGIN
  EXECUTE 'REVOKE ALL ON TABLE public.time_tracking_entries FROM anon, authenticated';
  EXECUTE 'GRANT ALL ON TABLE public.time_tracking_entries TO authenticated';
  EXECUTE 'ALTER TABLE public.time_tracking_entries ENABLE ROW LEVEL SECURITY';
  EXECUTE 'DROP POLICY IF EXISTS auth_full_time_tracking_entries ON public.time_tracking_entries';
  EXECUTE 'CREATE POLICY auth_full_time_tracking_entries ON public.time_tracking_entries FOR ALL TO authenticated USING (true) WITH CHECK (true)';
  EXECUTE 'DROP POLICY IF EXISTS svc_full_time_tracking_entries ON public.time_tracking_entries';
  EXECUTE 'CREATE POLICY svc_full_time_tracking_entries ON public.time_tracking_entries FOR ALL TO service_role USING (true) WITH CHECK (true)';

  EXECUTE 'REVOKE ALL ON TABLE public.time_tracking_entry_events FROM anon, authenticated';
  EXECUTE 'GRANT ALL ON TABLE public.time_tracking_entry_events TO authenticated';
  EXECUTE 'ALTER TABLE public.time_tracking_entry_events ENABLE ROW LEVEL SECURITY';
  EXECUTE 'DROP POLICY IF EXISTS auth_full_time_tracking_entry_events ON public.time_tracking_entry_events';
  EXECUTE 'CREATE POLICY auth_full_time_tracking_entry_events ON public.time_tracking_entry_events FOR ALL TO authenticated USING (true) WITH CHECK (true)';
  EXECUTE 'DROP POLICY IF EXISTS svc_full_time_tracking_entry_events ON public.time_tracking_entry_events';
  EXECUTE 'CREATE POLICY svc_full_time_tracking_entry_events ON public.time_tracking_entry_events FOR ALL TO service_role USING (true) WITH CHECK (true)';
END $$;
