DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'campaign_section') THEN
    CREATE TYPE campaign_section AS ENUM ('standard', 'flex', 'billa', 'kuehler', 'mhd');
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS campaigns (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  section campaign_section NOT NULL,
  current_fragebogen_id uuid,
  status fragebogen_status NOT NULL DEFAULT 'inactive',
  schedule_type fragebogen_schedule_type NOT NULL DEFAULT 'always',
  start_date date,
  end_date date,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT campaigns_schedule_dates_ck
    CHECK ((schedule_type = 'always') OR (start_date IS NOT NULL AND end_date IS NOT NULL AND start_date <= end_date))
);

CREATE INDEX IF NOT EXISTS campaigns_section_idx ON campaigns(section);
CREATE INDEX IF NOT EXISTS campaigns_status_idx ON campaigns(status);
CREATE INDEX IF NOT EXISTS campaigns_deleted_idx ON campaigns(is_deleted);
CREATE INDEX IF NOT EXISTS campaigns_current_fragebogen_idx ON campaigns(current_fragebogen_id);

CREATE TABLE IF NOT EXISTS campaign_market_assignments (
  campaign_id uuid NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
  market_id uuid NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
  assigned_at timestamptz NOT NULL DEFAULT now(),
  assigned_by_user_id uuid REFERENCES users(id) ON DELETE SET NULL,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (campaign_id, market_id)
);

CREATE INDEX IF NOT EXISTS campaign_market_assignments_market_idx
  ON campaign_market_assignments(market_id);
CREATE INDEX IF NOT EXISTS campaign_market_assignments_campaign_deleted_idx
  ON campaign_market_assignments(campaign_id, is_deleted);
CREATE INDEX IF NOT EXISTS campaign_market_assignments_deleted_idx
  ON campaign_market_assignments(is_deleted);

CREATE TABLE IF NOT EXISTS campaign_fragebogen_history (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  campaign_id uuid NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
  from_fragebogen_id uuid,
  to_fragebogen_id uuid NOT NULL,
  changed_at timestamptz NOT NULL DEFAULT now(),
  changed_by_user_id uuid REFERENCES users(id) ON DELETE SET NULL,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS campaign_fragebogen_history_campaign_idx
  ON campaign_fragebogen_history(campaign_id);
CREATE INDEX IF NOT EXISTS campaign_fragebogen_history_campaign_deleted_idx
  ON campaign_fragebogen_history(campaign_id, is_deleted);
CREATE INDEX IF NOT EXISTS campaign_fragebogen_history_changed_at_idx
  ON campaign_fragebogen_history(changed_at);
CREATE INDEX IF NOT EXISTS campaign_fragebogen_history_deleted_idx
  ON campaign_fragebogen_history(is_deleted);

DO $$
DECLARE
  t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'campaigns',
    'campaign_market_assignments',
    'campaign_fragebogen_history'
  ]
  LOOP
    EXECUTE format('REVOKE ALL ON TABLE public.%I FROM anon, authenticated', t);
    EXECUTE format('GRANT ALL ON TABLE public.%I TO authenticated', t);
    EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('DROP POLICY IF EXISTS auth_full_%I ON public.%I', t, t);
    EXECUTE format('CREATE POLICY auth_full_%I ON public.%I FOR ALL TO authenticated USING (true) WITH CHECK (true)', t, t);
    EXECUTE format('DROP POLICY IF EXISTS svc_full_%I ON public.%I', t, t);
    EXECUTE format('CREATE POLICY svc_full_%I ON public.%I FOR ALL TO service_role USING (true) WITH CHECK (true)', t, t);
  END LOOP;
END $$;
