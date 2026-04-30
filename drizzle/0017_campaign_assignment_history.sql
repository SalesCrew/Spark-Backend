CREATE TABLE IF NOT EXISTS campaign_market_assignment_history (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  market_id uuid NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
  section campaign_section NOT NULL,
  from_campaign_id uuid NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
  to_campaign_id uuid NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
  from_gm_user_id uuid REFERENCES users(id) ON DELETE SET NULL,
  to_gm_user_id uuid REFERENCES users(id) ON DELETE SET NULL,
  migrated_by_user_id uuid REFERENCES users(id) ON DELETE SET NULL,
  migrated_at timestamptz NOT NULL DEFAULT now(),
  reason text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS campaign_market_assignment_history_market_section_migrated_idx
  ON campaign_market_assignment_history(market_id, section, migrated_at);

CREATE INDEX IF NOT EXISTS campaign_market_assignment_history_from_campaign_idx
  ON campaign_market_assignment_history(from_campaign_id);

CREATE INDEX IF NOT EXISTS campaign_market_assignment_history_to_campaign_idx
  ON campaign_market_assignment_history(to_campaign_id);

REVOKE ALL ON TABLE public.campaign_market_assignment_history FROM PUBLIC, anon, authenticated;
GRANT ALL ON TABLE public.campaign_market_assignment_history TO authenticated;
GRANT ALL ON TABLE public.campaign_market_assignment_history TO service_role;
ALTER TABLE public.campaign_market_assignment_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.campaign_market_assignment_history FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS auth_full_campaign_market_assignment_history ON public.campaign_market_assignment_history;
DROP POLICY IF EXISTS svc_full_campaign_market_assignment_history ON public.campaign_market_assignment_history;

CREATE POLICY auth_full_campaign_market_assignment_history
  ON public.campaign_market_assignment_history
  FOR ALL
  TO authenticated
  USING (true)
  WITH CHECK (true);

CREATE POLICY svc_full_campaign_market_assignment_history
  ON public.campaign_market_assignment_history
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);
