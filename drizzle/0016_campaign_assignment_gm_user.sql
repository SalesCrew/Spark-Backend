ALTER TABLE campaign_market_assignments
ADD COLUMN IF NOT EXISTS gm_user_id uuid REFERENCES users(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS campaign_market_assignments_gm_deleted_idx
  ON campaign_market_assignments(gm_user_id, is_deleted);
