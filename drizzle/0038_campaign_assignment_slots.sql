ALTER TABLE "campaign_market_assignments"
  ADD COLUMN IF NOT EXISTS "assignment_slot" integer NOT NULL DEFAULT 1;

ALTER TABLE "campaign_market_assignments"
  DROP CONSTRAINT IF EXISTS "campaign_market_assignments_assignment_slot_ck";

ALTER TABLE "campaign_market_assignments"
  ADD CONSTRAINT "campaign_market_assignments_assignment_slot_ck"
  CHECK ("assignment_slot" >= 1);

DROP INDEX IF EXISTS "campaign_market_assignments_campaign_market_gm_active_unique";
DROP INDEX IF EXISTS "campaign_market_assignments_campaign_market_unassigned_active_unique";

CREATE UNIQUE INDEX IF NOT EXISTS "campaign_market_assignments_campaign_market_gm_active_unique"
  ON "campaign_market_assignments" ("campaign_id", "market_id", "gm_user_id", "assignment_slot")
  WHERE "is_deleted" = false AND "gm_user_id" IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS "campaign_market_assignments_campaign_market_unassigned_active_unique"
  ON "campaign_market_assignments" ("campaign_id", "market_id", "assignment_slot")
  WHERE "is_deleted" = false AND "gm_user_id" IS NULL;
