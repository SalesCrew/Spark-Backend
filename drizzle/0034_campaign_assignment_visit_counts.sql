ALTER TABLE "campaign_market_assignments"
  DROP CONSTRAINT IF EXISTS "campaign_market_assignments_campaign_id_market_id_pk";
--> statement-breakpoint
ALTER TABLE "campaign_market_assignments"
  ADD COLUMN IF NOT EXISTS "id" uuid DEFAULT gen_random_uuid();
--> statement-breakpoint
UPDATE "campaign_market_assignments"
SET "id" = gen_random_uuid()
WHERE "id" IS NULL;
--> statement-breakpoint
ALTER TABLE "campaign_market_assignments"
  ALTER COLUMN "id" SET NOT NULL;
--> statement-breakpoint
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'campaign_market_assignments_id_pk'
  ) THEN
    ALTER TABLE "campaign_market_assignments"
      ADD CONSTRAINT "campaign_market_assignments_id_pk" PRIMARY KEY ("id");
  END IF;
END $$;
--> statement-breakpoint
ALTER TABLE "campaign_market_assignments"
  ADD COLUMN IF NOT EXISTS "visit_target_count" integer DEFAULT 1;
--> statement-breakpoint
UPDATE "campaign_market_assignments"
SET "visit_target_count" = 1
WHERE "visit_target_count" IS NULL;
--> statement-breakpoint
ALTER TABLE "campaign_market_assignments"
  ALTER COLUMN "visit_target_count" SET DEFAULT 1,
  ALTER COLUMN "visit_target_count" SET NOT NULL;
--> statement-breakpoint
ALTER TABLE "campaign_market_assignments"
  ADD COLUMN IF NOT EXISTS "current_visits_count" integer DEFAULT 0;
--> statement-breakpoint
UPDATE "campaign_market_assignments"
SET "current_visits_count" = 0
WHERE "current_visits_count" IS NULL;
--> statement-breakpoint
ALTER TABLE "campaign_market_assignments"
  ALTER COLUMN "current_visits_count" SET DEFAULT 0,
  ALTER COLUMN "current_visits_count" SET NOT NULL;
--> statement-breakpoint
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'campaign_market_assignments_visit_target_count_ck'
  ) THEN
    ALTER TABLE "campaign_market_assignments"
      ADD CONSTRAINT "campaign_market_assignments_visit_target_count_ck"
      CHECK ("visit_target_count" >= 1);
  END IF;
END $$;
--> statement-breakpoint
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'campaign_market_assignments_current_visits_count_ck'
  ) THEN
    ALTER TABLE "campaign_market_assignments"
      ADD CONSTRAINT "campaign_market_assignments_current_visits_count_ck"
      CHECK ("current_visits_count" >= 0);
  END IF;
END $$;
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "campaign_market_assignments_campaign_market_gm_active_unique"
  ON "campaign_market_assignments" USING btree ("campaign_id", "market_id", "gm_user_id")
  WHERE "campaign_market_assignments"."is_deleted" = false AND "campaign_market_assignments"."gm_user_id" IS NOT NULL;
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "campaign_market_assignments_campaign_market_unassigned_active_unique"
  ON "campaign_market_assignments" USING btree ("campaign_id", "market_id")
  WHERE "campaign_market_assignments"."is_deleted" = false AND "campaign_market_assignments"."gm_user_id" IS NULL;
