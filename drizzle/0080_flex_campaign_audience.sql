ALTER TABLE "campaigns"
ADD COLUMN IF NOT EXISTS "assigned_gm_user_id" uuid;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'campaigns_assigned_gm_user_id_users_id_fk'
      AND conrelid = 'public.campaigns'::regclass
  ) THEN
    ALTER TABLE "campaigns"
    ADD CONSTRAINT "campaigns_assigned_gm_user_id_users_id_fk"
    FOREIGN KEY ("assigned_gm_user_id")
    REFERENCES "public"."users"("id")
    ON DELETE SET NULL;
  END IF;
END $$;

WITH legacy_audiences AS (
  SELECT
    cma.campaign_id,
    count(*) FILTER (WHERE cma.is_deleted = false AND cma.gm_user_id IS NULL) AS global_rows,
    count(DISTINCT cma.gm_user_id) FILTER (WHERE cma.is_deleted = false AND cma.gm_user_id IS NOT NULL) AS gm_count,
    (array_agg(DISTINCT cma.gm_user_id) FILTER (
      WHERE cma.is_deleted = false AND cma.gm_user_id IS NOT NULL
    ))[1] AS gm_user_id
  FROM campaign_market_assignments cma
  INNER JOIN campaigns c ON c.id = cma.campaign_id
  WHERE c.section = 'flex'
    AND c.is_deleted = false
  GROUP BY cma.campaign_id
), migrated_campaigns AS (
  UPDATE campaigns c
  SET assigned_gm_user_id = legacy.gm_user_id,
      updated_at = now()
  FROM legacy_audiences legacy
  WHERE c.id = legacy.campaign_id
    AND c.assigned_gm_user_id IS NULL
    AND legacy.global_rows = 0
    AND legacy.gm_count = 1
  RETURNING c.id
)
UPDATE campaign_market_assignments cma
SET gm_user_id = NULL,
    updated_at = now()
WHERE cma.is_deleted = false
  AND EXISTS (
    SELECT 1
    FROM campaigns c
    WHERE c.id = cma.campaign_id
      AND c.section = 'flex'
      AND c.assigned_gm_user_id IS NOT NULL
  );

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'campaigns_assigned_gm_flex_only_ck'
      AND conrelid = 'public.campaigns'::regclass
  ) THEN
    ALTER TABLE "campaigns"
    ADD CONSTRAINT "campaigns_assigned_gm_flex_only_ck"
    CHECK ("assigned_gm_user_id" IS NULL OR "section" = 'flex');
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS "campaigns_assigned_gm_active_idx"
ON "campaigns" ("assigned_gm_user_id", "section", "status")
WHERE "is_deleted" = false AND "assigned_gm_user_id" IS NOT NULL;
