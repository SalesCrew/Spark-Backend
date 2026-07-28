CREATE INDEX IF NOT EXISTS "campaigns_flex_open_sync_idx"
ON "campaigns" ("status", "schedule_type", "end_date")
WHERE "is_deleted" = false
  AND "section" = 'flex';

CREATE INDEX IF NOT EXISTS "markets_flex_eligible_sync_idx"
ON "markets" ("market_type", "id")
WHERE "is_deleted" = false
  AND "is_active" = true
  AND "market_type" IN ('universum', 'both');

CREATE SCHEMA IF NOT EXISTS "internal_security";

CREATE OR REPLACE FUNCTION "internal_security"."reconcile_open_flex_market_assignments"(
  p_market_ids uuid[] DEFAULT NULL,
  p_campaign_id uuid DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('coke_spark_dynamic_flex_assignments'));

  IF p_market_ids IS NOT NULL THEN
    UPDATE "public"."campaign_market_assignments" assignment_row
    SET
      "is_deleted" = true,
      "deleted_at" = now(),
      "updated_at" = now()
    FROM "public"."campaigns" campaign_row
    WHERE assignment_row."campaign_id" = campaign_row."id"
      AND assignment_row."market_id" = ANY(p_market_ids)
      AND assignment_row."is_deleted" = false
      AND assignment_row."gm_user_id" IS NULL
      AND campaign_row."section" = 'flex'
      AND campaign_row."is_deleted" = false
      AND campaign_row."status" IN ('active', 'scheduled')
      AND (
        campaign_row."schedule_type" = 'always'
        OR (
          campaign_row."schedule_type" = 'scheduled'
          AND campaign_row."end_date" IS NOT NULL
          AND campaign_row."end_date" >= current_date
        )
      )
      AND NOT EXISTS (
        SELECT 1
        FROM "public"."markets" eligible_market
        WHERE eligible_market."id" = assignment_row."market_id"
          AND eligible_market."is_deleted" = false
          AND eligible_market."is_active" = true
          AND eligible_market."market_type" IN ('universum', 'both')
      );
  END IF;

  WITH eligible_pairs AS (
    SELECT
      campaign_row."id" AS campaign_id,
      market_row."id" AS market_id
    FROM "public"."campaigns" campaign_row
    CROSS JOIN "public"."markets" market_row
    WHERE (p_campaign_id IS NULL OR campaign_row."id" = p_campaign_id)
      AND (p_market_ids IS NULL OR market_row."id" = ANY(p_market_ids))
      AND campaign_row."section" = 'flex'
      AND campaign_row."is_deleted" = false
      AND campaign_row."status" IN ('active', 'scheduled')
      AND (
        campaign_row."schedule_type" = 'always'
        OR (
          campaign_row."schedule_type" = 'scheduled'
          AND campaign_row."end_date" IS NOT NULL
          AND campaign_row."end_date" >= current_date
        )
      )
      AND market_row."is_deleted" = false
      AND market_row."is_active" = true
      AND market_row."market_type" IN ('universum', 'both')
  ),
  reactivation_candidates AS (
    SELECT DISTINCT ON (eligible_pair.campaign_id, eligible_pair.market_id)
      assignment_row."id"
    FROM eligible_pairs eligible_pair
    INNER JOIN "public"."campaign_market_assignments" assignment_row
      ON assignment_row."campaign_id" = eligible_pair.campaign_id
      AND assignment_row."market_id" = eligible_pair.market_id
      AND assignment_row."gm_user_id" IS NULL
      AND assignment_row."assignment_slot" = 1
      AND assignment_row."is_deleted" = true
    WHERE NOT EXISTS (
      SELECT 1
      FROM "public"."campaign_market_assignments" active_assignment
      WHERE active_assignment."campaign_id" = eligible_pair.campaign_id
        AND active_assignment."market_id" = eligible_pair.market_id
        AND active_assignment."gm_user_id" IS NULL
        AND active_assignment."assignment_slot" = 1
        AND active_assignment."is_deleted" = false
    )
    ORDER BY
      eligible_pair.campaign_id,
      eligible_pair.market_id,
      assignment_row."updated_at" DESC,
      assignment_row."created_at" DESC,
      assignment_row."id" DESC
  )
  UPDATE "public"."campaign_market_assignments" assignment_row
  SET
    "is_deleted" = false,
    "deleted_at" = NULL,
    "current_visits_count" = GREATEST(
      assignment_row."current_visits_count",
      (
        SELECT count(DISTINCT visit_row."id")::integer
        FROM "public"."visit_session_sections" visit_section_row
        INNER JOIN "public"."visit_sessions" visit_row
          ON visit_row."id" = visit_section_row."visit_session_id"
        WHERE visit_section_row."campaign_id" = assignment_row."campaign_id"
          AND visit_section_row."is_deleted" = false
          AND visit_row."market_id" = assignment_row."market_id"
          AND visit_row."status" = 'submitted'
          AND visit_row."is_deleted" = false
      )
    ),
    "updated_at" = now()
  FROM reactivation_candidates candidate
  WHERE assignment_row."id" = candidate."id";

  INSERT INTO "public"."campaign_market_assignments" (
    "campaign_id",
    "market_id",
    "gm_user_id",
    "assignment_slot",
    "visit_target_count",
    "current_visits_count",
    "assigned_at",
    "assigned_by_user_id",
    "is_deleted",
    "deleted_at",
    "created_at",
    "updated_at"
  )
  SELECT
    campaign_row."id",
    market_row."id",
    NULL,
    1,
    1,
    (
      SELECT count(DISTINCT visit_row."id")::integer
      FROM "public"."visit_session_sections" visit_section_row
      INNER JOIN "public"."visit_sessions" visit_row
        ON visit_row."id" = visit_section_row."visit_session_id"
      WHERE visit_section_row."campaign_id" = campaign_row."id"
        AND visit_section_row."is_deleted" = false
        AND visit_row."market_id" = market_row."id"
        AND visit_row."status" = 'submitted'
        AND visit_row."is_deleted" = false
    ),
    now(),
    NULL,
    false,
    NULL,
    now(),
    now()
  FROM "public"."campaigns" campaign_row
  CROSS JOIN "public"."markets" market_row
  WHERE (p_campaign_id IS NULL OR campaign_row."id" = p_campaign_id)
    AND (p_market_ids IS NULL OR market_row."id" = ANY(p_market_ids))
    AND campaign_row."section" = 'flex'
    AND campaign_row."is_deleted" = false
    AND campaign_row."status" IN ('active', 'scheduled')
    AND (
      campaign_row."schedule_type" = 'always'
      OR (
        campaign_row."schedule_type" = 'scheduled'
        AND campaign_row."end_date" IS NOT NULL
        AND campaign_row."end_date" >= current_date
      )
    )
    AND market_row."is_deleted" = false
    AND market_row."is_active" = true
    AND market_row."market_type" IN ('universum', 'both')
    AND NOT EXISTS (
      SELECT 1
      FROM "public"."campaign_market_assignments" active_assignment
      WHERE active_assignment."campaign_id" = campaign_row."id"
        AND active_assignment."market_id" = market_row."id"
        AND active_assignment."gm_user_id" IS NULL
        AND active_assignment."assignment_slot" = 1
        AND active_assignment."is_deleted" = false
    )
  ON CONFLICT DO NOTHING;
END;
$$;

REVOKE ALL
ON FUNCTION "internal_security"."reconcile_open_flex_market_assignments"(uuid[], uuid)
FROM PUBLIC, anon, authenticated;

CREATE OR REPLACE FUNCTION "internal_security"."sync_flex_assignments_after_market_write"()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
DECLARE
  changed_market_ids uuid[];
BEGIN
  SELECT array_agg(market_row."id")
  INTO changed_market_ids
  FROM flex_market_rows market_row;

  IF changed_market_ids IS NOT NULL THEN
    PERFORM "internal_security"."reconcile_open_flex_market_assignments"(changed_market_ids, NULL);
  END IF;
  RETURN NULL;
END;
$$;

REVOKE ALL
ON FUNCTION "internal_security"."sync_flex_assignments_after_market_write"()
FROM PUBLIC, anon, authenticated;

DROP TRIGGER IF EXISTS "markets_sync_open_flex_assignments_trg"
ON "public"."markets";

CREATE TRIGGER "markets_sync_open_flex_assignments_trg"
AFTER INSERT
ON "public"."markets"
REFERENCING NEW TABLE AS flex_market_rows
FOR EACH STATEMENT
EXECUTE FUNCTION "internal_security"."sync_flex_assignments_after_market_write"();

CREATE OR REPLACE FUNCTION "internal_security"."sync_flex_assignments_after_market_update"()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
DECLARE
  changed_market_ids uuid[];
BEGIN
  SELECT array_agg(new_market_row."id")
  INTO changed_market_ids
  FROM flex_market_rows new_market_row
  INNER JOIN flex_old_market_rows old_market_row
    ON old_market_row."id" = new_market_row."id"
  WHERE new_market_row."market_type" IS DISTINCT FROM old_market_row."market_type"
    OR new_market_row."is_active" IS DISTINCT FROM old_market_row."is_active"
    OR new_market_row."is_deleted" IS DISTINCT FROM old_market_row."is_deleted";

  IF changed_market_ids IS NOT NULL THEN
    PERFORM "internal_security"."reconcile_open_flex_market_assignments"(changed_market_ids, NULL);
  END IF;
  RETURN NULL;
END;
$$;

REVOKE ALL
ON FUNCTION "internal_security"."sync_flex_assignments_after_market_update"()
FROM PUBLIC, anon, authenticated;

DROP TRIGGER IF EXISTS "markets_update_open_flex_assignments_trg"
ON "public"."markets";

CREATE TRIGGER "markets_update_open_flex_assignments_trg"
AFTER UPDATE
ON "public"."markets"
REFERENCING OLD TABLE AS flex_old_market_rows NEW TABLE AS flex_market_rows
FOR EACH STATEMENT
EXECUTE FUNCTION "internal_security"."sync_flex_assignments_after_market_update"();

CREATE OR REPLACE FUNCTION "internal_security"."sync_flex_assignments_after_campaign_update"()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
BEGIN
  IF NEW."section" = 'flex' THEN
    PERFORM "internal_security"."reconcile_open_flex_market_assignments"(NULL, NEW."id");
  END IF;
  RETURN NEW;
END;
$$;

REVOKE ALL
ON FUNCTION "internal_security"."sync_flex_assignments_after_campaign_update"()
FROM PUBLIC, anon, authenticated;

DROP TRIGGER IF EXISTS "campaigns_sync_open_flex_assignments_trg"
ON "public"."campaigns";

CREATE TRIGGER "campaigns_sync_open_flex_assignments_trg"
AFTER UPDATE OF "section", "status", "schedule_type", "start_date", "end_date", "is_deleted"
ON "public"."campaigns"
FOR EACH ROW
EXECUTE FUNCTION "internal_security"."sync_flex_assignments_after_campaign_update"();

SELECT "internal_security"."reconcile_open_flex_market_assignments"(NULL, NULL);
