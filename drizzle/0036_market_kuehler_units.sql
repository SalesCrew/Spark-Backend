CREATE TABLE IF NOT EXISTS "market_kuehler_units" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "market_id" uuid NOT NULL REFERENCES "markets"("id") ON DELETE CASCADE,
  "name" text NOT NULL DEFAULT '',
  "employee" text NOT NULL DEFAULT '',
  "kuehler_internal_id" text,
  "kuehler_bd" text,
  "kuehler_anzahl_ks_am_standort" integer,
  "kuehler_serial_number" text,
  "kuehler_model" text,
  "import_source_file_name" text NOT NULL DEFAULT '',
  "imported_at" timestamptz NOT NULL DEFAULT now(),
  "is_deleted" boolean NOT NULL DEFAULT false,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS "market_kuehler_units_market_deleted_idx"
  ON "market_kuehler_units" ("market_id", "is_deleted");

CREATE INDEX IF NOT EXISTS "market_kuehler_units_internal_id_idx"
  ON "market_kuehler_units" ("kuehler_internal_id");

CREATE UNIQUE INDEX IF NOT EXISTS "market_kuehler_units_internal_id_active_unique"
  ON "market_kuehler_units" ("kuehler_internal_id")
  WHERE "market_kuehler_units"."is_deleted" = false AND "market_kuehler_units"."kuehler_internal_id" IS NOT NULL;

CREATE INDEX IF NOT EXISTS "market_kuehler_units_deleted_idx"
  ON "market_kuehler_units" ("is_deleted");

INSERT INTO "market_kuehler_units" (
  "market_id",
  "name",
  "employee",
  "kuehler_internal_id",
  "kuehler_bd",
  "kuehler_anzahl_ks_am_standort",
  "kuehler_serial_number",
  "kuehler_model",
  "import_source_file_name",
  "imported_at",
  "is_deleted",
  "created_at",
  "updated_at"
)
SELECT
  m."id" AS "market_id",
  COALESCE(m."name", '') AS "name",
  COALESCE(m."employee", '') AS "employee",
  m."kuehler_internal_id",
  m."kuehler_bd",
  m."kuehler_anzahl_ks_am_standort",
  m."kuehler_serial_number",
  m."kuehler_model",
  COALESCE(m."import_source_file_name", '') AS "import_source_file_name",
  COALESCE(m."imported_at", now()) AS "imported_at",
  false AS "is_deleted",
  now() AS "created_at",
  now() AS "updated_at"
FROM "markets" m
WHERE m."is_deleted" = false
  AND (
    NULLIF(trim(COALESCE(m."kuehler_internal_id", '')), '') IS NOT NULL
    OR NULLIF(trim(COALESCE(m."kuehler_bd", '')), '') IS NOT NULL
    OR m."kuehler_anzahl_ks_am_standort" IS NOT NULL
    OR NULLIF(trim(COALESCE(m."kuehler_serial_number", '')), '') IS NOT NULL
    OR NULLIF(trim(COALESCE(m."kuehler_model", '')), '') IS NOT NULL
  )
ON CONFLICT ("kuehler_internal_id")
WHERE "is_deleted" = false AND "kuehler_internal_id" IS NOT NULL
DO UPDATE
SET
  "market_id" = EXCLUDED."market_id",
  "name" = EXCLUDED."name",
  "employee" = EXCLUDED."employee",
  "kuehler_bd" = EXCLUDED."kuehler_bd",
  "kuehler_anzahl_ks_am_standort" = EXCLUDED."kuehler_anzahl_ks_am_standort",
  "kuehler_serial_number" = EXCLUDED."kuehler_serial_number",
  "kuehler_model" = EXCLUDED."kuehler_model",
  "import_source_file_name" = EXCLUDED."import_source_file_name",
  "imported_at" = EXCLUDED."imported_at",
  "updated_at" = now();
