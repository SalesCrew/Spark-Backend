ALTER TABLE "markets"
  ADD COLUMN IF NOT EXISTS "internal_id" text,
  ADD COLUMN IF NOT EXISTS "market_types" text[] NOT NULL DEFAULT '{}'::text[],
  ADD COLUMN IF NOT EXISTS "kuehler_stamm_nr" text,
  ADD COLUMN IF NOT EXISTS "kuehler_bd" text,
  ADD COLUMN IF NOT EXISTS "kuehler_count_on_site" integer,
  ADD COLUMN IF NOT EXISTS "kuehler_serial_number" text,
  ADD COLUMN IF NOT EXISTS "kuehler_model" text;

UPDATE "markets"
SET "internal_id" = NULLIF(BTRIM("internal_id"), '')
WHERE "internal_id" IS NOT NULL;

UPDATE "markets"
SET "market_types" = CASE
  WHEN "universe_market" = true THEN ARRAY['universum']::text[]
  ELSE ARRAY[]::text[]
END
WHERE "market_types" IS NULL OR cardinality("market_types") = 0;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'markets_market_types_allowed_ck'
  ) THEN
    ALTER TABLE "markets"
      ADD CONSTRAINT "markets_market_types_allowed_ck"
      CHECK ("market_types" <@ ARRAY['universum', 'kuehler']::text[]);
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS "markets_internal_id_unique"
  ON "markets" USING btree ("internal_id")
  WHERE "markets"."is_deleted" = false AND "markets"."internal_id" IS NOT NULL;

CREATE INDEX IF NOT EXISTS "markets_market_types_idx"
  ON "markets" USING gin ("market_types");
