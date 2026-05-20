DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'market_type') THEN
    CREATE TYPE market_type AS ENUM ('universum', 'kuehler', 'both');
  END IF;
END $$;

ALTER TABLE "markets"
  ADD COLUMN IF NOT EXISTS "market_type" market_type;

ALTER TABLE "markets"
  ALTER COLUMN "market_type" SET DEFAULT 'universum';

UPDATE "markets"
SET "market_type" = 'universum'
WHERE "market_type" IS NULL;

ALTER TABLE "markets"
  ALTER COLUMN "market_type" SET NOT NULL;

ALTER TABLE "markets"
  ADD COLUMN IF NOT EXISTS "kuehler_stammnr" text,
  ADD COLUMN IF NOT EXISTS "kuehler_bd" text,
  ADD COLUMN IF NOT EXISTS "kuehler_anzahl_ks_am_standort" integer,
  ADD COLUMN IF NOT EXISTS "kuehler_internal_id" text,
  ADD COLUMN IF NOT EXISTS "kuehler_serial_number" text,
  ADD COLUMN IF NOT EXISTS "kuehler_model" text;

CREATE INDEX IF NOT EXISTS "markets_market_type_idx"
  ON "markets" ("market_type");

CREATE UNIQUE INDEX IF NOT EXISTS "markets_kuehler_internal_id_unique"
  ON "markets" ("kuehler_internal_id")
  WHERE "markets"."is_deleted" = false AND "markets"."kuehler_internal_id" IS NOT NULL;

UPDATE "markets"
SET "universe_market" = CASE
  WHEN "market_type" IN ('universum', 'both') THEN true
  ELSE false
END;
