DROP INDEX IF EXISTS "markets_kuehler_internal_id_unique";

ALTER TABLE "markets"
  DROP COLUMN IF EXISTS "kuehler_bd",
  DROP COLUMN IF EXISTS "kuehler_anzahl_ks_am_standort",
  DROP COLUMN IF EXISTS "kuehler_internal_id",
  DROP COLUMN IF EXISTS "kuehler_serial_number",
  DROP COLUMN IF EXISTS "kuehler_model";
