ALTER TABLE "market_kuehler_units"
  ADD COLUMN IF NOT EXISTS "kuehler_technical_ident_no" text;
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "market_kuehler_units_technical_ident_no_active_unique"
  ON "market_kuehler_units" USING btree ("kuehler_technical_ident_no")
  WHERE "market_kuehler_units"."is_deleted" = false
    AND "market_kuehler_units"."kuehler_technical_ident_no" IS NOT NULL;
