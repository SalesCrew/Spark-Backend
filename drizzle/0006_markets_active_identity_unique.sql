DROP INDEX "markets_standard_market_number_unique";
--> statement-breakpoint
DROP INDEX "markets_coke_master_number_unique";
--> statement-breakpoint
DROP INDEX "markets_flex_number_unique";
--> statement-breakpoint
CREATE UNIQUE INDEX "markets_standard_market_number_unique"
  ON "markets" USING btree ("standard_market_number")
  WHERE "markets"."is_deleted" = false AND "markets"."standard_market_number" IS NOT NULL;
--> statement-breakpoint
CREATE UNIQUE INDEX "markets_coke_master_number_unique"
  ON "markets" USING btree ("coke_master_number")
  WHERE "markets"."is_deleted" = false AND "markets"."coke_master_number" IS NOT NULL;
--> statement-breakpoint
CREATE UNIQUE INDEX "markets_flex_number_unique"
  ON "markets" USING btree ("flex_number")
  WHERE "markets"."is_deleted" = false AND "markets"."flex_number" IS NOT NULL;
