CREATE TABLE "markets" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"standard_market_number" text,
	"coke_master_number" text,
	"flex_number" text,
	"name" text NOT NULL,
	"db_name" text DEFAULT '' NOT NULL,
	"address" text NOT NULL,
	"postal_code" text NOT NULL,
	"city" text NOT NULL,
	"region" text NOT NULL,
	"em_eh" text DEFAULT '' NOT NULL,
	"employee" text DEFAULT '' NOT NULL,
	"current_gm_name" text DEFAULT '' NOT NULL,
	"visit_frequency_per_year" integer DEFAULT 0 NOT NULL,
	"info_flag" boolean DEFAULT false NOT NULL,
	"info_note" text DEFAULT '' NOT NULL,
	"universe_market" boolean DEFAULT false NOT NULL,
	"import_source_file_name" text DEFAULT '' NOT NULL,
	"imported_at" timestamp with time zone DEFAULT now() NOT NULL,
	"planned_to_id" uuid,
	"is_deleted" boolean DEFAULT false NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX "markets_standard_market_number_unique" ON "markets" USING btree ("standard_market_number");--> statement-breakpoint
CREATE UNIQUE INDEX "markets_coke_master_number_unique" ON "markets" USING btree ("coke_master_number");--> statement-breakpoint
CREATE UNIQUE INDEX "markets_flex_number_unique" ON "markets" USING btree ("flex_number");--> statement-breakpoint
CREATE INDEX "markets_region_idx" ON "markets" USING btree ("region");--> statement-breakpoint
CREATE INDEX "markets_city_idx" ON "markets" USING btree ("city");--> statement-breakpoint
CREATE INDEX "markets_postal_code_idx" ON "markets" USING btree ("postal_code");--> statement-breakpoint
CREATE INDEX "markets_current_gm_name_idx" ON "markets" USING btree ("current_gm_name");--> statement-breakpoint
CREATE INDEX "markets_deleted_idx" ON "markets" USING btree ("is_deleted");
