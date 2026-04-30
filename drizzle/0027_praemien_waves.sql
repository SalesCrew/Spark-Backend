DO $$ BEGIN
 CREATE TYPE "public"."praemien_wave_status" AS ENUM('draft', 'active', 'archived');
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;
--> statement-breakpoint
DO $$ BEGIN
 CREATE TYPE "public"."praemien_distribution_freq_rule" AS ENUM('lt8', 'gt8');
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "praemien_waves" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"name" text NOT NULL,
	"year" integer NOT NULL,
	"quarter" integer NOT NULL,
	"start_date" date NOT NULL,
	"end_date" date NOT NULL,
	"status" "praemien_wave_status" DEFAULT 'draft' NOT NULL,
	"description" text DEFAULT '' NOT NULL,
	"timezone" text DEFAULT 'Europe/Vienna' NOT NULL,
	"is_deleted" boolean DEFAULT false NOT NULL,
	"deleted_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "praemien_waves_quarter_ck" CHECK ("praemien_waves"."quarter" in (1, 2, 3, 4)),
	CONSTRAINT "praemien_waves_date_range_ck" CHECK ("praemien_waves"."start_date" <= "praemien_waves"."end_date")
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "praemien_wave_thresholds" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"wave_id" uuid NOT NULL,
	"label" text NOT NULL,
	"order_index" integer DEFAULT 0 NOT NULL,
	"min_points" numeric(12, 2) DEFAULT '0' NOT NULL,
	"reward_eur" numeric(12, 2) DEFAULT '0' NOT NULL,
	"is_deleted" boolean DEFAULT false NOT NULL,
	"deleted_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "praemien_wave_thresholds_min_points_ck" CHECK ("praemien_wave_thresholds"."min_points" >= 0),
	CONSTRAINT "praemien_wave_thresholds_reward_eur_ck" CHECK ("praemien_wave_thresholds"."reward_eur" >= 0)
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "praemien_wave_pillars" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"wave_id" uuid NOT NULL,
	"name" text NOT NULL,
	"description" text DEFAULT '' NOT NULL,
	"color" text DEFAULT '#DC2626' NOT NULL,
	"order_index" integer DEFAULT 0 NOT NULL,
	"is_manual" boolean DEFAULT false NOT NULL,
	"is_deleted" boolean DEFAULT false NOT NULL,
	"deleted_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "praemien_wave_sources" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"wave_id" uuid NOT NULL,
	"pillar_id" uuid NOT NULL,
	"section_type" "fragebogen_section" NOT NULL,
	"fragebogen_id" uuid,
	"fragebogen_name" text DEFAULT '' NOT NULL,
	"module_id" uuid,
	"module_name" text DEFAULT '' NOT NULL,
	"question_id" uuid NOT NULL,
	"question_text" text DEFAULT '' NOT NULL,
	"score_key" text NOT NULL,
	"display_label" text DEFAULT '' NOT NULL,
	"is_factor_mode" boolean DEFAULT false NOT NULL,
	"boni_value" numeric(12, 2) DEFAULT '0' NOT NULL,
	"distribution_freq_rule" "praemien_distribution_freq_rule",
	"is_deleted" boolean DEFAULT false NOT NULL,
	"deleted_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "praemien_wave_quality_scores" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"wave_id" uuid NOT NULL,
	"gm_user_id" uuid NOT NULL,
	"zeiterfassung" integer DEFAULT 0 NOT NULL,
	"reporting" integer DEFAULT 0 NOT NULL,
	"accuracy" integer DEFAULT 0 NOT NULL,
	"total_points" integer DEFAULT 0 NOT NULL,
	"note" text,
	"is_deleted" boolean DEFAULT false NOT NULL,
	"deleted_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "praemien_wave_quality_scores_zeiterfassung_ck" CHECK ("praemien_wave_quality_scores"."zeiterfassung" between 0 and 100),
	CONSTRAINT "praemien_wave_quality_scores_reporting_ck" CHECK ("praemien_wave_quality_scores"."reporting" between 0 and 100),
	CONSTRAINT "praemien_wave_quality_scores_accuracy_ck" CHECK ("praemien_wave_quality_scores"."accuracy" between 0 and 100),
	CONSTRAINT "praemien_wave_quality_scores_total_points_ck" CHECK ("praemien_wave_quality_scores"."total_points" between 0 and 100)
);
--> statement-breakpoint
DO $$ BEGIN
 ALTER TABLE "praemien_wave_thresholds" ADD CONSTRAINT "praemien_wave_thresholds_wave_id_praemien_waves_id_fk" FOREIGN KEY ("wave_id") REFERENCES "public"."praemien_waves"("id") ON DELETE cascade ON UPDATE no action;
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;
--> statement-breakpoint
DO $$ BEGIN
 ALTER TABLE "praemien_wave_pillars" ADD CONSTRAINT "praemien_wave_pillars_wave_id_praemien_waves_id_fk" FOREIGN KEY ("wave_id") REFERENCES "public"."praemien_waves"("id") ON DELETE cascade ON UPDATE no action;
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;
--> statement-breakpoint
DO $$ BEGIN
 ALTER TABLE "praemien_wave_sources" ADD CONSTRAINT "praemien_wave_sources_wave_id_praemien_waves_id_fk" FOREIGN KEY ("wave_id") REFERENCES "public"."praemien_waves"("id") ON DELETE cascade ON UPDATE no action;
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;
--> statement-breakpoint
DO $$ BEGIN
 ALTER TABLE "praemien_wave_sources" ADD CONSTRAINT "praemien_wave_sources_pillar_id_praemien_wave_pillars_id_fk" FOREIGN KEY ("pillar_id") REFERENCES "public"."praemien_wave_pillars"("id") ON DELETE cascade ON UPDATE no action;
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;
--> statement-breakpoint
DO $$ BEGIN
 ALTER TABLE "praemien_wave_quality_scores" ADD CONSTRAINT "praemien_wave_quality_scores_wave_id_praemien_waves_id_fk" FOREIGN KEY ("wave_id") REFERENCES "public"."praemien_waves"("id") ON DELETE cascade ON UPDATE no action;
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;
--> statement-breakpoint
DO $$ BEGIN
 ALTER TABLE "praemien_wave_quality_scores" ADD CONSTRAINT "praemien_wave_quality_scores_gm_user_id_users_id_fk" FOREIGN KEY ("gm_user_id") REFERENCES "public"."users"("id") ON DELETE cascade ON UPDATE no action;
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "praemien_waves_year_quarter_active_unique" ON "praemien_waves" USING btree ("year","quarter") WHERE "praemien_waves"."is_deleted" = false;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "praemien_waves_status_idx" ON "praemien_waves" USING btree ("status","is_deleted");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "praemien_waves_period_idx" ON "praemien_waves" USING btree ("year","quarter");
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "praemien_wave_thresholds_wave_order_active_unique" ON "praemien_wave_thresholds" USING btree ("wave_id","order_index") WHERE "praemien_wave_thresholds"."is_deleted" = false;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "praemien_wave_thresholds_wave_idx" ON "praemien_wave_thresholds" USING btree ("wave_id","is_deleted");
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "praemien_wave_pillars_wave_order_active_unique" ON "praemien_wave_pillars" USING btree ("wave_id","order_index") WHERE "praemien_wave_pillars"."is_deleted" = false;
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "praemien_wave_pillars_wave_name_active_unique" ON "praemien_wave_pillars" USING btree ("wave_id","name") WHERE "praemien_wave_pillars"."is_deleted" = false;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "praemien_wave_pillars_wave_idx" ON "praemien_wave_pillars" USING btree ("wave_id","is_deleted");
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "praemien_wave_sources_wave_question_score_active_unique" ON "praemien_wave_sources" USING btree ("wave_id","question_id","score_key") WHERE "praemien_wave_sources"."is_deleted" = false;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "praemien_wave_sources_wave_idx" ON "praemien_wave_sources" USING btree ("wave_id","is_deleted");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "praemien_wave_sources_pillar_idx" ON "praemien_wave_sources" USING btree ("pillar_id","is_deleted");
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "praemien_wave_quality_scores_wave_gm_active_unique" ON "praemien_wave_quality_scores" USING btree ("wave_id","gm_user_id") WHERE "praemien_wave_quality_scores"."is_deleted" = false;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "praemien_wave_quality_scores_wave_idx" ON "praemien_wave_quality_scores" USING btree ("wave_id","is_deleted");
