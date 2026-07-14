-- Independent, configurable rewards per premium pillar.
-- Existing waves remain on the legacy global-threshold model until explicitly switched.

DO $$ BEGIN
  CREATE TYPE "public"."praemien_reward_model" AS ENUM('global_thresholds', 'pillar_targets', 'pillar_tiers');
EXCEPTION
  WHEN duplicate_object THEN null;
END $$;
--> statement-breakpoint

ALTER TYPE "public"."praemien_reward_model" ADD VALUE IF NOT EXISTS 'pillar_tiers';
--> statement-breakpoint

ALTER TABLE "praemien_waves"
  ADD COLUMN IF NOT EXISTS "reward_model" "praemien_reward_model" DEFAULT 'global_thresholds' NOT NULL;
--> statement-breakpoint

ALTER TABLE "praemien_wave_pillars"
  ADD COLUMN IF NOT EXISTS "payout_mode" text DEFAULT 'highest_tier' NOT NULL,
  ADD COLUMN IF NOT EXISTS "max_reward_eur" numeric(12, 2) DEFAULT '0' NOT NULL,
  ADD COLUMN IF NOT EXISTS "target_points" numeric(14, 4),
  ADD COLUMN IF NOT EXISTS "reward_eur" numeric(12, 2) DEFAULT '0' NOT NULL;
--> statement-breakpoint

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'praemien_wave_pillars_target_points_ck'
      AND conrelid = 'public.praemien_wave_pillars'::regclass
  ) THEN
    ALTER TABLE "public"."praemien_wave_pillars"
      ADD CONSTRAINT "praemien_wave_pillars_target_points_ck"
      CHECK ("target_points" IS NULL OR "target_points" > 0);
  END IF;
END $$;
--> statement-breakpoint

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'praemien_wave_pillars_payout_mode_ck'
      AND conrelid = 'public.praemien_wave_pillars'::regclass
  ) THEN
    ALTER TABLE "public"."praemien_wave_pillars"
      ADD CONSTRAINT "praemien_wave_pillars_payout_mode_ck"
      CHECK ("payout_mode" IN ('highest_tier', 'sum_earned_tiers'));
  END IF;
END $$;
--> statement-breakpoint

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'praemien_wave_pillars_max_reward_eur_ck'
      AND conrelid = 'public.praemien_wave_pillars'::regclass
  ) THEN
    ALTER TABLE "public"."praemien_wave_pillars"
      ADD CONSTRAINT "praemien_wave_pillars_max_reward_eur_ck"
      CHECK ("max_reward_eur" >= 0);
  END IF;
END $$;
--> statement-breakpoint

CREATE TABLE IF NOT EXISTS "public"."praemien_wave_pillar_metrics" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
  "wave_id" uuid NOT NULL,
  "pillar_id" uuid NOT NULL,
  "key" text NOT NULL,
  "label" text NOT NULL,
  "unit" text DEFAULT 'points' NOT NULL,
  "value_source" text DEFAULT 'contribution_points' NOT NULL,
  "source_key" text,
  "order_index" integer DEFAULT 0 NOT NULL,
  "is_deleted" boolean DEFAULT false NOT NULL,
  "deleted_at" timestamp with time zone,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  CONSTRAINT "praemien_wave_pillar_metrics_unit_ck" CHECK ("unit" IN ('points', 'percent', 'count', 'currency')),
  CONSTRAINT "praemien_wave_pillar_metrics_value_source_ck" CHECK ("value_source" IN ('contribution_points', 'contribution_percent', 'quality_zeiterfassung', 'quality_reporting', 'quality_accuracy', 'quality_average', 'flex_total_points', 'flex_component'))
);
--> statement-breakpoint

ALTER TABLE "public"."praemien_wave_pillar_metrics"
  ADD CONSTRAINT "praemien_wave_pillar_metrics_wave_id_praemien_waves_id_fk"
  FOREIGN KEY ("wave_id") REFERENCES "public"."praemien_waves"("id") ON DELETE CASCADE;
--> statement-breakpoint
ALTER TABLE "public"."praemien_wave_pillar_metrics"
  ADD CONSTRAINT "praemien_wave_pillar_metrics_pillar_id_praemien_wave_pillars_id_fk"
  FOREIGN KEY ("pillar_id") REFERENCES "public"."praemien_wave_pillars"("id") ON DELETE CASCADE;
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "praemien_wave_pillar_metrics_pillar_key_active_unique"
  ON "public"."praemien_wave_pillar_metrics" ("pillar_id", "key") WHERE "is_deleted" = false;
CREATE UNIQUE INDEX IF NOT EXISTS "praemien_wave_pillar_metrics_pillar_order_active_unique"
  ON "public"."praemien_wave_pillar_metrics" ("pillar_id", "order_index") WHERE "is_deleted" = false;
CREATE INDEX IF NOT EXISTS "praemien_wave_pillar_metrics_wave_idx"
  ON "public"."praemien_wave_pillar_metrics" ("wave_id", "is_deleted");
CREATE INDEX IF NOT EXISTS "praemien_wave_pillar_metrics_pillar_idx"
  ON "public"."praemien_wave_pillar_metrics" ("pillar_id", "is_deleted");
--> statement-breakpoint

CREATE TABLE IF NOT EXISTS "public"."praemien_wave_pillar_tiers" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
  "wave_id" uuid NOT NULL,
  "pillar_id" uuid NOT NULL,
  "label" text NOT NULL,
  "order_index" integer DEFAULT 0 NOT NULL,
  "reward_eur" numeric(12, 2) DEFAULT '0' NOT NULL,
  "is_deleted" boolean DEFAULT false NOT NULL,
  "deleted_at" timestamp with time zone,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  CONSTRAINT "praemien_wave_pillar_tiers_reward_eur_ck" CHECK ("reward_eur" >= 0)
);
--> statement-breakpoint
ALTER TABLE "public"."praemien_wave_pillar_tiers"
  ADD CONSTRAINT "praemien_wave_pillar_tiers_wave_id_praemien_waves_id_fk"
  FOREIGN KEY ("wave_id") REFERENCES "public"."praemien_waves"("id") ON DELETE CASCADE;
ALTER TABLE "public"."praemien_wave_pillar_tiers"
  ADD CONSTRAINT "praemien_wave_pillar_tiers_pillar_id_praemien_wave_pillars_id_fk"
  FOREIGN KEY ("pillar_id") REFERENCES "public"."praemien_wave_pillars"("id") ON DELETE CASCADE;
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "praemien_wave_pillar_tiers_pillar_order_active_unique"
  ON "public"."praemien_wave_pillar_tiers" ("pillar_id", "order_index") WHERE "is_deleted" = false;
CREATE UNIQUE INDEX IF NOT EXISTS "praemien_wave_pillar_tiers_pillar_label_active_unique"
  ON "public"."praemien_wave_pillar_tiers" ("pillar_id", "label") WHERE "is_deleted" = false;
CREATE INDEX IF NOT EXISTS "praemien_wave_pillar_tiers_wave_idx"
  ON "public"."praemien_wave_pillar_tiers" ("wave_id", "is_deleted");
CREATE INDEX IF NOT EXISTS "praemien_wave_pillar_tiers_pillar_idx"
  ON "public"."praemien_wave_pillar_tiers" ("pillar_id", "is_deleted");
--> statement-breakpoint

CREATE TABLE IF NOT EXISTS "public"."praemien_wave_pillar_tier_conditions" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
  "wave_id" uuid NOT NULL,
  "pillar_id" uuid NOT NULL,
  "tier_id" uuid NOT NULL,
  "metric_id" uuid NOT NULL,
  "operator" text DEFAULT 'gte' NOT NULL,
  "threshold_value" numeric(14, 4) NOT NULL,
  "order_index" integer DEFAULT 0 NOT NULL,
  "is_deleted" boolean DEFAULT false NOT NULL,
  "deleted_at" timestamp with time zone,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  CONSTRAINT "praemien_wave_pillar_tier_conditions_operator_ck" CHECK ("operator" IN ('gte', 'lte', 'eq'))
);
--> statement-breakpoint
ALTER TABLE "public"."praemien_wave_pillar_tier_conditions"
  ADD CONSTRAINT "praemien_wave_pillar_tier_conditions_wave_id_praemien_waves_id_fk"
  FOREIGN KEY ("wave_id") REFERENCES "public"."praemien_waves"("id") ON DELETE CASCADE;
ALTER TABLE "public"."praemien_wave_pillar_tier_conditions"
  ADD CONSTRAINT "praemien_wave_pillar_tier_conditions_pillar_id_praemien_wave_pillars_id_fk"
  FOREIGN KEY ("pillar_id") REFERENCES "public"."praemien_wave_pillars"("id") ON DELETE CASCADE;
ALTER TABLE "public"."praemien_wave_pillar_tier_conditions"
  ADD CONSTRAINT "praemien_wave_pillar_tier_conditions_tier_id_praemien_wave_pillar_tiers_id_fk"
  FOREIGN KEY ("tier_id") REFERENCES "public"."praemien_wave_pillar_tiers"("id") ON DELETE CASCADE;
ALTER TABLE "public"."praemien_wave_pillar_tier_conditions"
  ADD CONSTRAINT "praemien_wave_pillar_tier_conditions_metric_id_praemien_wave_pillar_metrics_id_fk"
  FOREIGN KEY ("metric_id") REFERENCES "public"."praemien_wave_pillar_metrics"("id") ON DELETE CASCADE;
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "praemien_wave_pillar_tier_conditions_tier_order_active_unique"
  ON "public"."praemien_wave_pillar_tier_conditions" ("tier_id", "order_index") WHERE "is_deleted" = false;
CREATE INDEX IF NOT EXISTS "praemien_wave_pillar_tier_conditions_wave_idx"
  ON "public"."praemien_wave_pillar_tier_conditions" ("wave_id", "is_deleted");
CREATE INDEX IF NOT EXISTS "praemien_wave_pillar_tier_conditions_pillar_idx"
  ON "public"."praemien_wave_pillar_tier_conditions" ("pillar_id", "is_deleted");
CREATE INDEX IF NOT EXISTS "praemien_wave_pillar_tier_conditions_tier_idx"
  ON "public"."praemien_wave_pillar_tier_conditions" ("tier_id", "is_deleted");
CREATE INDEX IF NOT EXISTS "praemien_wave_pillar_tier_conditions_metric_idx"
  ON "public"."praemien_wave_pillar_tier_conditions" ("metric_id", "is_deleted");
--> statement-breakpoint

ALTER TABLE "public"."praemien_wave_flex_scores"
  ADD COLUMN IF NOT EXISTS "component_values" jsonb DEFAULT '{}'::jsonb NOT NULL;
--> statement-breakpoint

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'praemien_wave_pillars_reward_eur_ck'
      AND conrelid = 'public.praemien_wave_pillars'::regclass
  ) THEN
    ALTER TABLE "public"."praemien_wave_pillars"
      ADD CONSTRAINT "praemien_wave_pillars_reward_eur_ck"
      CHECK ("reward_eur" >= 0);
  END IF;
END $$;
--> statement-breakpoint

CREATE TABLE IF NOT EXISTS "public"."praemien_gm_wave_pillar_totals" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
  "wave_id" uuid NOT NULL,
  "gm_user_id" uuid NOT NULL,
  "pillar_id" uuid NOT NULL,
  "points" numeric(14, 4) DEFAULT '0' NOT NULL,
  "target_points" numeric(14, 4),
  "reward_eur" numeric(12, 2) DEFAULT '0' NOT NULL,
  "earned_reward_eur" numeric(12, 2) DEFAULT '0' NOT NULL,
  "goal_achieved" boolean DEFAULT false NOT NULL,
  "metric_values" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "achieved_tier_ids" jsonb DEFAULT '[]'::jsonb NOT NULL,
  "achieved_tier_labels" jsonb DEFAULT '[]'::jsonb NOT NULL,
  "next_tier_id" uuid,
  "next_tier_label" text,
  "calculated_at" timestamp with time zone DEFAULT now() NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  CONSTRAINT "praemien_gm_wave_pillar_totals_points_ck" CHECK ("points" >= 0),
  CONSTRAINT "praemien_gm_wave_pillar_totals_target_points_ck" CHECK ("target_points" IS NULL OR "target_points" > 0),
  CONSTRAINT "praemien_gm_wave_pillar_totals_reward_eur_ck" CHECK ("reward_eur" >= 0),
  CONSTRAINT "praemien_gm_wave_pillar_totals_earned_reward_eur_ck" CHECK ("earned_reward_eur" >= 0)
);
--> statement-breakpoint

ALTER TABLE "public"."praemien_gm_wave_pillar_totals"
  ADD COLUMN IF NOT EXISTS "metric_values" jsonb DEFAULT '{}'::jsonb NOT NULL,
  ADD COLUMN IF NOT EXISTS "achieved_tier_ids" jsonb DEFAULT '[]'::jsonb NOT NULL,
  ADD COLUMN IF NOT EXISTS "achieved_tier_labels" jsonb DEFAULT '[]'::jsonb NOT NULL,
  ADD COLUMN IF NOT EXISTS "next_tier_id" uuid,
  ADD COLUMN IF NOT EXISTS "next_tier_label" text;
--> statement-breakpoint

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'praemien_gm_wave_pillar_totals_wave_id_praemien_waves_id_fk'
      AND conrelid = 'public.praemien_gm_wave_pillar_totals'::regclass
  ) THEN
    ALTER TABLE "public"."praemien_gm_wave_pillar_totals"
      ADD CONSTRAINT "praemien_gm_wave_pillar_totals_wave_id_praemien_waves_id_fk"
      FOREIGN KEY ("wave_id") REFERENCES "public"."praemien_waves"("id") ON DELETE CASCADE;
  END IF;
END $$;
--> statement-breakpoint

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'praemien_gm_wave_pillar_totals_next_tier_id_praemien_wave_pillar_tiers_id_fk'
      AND conrelid = 'public.praemien_gm_wave_pillar_totals'::regclass
  ) THEN
    ALTER TABLE "public"."praemien_gm_wave_pillar_totals"
      ADD CONSTRAINT "praemien_gm_wave_pillar_totals_next_tier_id_praemien_wave_pillar_tiers_id_fk"
      FOREIGN KEY ("next_tier_id") REFERENCES "public"."praemien_wave_pillar_tiers"("id") ON DELETE SET NULL;
  END IF;
END $$;
--> statement-breakpoint

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'praemien_gm_wave_pillar_totals_gm_user_id_users_id_fk'
      AND conrelid = 'public.praemien_gm_wave_pillar_totals'::regclass
  ) THEN
    ALTER TABLE "public"."praemien_gm_wave_pillar_totals"
      ADD CONSTRAINT "praemien_gm_wave_pillar_totals_gm_user_id_users_id_fk"
      FOREIGN KEY ("gm_user_id") REFERENCES "public"."users"("id") ON DELETE CASCADE;
  END IF;
END $$;
--> statement-breakpoint

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'praemien_gm_wave_pillar_totals_pillar_id_praemien_wave_pillars_id_fk'
      AND conrelid = 'public.praemien_gm_wave_pillar_totals'::regclass
  ) THEN
    ALTER TABLE "public"."praemien_gm_wave_pillar_totals"
      ADD CONSTRAINT "praemien_gm_wave_pillar_totals_pillar_id_praemien_wave_pillars_id_fk"
      FOREIGN KEY ("pillar_id") REFERENCES "public"."praemien_wave_pillars"("id") ON DELETE CASCADE;
  END IF;
END $$;
--> statement-breakpoint

CREATE UNIQUE INDEX IF NOT EXISTS "praemien_gm_wave_pillar_totals_wave_gm_pillar_unique"
  ON "public"."praemien_gm_wave_pillar_totals" ("wave_id", "gm_user_id", "pillar_id");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "praemien_gm_wave_pillar_totals_wave_gm_idx"
  ON "public"."praemien_gm_wave_pillar_totals" ("wave_id", "gm_user_id");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "praemien_gm_wave_pillar_totals_gm_wave_idx"
  ON "public"."praemien_gm_wave_pillar_totals" ("gm_user_id", "wave_id");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "praemien_gm_wave_pillar_totals_pillar_gm_idx"
  ON "public"."praemien_gm_wave_pillar_totals" ("pillar_id", "gm_user_id");
CREATE INDEX IF NOT EXISTS "praemien_gm_wave_pillar_totals_next_tier_idx"
  ON "public"."praemien_gm_wave_pillar_totals" ("next_tier_id");
--> statement-breakpoint

ALTER TABLE "public"."praemien_gm_wave_pillar_totals" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "public"."praemien_gm_wave_pillar_totals" FORCE ROW LEVEL SECURITY;

REVOKE ALL PRIVILEGES ON TABLE "public"."praemien_gm_wave_pillar_totals" FROM PUBLIC, anon, authenticated;
GRANT ALL PRIVILEGES ON TABLE "public"."praemien_gm_wave_pillar_totals" TO service_role;

ALTER TABLE "public"."praemien_wave_pillar_metrics" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "public"."praemien_wave_pillar_metrics" FORCE ROW LEVEL SECURITY;
ALTER TABLE "public"."praemien_wave_pillar_tiers" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "public"."praemien_wave_pillar_tiers" FORCE ROW LEVEL SECURITY;
ALTER TABLE "public"."praemien_wave_pillar_tier_conditions" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "public"."praemien_wave_pillar_tier_conditions" FORCE ROW LEVEL SECURITY;
REVOKE ALL PRIVILEGES ON TABLE "public"."praemien_wave_pillar_metrics" FROM PUBLIC, anon, authenticated;
REVOKE ALL PRIVILEGES ON TABLE "public"."praemien_wave_pillar_tiers" FROM PUBLIC, anon, authenticated;
REVOKE ALL PRIVILEGES ON TABLE "public"."praemien_wave_pillar_tier_conditions" FROM PUBLIC, anon, authenticated;
GRANT ALL PRIVILEGES ON TABLE "public"."praemien_wave_pillar_metrics" TO service_role;
GRANT ALL PRIVILEGES ON TABLE "public"."praemien_wave_pillar_tiers" TO service_role;
GRANT ALL PRIVILEGES ON TABLE "public"."praemien_wave_pillar_tier_conditions" TO service_role;
