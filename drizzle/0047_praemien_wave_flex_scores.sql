CREATE TABLE IF NOT EXISTS "praemien_wave_flex_scores" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
  "wave_id" uuid NOT NULL,
  "gm_user_id" uuid NOT NULL,
  "total_points" integer DEFAULT 0 NOT NULL,
  "note" text,
  "is_deleted" boolean DEFAULT false NOT NULL,
  "deleted_at" timestamp with time zone,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  CONSTRAINT "praemien_wave_flex_scores_total_points_ck" CHECK ("praemien_wave_flex_scores"."total_points" between 0 and 100)
);

DO $$ BEGIN
 ALTER TABLE "praemien_wave_flex_scores" ADD CONSTRAINT "praemien_wave_flex_scores_wave_id_praemien_waves_id_fk" FOREIGN KEY ("wave_id") REFERENCES "public"."praemien_waves"("id") ON DELETE cascade ON UPDATE no action;
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
 ALTER TABLE "praemien_wave_flex_scores" ADD CONSTRAINT "praemien_wave_flex_scores_gm_user_id_users_id_fk" FOREIGN KEY ("gm_user_id") REFERENCES "public"."users"("id") ON DELETE cascade ON UPDATE no action;
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS "praemien_wave_flex_scores_wave_gm_active_unique"
  ON "praemien_wave_flex_scores" USING btree ("wave_id","gm_user_id")
  WHERE "praemien_wave_flex_scores"."is_deleted" = false;

CREATE INDEX IF NOT EXISTS "praemien_wave_flex_scores_wave_idx"
  ON "praemien_wave_flex_scores" USING btree ("wave_id","is_deleted");
