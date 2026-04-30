DROP INDEX IF EXISTS "praemien_waves_year_quarter_active_unique";
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "praemien_waves_year_quarter_active_unique"
ON "praemien_waves" USING btree ("year","quarter")
WHERE "praemien_waves"."is_deleted" = false AND "praemien_waves"."status" = 'active';
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "question_scoring_boni_active_idx"
ON "question_scoring" USING btree ("question_id","boni")
WHERE "question_scoring"."is_deleted" = false AND "question_scoring"."boni" IS NOT NULL;
