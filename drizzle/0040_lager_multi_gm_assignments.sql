CREATE TABLE IF NOT EXISTS "lager_gm_assignments" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
  "lager_id" uuid NOT NULL,
  "gm_user_id" uuid NOT NULL,
  "is_deleted" boolean DEFAULT false NOT NULL,
  "deleted_at" timestamp with time zone,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "lager_gm_assignments"
  ADD CONSTRAINT "lager_gm_assignments_lager_id_lager_id_fk"
  FOREIGN KEY ("lager_id") REFERENCES "public"."lager"("id") ON DELETE cascade ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "lager_gm_assignments"
  ADD CONSTRAINT "lager_gm_assignments_gm_user_id_users_id_fk"
  FOREIGN KEY ("gm_user_id") REFERENCES "public"."users"("id") ON DELETE cascade ON UPDATE no action;
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "lager_gm_assignments_lager_gm_active_unique"
  ON "lager_gm_assignments" USING btree ("lager_id", "gm_user_id")
  WHERE "is_deleted" = false;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "lager_gm_assignments_lager_deleted_idx"
  ON "lager_gm_assignments" USING btree ("lager_id", "is_deleted");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "lager_gm_assignments_gm_deleted_idx"
  ON "lager_gm_assignments" USING btree ("gm_user_id", "is_deleted");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "lager_gm_assignments_deleted_idx"
  ON "lager_gm_assignments" USING btree ("is_deleted");
--> statement-breakpoint
INSERT INTO "lager_gm_assignments" (
  "id",
  "lager_id",
  "gm_user_id",
  "is_deleted",
  "deleted_at",
  "created_at",
  "updated_at"
)
SELECT
  gen_random_uuid(),
  l."id",
  l."gm_user_id",
  false,
  null,
  now(),
  now()
FROM "lager" l
WHERE
  l."is_deleted" = false
  AND l."gm_user_id" IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM "lager_gm_assignments" a
    WHERE
      a."lager_id" = l."id"
      AND a."gm_user_id" = l."gm_user_id"
      AND a."is_deleted" = false
  );
