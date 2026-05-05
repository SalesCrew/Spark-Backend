CREATE TABLE IF NOT EXISTS "question_answer_history" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
  "question_id" uuid,
  "fragebogen_id" uuid,
  "spezial_item_id" uuid,
  "change_kind" text NOT NULL,
  "previous_question_type" "question_type",
  "next_question_type" "question_type",
  "previous_answer_state" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "next_answer_state" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "changed_at" timestamp with time zone DEFAULT now() NOT NULL,
  "is_deleted" boolean DEFAULT false NOT NULL,
  "deleted_at" timestamp with time zone,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);

DO $$ BEGIN
 ALTER TABLE "question_answer_history" ADD CONSTRAINT "question_answer_history_question_id_question_bank_shared_id_fk"
 FOREIGN KEY ("question_id") REFERENCES "public"."question_bank_shared"("id") ON DELETE set null ON UPDATE no action;
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
 ALTER TABLE "question_answer_history" ADD CONSTRAINT "question_answer_history_fragebogen_id_fragebogen_main_id_fk"
 FOREIGN KEY ("fragebogen_id") REFERENCES "public"."fragebogen_main"("id") ON DELETE set null ON UPDATE no action;
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
 ALTER TABLE "question_answer_history" ADD CONSTRAINT "question_answer_history_spezial_item_id_fragebogen_main_spezial_items_id_fk"
 FOREIGN KEY ("spezial_item_id") REFERENCES "public"."fragebogen_main_spezial_items"("id") ON DELETE set null ON UPDATE no action;
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
 ALTER TABLE "question_answer_history" ADD CONSTRAINT "question_answer_history_change_kind_ck"
 CHECK ("question_answer_history"."change_kind" in ('type_change','answer_edit'));
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;

CREATE INDEX IF NOT EXISTS "question_answer_history_question_changed_idx"
  ON "question_answer_history" ("question_id","changed_at");
CREATE INDEX IF NOT EXISTS "question_answer_history_fragebogen_changed_idx"
  ON "question_answer_history" ("fragebogen_id","changed_at");
CREATE INDEX IF NOT EXISTS "question_answer_history_spezial_changed_idx"
  ON "question_answer_history" ("spezial_item_id","changed_at");
CREATE INDEX IF NOT EXISTS "question_answer_history_deleted_idx"
  ON "question_answer_history" ("is_deleted");
