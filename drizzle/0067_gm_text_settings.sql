CREATE TABLE IF NOT EXISTS "gm_text_settings" (
  "user_id" uuid PRIMARY KEY REFERENCES "users"("id") ON DELETE CASCADE,
  "text_scale_percent" integer DEFAULT 0 NOT NULL,
  "is_deleted" boolean DEFAULT false NOT NULL,
  "deleted_at" timestamptz,
  "created_at" timestamptz DEFAULT now() NOT NULL,
  "updated_at" timestamptz DEFAULT now() NOT NULL,
  CONSTRAINT "gm_text_settings_percent_range_ck" CHECK ("text_scale_percent" >= 0 AND "text_scale_percent" <= 50)
);

CREATE INDEX IF NOT EXISTS "gm_text_settings_deleted_idx"
  ON "gm_text_settings" ("is_deleted");

REVOKE ALL ON TABLE public.gm_text_settings FROM PUBLIC;
REVOKE ALL ON TABLE public.gm_text_settings FROM anon;
REVOKE ALL ON TABLE public.gm_text_settings FROM authenticated;
GRANT ALL ON TABLE public.gm_text_settings TO service_role;
ALTER TABLE public.gm_text_settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.gm_text_settings FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS gm_text_settings_service_role_full ON public.gm_text_settings;
CREATE POLICY gm_text_settings_service_role_full
  ON public.gm_text_settings
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);
