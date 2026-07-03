CREATE TABLE IF NOT EXISTS "special_arthur_filter" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
  "gm_user_id" uuid NOT NULL REFERENCES "users"("id") ON DELETE CASCADE,
  "match_value" text NOT NULL,
  "created_by_user_id" uuid REFERENCES "users"("id") ON DELETE SET NULL,
  "is_deleted" boolean DEFAULT false NOT NULL,
  "deleted_at" timestamptz,
  "created_at" timestamptz DEFAULT now() NOT NULL,
  "updated_at" timestamptz DEFAULT now() NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS "special_arthur_filter_gm_value_active_unique"
  ON "special_arthur_filter" ("gm_user_id", "match_value")
  WHERE "is_deleted" = false;

CREATE INDEX IF NOT EXISTS "special_arthur_filter_gm_active_idx"
  ON "special_arthur_filter" ("gm_user_id", "is_deleted");

CREATE INDEX IF NOT EXISTS "special_arthur_filter_value_idx"
  ON "special_arthur_filter" ("match_value");

REVOKE ALL ON TABLE public.special_arthur_filter FROM PUBLIC;
REVOKE ALL ON TABLE public.special_arthur_filter FROM anon;
REVOKE ALL ON TABLE public.special_arthur_filter FROM authenticated;
GRANT ALL ON TABLE public.special_arthur_filter TO service_role;
ALTER TABLE public.special_arthur_filter ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.special_arthur_filter FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS special_arthur_filter_service_role_full ON public.special_arthur_filter;
CREATE POLICY special_arthur_filter_service_role_full
  ON public.special_arthur_filter
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);
