CREATE TABLE IF NOT EXISTS "admin_kurti_window_preferences" (
  "admin_user_id" uuid PRIMARY KEY REFERENCES "users"("id") ON DELETE CASCADE,
  "panel_x" integer NOT NULL,
  "panel_y" integer NOT NULL,
  "panel_width" integer NOT NULL,
  "panel_height" integer NOT NULL,
  "bubble_x" integer NOT NULL,
  "bubble_y" integer NOT NULL,
  "bubble_dismissed" boolean DEFAULT false NOT NULL,
  "created_at" timestamptz DEFAULT now() NOT NULL,
  "updated_at" timestamptz DEFAULT now() NOT NULL,
  CONSTRAINT "admin_kurti_window_preferences_panel_position_ck"
    CHECK ("panel_x" BETWEEN 0 AND 100000 AND "panel_y" BETWEEN 0 AND 100000),
  CONSTRAINT "admin_kurti_window_preferences_panel_size_ck"
    CHECK ("panel_width" BETWEEN 280 AND 10000 AND "panel_height" BETWEEN 300 AND 10000),
  CONSTRAINT "admin_kurti_window_preferences_bubble_position_ck"
    CHECK ("bubble_x" BETWEEN 0 AND 100000 AND "bubble_y" BETWEEN 0 AND 100000)
);

REVOKE ALL ON TABLE public.admin_kurti_window_preferences FROM PUBLIC;
REVOKE ALL ON TABLE public.admin_kurti_window_preferences FROM anon;
REVOKE ALL ON TABLE public.admin_kurti_window_preferences FROM authenticated;
GRANT ALL ON TABLE public.admin_kurti_window_preferences TO service_role;
ALTER TABLE public.admin_kurti_window_preferences ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.admin_kurti_window_preferences FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS admin_kurti_window_preferences_service_role_full ON public.admin_kurti_window_preferences;
CREATE POLICY admin_kurti_window_preferences_service_role_full
  ON public.admin_kurti_window_preferences
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);
