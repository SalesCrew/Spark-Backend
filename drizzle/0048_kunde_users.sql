ALTER TYPE "user_role" ADD VALUE IF NOT EXISTS 'kunde';

CREATE TABLE IF NOT EXISTS "kunde_users" (
  "user_id" uuid PRIMARY KEY NOT NULL REFERENCES "users"("id") ON DELETE cascade,
  "page_permissions" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "created_by_user_id" uuid REFERENCES "users"("id") ON DELETE set null,
  "is_deleted" boolean DEFAULT false NOT NULL,
  "deleted_at" timestamp with time zone,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);

CREATE INDEX IF NOT EXISTS "kunde_users_created_by_idx" ON "kunde_users" ("created_by_user_id");
CREATE INDEX IF NOT EXISTS "kunde_users_deleted_idx" ON "kunde_users" ("is_deleted");

REVOKE ALL ON TABLE public.kunde_users FROM anon;
REVOKE ALL ON TABLE public.kunde_users FROM authenticated;
GRANT ALL ON TABLE public.kunde_users TO authenticated;

ALTER TABLE public.kunde_users ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS auth_full_kunde_users ON public.kunde_users;
CREATE POLICY auth_full_kunde_users
  ON public.kunde_users
  FOR ALL
  TO authenticated
  USING (true)
  WITH CHECK (true);

DROP POLICY IF EXISTS svc_full_kunde_users ON public.kunde_users;
CREATE POLICY svc_full_kunde_users
  ON public.kunde_users
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);
