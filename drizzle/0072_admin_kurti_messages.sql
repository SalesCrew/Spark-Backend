CREATE TABLE IF NOT EXISTS "admin_kurti_messages" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
  "admin_user_id" uuid NOT NULL REFERENCES "users"("id") ON DELETE CASCADE,
  "role" text NOT NULL,
  "content" text NOT NULL,
  "expires_at" timestamptz NOT NULL,
  "created_at" timestamptz DEFAULT now() NOT NULL,
  CONSTRAINT "admin_kurti_messages_role_ck" CHECK ("role" IN ('user', 'assistant')),
  CONSTRAINT "admin_kurti_messages_content_length_ck" CHECK (char_length("content") BETWEEN 1 AND 60000)
);

CREATE INDEX IF NOT EXISTS "admin_kurti_messages_admin_created_idx"
  ON "admin_kurti_messages" ("admin_user_id", "created_at");

CREATE INDEX IF NOT EXISTS "admin_kurti_messages_expires_idx"
  ON "admin_kurti_messages" ("expires_at");

REVOKE ALL ON TABLE public.admin_kurti_messages FROM PUBLIC;
REVOKE ALL ON TABLE public.admin_kurti_messages FROM anon;
REVOKE ALL ON TABLE public.admin_kurti_messages FROM authenticated;
GRANT ALL ON TABLE public.admin_kurti_messages TO service_role;
ALTER TABLE public.admin_kurti_messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.admin_kurti_messages FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS admin_kurti_messages_service_role_full ON public.admin_kurti_messages;
CREATE POLICY admin_kurti_messages_service_role_full
  ON public.admin_kurti_messages
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);
