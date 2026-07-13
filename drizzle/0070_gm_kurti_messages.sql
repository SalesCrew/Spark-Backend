CREATE TABLE IF NOT EXISTS "gm_kurti_messages" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
  "gm_user_id" uuid NOT NULL REFERENCES "users"("id") ON DELETE CASCADE,
  "role" text NOT NULL,
  "content" text NOT NULL,
  "expires_at" timestamptz NOT NULL,
  "created_at" timestamptz DEFAULT now() NOT NULL,
  CONSTRAINT "gm_kurti_messages_role_ck" CHECK ("role" IN ('user', 'assistant')),
  CONSTRAINT "gm_kurti_messages_content_length_ck" CHECK (char_length("content") BETWEEN 1 AND 12000)
);

CREATE INDEX IF NOT EXISTS "gm_kurti_messages_gm_created_idx"
  ON "gm_kurti_messages" ("gm_user_id", "created_at");

CREATE INDEX IF NOT EXISTS "gm_kurti_messages_expires_idx"
  ON "gm_kurti_messages" ("expires_at");

REVOKE ALL ON TABLE public.gm_kurti_messages FROM PUBLIC;
REVOKE ALL ON TABLE public.gm_kurti_messages FROM anon;
REVOKE ALL ON TABLE public.gm_kurti_messages FROM authenticated;
GRANT ALL ON TABLE public.gm_kurti_messages TO service_role;
ALTER TABLE public.gm_kurti_messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.gm_kurti_messages FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS gm_kurti_messages_service_role_full ON public.gm_kurti_messages;
CREATE POLICY gm_kurti_messages_service_role_full
  ON public.gm_kurti_messages
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);
