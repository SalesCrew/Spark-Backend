DO $$
BEGIN
  CREATE TYPE "dsar_request_type" AS ENUM (
    'access',
    'rectification',
    'erasure',
    'restriction',
    'portability',
    'objection',
    'mixed'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  CREATE TYPE "dsar_request_status" AS ENUM (
    'open',
    'identity_check',
    'collecting',
    'decision',
    'responded',
    'closed',
    'cancelled'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS "dsar_requests" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
  "request_type" "dsar_request_type" NOT NULL,
  "status" "dsar_request_status" DEFAULT 'open' NOT NULL,
  "intake_channel" text DEFAULT 'email' NOT NULL,
  "requester_name" text NOT NULL,
  "requester_email" text NOT NULL,
  "requester_user_id" uuid REFERENCES "users"("id") ON DELETE SET NULL,
  "subject_user_id" uuid REFERENCES "users"("id") ON DELETE SET NULL,
  "subject_name_snapshot" text NOT NULL,
  "subject_email_snapshot" text NOT NULL,
  "subject_role_snapshot" text,
  "request_summary" text DEFAULT '' NOT NULL,
  "assigned_to_user_id" uuid REFERENCES "users"("id") ON DELETE SET NULL,
  "received_at" timestamptz DEFAULT now() NOT NULL,
  "due_at" timestamptz NOT NULL,
  "extended_until" timestamptz,
  "extension_reason" text,
  "identity_verified_at" timestamptz,
  "identity_verified_by_user_id" uuid REFERENCES "users"("id") ON DELETE SET NULL,
  "decision_summary" text,
  "legal_blockers" text,
  "response_channel" text,
  "response_sent_at" timestamptz,
  "response_sent_by_user_id" uuid REFERENCES "users"("id") ON DELETE SET NULL,
  "export_package_summary" jsonb,
  "is_deleted" boolean DEFAULT false NOT NULL,
  "deleted_at" timestamptz,
  "created_by_user_id" uuid REFERENCES "users"("id") ON DELETE SET NULL,
  "created_at" timestamptz DEFAULT now() NOT NULL,
  "updated_at" timestamptz DEFAULT now() NOT NULL
);

CREATE TABLE IF NOT EXISTS "dsar_request_events" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
  "request_id" uuid NOT NULL REFERENCES "dsar_requests"("id") ON DELETE CASCADE,
  "actor_user_id" uuid REFERENCES "users"("id") ON DELETE SET NULL,
  "event_type" text NOT NULL,
  "message" text DEFAULT '' NOT NULL,
  "metadata" jsonb,
  "created_at" timestamptz DEFAULT now() NOT NULL
);

CREATE INDEX IF NOT EXISTS "dsar_requests_status_due_idx" ON "dsar_requests" ("status", "due_at");
CREATE INDEX IF NOT EXISTS "dsar_requests_subject_user_idx" ON "dsar_requests" ("subject_user_id");
CREATE INDEX IF NOT EXISTS "dsar_requests_requester_user_idx" ON "dsar_requests" ("requester_user_id");
CREATE INDEX IF NOT EXISTS "dsar_requests_received_at_idx" ON "dsar_requests" ("received_at");
CREATE INDEX IF NOT EXISTS "dsar_requests_deleted_idx" ON "dsar_requests" ("is_deleted");
CREATE INDEX IF NOT EXISTS "dsar_request_events_request_idx" ON "dsar_request_events" ("request_id", "created_at");
CREATE INDEX IF NOT EXISTS "dsar_request_events_actor_idx" ON "dsar_request_events" ("actor_user_id");

REVOKE ALL ON TABLE public.dsar_requests FROM PUBLIC;
REVOKE ALL ON TABLE public.dsar_requests FROM anon;
REVOKE ALL ON TABLE public.dsar_requests FROM authenticated;
GRANT ALL ON TABLE public.dsar_requests TO service_role;
ALTER TABLE public.dsar_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.dsar_requests FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS dsar_requests_service_role_full ON public.dsar_requests;
CREATE POLICY dsar_requests_service_role_full
  ON public.dsar_requests
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

REVOKE ALL ON TABLE public.dsar_request_events FROM PUBLIC;
REVOKE ALL ON TABLE public.dsar_request_events FROM anon;
REVOKE ALL ON TABLE public.dsar_request_events FROM authenticated;
GRANT ALL ON TABLE public.dsar_request_events TO service_role;
ALTER TABLE public.dsar_request_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.dsar_request_events FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS dsar_request_events_service_role_full ON public.dsar_request_events;
CREATE POLICY dsar_request_events_service_role_full
  ON public.dsar_request_events
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);
