-- GDPR / DSG data-boundary hardening.
--
-- Coke Spark's business data is authorized by the Express backend, not by
-- browser-side Supabase table access. Frontend Supabase usage is limited to
-- Auth flows and signed Storage URLs issued by the backend. Therefore public
-- app tables should not be reachable by anon/authenticated Data API roles.
--
-- This migration intentionally:
--   1. Enables and forces RLS on every current public app table.
--   2. Removes all anon/authenticated/public table and sequence grants.
--   3. Removes broad permissive public policies such as USING (true).
--   4. Sets future default privileges so new public tables are backend-only
--      unless a later migration explicitly designs safe direct-client access.

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM anon;
REVOKE CREATE ON SCHEMA public FROM authenticated;

REVOKE USAGE ON SCHEMA public FROM PUBLIC;
REVOKE USAGE ON SCHEMA public FROM anon;
REVOKE USAGE ON SCHEMA public FROM authenticated;

GRANT USAGE ON SCHEMA public TO postgres;
GRANT USAGE ON SCHEMA public TO service_role;

REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM PUBLIC, anon, authenticated;
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM PUBLIC, anon, authenticated;
REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC, anon, authenticated;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO service_role;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO service_role;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
  REVOKE ALL ON TABLES FROM PUBLIC, anon, authenticated;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  REVOKE ALL ON SEQUENCES FROM PUBLIC, anon, authenticated;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  REVOKE ALL ON FUNCTIONS FROM PUBLIC, anon, authenticated;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT ALL ON TABLES TO service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT ALL ON SEQUENCES TO service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT ALL ON FUNCTIONS TO service_role;

DO $$
DECLARE
  table_record record;
  policy_record record;
  table_ident text;
BEGIN
  FOR table_record IN
    SELECT n.nspname AS schema_name, c.relname AS table_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relkind IN ('r', 'p')
    ORDER BY c.relname
  LOOP
    table_ident := format('%I.%I', table_record.schema_name, table_record.table_name);

    EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', table_ident);
    EXECUTE format('ALTER TABLE %s FORCE ROW LEVEL SECURITY', table_ident);

    FOR policy_record IN
      SELECT policyname
      FROM pg_policies
      WHERE schemaname = table_record.schema_name
        AND tablename = table_record.table_name
    LOOP
      EXECUTE format('DROP POLICY IF EXISTS %I ON %s', policy_record.policyname, table_ident);
    END LOOP;
  END LOOP;
END $$;
