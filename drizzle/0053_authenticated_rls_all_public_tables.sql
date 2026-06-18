-- Lock every current public app table behind non-recursive authenticated RLS.
--
-- This intentionally does not inspect app roles or join public.users from RLS
-- policies. Application permissions stay in the Express backend; Supabase RLS
-- acts as a broad Data API guardrail: anon receives no table access,
-- authenticated receives row access, and server-side service/postgres roles keep
-- their normal bypass path.

REVOKE USAGE ON SCHEMA public FROM PUBLIC;
REVOKE USAGE ON SCHEMA public FROM anon;
GRANT USAGE ON SCHEMA public TO authenticated;
GRANT USAGE ON SCHEMA public TO service_role;
GRANT USAGE ON SCHEMA public TO postgres;

REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM PUBLIC, anon, authenticated;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO authenticated;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
  REVOKE ALL ON TABLES FROM PUBLIC, anon, authenticated;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO authenticated;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT ALL ON TABLES TO service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  REVOKE ALL ON SEQUENCES FROM PUBLIC, anon, authenticated;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO authenticated;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT ALL ON SEQUENCES TO service_role;

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

    EXECUTE format('REVOKE ALL PRIVILEGES ON TABLE %s FROM PUBLIC, anon, authenticated', table_ident);
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE %s TO authenticated', table_ident);
    EXECUTE format('GRANT ALL PRIVILEGES ON TABLE %s TO service_role', table_ident);

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

    EXECUTE format(
      'CREATE POLICY %I ON %s FOR ALL TO authenticated USING (true) WITH CHECK (true)',
      'authenticated_full_access',
      table_ident
    );
  END LOOP;
END $$;
