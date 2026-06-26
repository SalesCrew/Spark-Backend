-- Defense-in-depth for future migrations.
--
-- Existing tables are locked by 0056_lock_public_tables_to_backend.sql.
-- This trigger keeps future public tables aligned with the same rule:
-- every app table in the exposed public schema gets RLS + FORCE RLS by
-- default. It does not grant anon/authenticated access.

CREATE SCHEMA IF NOT EXISTS internal_security;

REVOKE ALL ON SCHEMA internal_security FROM PUBLIC;
REVOKE ALL ON SCHEMA internal_security FROM anon;
REVOKE ALL ON SCHEMA internal_security FROM authenticated;
GRANT USAGE ON SCHEMA internal_security TO postgres;
GRANT USAGE ON SCHEMA internal_security TO service_role;

CREATE OR REPLACE FUNCTION internal_security.force_public_table_rls()
RETURNS event_trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  command record;
BEGIN
  FOR command IN
    SELECT *
    FROM pg_event_trigger_ddl_commands()
    WHERE command_tag IN ('CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO')
      AND object_type IN ('table', 'partitioned table')
  LOOP
    IF command.schema_name = 'public' THEN
      EXECUTE format('ALTER TABLE IF EXISTS %s ENABLE ROW LEVEL SECURITY', command.object_identity);
      EXECUTE format('ALTER TABLE IF EXISTS %s FORCE ROW LEVEL SECURITY', command.object_identity);
    END IF;
  END LOOP;
END;
$$;

REVOKE ALL ON FUNCTION internal_security.force_public_table_rls() FROM PUBLIC;
REVOKE ALL ON FUNCTION internal_security.force_public_table_rls() FROM anon;
REVOKE ALL ON FUNCTION internal_security.force_public_table_rls() FROM authenticated;
GRANT EXECUTE ON FUNCTION internal_security.force_public_table_rls() TO service_role;

DROP EVENT TRIGGER IF EXISTS coke_spark_force_public_table_rls;

CREATE EVENT TRIGGER coke_spark_force_public_table_rls
  ON ddl_command_end
  WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO')
  EXECUTE FUNCTION internal_security.force_public_table_rls();

