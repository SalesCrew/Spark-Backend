-- Lock down public table access for client roles
REVOKE ALL ON TABLE public.users FROM anon, authenticated;
REVOKE ALL ON TABLE public.auth_audit_logs FROM anon, authenticated;

-- Enforce RLS
ALTER TABLE public.users ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.auth_audit_logs ENABLE ROW LEVEL SECURITY;

-- Allow only service_role access through Supabase
DROP POLICY IF EXISTS users_service_role_full ON public.users;
CREATE POLICY users_service_role_full
  ON public.users
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

DROP POLICY IF EXISTS auth_audit_logs_service_role_full ON public.auth_audit_logs;
CREATE POLICY auth_audit_logs_service_role_full
  ON public.auth_audit_logs
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);
