REVOKE ALL ON TABLE public.markets FROM anon, authenticated;
GRANT SELECT ON TABLE public.markets TO authenticated;

ALTER TABLE public.markets ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS markets_authenticated_select ON public.markets;
CREATE POLICY markets_authenticated_select
  ON public.markets
  FOR SELECT
  TO authenticated
  USING (true);

DROP POLICY IF EXISTS markets_service_role_full ON public.markets;
CREATE POLICY markets_service_role_full
  ON public.markets
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);
