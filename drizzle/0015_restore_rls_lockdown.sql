GRANT USAGE ON SCHEMA public TO authenticated;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO authenticated;
REVOKE USAGE ON SCHEMA public FROM anon;

DO $$
DECLARE
  t text;
  p record;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'users',
    'auth_audit_logs',
    'markets',
    'photo_tags',
    'question_bank_shared',
    'question_rules',
    'question_rule_targets',
    'question_scoring',
    'question_matrix',
    'question_attachments',
    'question_photo_tags',
    'module_main',
    'module_main_question',
    'fragebogen_main',
    'fragebogen_main_module',
    'fragebogen_main_spezial_question',
    'fragebogen_main_spezial_items',
    'module_kuehler',
    'module_kuehler_question',
    'fragebogen_kuehler',
    'fragebogen_kuehler_module',
    'module_mhd',
    'module_mhd_question',
    'fragebogen_mhd',
    'fragebogen_mhd_module',
    'campaigns',
    'campaign_market_assignments',
    'campaign_fragebogen_history',
    'campaign_market_assignment_history'
  ]
  LOOP
    EXECUTE format('REVOKE ALL ON TABLE public.%I FROM PUBLIC, anon, authenticated', t);
    EXECUTE format('GRANT ALL ON TABLE public.%I TO authenticated', t);
    EXECUTE format('GRANT ALL ON TABLE public.%I TO service_role', t);
    EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('ALTER TABLE public.%I FORCE ROW LEVEL SECURITY', t);

    FOR p IN
      SELECT policyname
      FROM pg_policies
      WHERE schemaname = 'public'
        AND tablename = t
    LOOP
      EXECUTE format('DROP POLICY IF EXISTS %I ON public.%I', p.policyname, t);
    END LOOP;

    EXECUTE format('CREATE POLICY auth_full_%I ON public.%I FOR ALL TO authenticated USING (true) WITH CHECK (true)', t, t);
    EXECUTE format('CREATE POLICY svc_full_%I ON public.%I FOR ALL TO service_role USING (true) WITH CHECK (true)', t, t);
  END LOOP;
END $$;
