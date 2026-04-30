DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'fragebogen_scope') THEN
    CREATE TYPE fragebogen_scope AS ENUM ('main', 'kuehler', 'mhd');
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS module_question_chains (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  scope fragebogen_scope NOT NULL,
  module_id uuid NOT NULL,
  question_id uuid NOT NULL REFERENCES question_bank_shared(id) ON DELETE CASCADE,
  chain_db_name text NOT NULL,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS module_question_chains_scope_module_question_chain_active_unique
  ON module_question_chains(scope, module_id, question_id, chain_db_name)
  WHERE is_deleted = false;

CREATE INDEX IF NOT EXISTS module_question_chains_scope_module_question_deleted_idx
  ON module_question_chains(scope, module_id, question_id, is_deleted);

INSERT INTO module_question_chains (
  scope,
  module_id,
  question_id,
  chain_db_name,
  is_deleted,
  deleted_at,
  created_at,
  updated_at
)
SELECT
  source.scope::fragebogen_scope,
  source.module_id,
  source.question_id,
  source.chain_db_name,
  false,
  null,
  now(),
  now()
FROM (
  SELECT
    'main' AS scope,
    mmq.module_id,
    mmq.question_id,
    btrim(unnest(coalesce(q.chains, '{}'::text[]))) AS chain_db_name
  FROM module_main_question mmq
  INNER JOIN question_bank_shared q ON q.id = mmq.question_id
  WHERE mmq.is_deleted = false
    AND q.is_deleted = false

  UNION ALL

  SELECT
    'kuehler' AS scope,
    mkq.module_id,
    mkq.question_id,
    btrim(unnest(coalesce(q.chains, '{}'::text[]))) AS chain_db_name
  FROM module_kuehler_question mkq
  INNER JOIN question_bank_shared q ON q.id = mkq.question_id
  WHERE mkq.is_deleted = false
    AND q.is_deleted = false

  UNION ALL

  SELECT
    'mhd' AS scope,
    mmhq.module_id,
    mmhq.question_id,
    btrim(unnest(coalesce(q.chains, '{}'::text[]))) AS chain_db_name
  FROM module_mhd_question mmhq
  INNER JOIN question_bank_shared q ON q.id = mmhq.question_id
  WHERE mmhq.is_deleted = false
    AND q.is_deleted = false
) AS source
WHERE source.chain_db_name <> ''
ON CONFLICT (scope, module_id, question_id, chain_db_name)
DO UPDATE
SET
  is_deleted = false,
  deleted_at = null,
  updated_at = now();

REVOKE ALL ON TABLE public.module_question_chains FROM PUBLIC, anon, authenticated;
GRANT ALL ON TABLE public.module_question_chains TO authenticated;
GRANT ALL ON TABLE public.module_question_chains TO service_role;
ALTER TABLE public.module_question_chains ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.module_question_chains FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS auth_full_module_question_chains ON public.module_question_chains;
DROP POLICY IF EXISTS svc_full_module_question_chains ON public.module_question_chains;
CREATE POLICY auth_full_module_question_chains ON public.module_question_chains FOR ALL TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY svc_full_module_question_chains ON public.module_question_chains FOR ALL TO service_role USING (true) WITH CHECK (true);
