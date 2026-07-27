-- Durcharbeit is an isolated questionnaire/campaign scope. Existing question
-- rows remain in the legacy shared pool (pool_scope IS NULL); every
-- Durcharbeit question is stamped explicitly and may only be linked through
-- the Durcharbeit module/questionnaire tables.

ALTER TYPE public.fragebogen_section ADD VALUE IF NOT EXISTS 'durcharbeit';
ALTER TYPE public.campaign_section ADD VALUE IF NOT EXISTS 'durcharbeit';
ALTER TYPE public.fragebogen_scope ADD VALUE IF NOT EXISTS 'durcharbeit';

ALTER TABLE public.question_bank_shared
  ADD COLUMN IF NOT EXISTS pool_scope public.fragebogen_scope;

CREATE INDEX IF NOT EXISTS question_bank_shared_pool_scope_idx
  ON public.question_bank_shared(pool_scope, is_spezial, is_deleted);

COMMENT ON COLUMN public.question_bank_shared.pool_scope IS
  'NULL is the legacy pool shared by main/kuehler/mhd. durcharbeit is a strictly isolated question pool.';

CREATE TABLE IF NOT EXISTS public.module_durcharbeit (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  description text NOT NULL DEFAULT '',
  is_deleted boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.module_durcharbeit_question (
  module_id uuid NOT NULL REFERENCES public.module_durcharbeit(id) ON DELETE CASCADE,
  question_id uuid NOT NULL REFERENCES public.question_bank_shared(id) ON DELETE CASCADE,
  order_index integer NOT NULL DEFAULT 0,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (module_id, question_id)
);

CREATE INDEX IF NOT EXISTS module_durcharbeit_question_question_idx
  ON public.module_durcharbeit_question(question_id);
CREATE INDEX IF NOT EXISTS module_durcharbeit_question_deleted_idx
  ON public.module_durcharbeit_question(is_deleted);

CREATE TABLE IF NOT EXISTS public.fragebogen_durcharbeit (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  description text NOT NULL DEFAULT '',
  nur_einmal_ausfuellbar boolean NOT NULL DEFAULT false,
  status public.fragebogen_status NOT NULL DEFAULT 'inactive',
  schedule_type public.fragebogen_schedule_type NOT NULL DEFAULT 'always',
  start_date date,
  end_date date,
  is_deleted boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT fragebogen_durcharbeit_schedule_dates_ck CHECK (
    schedule_type = 'always'
    OR (
      start_date IS NOT NULL
      AND end_date IS NOT NULL
      AND start_date <= end_date
    )
  )
);

CREATE TABLE IF NOT EXISTS public.fragebogen_durcharbeit_module (
  fragebogen_id uuid NOT NULL REFERENCES public.fragebogen_durcharbeit(id) ON DELETE CASCADE,
  module_id uuid NOT NULL REFERENCES public.module_durcharbeit(id) ON DELETE CASCADE,
  order_index integer NOT NULL DEFAULT 0,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (fragebogen_id, module_id)
);

CREATE INDEX IF NOT EXISTS fragebogen_durcharbeit_module_module_idx
  ON public.fragebogen_durcharbeit_module(module_id);
CREATE INDEX IF NOT EXISTS fragebogen_durcharbeit_module_deleted_idx
  ON public.fragebogen_durcharbeit_module(is_deleted);

CREATE TABLE IF NOT EXISTS public.fragebogen_durcharbeit_spezial_question (
  fragebogen_id uuid NOT NULL REFERENCES public.fragebogen_durcharbeit(id) ON DELETE CASCADE,
  question_id uuid NOT NULL REFERENCES public.question_bank_shared(id) ON DELETE CASCADE,
  order_index integer NOT NULL DEFAULT 0,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (fragebogen_id, question_id)
);

CREATE INDEX IF NOT EXISTS fragebogen_durcharbeit_spezial_question_question_idx
  ON public.fragebogen_durcharbeit_spezial_question(question_id);
CREATE INDEX IF NOT EXISTS fragebogen_durcharbeit_spezial_question_active_order_idx
  ON public.fragebogen_durcharbeit_spezial_question(fragebogen_id, is_deleted, order_index);
CREATE INDEX IF NOT EXISTS fragebogen_durcharbeit_spezial_question_deleted_idx
  ON public.fragebogen_durcharbeit_spezial_question(is_deleted);

REVOKE ALL ON TABLE
  public.module_durcharbeit,
  public.module_durcharbeit_question,
  public.fragebogen_durcharbeit,
  public.fragebogen_durcharbeit_module,
  public.fragebogen_durcharbeit_spezial_question
FROM anon, authenticated;

GRANT SELECT ON TABLE
  public.module_durcharbeit,
  public.module_durcharbeit_question,
  public.fragebogen_durcharbeit,
  public.fragebogen_durcharbeit_module,
  public.fragebogen_durcharbeit_spezial_question
TO authenticated;

ALTER TABLE public.module_durcharbeit ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.module_durcharbeit_question ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.fragebogen_durcharbeit ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.fragebogen_durcharbeit_module ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.fragebogen_durcharbeit_spezial_question ENABLE ROW LEVEL SECURITY;

CREATE POLICY module_durcharbeit_authenticated_select
  ON public.module_durcharbeit
  FOR SELECT TO authenticated USING (true);
CREATE POLICY module_durcharbeit_service_role_full
  ON public.module_durcharbeit
  FOR ALL TO service_role USING (true) WITH CHECK (true);

CREATE POLICY module_durcharbeit_question_authenticated_select
  ON public.module_durcharbeit_question
  FOR SELECT TO authenticated USING (true);
CREATE POLICY module_durcharbeit_question_service_role_full
  ON public.module_durcharbeit_question
  FOR ALL TO service_role USING (true) WITH CHECK (true);

CREATE POLICY fragebogen_durcharbeit_authenticated_select
  ON public.fragebogen_durcharbeit
  FOR SELECT TO authenticated USING (true);
CREATE POLICY fragebogen_durcharbeit_service_role_full
  ON public.fragebogen_durcharbeit
  FOR ALL TO service_role USING (true) WITH CHECK (true);

CREATE POLICY fragebogen_durcharbeit_module_authenticated_select
  ON public.fragebogen_durcharbeit_module
  FOR SELECT TO authenticated USING (true);
CREATE POLICY fragebogen_durcharbeit_module_service_role_full
  ON public.fragebogen_durcharbeit_module
  FOR ALL TO service_role USING (true) WITH CHECK (true);

CREATE POLICY fragebogen_durcharbeit_spezial_question_authenticated_select
  ON public.fragebogen_durcharbeit_spezial_question
  FOR SELECT TO authenticated USING (true);
CREATE POLICY fragebogen_durcharbeit_spezial_question_service_role_full
  ON public.fragebogen_durcharbeit_spezial_question
  FOR ALL TO service_role USING (true) WITH CHECK (true);
