-- Spezialfragen are permanent shared questions. A Fragebogen only owns an
-- active/inactive assignment; deactivation never removes the question or any
-- visit-session snapshot/answer that already references it.

ALTER TABLE public.question_bank_shared
  ADD COLUMN IF NOT EXISTS is_spezial boolean NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS question_bank_shared_spezial_idx
  ON public.question_bank_shared(is_spezial, is_deleted);

-- Preserve every currently configured inline Spezialfrage by promoting it to
-- the shared question bank with the same UUID and complete legacy payload.
INSERT INTO public.question_bank_shared (
  id,
  question_type,
  text,
  required,
  red_survey,
  single_choice_availability,
  single_choice_availability_type,
  chains,
  config,
  rules,
  scoring,
  is_spezial,
  is_deleted,
  created_at,
  updated_at
)
SELECT
  item.id,
  item.question_type,
  item.text,
  item.required,
  item.red_survey,
  item.single_choice_availability,
  item.single_choice_availability_type,
  item.chains,
  item.config,
  item.rules,
  item.scoring,
  true,
  false,
  item.created_at,
  item.updated_at
FROM public.fragebogen_main_spezial_items item
ON CONFLICT (id) DO UPDATE SET
  question_type = EXCLUDED.question_type,
  text = EXCLUDED.text,
  required = EXCLUDED.required,
  red_survey = EXCLUDED.red_survey,
  single_choice_availability = EXCLUDED.single_choice_availability,
  single_choice_availability_type = EXCLUDED.single_choice_availability_type,
  chains = EXCLUDED.chains,
  config = EXCLUDED.config,
  rules = EXCLUDED.rules,
  scoring = EXCLUDED.scoring,
  is_spezial = true,
  is_deleted = false,
  updated_at = EXCLUDED.updated_at;

-- Migration 0011 copied the old link model into inline rows. Rebuild the
-- active assignment set from those inline rows while retaining every old link
-- as a soft-deleted audit record.
UPDATE public.fragebogen_main_spezial_question
SET is_deleted = true,
    deleted_at = COALESCE(deleted_at, now()),
    updated_at = now()
WHERE is_deleted = false;

INSERT INTO public.fragebogen_main_spezial_question (
  fragebogen_id,
  question_id,
  order_index,
  is_deleted,
  deleted_at,
  created_at,
  updated_at
)
SELECT
  item.fragebogen_id,
  item.id,
  item.order_index,
  item.is_deleted,
  item.deleted_at,
  item.created_at,
  item.updated_at
FROM public.fragebogen_main_spezial_items item
ON CONFLICT (fragebogen_id, question_id) DO UPDATE SET
  order_index = EXCLUDED.order_index,
  is_deleted = EXCLUDED.is_deleted,
  deleted_at = EXCLUDED.deleted_at,
  updated_at = EXCLUDED.updated_at;

CREATE INDEX IF NOT EXISTS fragebogen_main_spezial_question_question_idx
  ON public.fragebogen_main_spezial_question(question_id);
CREATE INDEX IF NOT EXISTS fragebogen_main_spezial_question_active_order_idx
  ON public.fragebogen_main_spezial_question(fragebogen_id, is_deleted, order_index);

-- Kühler and MHD use the same permanent question pool, but keep independent
-- soft assignments because their Fragebogen live in separate scope tables.
CREATE TABLE IF NOT EXISTS public.fragebogen_kuehler_spezial_question (
  fragebogen_id uuid NOT NULL REFERENCES public.fragebogen_kuehler(id) ON DELETE CASCADE,
  question_id uuid NOT NULL REFERENCES public.question_bank_shared(id) ON DELETE CASCADE,
  order_index integer NOT NULL DEFAULT 0,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (fragebogen_id, question_id)
);

CREATE INDEX IF NOT EXISTS fragebogen_kuehler_spezial_question_question_idx
  ON public.fragebogen_kuehler_spezial_question(question_id);
CREATE INDEX IF NOT EXISTS fragebogen_kuehler_spezial_question_active_order_idx
  ON public.fragebogen_kuehler_spezial_question(fragebogen_id, is_deleted, order_index);
CREATE INDEX IF NOT EXISTS fragebogen_kuehler_spezial_question_deleted_idx
  ON public.fragebogen_kuehler_spezial_question(is_deleted);

CREATE TABLE IF NOT EXISTS public.fragebogen_mhd_spezial_question (
  fragebogen_id uuid NOT NULL REFERENCES public.fragebogen_mhd(id) ON DELETE CASCADE,
  question_id uuid NOT NULL REFERENCES public.question_bank_shared(id) ON DELETE CASCADE,
  order_index integer NOT NULL DEFAULT 0,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (fragebogen_id, question_id)
);

CREATE INDEX IF NOT EXISTS fragebogen_mhd_spezial_question_question_idx
  ON public.fragebogen_mhd_spezial_question(question_id);
CREATE INDEX IF NOT EXISTS fragebogen_mhd_spezial_question_active_order_idx
  ON public.fragebogen_mhd_spezial_question(fragebogen_id, is_deleted, order_index);
CREATE INDEX IF NOT EXISTS fragebogen_mhd_spezial_question_deleted_idx
  ON public.fragebogen_mhd_spezial_question(is_deleted);

REVOKE ALL ON TABLE public.fragebogen_kuehler_spezial_question FROM anon, authenticated;
GRANT SELECT ON TABLE public.fragebogen_kuehler_spezial_question TO authenticated;
ALTER TABLE public.fragebogen_kuehler_spezial_question ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS fragebogen_kuehler_spezial_question_authenticated_select
  ON public.fragebogen_kuehler_spezial_question;
CREATE POLICY fragebogen_kuehler_spezial_question_authenticated_select
  ON public.fragebogen_kuehler_spezial_question
  FOR SELECT TO authenticated USING (true);
DROP POLICY IF EXISTS fragebogen_kuehler_spezial_question_service_role_full
  ON public.fragebogen_kuehler_spezial_question;
CREATE POLICY fragebogen_kuehler_spezial_question_service_role_full
  ON public.fragebogen_kuehler_spezial_question
  FOR ALL TO service_role USING (true) WITH CHECK (true);

REVOKE ALL ON TABLE public.fragebogen_mhd_spezial_question FROM anon, authenticated;
GRANT SELECT ON TABLE public.fragebogen_mhd_spezial_question TO authenticated;
ALTER TABLE public.fragebogen_mhd_spezial_question ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS fragebogen_mhd_spezial_question_authenticated_select
  ON public.fragebogen_mhd_spezial_question;
CREATE POLICY fragebogen_mhd_spezial_question_authenticated_select
  ON public.fragebogen_mhd_spezial_question
  FOR SELECT TO authenticated USING (true);
DROP POLICY IF EXISTS fragebogen_mhd_spezial_question_service_role_full
  ON public.fragebogen_mhd_spezial_question;
CREATE POLICY fragebogen_mhd_spezial_question_service_role_full
  ON public.fragebogen_mhd_spezial_question
  FOR ALL TO service_role USING (true) WITH CHECK (true);
