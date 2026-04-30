DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'fragebogen_section') THEN
    CREATE TYPE fragebogen_section AS ENUM ('standard', 'flex', 'billa', 'kuehler', 'mhd');
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'fragebogen_question_type') THEN
    CREATE TYPE fragebogen_question_type AS ENUM (
      'single','yesno','yesnomulti','multiple','likert','text','numeric','slider','photo','matrix'
    );
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'fragebogen_status') THEN
    CREATE TYPE fragebogen_status AS ENUM ('active', 'scheduled', 'inactive');
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS photo_tags (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  label text NOT NULL,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS photo_tags_label_active_unique ON photo_tags(label) WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS question_bank_shared (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  question_type fragebogen_question_type NOT NULL,
  text text NOT NULL DEFAULT '',
  required boolean NOT NULL DEFAULT true,
  chains text[],
  config jsonb NOT NULL DEFAULT '{}'::jsonb,
  rules jsonb NOT NULL DEFAULT '[]'::jsonb,
  scoring jsonb NOT NULL DEFAULT '{}'::jsonb,
  is_deleted boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS question_bank_shared_type_idx ON question_bank_shared(question_type);
CREATE INDEX IF NOT EXISTS question_bank_shared_deleted_idx ON question_bank_shared(is_deleted);

CREATE TABLE IF NOT EXISTS question_rules (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  question_id uuid NOT NULL REFERENCES question_bank_shared(id) ON DELETE CASCADE,
  trigger_question_id uuid REFERENCES question_bank_shared(id) ON DELETE SET NULL,
  operator text NOT NULL,
  trigger_value text,
  trigger_value_max text,
  action text NOT NULL,
  order_index integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS question_rules_question_idx ON question_rules(question_id);

CREATE TABLE IF NOT EXISTS question_rule_targets (
  rule_id uuid NOT NULL REFERENCES question_rules(id) ON DELETE CASCADE,
  target_question_id uuid NOT NULL REFERENCES question_bank_shared(id) ON DELETE CASCADE,
  order_index integer NOT NULL DEFAULT 0,
  PRIMARY KEY (rule_id, target_question_id)
);

CREATE TABLE IF NOT EXISTS question_scoring (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  question_id uuid NOT NULL REFERENCES question_bank_shared(id) ON DELETE CASCADE,
  score_key text NOT NULL,
  ipp numeric(8,2),
  boni numeric(8,2),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS question_scoring_question_key_unique ON question_scoring(question_id, score_key);

CREATE TABLE IF NOT EXISTS question_matrix (
  question_id uuid PRIMARY KEY REFERENCES question_bank_shared(id) ON DELETE CASCADE,
  matrix_subtype text NOT NULL DEFAULT 'toggle',
  rows jsonb NOT NULL DEFAULT '[]'::jsonb,
  columns jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS question_attachments (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  question_id uuid NOT NULL REFERENCES question_bank_shared(id) ON DELETE CASCADE,
  payload text NOT NULL,
  order_index integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS question_attachments_question_idx ON question_attachments(question_id);

CREATE TABLE IF NOT EXISTS question_photo_tags (
  question_id uuid NOT NULL REFERENCES question_bank_shared(id) ON DELETE CASCADE,
  photo_tag_id uuid NOT NULL REFERENCES photo_tags(id) ON DELETE CASCADE,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (question_id, photo_tag_id)
);

CREATE TABLE IF NOT EXISTS module_main (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  description text NOT NULL DEFAULT '',
  section_keywords fragebogen_section[] NOT NULL DEFAULT '{standard}'::fragebogen_section[],
  is_deleted boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS module_main_question (
  module_id uuid NOT NULL REFERENCES module_main(id) ON DELETE CASCADE,
  question_id uuid NOT NULL REFERENCES question_bank_shared(id) ON DELETE CASCADE,
  order_index integer NOT NULL DEFAULT 0,
  PRIMARY KEY (module_id, question_id)
);

CREATE TABLE IF NOT EXISTS fragebogen_main (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  description text NOT NULL DEFAULT '',
  section_keywords fragebogen_section[] NOT NULL DEFAULT '{standard}'::fragebogen_section[],
  nur_einmal_ausfuellbar boolean NOT NULL DEFAULT false,
  status fragebogen_status NOT NULL DEFAULT 'inactive',
  schedule_type text NOT NULL DEFAULT 'always',
  start_date date,
  end_date date,
  is_deleted boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fragebogen_main_module (
  fragebogen_id uuid NOT NULL REFERENCES fragebogen_main(id) ON DELETE CASCADE,
  module_id uuid NOT NULL REFERENCES module_main(id) ON DELETE CASCADE,
  order_index integer NOT NULL DEFAULT 0,
  PRIMARY KEY (fragebogen_id, module_id)
);

CREATE TABLE IF NOT EXISTS fragebogen_main_spezial_question (
  fragebogen_id uuid NOT NULL REFERENCES fragebogen_main(id) ON DELETE CASCADE,
  question_id uuid NOT NULL REFERENCES question_bank_shared(id) ON DELETE CASCADE,
  order_index integer NOT NULL DEFAULT 0,
  PRIMARY KEY (fragebogen_id, question_id)
);

CREATE TABLE IF NOT EXISTS module_kuehler (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  description text NOT NULL DEFAULT '',
  is_deleted boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS module_kuehler_question (
  module_id uuid NOT NULL REFERENCES module_kuehler(id) ON DELETE CASCADE,
  question_id uuid NOT NULL REFERENCES question_bank_shared(id) ON DELETE CASCADE,
  order_index integer NOT NULL DEFAULT 0,
  PRIMARY KEY (module_id, question_id)
);

CREATE TABLE IF NOT EXISTS fragebogen_kuehler (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  description text NOT NULL DEFAULT '',
  nur_einmal_ausfuellbar boolean NOT NULL DEFAULT false,
  status fragebogen_status NOT NULL DEFAULT 'inactive',
  schedule_type text NOT NULL DEFAULT 'always',
  start_date date,
  end_date date,
  is_deleted boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fragebogen_kuehler_module (
  fragebogen_id uuid NOT NULL REFERENCES fragebogen_kuehler(id) ON DELETE CASCADE,
  module_id uuid NOT NULL REFERENCES module_kuehler(id) ON DELETE CASCADE,
  order_index integer NOT NULL DEFAULT 0,
  PRIMARY KEY (fragebogen_id, module_id)
);

CREATE TABLE IF NOT EXISTS module_mhd (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  description text NOT NULL DEFAULT '',
  is_deleted boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS module_mhd_question (
  module_id uuid NOT NULL REFERENCES module_mhd(id) ON DELETE CASCADE,
  question_id uuid NOT NULL REFERENCES question_bank_shared(id) ON DELETE CASCADE,
  order_index integer NOT NULL DEFAULT 0,
  PRIMARY KEY (module_id, question_id)
);

CREATE TABLE IF NOT EXISTS fragebogen_mhd (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  description text NOT NULL DEFAULT '',
  nur_einmal_ausfuellbar boolean NOT NULL DEFAULT false,
  status fragebogen_status NOT NULL DEFAULT 'inactive',
  schedule_type text NOT NULL DEFAULT 'always',
  start_date date,
  end_date date,
  is_deleted boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fragebogen_mhd_module (
  fragebogen_id uuid NOT NULL REFERENCES fragebogen_mhd(id) ON DELETE CASCADE,
  module_id uuid NOT NULL REFERENCES module_mhd(id) ON DELETE CASCADE,
  order_index integer NOT NULL DEFAULT 0,
  PRIMARY KEY (fragebogen_id, module_id)
);

-- Restrict table access to authenticated/service role.
DO $$
DECLARE
  t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
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
    'module_kuehler',
    'module_kuehler_question',
    'fragebogen_kuehler',
    'fragebogen_kuehler_module',
    'module_mhd',
    'module_mhd_question',
    'fragebogen_mhd',
    'fragebogen_mhd_module'
  ]
  LOOP
    EXECUTE format('REVOKE ALL ON TABLE public.%I FROM anon, authenticated', t);
    EXECUTE format('GRANT SELECT ON TABLE public.%I TO authenticated', t);
    EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('DROP POLICY IF EXISTS %I_authenticated_select ON public.%I', t, t);
    EXECUTE format('CREATE POLICY %I_authenticated_select ON public.%I FOR SELECT TO authenticated USING (true)', t, t);
    EXECUTE format('DROP POLICY IF EXISTS %I_service_role_full ON public.%I', t, t);
    EXECUTE format('CREATE POLICY %I_service_role_full ON public.%I FOR ALL TO service_role USING (true) WITH CHECK (true)', t, t);
  END LOOP;
END $$;
