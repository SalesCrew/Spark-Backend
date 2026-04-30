DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'visit_session_status') THEN
    CREATE TYPE visit_session_status AS ENUM ('draft', 'submitted', 'cancelled');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'visit_section_status') THEN
    CREATE TYPE visit_section_status AS ENUM ('draft', 'submitted');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'visit_answer_status') THEN
    CREATE TYPE visit_answer_status AS ENUM ('unanswered', 'answered', 'hidden_by_rule', 'skipped', 'invalid');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'visit_answer_option_role') THEN
    CREATE TYPE visit_answer_option_role AS ENUM ('top', 'sub');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'visit_answer_event_type') THEN
    CREATE TYPE visit_answer_event_type AS ENUM ('set', 'clear', 'status_change');
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS visit_sessions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  gm_user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  market_id uuid NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
  status visit_session_status NOT NULL DEFAULT 'draft',
  started_at timestamptz NOT NULL DEFAULT now(),
  submitted_at timestamptz,
  cancelled_at timestamptz,
  last_saved_at timestamptz NOT NULL DEFAULT now(),
  client_session_token text,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS visit_sessions_gm_status_idx ON visit_sessions(gm_user_id, status);
CREATE INDEX IF NOT EXISTS visit_sessions_market_idx ON visit_sessions(market_id);
CREATE INDEX IF NOT EXISTS visit_sessions_deleted_idx ON visit_sessions(is_deleted);
CREATE UNIQUE INDEX IF NOT EXISTS visit_sessions_client_token_active_unique
  ON visit_sessions(gm_user_id, client_session_token)
  WHERE is_deleted = false AND client_session_token IS NOT NULL;

CREATE TABLE IF NOT EXISTS visit_session_sections (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  visit_session_id uuid NOT NULL REFERENCES visit_sessions(id) ON DELETE CASCADE,
  campaign_id uuid NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
  section campaign_section NOT NULL,
  fragebogen_id uuid,
  fragebogen_name_snapshot text NOT NULL DEFAULT '',
  order_index integer NOT NULL DEFAULT 0,
  status visit_section_status NOT NULL DEFAULT 'draft',
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS visit_session_sections_session_idx ON visit_session_sections(visit_session_id, order_index);
CREATE INDEX IF NOT EXISTS visit_session_sections_campaign_idx ON visit_session_sections(campaign_id);
CREATE INDEX IF NOT EXISTS visit_session_sections_deleted_idx ON visit_session_sections(is_deleted);

CREATE TABLE IF NOT EXISTS visit_session_questions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  visit_session_section_id uuid NOT NULL REFERENCES visit_session_sections(id) ON DELETE CASCADE,
  question_id uuid NOT NULL REFERENCES question_bank_shared(id) ON DELETE RESTRICT,
  module_id uuid,
  module_name_snapshot text NOT NULL DEFAULT '',
  question_type fragebogen_question_type NOT NULL,
  question_text_snapshot text NOT NULL DEFAULT '',
  question_config_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
  question_rules_snapshot jsonb NOT NULL DEFAULT '[]'::jsonb,
  required_snapshot boolean NOT NULL DEFAULT true,
  order_index integer NOT NULL DEFAULT 0,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS visit_session_questions_section_idx ON visit_session_questions(visit_session_section_id, order_index);
CREATE INDEX IF NOT EXISTS visit_session_questions_question_idx ON visit_session_questions(question_id);
CREATE INDEX IF NOT EXISTS visit_session_questions_deleted_idx ON visit_session_questions(is_deleted);

CREATE TABLE IF NOT EXISTS visit_answers (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  visit_session_id uuid NOT NULL REFERENCES visit_sessions(id) ON DELETE CASCADE,
  visit_session_section_id uuid NOT NULL REFERENCES visit_session_sections(id) ON DELETE CASCADE,
  visit_session_question_id uuid NOT NULL REFERENCES visit_session_questions(id) ON DELETE CASCADE,
  question_id uuid NOT NULL REFERENCES question_bank_shared(id) ON DELETE RESTRICT,
  question_type fragebogen_question_type NOT NULL,
  answer_status visit_answer_status NOT NULL DEFAULT 'unanswered',
  value_text text,
  value_number numeric(16, 4),
  value_json jsonb,
  is_valid boolean NOT NULL DEFAULT true,
  validation_error text,
  answered_at timestamptz,
  changed_at timestamptz NOT NULL DEFAULT now(),
  version integer NOT NULL DEFAULT 1,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS visit_answers_question_active_unique
  ON visit_answers(visit_session_question_id)
  WHERE is_deleted = false;
CREATE INDEX IF NOT EXISTS visit_answers_session_idx ON visit_answers(visit_session_id);
CREATE INDEX IF NOT EXISTS visit_answers_section_idx ON visit_answers(visit_session_section_id);
CREATE INDEX IF NOT EXISTS visit_answers_deleted_idx ON visit_answers(is_deleted);

CREATE TABLE IF NOT EXISTS visit_answer_options (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  visit_answer_id uuid NOT NULL REFERENCES visit_answers(id) ON DELETE CASCADE,
  option_role visit_answer_option_role NOT NULL DEFAULT 'sub',
  option_value text NOT NULL,
  order_index integer NOT NULL DEFAULT 0,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS visit_answer_options_answer_idx ON visit_answer_options(visit_answer_id, order_index);
CREATE INDEX IF NOT EXISTS visit_answer_options_deleted_idx ON visit_answer_options(is_deleted);

CREATE TABLE IF NOT EXISTS visit_answer_matrix_cells (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  visit_answer_id uuid NOT NULL REFERENCES visit_answers(id) ON DELETE CASCADE,
  row_key text NOT NULL,
  column_key text NOT NULL,
  cell_value_text text,
  cell_value_date date,
  cell_selected boolean,
  order_index integer NOT NULL DEFAULT 0,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS visit_answer_matrix_cells_active_unique
  ON visit_answer_matrix_cells(visit_answer_id, row_key, column_key)
  WHERE is_deleted = false;
CREATE INDEX IF NOT EXISTS visit_answer_matrix_cells_answer_idx ON visit_answer_matrix_cells(visit_answer_id, order_index);
CREATE INDEX IF NOT EXISTS visit_answer_matrix_cells_deleted_idx ON visit_answer_matrix_cells(is_deleted);

CREATE TABLE IF NOT EXISTS visit_question_comments (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  visit_session_question_id uuid NOT NULL REFERENCES visit_session_questions(id) ON DELETE CASCADE,
  comment_text text NOT NULL DEFAULT '',
  commented_at timestamptz NOT NULL DEFAULT now(),
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS visit_question_comments_active_unique
  ON visit_question_comments(visit_session_question_id)
  WHERE is_deleted = false;
CREATE INDEX IF NOT EXISTS visit_question_comments_deleted_idx ON visit_question_comments(is_deleted);

CREATE TABLE IF NOT EXISTS visit_answer_photos (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  visit_answer_id uuid NOT NULL REFERENCES visit_answers(id) ON DELETE CASCADE,
  storage_bucket text NOT NULL,
  storage_path text NOT NULL,
  mime_type text,
  byte_size integer,
  width_px integer,
  height_px integer,
  sha256 text,
  uploaded_at timestamptz NOT NULL DEFAULT now(),
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS visit_answer_photos_path_active_unique
  ON visit_answer_photos(visit_answer_id, storage_path)
  WHERE is_deleted = false;
CREATE UNIQUE INDEX IF NOT EXISTS visit_answer_photos_sha_path_active_unique
  ON visit_answer_photos(visit_answer_id, sha256, storage_path)
  WHERE is_deleted = false AND sha256 IS NOT NULL;
CREATE INDEX IF NOT EXISTS visit_answer_photos_answer_idx ON visit_answer_photos(visit_answer_id);
CREATE INDEX IF NOT EXISTS visit_answer_photos_answer_deleted_idx ON visit_answer_photos(visit_answer_id, is_deleted);
CREATE INDEX IF NOT EXISTS visit_answer_photos_deleted_idx ON visit_answer_photos(is_deleted);

CREATE TABLE IF NOT EXISTS visit_answer_photo_tags (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  visit_answer_photo_id uuid NOT NULL REFERENCES visit_answer_photos(id) ON DELETE CASCADE,
  photo_tag_id uuid REFERENCES photo_tags(id) ON DELETE SET NULL,
  photo_tag_label_snapshot text NOT NULL DEFAULT '',
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS visit_answer_photo_tags_active_unique
  ON visit_answer_photo_tags(visit_answer_photo_id, photo_tag_label_snapshot)
  WHERE is_deleted = false;
CREATE INDEX IF NOT EXISTS visit_answer_photo_tags_photo_idx ON visit_answer_photo_tags(visit_answer_photo_id);
CREATE INDEX IF NOT EXISTS visit_answer_photo_tags_photo_deleted_idx ON visit_answer_photo_tags(visit_answer_photo_id, is_deleted);
CREATE INDEX IF NOT EXISTS visit_answer_photo_tags_deleted_idx ON visit_answer_photo_tags(is_deleted);

CREATE TABLE IF NOT EXISTS visit_answer_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  visit_answer_id uuid NOT NULL REFERENCES visit_answers(id) ON DELETE CASCADE,
  event_type visit_answer_event_type NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  actor_user_id uuid REFERENCES users(id) ON DELETE SET NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS visit_answer_events_answer_idx ON visit_answer_events(visit_answer_id, created_at);

DO $$
DECLARE
  t text;
  p record;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'visit_sessions',
    'visit_session_sections',
    'visit_session_questions',
    'visit_answers',
    'visit_answer_options',
    'visit_answer_matrix_cells',
    'visit_question_comments',
    'visit_answer_photos',
    'visit_answer_photo_tags',
    'visit_answer_events'
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
