ALTER TABLE photo_tags
  ADD COLUMN IF NOT EXISTS is_deleted boolean NOT NULL DEFAULT false;

UPDATE photo_tags
SET is_deleted = (deleted_at IS NOT NULL)
WHERE is_deleted IS DISTINCT FROM (deleted_at IS NOT NULL);

DROP INDEX IF EXISTS photo_tags_label_active_unique;
CREATE UNIQUE INDEX IF NOT EXISTS photo_tags_label_active_unique
  ON photo_tags(label)
  WHERE is_deleted = false;

ALTER TABLE question_rules
  ADD COLUMN IF NOT EXISTS is_deleted boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS deleted_at timestamptz;
CREATE INDEX IF NOT EXISTS question_rules_deleted_idx ON question_rules(is_deleted);

ALTER TABLE question_rule_targets
  ADD COLUMN IF NOT EXISTS id uuid DEFAULT gen_random_uuid(),
  ADD COLUMN IF NOT EXISTS is_deleted boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS deleted_at timestamptz,
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.table_constraints
    WHERE table_schema = 'public'
      AND table_name = 'question_rule_targets'
      AND constraint_name = 'question_rule_targets_pkey'
  ) THEN
    ALTER TABLE question_rule_targets DROP CONSTRAINT question_rule_targets_pkey;
  END IF;
END $$;

ALTER TABLE question_rule_targets
  ALTER COLUMN id SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.table_constraints
    WHERE table_schema = 'public'
      AND table_name = 'question_rule_targets'
      AND constraint_name = 'question_rule_targets_pkey'
  ) THEN
    ALTER TABLE question_rule_targets ADD CONSTRAINT question_rule_targets_pkey PRIMARY KEY (id);
  END IF;
END $$;

DROP INDEX IF EXISTS question_rule_targets_rule_target_active_unique;
CREATE UNIQUE INDEX IF NOT EXISTS question_rule_targets_rule_target_active_unique
  ON question_rule_targets(rule_id, target_question_id)
  WHERE is_deleted = false;
CREATE INDEX IF NOT EXISTS question_rule_targets_deleted_idx ON question_rule_targets(is_deleted);

ALTER TABLE question_scoring
  ADD COLUMN IF NOT EXISTS is_deleted boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS deleted_at timestamptz;
DROP INDEX IF EXISTS question_scoring_question_key_unique;
CREATE UNIQUE INDEX IF NOT EXISTS question_scoring_question_key_unique
  ON question_scoring(question_id, score_key)
  WHERE is_deleted = false;
CREATE INDEX IF NOT EXISTS question_scoring_deleted_idx ON question_scoring(is_deleted);

ALTER TABLE question_matrix
  ADD COLUMN IF NOT EXISTS is_deleted boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS deleted_at timestamptz;
CREATE INDEX IF NOT EXISTS question_matrix_deleted_idx ON question_matrix(is_deleted);

ALTER TABLE question_attachments
  ADD COLUMN IF NOT EXISTS is_deleted boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS deleted_at timestamptz,
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS question_attachments_deleted_idx ON question_attachments(is_deleted);

ALTER TABLE question_photo_tags
  ADD COLUMN IF NOT EXISTS is_deleted boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS deleted_at timestamptz,
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS question_photo_tags_deleted_idx ON question_photo_tags(is_deleted);

ALTER TABLE module_main_question
  ADD COLUMN IF NOT EXISTS is_deleted boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS deleted_at timestamptz,
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS module_main_question_deleted_idx ON module_main_question(is_deleted);

ALTER TABLE module_kuehler_question
  ADD COLUMN IF NOT EXISTS is_deleted boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS deleted_at timestamptz,
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS module_kuehler_question_deleted_idx ON module_kuehler_question(is_deleted);

ALTER TABLE module_mhd_question
  ADD COLUMN IF NOT EXISTS is_deleted boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS deleted_at timestamptz,
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS module_mhd_question_deleted_idx ON module_mhd_question(is_deleted);

ALTER TABLE fragebogen_main_module
  ADD COLUMN IF NOT EXISTS is_deleted boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS deleted_at timestamptz,
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS fragebogen_main_module_deleted_idx ON fragebogen_main_module(is_deleted);

ALTER TABLE fragebogen_kuehler_module
  ADD COLUMN IF NOT EXISTS is_deleted boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS deleted_at timestamptz,
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS fragebogen_kuehler_module_deleted_idx ON fragebogen_kuehler_module(is_deleted);

ALTER TABLE fragebogen_mhd_module
  ADD COLUMN IF NOT EXISTS is_deleted boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS deleted_at timestamptz,
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS fragebogen_mhd_module_deleted_idx ON fragebogen_mhd_module(is_deleted);

ALTER TABLE fragebogen_main_spezial_question
  ADD COLUMN IF NOT EXISTS is_deleted boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS deleted_at timestamptz,
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS fragebogen_main_spezial_question_deleted_idx ON fragebogen_main_spezial_question(is_deleted);

ALTER TABLE fragebogen_main_spezial_items
  ADD COLUMN IF NOT EXISTS is_deleted boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS deleted_at timestamptz;
CREATE INDEX IF NOT EXISTS fragebogen_main_spezial_items_deleted_idx ON fragebogen_main_spezial_items(is_deleted);
