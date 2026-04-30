DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'fragebogen_main_section') THEN
    CREATE TYPE fragebogen_main_section AS ENUM ('standard', 'flex', 'billa');
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'fragebogen_schedule_type') THEN
    CREATE TYPE fragebogen_schedule_type AS ENUM ('always', 'scheduled');
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'fragebogen_rule_action') THEN
    CREATE TYPE fragebogen_rule_action AS ENUM ('hide', 'show');
  END IF;
END $$;

UPDATE module_main
SET section_keywords = '{standard}'::fragebogen_section[]
WHERE section_keywords IS NULL
   OR cardinality(section_keywords) = 0;

UPDATE fragebogen_main
SET section_keywords = '{standard}'::fragebogen_section[]
WHERE section_keywords IS NULL
   OR cardinality(section_keywords) = 0;

ALTER TABLE module_main
  ALTER COLUMN section_keywords TYPE fragebogen_main_section[]
  USING (
    CASE
      WHEN cardinality(ARRAY(
        SELECT v
        FROM unnest(section_keywords::text[]) AS v
        WHERE v IN ('standard', 'flex', 'billa')
      )) = 0 THEN ARRAY['standard']::fragebogen_main_section[]
      ELSE ARRAY(
        SELECT v::fragebogen_main_section
        FROM unnest(section_keywords::text[]) AS v
        WHERE v IN ('standard', 'flex', 'billa')
      )
    END
  );
ALTER TABLE module_main
  ALTER COLUMN section_keywords SET DEFAULT '{standard}'::fragebogen_main_section[];

ALTER TABLE fragebogen_main
  ALTER COLUMN section_keywords TYPE fragebogen_main_section[]
  USING (
    CASE
      WHEN cardinality(ARRAY(
        SELECT v
        FROM unnest(section_keywords::text[]) AS v
        WHERE v IN ('standard', 'flex', 'billa')
      )) = 0 THEN ARRAY['standard']::fragebogen_main_section[]
      ELSE ARRAY(
        SELECT v::fragebogen_main_section
        FROM unnest(section_keywords::text[]) AS v
        WHERE v IN ('standard', 'flex', 'billa')
      )
    END
  );
ALTER TABLE fragebogen_main
  ALTER COLUMN section_keywords SET DEFAULT '{standard}'::fragebogen_main_section[];

UPDATE fragebogen_main
SET schedule_type = 'always', start_date = NULL, end_date = NULL
WHERE schedule_type NOT IN ('always', 'scheduled')
   OR schedule_type IS NULL
   OR (schedule_type = 'scheduled' AND (start_date IS NULL OR end_date IS NULL OR start_date > end_date));

UPDATE fragebogen_kuehler
SET schedule_type = 'always', start_date = NULL, end_date = NULL
WHERE schedule_type NOT IN ('always', 'scheduled')
   OR schedule_type IS NULL
   OR (schedule_type = 'scheduled' AND (start_date IS NULL OR end_date IS NULL OR start_date > end_date));

UPDATE fragebogen_mhd
SET schedule_type = 'always', start_date = NULL, end_date = NULL
WHERE schedule_type NOT IN ('always', 'scheduled')
   OR schedule_type IS NULL
   OR (schedule_type = 'scheduled' AND (start_date IS NULL OR end_date IS NULL OR start_date > end_date));

ALTER TABLE fragebogen_main
  ALTER COLUMN schedule_type TYPE fragebogen_schedule_type
  USING schedule_type::fragebogen_schedule_type;
ALTER TABLE fragebogen_main
  ALTER COLUMN schedule_type SET DEFAULT 'always'::fragebogen_schedule_type;

ALTER TABLE fragebogen_kuehler
  ALTER COLUMN schedule_type TYPE fragebogen_schedule_type
  USING schedule_type::fragebogen_schedule_type;
ALTER TABLE fragebogen_kuehler
  ALTER COLUMN schedule_type SET DEFAULT 'always'::fragebogen_schedule_type;

ALTER TABLE fragebogen_mhd
  ALTER COLUMN schedule_type TYPE fragebogen_schedule_type
  USING schedule_type::fragebogen_schedule_type;
ALTER TABLE fragebogen_mhd
  ALTER COLUMN schedule_type SET DEFAULT 'always'::fragebogen_schedule_type;

ALTER TABLE question_rules
  ALTER COLUMN action TYPE fragebogen_rule_action
  USING action::fragebogen_rule_action;
ALTER TABLE question_rules
  ALTER COLUMN action SET DEFAULT 'hide'::fragebogen_rule_action;

ALTER TABLE fragebogen_main
  DROP CONSTRAINT IF EXISTS fragebogen_main_schedule_dates_ck;
ALTER TABLE fragebogen_main
  ADD CONSTRAINT fragebogen_main_schedule_dates_ck
  CHECK (
    (schedule_type = 'always')
    OR (start_date IS NOT NULL AND end_date IS NOT NULL AND start_date <= end_date)
  );

ALTER TABLE fragebogen_kuehler
  DROP CONSTRAINT IF EXISTS fragebogen_kuehler_schedule_dates_ck;
ALTER TABLE fragebogen_kuehler
  ADD CONSTRAINT fragebogen_kuehler_schedule_dates_ck
  CHECK (
    (schedule_type = 'always')
    OR (start_date IS NOT NULL AND end_date IS NOT NULL AND start_date <= end_date)
  );

ALTER TABLE fragebogen_mhd
  DROP CONSTRAINT IF EXISTS fragebogen_mhd_schedule_dates_ck;
ALTER TABLE fragebogen_mhd
  ADD CONSTRAINT fragebogen_mhd_schedule_dates_ck
  CHECK (
    (schedule_type = 'always')
    OR (start_date IS NOT NULL AND end_date IS NOT NULL AND start_date <= end_date)
  );

CREATE INDEX IF NOT EXISTS question_rule_targets_target_idx ON question_rule_targets(target_question_id);
CREATE INDEX IF NOT EXISTS question_photo_tags_tag_idx ON question_photo_tags(photo_tag_id);
CREATE INDEX IF NOT EXISTS module_main_question_question_idx ON module_main_question(question_id);
CREATE INDEX IF NOT EXISTS module_kuehler_question_question_idx ON module_kuehler_question(question_id);
CREATE INDEX IF NOT EXISTS module_mhd_question_question_idx ON module_mhd_question(question_id);
CREATE INDEX IF NOT EXISTS fragebogen_main_module_module_idx ON fragebogen_main_module(module_id);
CREATE INDEX IF NOT EXISTS fragebogen_kuehler_module_module_idx ON fragebogen_kuehler_module(module_id);
CREATE INDEX IF NOT EXISTS fragebogen_mhd_module_module_idx ON fragebogen_mhd_module(module_id);
