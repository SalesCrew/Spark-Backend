-- Carry answers for selected premium pillars across the complete active wave.

ALTER TABLE "public"."praemien_wave_pillars"
  ADD COLUMN IF NOT EXISTS "carry_answers_for_wave" boolean DEFAULT false NOT NULL;
--> statement-breakpoint

WITH normalized_pillars AS (
  SELECT
    id,
    regexp_replace(
      lower(translate(name, 'ÄÖÜäöüß', 'AOUaous')),
      '[^a-z0-9]+',
      '',
      'g'
    ) AS normalized_name
  FROM "public"."praemien_wave_pillars"
)
UPDATE "public"."praemien_wave_pillars" AS pillar
SET "carry_answers_for_wave" = true
FROM normalized_pillars
WHERE pillar.id = normalized_pillars.id
  AND (
    normalized_pillars.normalized_name = 'distributionsziel'
    OR normalized_pillars.normalized_name LIKE 'schutten%display%'
    OR normalized_pillars.normalized_name LIKE 'schuetten%display%'
  );
