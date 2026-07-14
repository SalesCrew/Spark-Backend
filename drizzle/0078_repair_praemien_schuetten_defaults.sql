-- Repair draft Schütten / Displays pillars that were accidentally created
-- with the qualitative Reporting / Survey / Zeitmanagement payout model.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TEMP TABLE repair_schuetten_pillars ON COMMIT DROP AS
SELECT
  pillar.id AS pillar_id,
  pillar.wave_id
FROM "public"."praemien_wave_pillars" AS pillar
JOIN "public"."praemien_waves" AS wave
  ON wave.id = pillar.wave_id
WHERE pillar.is_deleted = false
  AND wave.is_deleted = false
  AND wave.status = 'draft'
  AND (
    regexp_replace(lower(translate(pillar.name, 'ÄÖÜäöüß', 'AOUaous')), '[^a-z0-9]+', '', 'g') LIKE 'schutten%display%'
    OR regexp_replace(lower(translate(pillar.name, 'ÄÖÜäöüß', 'AOUaous')), '[^a-z0-9]+', '', 'g') LIKE 'schuetten%display%'
  )
  AND (
    SELECT count(*)
    FROM "public"."praemien_wave_pillar_metrics" AS metric
    WHERE metric.pillar_id = pillar.id
      AND metric.is_deleted = false
  ) = 3
  AND (
    SELECT count(*)
    FROM "public"."praemien_wave_pillar_metrics" AS metric
    WHERE metric.pillar_id = pillar.id
      AND metric.is_deleted = false
      AND metric.value_source IN ('quality_reporting', 'quality_accuracy', 'quality_zeiterfassung')
  ) = 3;
--> statement-breakpoint

CREATE TEMP TABLE repair_schuetten_metrics ON COMMIT DROP AS
SELECT pillar_id, wave_id, gen_random_uuid() AS metric_id
FROM repair_schuetten_pillars;
--> statement-breakpoint

CREATE TEMP TABLE repair_schuetten_tiers ON COMMIT DROP AS
SELECT
  target.pillar_id,
  target.wave_id,
  target.metric_id,
  gen_random_uuid() AS tier_id,
  tier.label,
  tier.order_index,
  tier.reward_eur,
  tier.threshold_value
FROM repair_schuetten_metrics AS target
CROSS JOIN (
  VALUES
    ('50 % der Säule'::text, 0, 275::numeric, 70::numeric),
    ('80 % der Säule'::text, 1, 440::numeric, 80::numeric),
    ('100 % der Säule'::text, 2, 550::numeric, 95::numeric)
) AS tier(label, order_index, reward_eur, threshold_value);
--> statement-breakpoint

UPDATE "public"."praemien_wave_pillar_tier_conditions" AS condition
SET
  is_deleted = true,
  deleted_at = now(),
  updated_at = now()
WHERE condition.is_deleted = false
  AND condition.pillar_id IN (SELECT pillar_id FROM repair_schuetten_pillars);
--> statement-breakpoint

UPDATE "public"."praemien_wave_pillar_tiers" AS tier
SET
  is_deleted = true,
  deleted_at = now(),
  updated_at = now()
WHERE tier.is_deleted = false
  AND tier.pillar_id IN (SELECT pillar_id FROM repair_schuetten_pillars);
--> statement-breakpoint

UPDATE "public"."praemien_wave_pillar_metrics" AS metric
SET
  is_deleted = true,
  deleted_at = now(),
  updated_at = now()
WHERE metric.is_deleted = false
  AND metric.pillar_id IN (SELECT pillar_id FROM repair_schuetten_pillars);
--> statement-breakpoint

UPDATE "public"."praemien_wave_pillars" AS pillar
SET
  payout_mode = 'highest_tier',
  max_reward_eur = 550,
  target_points = null,
  reward_eur = 0,
  updated_at = now()
WHERE pillar.id IN (SELECT pillar_id FROM repair_schuetten_pillars);
--> statement-breakpoint

INSERT INTO "public"."praemien_wave_pillar_metrics" (
  id,
  wave_id,
  pillar_id,
  key,
  label,
  unit,
  value_source,
  source_key,
  order_index,
  is_deleted,
  deleted_at,
  created_at,
  updated_at
)
SELECT
  metric_id,
  wave_id,
  pillar_id,
  'achievement_percent',
  'Zielerreichung',
  'percent',
  'contribution_percent',
  null,
  0,
  false,
  null,
  now(),
  now()
FROM repair_schuetten_metrics;
--> statement-breakpoint

INSERT INTO "public"."praemien_wave_pillar_tiers" (
  id,
  wave_id,
  pillar_id,
  label,
  order_index,
  reward_eur,
  is_deleted,
  deleted_at,
  created_at,
  updated_at
)
SELECT
  tier_id,
  wave_id,
  pillar_id,
  label,
  order_index,
  reward_eur,
  false,
  null,
  now(),
  now()
FROM repair_schuetten_tiers;
--> statement-breakpoint

INSERT INTO "public"."praemien_wave_pillar_tier_conditions" (
  wave_id,
  pillar_id,
  tier_id,
  metric_id,
  operator,
  threshold_value,
  order_index,
  is_deleted,
  deleted_at,
  created_at,
  updated_at
)
SELECT
  wave_id,
  pillar_id,
  tier_id,
  metric_id,
  'gte',
  threshold_value,
  0,
  false,
  null,
  now(),
  now()
FROM repair_schuetten_tiers;
