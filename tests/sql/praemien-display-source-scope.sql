-- Bind only reviewed task constants. This is an explicit configuration operation,
-- not a migration or application test. Every guard runs before either write.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '8s';
DO $guard$
DECLARE
  current_row record;
  display_metric jsonb;
  matching_metrics integer;
BEGIN
  SELECT w.status,w.year,w.quarter,w.start_date,w.end_date,s.revision,s.closed_at,s.closed_snapshot,s.model
    INTO current_row FROM praemien_waves w JOIN praemien_wave_settings s ON s.wave_id=w.id
    WHERE w.id='__WAVE_ID__'::uuid AND w.is_deleted=false FOR UPDATE OF w,s;
  IF NOT FOUND THEN RAISE EXCEPTION 'Expected bonus wave is missing'; END IF;
  IF current_row.status<>'active' OR current_row.year<>2026 OR current_row.quarter<>4
    OR current_row.start_date<>'2026-10-01'::date OR current_row.end_date<>'2026-12-31'::date
    OR current_row.closed_at IS NOT NULL OR current_row.closed_snapshot IS NOT NULL THEN
    RAISE EXCEPTION 'Expected open active Q4 wave only';
  END IF;
  IF current_row.revision<>__EXPECTED_REVISION__ THEN RAISE EXCEPTION 'Wave revision changed; no configuration updated'; END IF;
  SELECT count(*) INTO matching_metrics FROM jsonb_array_elements(current_row.model->'pillars') p
    CROSS JOIN LATERAL jsonb_array_elements(p->'metrics') m
    WHERE p->>'key'='__PILLAR_KEY__' AND p->>'kind'='displays' AND m->>'key'='__METRIC_KEY__';
  IF matching_metrics<>1 THEN RAISE EXCEPTION 'Expected exactly one display metric'; END IF;
  SELECT m INTO display_metric FROM jsonb_array_elements(current_row.model->'pillars') p
    CROSS JOIN LATERAL jsonb_array_elements(p->'metrics') m
    WHERE p->>'key'='__PILLAR_KEY__' AND m->>'key'='__METRIC_KEY__';
  IF display_metric->>'method'<>'answer_sum' OR display_metric->>'unit'<>'percent'
    OR jsonb_array_length(display_metric->'sources')<>__EXPECTED_SOURCE_COUNT__
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(display_metric->'sources') s WHERE s->>'section' IS DISTINCT FROM 'standard')
    OR (display_metric ? 'sourceSectionOverrides' AND display_metric->'sourceSectionOverrides'<>'[]'::jsonb) THEN
    RAISE EXCEPTION 'Source configuration changed; no configuration updated';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM users WHERE id='__GM_ID__'::uuid AND role='gm' AND is_supplemental_gm=false) THEN
    RAISE EXCEPTION 'Expected regular GM is missing';
  END IF;
END $guard$;
WITH original AS MATERIALIZED (
  SELECT wave_id,model,revision FROM praemien_wave_settings WHERE wave_id='__WAVE_ID__'::uuid
), metric_path AS (
  SELECT ARRAY['pillars',(p.ordinality-1)::text,'metrics',(m.ordinality-1)::text,'sourceSectionOverrides'] AS path
  FROM original o CROSS JOIN LATERAL jsonb_array_elements(o.model->'pillars') WITH ORDINALITY p(value,ordinality)
  CROSS JOIN LATERAL jsonb_array_elements(p.value->'metrics') WITH ORDINALITY m(value,ordinality)
  WHERE p.value->>'key'='__PILLAR_KEY__' AND m.value->>'key'='__METRIC_KEY__'
), updated AS (
  UPDATE praemien_wave_settings s SET
    model=jsonb_set(o.model,mp.path,jsonb_build_array(jsonb_build_object('gmId','__GM_ID__','sections',jsonb_build_array('standard','flex','billa'))),true),
    revision=o.revision+1,updated_at=now()
  FROM original o CROSS JOIN metric_path mp
  WHERE s.wave_id=o.wave_id AND s.revision=__EXPECTED_REVISION__
  RETURNING s.wave_id,s.model,s.revision
), audited AS (
  INSERT INTO praemien_wave_events(wave_id,revision,event_type,actor_id,actor_name,payload)
  SELECT u.wave_id,u.revision,'rules',NULL,'Codex · von Kilian freigegebene Quellen-Ausnahme',
    jsonb_build_object('type','rules','model',u.model,'previousModel',o.model,
      'reason','Zusätzliche Hauptbesuchsbereiche ausschließlich für den freigegebenen GM; bestehende Quellen, Mitarbeiterwerte und Historie erhalten')
  FROM updated u JOIN original o ON o.wave_id=u.wave_id
  RETURNING id,wave_id,revision
)
SELECT wave_id,revision,id AS audit_event_id FROM audited;
COMMIT;
