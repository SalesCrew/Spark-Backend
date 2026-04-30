CREATE TABLE IF NOT EXISTS fragebogen_main_spezial_items (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  fragebogen_id uuid NOT NULL REFERENCES fragebogen_main(id) ON DELETE CASCADE,
  question_type fragebogen_question_type NOT NULL,
  text text NOT NULL DEFAULT '',
  required boolean NOT NULL DEFAULT true,
  chains text[],
  config jsonb NOT NULL DEFAULT '{}'::jsonb,
  rules jsonb NOT NULL DEFAULT '[]'::jsonb,
  scoring jsonb NOT NULL DEFAULT '{}'::jsonb,
  order_index integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS fragebogen_main_spezial_items_fb_idx
  ON fragebogen_main_spezial_items(fragebogen_id);
CREATE INDEX IF NOT EXISTS fragebogen_main_spezial_items_order_idx
  ON fragebogen_main_spezial_items(fragebogen_id, order_index);

INSERT INTO fragebogen_main_spezial_items (
  fragebogen_id,
  question_type,
  text,
  required,
  chains,
  config,
  rules,
  scoring,
  order_index,
  created_at,
  updated_at
)
SELECT
  link.fragebogen_id,
  q.question_type,
  q.text,
  q.required,
  q.chains,
  q.config,
  COALESCE(
    (
      SELECT jsonb_agg(
        jsonb_build_object(
          'id', qr.id,
          'triggerQuestionId', qr.trigger_question_id,
          'operator', qr.operator,
          'triggerValue', qr.trigger_value,
          'triggerValueMax', qr.trigger_value_max,
          'action', qr.action,
          'targetQuestionIds',
            COALESCE(
              (
                SELECT jsonb_agg(qrt.target_question_id ORDER BY qrt.order_index)
                FROM question_rule_targets qrt
                WHERE qrt.rule_id = qr.id
              ),
              '[]'::jsonb
            )
        )
        ORDER BY qr.order_index
      )
      FROM question_rules qr
      WHERE qr.question_id = q.id
    ),
    q.rules,
    '[]'::jsonb
  ),
  COALESCE(
    (
      SELECT jsonb_object_agg(
        qs.score_key,
        jsonb_strip_nulls(
          jsonb_build_object(
            'ipp', CASE WHEN qs.ipp IS NOT NULL THEN (qs.ipp)::numeric END,
            'boni', CASE WHEN qs.boni IS NOT NULL THEN (qs.boni)::numeric END
          )
        )
      )
      FROM question_scoring qs
      WHERE qs.question_id = q.id
    ),
    q.scoring,
    '{}'::jsonb
  ),
  link.order_index,
  now(),
  now()
FROM fragebogen_main_spezial_question link
JOIN question_bank_shared q ON q.id = link.question_id
WHERE NOT EXISTS (
  SELECT 1
  FROM fragebogen_main_spezial_items existing
  WHERE existing.fragebogen_id = link.fragebogen_id
    AND existing.order_index = link.order_index
    AND existing.text = q.text
    AND existing.question_type = q.question_type
);

REVOKE ALL ON TABLE public.fragebogen_main_spezial_items FROM anon, authenticated;
GRANT SELECT ON TABLE public.fragebogen_main_spezial_items TO authenticated;
ALTER TABLE public.fragebogen_main_spezial_items ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS fragebogen_main_spezial_items_authenticated_select ON public.fragebogen_main_spezial_items;
CREATE POLICY fragebogen_main_spezial_items_authenticated_select
  ON public.fragebogen_main_spezial_items
  FOR SELECT
  TO authenticated
  USING (true);
DROP POLICY IF EXISTS fragebogen_main_spezial_items_service_role_full ON public.fragebogen_main_spezial_items;
CREATE POLICY fragebogen_main_spezial_items_service_role_full
  ON public.fragebogen_main_spezial_items
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);
