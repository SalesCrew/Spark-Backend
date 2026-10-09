-- SMDurcharbeit SM identity link only. Name matching and backfill are separate work.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE public.sm_smdurcharbeit_markets
  ADD COLUMN smdurcharbeit_sm_user_id uuid,
  ADD CONSTRAINT sm_smdurcharbeit_market_sm_user_fk
    FOREIGN KEY (smdurcharbeit_sm_user_id)
    REFERENCES public.users(id) ON DELETE RESTRICT;

CREATE INDEX sm_smdurcharbeit_market_sm_user_idx
  ON public.sm_smdurcharbeit_markets (smdurcharbeit_sm_user_id)
  WHERE smdurcharbeit_sm_user_id IS NOT NULL;

COMMENT ON COLUMN public.sm_smdurcharbeit_markets.smdurcharbeit_sm_user_id IS
  'SMDurcharbeit reviewed SM account link. NULL until separately authorized matching/backfill; the imported Verplanung name remains unchanged.';

COMMIT;
