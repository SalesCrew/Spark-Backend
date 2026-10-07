-- Additive registry only. Empty on creation; no historical market/visit rewrites.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
CREATE TABLE public.sm_smdurcharbeit_markets (
  sm_market_id uuid PRIMARY KEY REFERENCES public.sm_markets(id) ON DELETE RESTRICT,
  created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.sm_smdurcharbeit_markets ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.sm_smdurcharbeit_markets FROM PUBLIC, anon, authenticated;
GRANT SELECT ON public.sm_smdurcharbeit_markets TO service_role;
COMMENT ON TABLE public.sm_smdurcharbeit_markets IS 'SMDurcharbeit dedicated market registry; populated only by a separately approved import.';
COMMIT;
