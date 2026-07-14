-- Keep module-independent Spezialfragen feature-compatible with normal questions.

ALTER TABLE "public"."fragebogen_main_spezial_items"
  ADD COLUMN IF NOT EXISTS "red_survey" boolean,
  ADD COLUMN IF NOT EXISTS "single_choice_availability" boolean,
  ADD COLUMN IF NOT EXISTS "single_choice_availability_type" text;
