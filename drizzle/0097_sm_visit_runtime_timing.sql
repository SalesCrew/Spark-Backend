-- Additive timing state for the SM Marktbesuch runtime. Existing submissions
-- remain valid because every new value is nullable.

alter table public.sm_questionnaire_submissions
  add column if not exists visit_time_mode text,
  add column if not exists travel_minutes integer,
  add column if not exists manual_visit_minutes integer;

alter table public.sm_questionnaire_submissions
  drop constraint if exists sm_questionnaire_submissions_visit_mode_ck,
  add constraint sm_questionnaire_submissions_visit_mode_ck
    check (visit_time_mode is null or visit_time_mode in ('timer', 'manual')),
  drop constraint if exists sm_questionnaire_submissions_travel_minutes_ck,
  add constraint sm_questionnaire_submissions_travel_minutes_ck
    check (travel_minutes is null or (travel_minutes >= 0 and travel_minutes <= 1440)),
  drop constraint if exists sm_questionnaire_submissions_manual_minutes_ck,
  add constraint sm_questionnaire_submissions_manual_minutes_ck
    check (
      (visit_time_mode = 'manual' and (
        manual_visit_minutes is null
        or (manual_visit_minutes > 0 and manual_visit_minutes <= 1440)
      ))
      or (visit_time_mode is distinct from 'manual' and manual_visit_minutes is null)
    );

comment on column public.sm_questionnaire_submissions.visit_time_mode is
  'SM visit duration capture mode. Timer uses server timestamps; manual is completed during review.';
comment on column public.sm_questionnaire_submissions.travel_minutes is
  'Optional Fahrtzeit in minutes. Available only through the SM visit runtime for Fahrtzeiten-enabled users.';
comment on column public.sm_questionnaire_submissions.manual_visit_minutes is
  'Optional while drafting a manual visit; required by the runtime before final submission.';

-- The table is already RLS-enabled, forced, and restricted to the trusted
-- backend role by the questionnaire-domain migrations. Reassert that posture
-- so a partially provisioned environment cannot expose the new columns.
alter table public.sm_questionnaire_submissions enable row level security;
alter table public.sm_questionnaire_submissions force row level security;
revoke all on table public.sm_questionnaire_submissions from anon, authenticated;
