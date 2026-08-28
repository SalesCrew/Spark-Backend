-- Cover the composite audit FK added by 0100. This only replaces the
-- first-column index created there; no request or questionnaire data changes.

set lock_timeout = '5s';
set statement_timeout = '120s';

drop index if exists public.sm_answer_change_requests_applied_answer_idx;

create index sm_answer_change_requests_applied_answer_submission_idx
  on public.sm_answer_change_requests (applied_answer_id, submission_id);
