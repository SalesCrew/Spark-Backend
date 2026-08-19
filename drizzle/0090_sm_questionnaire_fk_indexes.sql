-- Cover the complete column order of the six composite SM questionnaire FKs.
-- These tables are empty at introduction time, so ordinary index creation is
-- short-lived and keeps the migration transaction-safe.

set lock_timeout = '5s';
set statement_timeout = '120s';

create index sm_questionnaire_submissions_template_version_idx
  on public.sm_questionnaire_submissions(questionnaire_version_id, questionnaire_template_id);

create index sm_questionnaire_submission_questions_section_submission_idx
  on public.sm_questionnaire_submission_questions(submission_section_id, submission_id);

create index sm_question_answers_question_submission_idx
  on public.sm_question_answers(submission_question_id, submission_id);

create index sm_question_answer_events_answer_submission_idx
  on public.sm_question_answer_events(answer_id, submission_id);

create index sm_answer_change_requests_question_submission_idx
  on public.sm_answer_change_requests(submission_question_id, submission_id);

create index sm_answer_change_requests_answer_submission_idx
  on public.sm_answer_change_requests(original_answer_id, submission_id);
