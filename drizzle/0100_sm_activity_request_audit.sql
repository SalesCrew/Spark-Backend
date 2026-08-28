-- Complete the already isolated SM questionnaire request tables with durable
-- retry tokens and exact applied-result audit links. No GM table is touched.

set lock_timeout = '5s';
set statement_timeout = '120s';

alter table public.sm_answer_change_requests
  add column client_request_token text,
  add column applied_answer_id uuid,
  add column applied_at timestamptz;

alter table public.sm_questionnaire_submission_delete_requests
  add column client_request_token text,
  add column applied_at timestamptz;

update public.sm_answer_change_requests
set client_request_token = 'legacy:' || id::text
where client_request_token is null;

update public.sm_questionnaire_submission_delete_requests
set client_request_token = 'legacy:' || id::text
where client_request_token is null;

alter table public.sm_answer_change_requests
  alter column client_request_token set not null,
  add constraint sm_answer_change_requests_applied_answer_submission_fk
    foreign key (applied_answer_id, submission_id)
    references public.sm_question_answers(id, submission_id)
    on delete restrict,
  add constraint sm_answer_change_requests_client_token_ck
    check (btrim(client_request_token) <> '');

alter table public.sm_questionnaire_submission_delete_requests
  alter column client_request_token set not null,
  add constraint sm_questionnaire_submission_delete_requests_client_token_ck
    check (btrim(client_request_token) <> '');

alter table public.sm_answer_change_requests
  drop constraint sm_answer_change_requests_review_ck,
  add constraint sm_answer_change_requests_review_ck check (
    (status in ('pending', 'cancelled') and reviewed_by_user_id is null and reviewed_at is null and applied_answer_id is null and applied_at is null)
    or (status = 'rejected' and reviewed_by_user_id is not null and reviewed_at is not null and applied_answer_id is null and applied_at is null)
    or (status = 'approved' and reviewed_by_user_id is not null and reviewed_at is not null and applied_answer_id is not null and applied_at is not null)
  );

alter table public.sm_questionnaire_submission_delete_requests
  drop constraint sm_questionnaire_submission_delete_requests_review_ck,
  add constraint sm_questionnaire_submission_delete_requests_review_ck check (
    (status in ('pending', 'cancelled') and reviewed_by_user_id is null and reviewed_at is null and applied_at is null)
    or (status = 'rejected' and reviewed_by_user_id is not null and reviewed_at is not null and applied_at is null)
    or (status = 'approved' and reviewed_by_user_id is not null and reviewed_at is not null and applied_at is not null)
  );

create unique index sm_answer_change_requests_client_token_active_unique
  on public.sm_answer_change_requests (sm_user_id, client_request_token)
  where is_deleted = false;
create index sm_answer_change_requests_applied_answer_idx
  on public.sm_answer_change_requests (applied_answer_id);

create unique index sm_questionnaire_submission_delete_requests_client_token_active_unique
  on public.sm_questionnaire_submission_delete_requests (sm_user_id, client_request_token)
  where is_deleted = false;

comment on column public.sm_answer_change_requests.client_request_token is
  'SM-scoped idempotency token; retries replay the same correction request.';
comment on column public.sm_answer_change_requests.applied_answer_id is
  'Exact immutable answer revision created when the request was approved.';
comment on column public.sm_questionnaire_submission_delete_requests.client_request_token is
  'SM-scoped idempotency token; retries replay the same questionnaire deletion request.';
