create index if not exists visit_sessions_submitted_period_market_gm_idx
  on visit_sessions (submitted_at, market_id, gm_user_id)
  where is_deleted = false
    and status = 'submitted'
    and submitted_at is not null;

create index if not exists visit_answers_session_question_changed_active_idx
  on visit_answers (visit_session_id, question_id, changed_at desc, updated_at desc, created_at desc)
  where is_deleted = false;

create index if not exists visit_answer_options_answer_order_active_idx
  on visit_answer_options (visit_answer_id, order_index)
  where is_deleted = false;

create index if not exists question_scoring_ipp_question_key_active_idx
  on question_scoring (question_id, score_key)
  where is_deleted = false
    and ipp is not null;

create index if not exists ipp_market_redmonth_results_period_market_active_idx
  on ipp_market_redmonth_results (red_period_start, market_id)
  where is_deleted = false;
