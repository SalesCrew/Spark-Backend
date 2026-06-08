create index if not exists visit_sessions_gm_status_started_idx
  on visit_sessions (gm_user_id, status, started_at)
  where is_deleted = false;
