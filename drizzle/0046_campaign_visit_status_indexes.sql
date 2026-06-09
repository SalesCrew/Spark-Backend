create index if not exists campaign_market_assignments_campaign_market_active_idx
  on campaign_market_assignments (campaign_id, market_id)
  where is_deleted = false;

create index if not exists visit_sessions_market_submitted_active_idx
  on visit_sessions (market_id, submitted_at desc, created_at desc)
  where is_deleted = false
    and status = 'submitted'
    and submitted_at is not null;

create index if not exists visit_session_sections_campaign_session_active_idx
  on visit_session_sections (campaign_id, visit_session_id)
  where is_deleted = false;
