alter table "time_entry_change_requests"
  add column if not exists "requested_activity_type" "time_tracking_activity_type";
