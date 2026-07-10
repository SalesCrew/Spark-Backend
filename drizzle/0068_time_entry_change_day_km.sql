alter type "time_entry_change_request_source_kind" add value if not exists 'day_km';

alter table "time_entry_change_requests"
  add column if not exists "original_start_km" integer,
  add column if not exists "original_end_km" integer,
  add column if not exists "requested_start_km" integer,
  add column if not exists "requested_end_km" integer;
