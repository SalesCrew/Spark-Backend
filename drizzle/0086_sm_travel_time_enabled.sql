ALTER TABLE "users"
  ADD COLUMN IF NOT EXISTS "sm_travel_time_enabled" boolean DEFAULT false NOT NULL;
