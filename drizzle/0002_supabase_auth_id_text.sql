ALTER TABLE "users"
  ALTER COLUMN "supabase_auth_id" TYPE text
  USING "supabase_auth_id"::text;
