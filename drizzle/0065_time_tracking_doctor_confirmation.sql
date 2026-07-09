alter table "time_tracking_entries"
  add column if not exists "doctor_confirmation_bucket" text,
  add column if not exists "doctor_confirmation_path" text,
  add column if not exists "doctor_confirmation_file_name" text,
  add column if not exists "doctor_confirmation_mime_type" text,
  add column if not exists "doctor_confirmation_byte_size" integer,
  add column if not exists "doctor_confirmation_uploaded_at" timestamp with time zone;
