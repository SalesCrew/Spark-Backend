alter table "users"
  add column if not exists "profile_photo_bucket" text,
  add column if not exists "profile_photo_path" text,
  add column if not exists "profile_photo_updated_at" timestamptz;

insert into storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
values (
  'gm-profile-photos',
  'gm-profile-photos',
  false,
  5242880,
  array['image/jpeg', 'image/png', 'image/webp', 'image/heic', 'image/heif']::text[]
)
on conflict (id) do update
set
  public = excluded.public,
  file_size_limit = excluded.file_size_limit,
  allowed_mime_types = excluded.allowed_mime_types;
