-- Additive only: no existing time entries, answers, photos or tag definitions are rewritten.
begin;
create table public.time_tracking_entry_photos (
 id uuid primary key default gen_random_uuid(),
 entry_id uuid not null references public.time_tracking_entries(id) on delete restrict,
 storage_bucket text not null default 'gm-time-entry-photos' check(storage_bucket = 'gm-time-entry-photos'),
 storage_path text not null, original_file_name text not null,
 mime_type text not null check(mime_type in ('image/jpeg','image/png','image/webp','image/heic','image/heif')),
 byte_size integer not null check(byte_size between 1 and 10485760),
 sha256 text not null check(sha256 ~ '^[a-f0-9]{64}$'), revision integer not null default 1 check(revision > 0),
 uploaded_by_user_id uuid references public.users(id) on delete set null,
 uploaded_at timestamptz not null default now(), updated_at timestamptz not null default now(),
 is_deleted boolean not null default false, deleted_at timestamptz, deleted_by_user_id uuid references public.users(id) on delete set null
);
create unique index time_tracking_entry_photos_storage_path_unique on public.time_tracking_entry_photos(storage_path);
create index time_tracking_entry_photos_entry_idx on public.time_tracking_entry_photos(entry_id,uploaded_at) where is_deleted = false;
create index time_tracking_entry_photos_uploader_idx on public.time_tracking_entry_photos(uploaded_by_user_id);
create index time_tracking_entry_photos_deleter_idx on public.time_tracking_entry_photos(deleted_by_user_id);
create table public.time_tracking_entry_photo_tags (
 id uuid primary key default gen_random_uuid(),
 photo_id uuid not null references public.time_tracking_entry_photos(id) on delete restrict,
 photo_tag_id uuid references public.photo_tags(id) on delete set null,
 label_snapshot text not null, is_deleted boolean not null default false,
 created_by_user_id uuid references public.users(id) on delete set null,
 created_at timestamptz not null default now(), deleted_at timestamptz
);
create unique index time_tracking_entry_photo_tags_active_idx on public.time_tracking_entry_photo_tags(photo_id,photo_tag_id) where is_deleted = false;
create index time_tracking_entry_photo_tags_tag_idx on public.time_tracking_entry_photo_tags(photo_tag_id);
create index time_tracking_entry_photo_tags_creator_idx on public.time_tracking_entry_photo_tags(created_by_user_id);
alter table public.time_tracking_entry_photos enable row level security;
alter table public.time_tracking_entry_photo_tags enable row level security;
revoke all on public.time_tracking_entry_photos, public.time_tracking_entry_photo_tags from public, anon, authenticated;
-- Only the authenticated application backend may access these records and sign temporary file links.
insert into storage.buckets(id,name,public,file_size_limit,allowed_mime_types)
 values ('gm-time-entry-photos','gm-time-entry-photos',false,10485760,array['image/jpeg','image/png','image/webp','image/heic','image/heif']) on conflict(id) do nothing;
do $$ begin
 if exists(select 1 from storage.buckets where id='gm-time-entry-photos' and (public or file_size_limit is distinct from 10485760::bigint or allowed_mime_types is distinct from array['image/jpeg','image/png','image/webp','image/heic','image/heif'])) then
 raise exception 'Existing time-entry photo bucket must be reviewed'; end if;
end $$;
create policy gm_time_entry_photos_private on storage.objects as restrictive for all to anon, authenticated
 using (bucket_id <> 'gm-time-entry-photos') with check (bucket_id <> 'gm-time-entry-photos');
commit;
