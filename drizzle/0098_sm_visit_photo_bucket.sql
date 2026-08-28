-- Private bucket for SM Marktbesuch photo answers. Uploads use short-lived
-- signed URLs issued only after backend ownership checks.

insert into storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
values (
  'sm-visit-photos',
  'sm-visit-photos',
  false,
  15728640,
  array['image/jpeg', 'image/png', 'image/webp']::text[]
)
on conflict (id) do update
set public = false,
    file_size_limit = excluded.file_size_limit,
    allowed_mime_types = excluded.allowed_mime_types;
