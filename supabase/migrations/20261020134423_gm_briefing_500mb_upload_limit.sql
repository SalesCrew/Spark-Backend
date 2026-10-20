-- Only the private weekly-PDF upload limit changes. Existing documents, files,
-- publications, read receipts and access policies are untouched.
begin;
do $$
begin
  if not exists (
    select 1 from storage.buckets
    where id = 'gm-weekly-briefings'
      and public = false
      and allowed_mime_types = array['application/pdf']
      and file_size_limit in (26214400, 524288000)
  ) then
    raise exception 'Expected private PDF briefing bucket must be reviewed';
  end if;
end $$;
update storage.buckets
set file_size_limit = 524288000
where id = 'gm-weekly-briefings';
commit;
