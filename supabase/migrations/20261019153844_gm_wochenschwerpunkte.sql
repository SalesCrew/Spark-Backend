begin;
create table public.gm_weekly_briefings (
 id uuid primary key default gen_random_uuid(), title text not null check (length(title) between 1 and 160), description text not null default '' check (length(description) <= 2000),
 file jsonb not null check (jsonb_typeof(file) = 'object' and (file->>'pageCount')::int between 1 and 80), pages jsonb not null check (jsonb_typeof(pages) = 'array' and jsonb_array_length(pages) between 1 and 80),
 visible_from date, visible_until date, draft_revision integer not null default 1 check (draft_revision > 0), archived_at timestamptz,
 created_by_user_id uuid references public.users(id) on delete set null, created_at timestamptz not null default now(), updated_at timestamptz not null default now(),
 check (visible_from is null or visible_until is null or visible_until >= visible_from)
);
create index gm_weekly_briefings_creator_idx on public.gm_weekly_briefings(created_by_user_id);
create table public.gm_weekly_briefing_publications (
 id uuid primary key default gen_random_uuid(), briefing_id uuid not null references public.gm_weekly_briefings(id) on delete restrict,
 version integer not null check (version > 0), source_draft_revision integer not null check (source_draft_revision > 0), snapshot jsonb not null check (jsonb_typeof(snapshot) = 'object'),
 visible_from date not null, visible_until date, operation_id uuid not null unique, published_by_user_id uuid references public.users(id) on delete set null,
 published_at timestamptz not null default now(), check (visible_until is null or visible_until >= visible_from)
);
create unique index gm_weekly_briefing_version_idx on public.gm_weekly_briefing_publications(briefing_id,version);
create index gm_weekly_briefing_publisher_idx on public.gm_weekly_briefing_publications(published_by_user_id);
create index gm_weekly_briefing_schedule_idx on public.gm_weekly_briefing_publications(visible_from);
create table public.gm_weekly_briefing_reads (
 publication_id uuid not null references public.gm_weekly_briefing_publications(id) on delete restrict,
 gm_user_id uuid not null references public.users(id) on delete cascade,
 seen_pages integer[] not null default '{}', read_at timestamptz, updated_at timestamptz not null default now(), primary key(publication_id,gm_user_id)
);
create index gm_weekly_briefing_reader_idx on public.gm_weekly_briefing_reads(gm_user_id);
alter table public.gm_weekly_briefings enable row level security;
alter table public.gm_weekly_briefing_publications enable row level security;
alter table public.gm_weekly_briefing_reads enable row level security;
revoke all on public.gm_weekly_briefings, public.gm_weekly_briefing_publications, public.gm_weekly_briefing_reads from public, anon, authenticated;
-- Backend-only access. No worker JWT or anonymous storage policy can expose these PDFs.
insert into storage.buckets(id,name,public,file_size_limit,allowed_mime_types) values ('gm-weekly-briefings','gm-weekly-briefings',false,26214400,array['application/pdf']) on conflict (id) do nothing;
do $$ begin
 if exists (select 1 from storage.buckets where id='gm-weekly-briefings' and (public or file_size_limit is distinct from 26214400::bigint or allowed_mime_types is distinct from array['application/pdf'])) then raise exception 'Existing briefing bucket configuration must be reviewed'; end if;
end $$;
create policy gm_weekly_briefings_private on storage.objects as restrictive for all to anon, authenticated using (bucket_id <> 'gm-weekly-briefings') with check (bucket_id <> 'gm-weekly-briefings');
create schema if not exists private;
create function private.protect_gm_weekly_publication() returns trigger language plpgsql set search_path = '' as $$ begin
 -- Allow account erasure to remove the nullable actor link while preserving the publication itself.
 if tg_op = 'UPDATE' and new.published_by_user_id is null and old.published_by_user_id is not null and (to_jsonb(new) - 'published_by_user_id') = (to_jsonb(old) - 'published_by_user_id') then return new; end if;
 raise exception 'Published GM briefings are immutable';
end $$;
revoke all on function private.protect_gm_weekly_publication() from public, anon, authenticated;
create trigger gm_weekly_publication_immutable before update or delete on public.gm_weekly_briefing_publications for each row execute function private.protect_gm_weekly_publication();
commit;
