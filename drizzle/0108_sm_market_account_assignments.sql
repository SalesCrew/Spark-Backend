alter table public.sm_markets
  add column if not exists field_service_manager_user_id uuid;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'sm_markets_field_service_manager_user_id_fkey'
      and conrelid = 'public.sm_markets'::regclass
  ) then
    alter table public.sm_markets
      add constraint sm_markets_field_service_manager_user_id_fkey
      foreign key (field_service_manager_user_id)
      references public.users(id)
      on delete restrict;
  end if;
end $$;

create index if not exists sm_markets_field_service_manager_active_idx
  on public.sm_markets(field_service_manager_user_id)
  where is_deleted = false and field_service_manager_user_id is not null;
