-- Persist the operational SM assignment independently from the employee name
-- imported from the market workbook. The nullable FK keeps existing imports
-- valid while preventing accidental references to non-existing users.
alter table public.sm_markets
  add column if not exists assigned_sm_user_id uuid;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'sm_markets_assigned_sm_user_id_fk'
      and conrelid = 'public.sm_markets'::regclass
  ) then
    alter table public.sm_markets
      add constraint sm_markets_assigned_sm_user_id_fk
      foreign key (assigned_sm_user_id)
      references public.users(id)
      on delete restrict;
  end if;
end
$$;

create index if not exists sm_markets_assigned_sm_user_active_idx
  on public.sm_markets (assigned_sm_user_id)
  where is_deleted = false and assigned_sm_user_id is not null;
