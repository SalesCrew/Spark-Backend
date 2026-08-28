-- Per-message SM inbox visibility after a recipient marks the message as read.
-- NULL deliberately preserves the historical behavior (visible indefinitely).
-- New application sends always choose 0 (one-time) or an explicit number of days.

set lock_timeout = '5s';
set statement_timeout = '60s';

alter table public.sm_messages
  add column visible_after_read_days integer;

alter table public.sm_messages
  add constraint sm_messages_visible_after_read_days_ck
  check (visible_after_read_days is null or visible_after_read_days between 0 and 3650);

create or replace function public.sm_guard_message_mutation()
returns trigger
language plpgsql
set search_path = ''
as $$
begin
  if new.idempotency_key is distinct from old.idempotency_key
    or new.subject is distinct from old.subject
    or new.body is distinct from old.body
    or new.sender_user_id is distinct from old.sender_user_id
    or new.sender_name_snapshot is distinct from old.sender_name_snapshot
    or new.sent_at is distinct from old.sent_at
    or new.visible_after_read_days is distinct from old.visible_after_read_days
    or new.created_at is distinct from old.created_at
  then
    raise exception using
      errcode = 'P0001',
      message = 'SM message content, sender, and read visibility fields are immutable';
  end if;
  return new;
end;
$$;

comment on column public.sm_messages.visible_after_read_days is
  'NULL keeps historical messages visible indefinitely; 0 hides immediately after read; positive values keep the message visible for that many 24-hour days after read_at.';

revoke all on function public.sm_guard_message_mutation() from public, anon, authenticated;
grant execute on function public.sm_guard_message_mutation() to service_role;
