-- Persistent Shelf Merchandising message delivery and read-receipt domain.
-- Additive only: no existing user or SM-domain row is updated or deleted.

set lock_timeout = '5s';
set statement_timeout = '60s';

create table public.sm_messages (
  id uuid primary key default gen_random_uuid(),
  idempotency_key text not null,
  subject text not null,
  body text not null,
  sender_user_id uuid not null references public.users(id) on delete restrict,
  sender_name_snapshot text not null,
  sent_at timestamp with time zone not null default now(),
  is_deleted boolean not null default false,
  deleted_at timestamp with time zone,
  created_at timestamp with time zone not null default now(),
  updated_at timestamp with time zone not null default now(),
  constraint sm_messages_idempotency_ck check (btrim(idempotency_key) <> ''),
  constraint sm_messages_subject_ck check (char_length(btrim(subject)) between 1 and 200),
  constraint sm_messages_body_ck check (char_length(btrim(body)) between 1 and 12000),
  constraint sm_messages_sender_name_ck check (char_length(btrim(sender_name_snapshot)) between 1 and 300),
  constraint sm_messages_soft_delete_ck check (
    (is_deleted and deleted_at is not null)
    or (not is_deleted and deleted_at is null)
  )
);

create unique index sm_messages_idempotency_active_unique
  on public.sm_messages (idempotency_key)
  where is_deleted = false;
create index sm_messages_sender_idx
  on public.sm_messages (sender_user_id);
create index sm_messages_sent_active_idx
  on public.sm_messages (sent_at desc)
  where is_deleted = false;

create table public.sm_message_recipients (
  id uuid primary key default gen_random_uuid(),
  message_id uuid not null references public.sm_messages(id) on delete restrict,
  sm_user_id uuid not null references public.users(id) on delete restrict,
  recipient_name_snapshot text not null,
  recipient_email_snapshot text not null,
  delivered_at timestamp with time zone not null default now(),
  read_at timestamp with time zone,
  is_deleted boolean not null default false,
  deleted_at timestamp with time zone,
  created_at timestamp with time zone not null default now(),
  updated_at timestamp with time zone not null default now(),
  constraint sm_message_recipients_name_ck check (
    char_length(btrim(recipient_name_snapshot)) between 1 and 300
  ),
  constraint sm_message_recipients_email_ck check (
    char_length(btrim(recipient_email_snapshot)) between 3 and 500
  ),
  constraint sm_message_recipients_read_time_ck check (
    read_at is null or read_at >= delivered_at
  ),
  constraint sm_message_recipients_soft_delete_ck check (
    (is_deleted and deleted_at is not null)
    or (not is_deleted and deleted_at is null)
  ),
  constraint sm_message_recipients_message_user_unique unique (message_id, sm_user_id)
);

create index sm_message_recipients_message_idx
  on public.sm_message_recipients (message_id);
create index sm_message_recipients_user_idx
  on public.sm_message_recipients (sm_user_id);
create index sm_message_recipients_user_inbox_active_idx
  on public.sm_message_recipients (sm_user_id, delivered_at desc)
  where is_deleted = false;
create index sm_message_recipients_user_unread_active_idx
  on public.sm_message_recipients (sm_user_id, delivered_at desc)
  where is_deleted = false and read_at is null;

create function public.sm_guard_message_mutation()
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
    or new.created_at is distinct from old.created_at
  then
    raise exception using
      errcode = 'P0001',
      message = 'SM message content and sender fields are immutable';
  end if;
  return new;
end;
$$;

create function public.sm_guard_message_recipient_mutation()
returns trigger
language plpgsql
set search_path = ''
as $$
begin
  if new.message_id is distinct from old.message_id
    or new.sm_user_id is distinct from old.sm_user_id
    or new.recipient_name_snapshot is distinct from old.recipient_name_snapshot
    or new.recipient_email_snapshot is distinct from old.recipient_email_snapshot
    or new.delivered_at is distinct from old.delivered_at
    or new.created_at is distinct from old.created_at
  then
    raise exception using
      errcode = 'P0001',
      message = 'SM message recipient identity and delivery fields are immutable';
  end if;
  if old.read_at is not null and new.read_at is distinct from old.read_at then
    raise exception using
      errcode = 'P0001',
      message = 'SM message read timestamp is immutable once set';
  end if;
  return new;
end;
$$;

create trigger sm_messages_immutable_trg
before update on public.sm_messages
for each row execute function public.sm_guard_message_mutation();

create trigger sm_message_recipients_immutable_trg
before update on public.sm_message_recipients
for each row execute function public.sm_guard_message_recipient_mutation();

create trigger sm_messages_soft_delete_fields_trg
before insert or update on public.sm_messages
for each row execute function public.sm_sync_soft_delete_fields();
create trigger sm_messages_reject_hard_delete_trg
before delete on public.sm_messages
for each row execute function public.sm_reject_hard_delete();
create trigger sm_messages_reject_truncate_trg
before truncate on public.sm_messages
for each statement execute function public.sm_reject_hard_delete();

create trigger sm_message_recipients_soft_delete_fields_trg
before insert or update on public.sm_message_recipients
for each row execute function public.sm_sync_soft_delete_fields();
create trigger sm_message_recipients_reject_hard_delete_trg
before delete on public.sm_message_recipients
for each row execute function public.sm_reject_hard_delete();
create trigger sm_message_recipients_reject_truncate_trg
before truncate on public.sm_message_recipients
for each statement execute function public.sm_reject_hard_delete();

alter table public.sm_messages enable row level security;
alter table public.sm_messages force row level security;
alter table public.sm_message_recipients enable row level security;
alter table public.sm_message_recipients force row level security;

revoke all privileges on table public.sm_messages from public, anon, authenticated, service_role;
revoke all privileges on table public.sm_message_recipients from public, anon, authenticated, service_role;
grant select, insert, update on table public.sm_messages to service_role;
grant select, insert, update on table public.sm_message_recipients to service_role;

revoke all on function public.sm_guard_message_mutation() from public, anon, authenticated;
revoke all on function public.sm_guard_message_recipient_mutation() from public, anon, authenticated;
grant execute on function public.sm_guard_message_mutation() to service_role;
grant execute on function public.sm_guard_message_recipient_mutation() to service_role;

comment on table public.sm_messages is
  'Immutable Shelf Merchandising admin messages with sender snapshots and retry-safe identities.';
comment on table public.sm_message_recipients is
  'Immutable per-SM delivery rows; read_at is set once by the addressed SM through the backend.';
