begin;
set local lock_timeout = '3s';
alter table public.gm_action_briefings
  add column action_from date,
  add column action_until date,
  add constraint gm_action_briefing_action_dates_order
    check (action_from is null or action_until is null or action_until >= action_from);
-- Publication action dates are part of the existing immutable JSON snapshot.
-- Existing drafts/publications keep unknown action dates; no dates are inferred.
commit;
