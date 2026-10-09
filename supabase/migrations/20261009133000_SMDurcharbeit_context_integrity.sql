-- Additive integrity for the new monthly domain only. No backfill, row rewrite or delete.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
alter table public.sm_smdurcharbeit_assignment_revisions
  add constraint sm_smdurcharbeit_owner_member_unique unique(id,campaign_market_id),
  add constraint sm_smdurcharbeit_owner_user_unique unique(id,sm_user_id);
alter table public.sm_smdurcharbeit_month_targets
  add constraint sm_smdurcharbeit_target_campaign_unique unique(id,campaign_id),
  add constraint sm_smdurcharbeit_target_owner_member_fk foreign key(owner_revision_id,campaign_market_id)
    references public.sm_smdurcharbeit_assignment_revisions(id,campaign_market_id) on delete restrict;
alter table public.sm_smdurcharbeit_visits
  add constraint sm_smdurcharbeit_visit_user_unique unique(id,sm_user_id),
  add constraint sm_smdurcharbeit_visit_owner_user_fk foreign key(owner_revision_id,sm_user_id)
    references public.sm_smdurcharbeit_assignment_revisions(id,sm_user_id) on delete restrict;
alter table public.sm_questionnaire_submissions
  add constraint sm_submission_smdurcharbeit_target_unique unique(id,smdurcharbeit_target_id),
  add constraint sm_submission_smdurcharbeit_author_fk foreign key(smdurcharbeit_visit_id,sm_user_id)
    references public.sm_smdurcharbeit_visits(id,sm_user_id) on delete restrict;
alter table public.sm_smdurcharbeit_month_targets
  add constraint sm_smdurcharbeit_latest_target_fk foreign key(latest_submission_id,id)
    references public.sm_questionnaire_submissions(id,smdurcharbeit_target_id) on delete restrict;
alter table public.sm_smdurcharbeit_visits
  add constraint sm_smdurcharbeit_basis_target_fk foreign key(basis_submission_id,target_id)
    references public.sm_questionnaire_submissions(id,smdurcharbeit_target_id) on delete restrict;
alter table public.sm_smdurcharbeit_time_change_requests
  add constraint sm_smdurcharbeit_request_author_fk foreign key(visit_id,sm_user_id)
    references public.sm_smdurcharbeit_visits(id,sm_user_id) on delete restrict;
alter table public.sm_smdurcharbeit_answer_provenance
  add constraint sm_smdurcharbeit_source_answer_submission_fk foreign key(source_answer_id,source_submission_id)
    references public.sm_question_answers(id,submission_id) on delete restrict,
  add constraint sm_smdurcharbeit_source_distinct_ck check(answer_id<>source_answer_id and source_revision>0);
alter table public.sm_smdurcharbeit_events
  add constraint sm_smdurcharbeit_event_target_campaign_fk foreign key(target_id,campaign_id)
    references public.sm_smdurcharbeit_month_targets(id,campaign_id) on delete restrict,
  add constraint sm_smdurcharbeit_event_visit_target_fk foreign key(visit_id,target_id)
    references public.sm_smdurcharbeit_visits(id,target_id) on delete restrict,
  add constraint sm_smdurcharbeit_event_visit_context_ck check(visit_id is null or target_id is not null);

-- Month/membership checks require joins: a visit keeps its original owner revision
-- after reassignment, so a foreign key to the target's *current* owner would be incorrect.
create function public.sm_smdurcharbeit_validate_context() returns trigger language plpgsql set search_path=public,pg_temp as $$
declare valid boolean;
begin
  if tg_table_name='sm_smdurcharbeit_month_targets' then
    select o.month=p.month into valid from public.sm_smdurcharbeit_assignment_revisions o
      join public.sm_smdurcharbeit_campaign_periods p on p.id=new.period_id where o.id=new.owner_revision_id;
  elsif tg_table_name='sm_smdurcharbeit_visits' then
    select o.campaign_market_id=t.campaign_market_id and o.month=p.month into valid
      from public.sm_smdurcharbeit_month_targets t join public.sm_smdurcharbeit_campaign_periods p on p.id=t.period_id
      join public.sm_smdurcharbeit_assignment_revisions o on o.id=new.owner_revision_id where t.id=new.target_id;
  elsif tg_table_name='sm_smdurcharbeit_answer_provenance' then
    select dst.smdurcharbeit_target_id is not null and dst.smdurcharbeit_target_id=src.smdurcharbeit_target_id into valid
      from public.sm_question_answers a join public.sm_questionnaire_submissions dst on dst.id=a.submission_id
      join public.sm_questionnaire_submissions src on src.id=new.source_submission_id where a.id=new.answer_id;
  elsif tg_table_name='sm_smdurcharbeit_answer_file_links' then
    select dst.smdurcharbeit_target_id is not null and dst.smdurcharbeit_target_id=src.smdurcharbeit_target_id and not f.is_deleted into valid
      from public.sm_question_answers a join public.sm_questionnaire_submissions dst on dst.id=a.submission_id
      join public.sm_question_answer_files f on f.id=new.file_id join public.sm_question_answers origin on origin.id=f.answer_id
      join public.sm_questionnaire_submissions src on src.id=origin.submission_id where a.id=new.answer_id;
  end if;
  if valid is distinct from true then raise exception 'SMDurcharbeit context must retain the same market and calendar month' using errcode='23514'; end if;
  return new;
end $$;
revoke all on function public.sm_smdurcharbeit_validate_context() from public,anon,authenticated;
create trigger sm_smdurcharbeit_target_month_guard before insert or update of owner_revision_id,period_id on public.sm_smdurcharbeit_month_targets for each row execute function public.sm_smdurcharbeit_validate_context();
create trigger sm_smdurcharbeit_visit_context_guard before insert or update of owner_revision_id,target_id on public.sm_smdurcharbeit_visits for each row execute function public.sm_smdurcharbeit_validate_context();
create trigger sm_smdurcharbeit_provenance_context_guard before insert or update of answer_id,source_submission_id on public.sm_smdurcharbeit_answer_provenance for each row execute function public.sm_smdurcharbeit_validate_context();
create trigger sm_smdurcharbeit_photo_context_guard before insert or update of answer_id,file_id on public.sm_smdurcharbeit_answer_file_links for each row execute function public.sm_smdurcharbeit_validate_context();

create function public.sm_smdurcharbeit_guard_identity() returns trigger language plpgsql set search_path=public,pg_temp as $$
declare field text;
begin
  foreach field in array tg_argv loop
    if to_jsonb(new)->field is distinct from to_jsonb(old)->field then
      raise exception 'SMDurcharbeit historical identities are immutable' using errcode='23514';
    end if;
  end loop;
  return new;
end $$;
revoke all on function public.sm_smdurcharbeit_guard_identity() from public,anon,authenticated;
create trigger sm_smdurcharbeit_membership_identity_guard before update on public.sm_smdurcharbeit_campaign_markets for each row execute function public.sm_smdurcharbeit_guard_identity('id','campaign_id','sm_market_id');
create trigger sm_smdurcharbeit_period_identity_guard before update on public.sm_smdurcharbeit_campaign_periods for each row execute function public.sm_smdurcharbeit_guard_identity('id','campaign_id','month');
create trigger sm_smdurcharbeit_owner_identity_guard before update on public.sm_smdurcharbeit_assignment_revisions for each row execute function public.sm_smdurcharbeit_guard_identity('id','campaign_market_id','month','sm_user_id','actor_user_id');
create trigger sm_smdurcharbeit_target_identity_guard before update on public.sm_smdurcharbeit_month_targets for each row execute function public.sm_smdurcharbeit_guard_identity('id','campaign_id','campaign_market_id','period_id');
create trigger sm_smdurcharbeit_visit_identity_guard before update on public.sm_smdurcharbeit_visits for each row execute function public.sm_smdurcharbeit_guard_identity('id','target_id','owner_revision_id','sm_user_id');
alter table public.sm_smdurcharbeit_visit_time_revisions
  add constraint sm_smdurcharbeit_time_duration_ck check(completed_at-started_at<=interval '24 hours' and actual_minutes=round(extract(epoch from completed_at-started_at)/60)::integer and btrim(reason)<>'');
COMMIT;
