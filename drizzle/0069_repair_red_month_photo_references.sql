create temporary table "_red_month_orphan_photo_clones" on commit drop as
select distinct
  cloned_photo."id" as "photo_id",
  cloned_photo."visit_answer_id" as "answer_id"
from "visit_answer_photos" cloned_photo
inner join "visit_answers" cloned_answer
  on cloned_answer."id" = cloned_photo."visit_answer_id"
inner join "visit_sessions" cloned_session
  on cloned_session."id" = cloned_answer."visit_session_id"
left join "storage"."objects" exact_object
  on exact_object."bucket_id" = cloned_photo."storage_bucket"
 and exact_object."name" = cloned_photo."storage_path"
where cloned_photo."is_deleted" = false
  and cloned_answer."is_deleted" = false
  and cloned_session."is_deleted" = false
  and exact_object."id" is null
  and exists (
    select 1
    from "storage"."objects" source_object
    inner join "visit_answer_photos" source_photo
      on source_photo."storage_bucket" = source_object."bucket_id"
     and source_photo."storage_path" = source_object."name"
     and source_photo."is_deleted" = false
    inner join "visit_answers" source_answer
      on source_answer."id" = source_photo."visit_answer_id"
     and source_answer."is_deleted" = false
    inner join "visit_sessions" source_session
      on source_session."id" = source_answer."visit_session_id"
     and source_session."is_deleted" = false
    where source_object."bucket_id" = cloned_photo."storage_bucket"
      and regexp_replace(source_object."name", '^.*/', '') = regexp_replace(cloned_photo."storage_path", '^.*/', '')
      and source_session."gm_user_id" = cloned_session."gm_user_id"
      and source_session."market_id" = cloned_session."market_id"
      and source_answer."question_id" = cloned_answer."question_id"
      and source_session."created_at" < cloned_session."created_at"
  );

update "visit_answer_photo_tags" photo_tag
set
  "is_deleted" = true,
  "deleted_at" = now(),
  "updated_at" = now()
where photo_tag."is_deleted" = false
  and photo_tag."visit_answer_photo_id" in (
    select "photo_id" from "_red_month_orphan_photo_clones"
  );

update "visit_answer_photos" photo
set
  "is_deleted" = true,
  "deleted_at" = now(),
  "updated_at" = now()
where photo."is_deleted" = false
  and photo."id" in (
    select "photo_id" from "_red_month_orphan_photo_clones"
  );

with affected_answer_state as (
  select
    answer."id" as "answer_id",
    question."required_snapshot" as "is_required",
    coalesce(question."question_config_snapshot" ->> 'tagsEnabled', 'false') = 'true'
      and case
        when jsonb_typeof(question."question_config_snapshot" -> 'tagIds') = 'array'
          then jsonb_array_length(question."question_config_snapshot" -> 'tagIds') > 0
        else false
      end as "tags_required",
    count(photo."id") > 0 as "has_photos",
    coalesce(
      bool_and(
        exists (
          select 1
          from "visit_answer_photo_tags" photo_tag
          where photo_tag."visit_answer_photo_id" = photo."id"
            and photo_tag."is_deleted" = false
        )
      ) filter (where photo."id" is not null),
      true
    ) as "every_photo_has_tag",
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'id', photo."id",
          'bucket', photo."storage_bucket",
          'path', photo."storage_path"
        )
        order by photo."created_at"
      ) filter (where photo."id" is not null),
      '[]'::jsonb
    ) as "storage_json"
  from "visit_answers" answer
  inner join "visit_session_questions" question
    on question."id" = answer."visit_session_question_id"
  left join "visit_answer_photos" photo
    on photo."visit_answer_id" = answer."id"
   and photo."is_deleted" = false
  where answer."id" in (
    select distinct "answer_id" from "_red_month_orphan_photo_clones"
  )
  group by answer."id", question."required_snapshot", question."question_config_snapshot"
)
update "visit_answers" answer
set
  "answer_status" = case
    when not state."has_photos" then 'unanswered'::"visit_answer_status"
    when state."is_required" and state."tags_required" and not state."every_photo_has_tag"
      then 'invalid'::"visit_answer_status"
    else 'answered'::"visit_answer_status"
  end,
  "value_json" = jsonb_set(
    coalesce(answer."value_json", '{}'::jsonb),
    '{storage}',
    state."storage_json",
    true
  ),
  "is_valid" = case
    when state."is_required"
      then state."has_photos" and (not state."tags_required" or state."every_photo_has_tag")
    else true
  end,
  "validation_error" = case
    when state."is_required" and (not state."has_photos" or (state."tags_required" and not state."every_photo_has_tag"))
      then case
        when state."tags_required" then 'Foto-Frage braucht mindestens einen Tag.'
        else null
      end
    else null
  end,
  "answered_at" = case when state."has_photos" then answer."answered_at" else null end,
  "version" = answer."version" + 1,
  "updated_at" = now()
from affected_answer_state state
where answer."id" = state."answer_id";
