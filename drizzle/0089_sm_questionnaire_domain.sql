-- Independent Shelf Merchandising questionnaire domain.
-- Additive only: this migration does not alter or read GM questionnaire,
-- campaign, visit-session, answer, market, or time-tracking data.
-- All new tables, enum types, indexes, constraints, triggers, and helper
-- functions are prefixed with `sm_`.

set lock_timeout = '5s';
set statement_timeout = '120s';

create type public.sm_question_type as enum (
  'single',
  'yesno',
  'yesnomulti',
  'multiple',
  'likert',
  'text',
  'numeric',
  'slider',
  'photo',
  'matrix'
);

create type public.sm_content_version_status as enum ('draft', 'published');
create type public.sm_questionnaire_template_status as enum ('active', 'inactive', 'archived');
create type public.sm_questionnaire_version_status as enum ('draft', 'published');
create type public.sm_metric_role as enum (
  'none',
  'execution',
  'context',
  'oos_detection',
  'oos_remediation',
  'information',
  'free_text'
);
create type public.sm_oos_category as enum (
  'action_placements',
  'softdrinks_energy',
  'water_near_water',
  'juice_iced_tea'
);
create type public.sm_logic_operator as enum (
  'equals',
  'not_equals',
  'contains',
  'not_contains',
  'greater_than',
  'less_than',
  'between',
  'is_answered',
  'is_not_answered'
);
create type public.sm_logic_action as enum ('show', 'hide');
create type public.sm_submission_status as enum ('draft', 'submitted', 'invalidated', 'cancelled');
create type public.sm_answer_state as enum ('unanswered', 'answered', 'not_applicable', 'invalidated');
create type public.sm_answer_event_type as enum ('set', 'clear', 'state_change', 'correction');
create type public.sm_change_request_status as enum ('pending', 'approved', 'rejected', 'cancelled');

create table public.sm_questions (
  id uuid primary key default gen_random_uuid(),
  stable_code text not null,
  created_by_user_id uuid references public.users(id) on delete set null,
  updated_by_user_id uuid references public.users(id) on delete set null,
  is_deleted boolean not null default false,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sm_questions_stable_code_ck check (
    stable_code = lower(btrim(stable_code))
    and stable_code ~ '^[a-z0-9][a-z0-9_-]*$'
  )
);

create unique index sm_questions_stable_code_active_unique
  on public.sm_questions(stable_code)
  where is_deleted = false;
create index sm_questions_created_by_idx on public.sm_questions(created_by_user_id);
create index sm_questions_updated_by_idx on public.sm_questions(updated_by_user_id);
create index sm_questions_active_updated_idx
  on public.sm_questions(updated_at desc)
  where is_deleted = false;

create table public.sm_question_versions (
  id uuid primary key default gen_random_uuid(),
  question_id uuid not null references public.sm_questions(id) on delete restrict,
  version_number integer not null,
  status public.sm_content_version_status not null default 'draft',
  question_type public.sm_question_type not null,
  question_text text not null,
  required boolean not null default true,
  metric_role public.sm_metric_role not null default 'none',
  oos_category public.sm_oos_category,
  max_points numeric(10, 4) not null default 0,
  config jsonb not null default '{}'::jsonb,
  metric_config jsonb not null default '{}'::jsonb,
  published_at timestamptz,
  created_by_user_id uuid references public.users(id) on delete set null,
  is_deleted boolean not null default false,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sm_question_versions_version_ck check (version_number >= 1),
  constraint sm_question_versions_text_ck check (btrim(question_text) <> ''),
  constraint sm_question_versions_points_ck check (max_points >= 0),
  constraint sm_question_versions_config_ck check (jsonb_typeof(config) = 'object'),
  constraint sm_question_versions_metric_config_ck check (jsonb_typeof(metric_config) = 'object'),
  constraint sm_question_versions_oos_category_ck check (
    metric_role not in ('oos_detection', 'oos_remediation') or oos_category is not null
  ),
  constraint sm_question_versions_publish_state_ck check (
    (status = 'draft' and published_at is null)
    or (status = 'published' and published_at is not null)
  ),
  constraint sm_question_versions_question_version_unique unique (question_id, version_number)
);

create index sm_question_versions_question_status_idx
  on public.sm_question_versions(question_id, status, version_number desc)
  where is_deleted = false;
create index sm_question_versions_type_active_idx
  on public.sm_question_versions(question_type, updated_at desc)
  where is_deleted = false;
create index sm_question_versions_metric_active_idx
  on public.sm_question_versions(metric_role, oos_category)
  where is_deleted = false and metric_role <> 'none';
create index sm_question_versions_created_by_idx on public.sm_question_versions(created_by_user_id);

create table public.sm_answer_option_versions (
  id uuid primary key default gen_random_uuid(),
  question_version_id uuid not null references public.sm_question_versions(id) on delete restrict,
  stable_code text not null,
  label text not null,
  earned_points numeric(10, 4) not null default 0,
  possible_points numeric(10, 4) not null default 0,
  metric_outcome_code text,
  marks_not_applicable boolean not null default false,
  counts_in_denominator boolean not null default true,
  order_index integer not null default 0,
  config jsonb not null default '{}'::jsonb,
  is_deleted boolean not null default false,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sm_answer_option_versions_code_ck check (
    stable_code = lower(btrim(stable_code))
    and stable_code ~ '^[a-z0-9][a-z0-9_-]*$'
  ),
  constraint sm_answer_option_versions_label_ck check (btrim(label) <> ''),
  constraint sm_answer_option_versions_order_ck check (order_index >= 0),
  constraint sm_answer_option_versions_points_ck check (
    earned_points >= 0 and possible_points >= 0 and earned_points <= possible_points
  ),
  constraint sm_answer_option_versions_na_ck check (
    not marks_not_applicable
    or (earned_points = 0 and possible_points = 0 and counts_in_denominator = false)
  ),
  constraint sm_answer_option_versions_config_ck check (jsonb_typeof(config) = 'object')
);

create unique index sm_answer_option_versions_code_active_unique
  on public.sm_answer_option_versions(question_version_id, stable_code)
  where is_deleted = false;
create unique index sm_answer_option_versions_order_active_unique
  on public.sm_answer_option_versions(question_version_id, order_index)
  where is_deleted = false;
create index sm_answer_option_versions_question_idx
  on public.sm_answer_option_versions(question_version_id, order_index);

create table public.sm_question_logic_rules (
  id uuid primary key default gen_random_uuid(),
  trigger_question_version_id uuid not null references public.sm_question_versions(id) on delete restrict,
  operator public.sm_logic_operator not null,
  trigger_value jsonb,
  trigger_value_max jsonb,
  action public.sm_logic_action not null,
  group_code text not null default 'default',
  group_match text not null default 'all',
  order_index integer not null default 0,
  is_deleted boolean not null default false,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sm_question_logic_rules_group_code_ck check (btrim(group_code) <> ''),
  constraint sm_question_logic_rules_group_match_ck check (group_match in ('all', 'any')),
  constraint sm_question_logic_rules_order_ck check (order_index >= 0),
  constraint sm_question_logic_rules_between_ck check (
    operator <> 'between' or (trigger_value is not null and trigger_value_max is not null)
  )
);

create index sm_question_logic_rules_trigger_active_idx
  on public.sm_question_logic_rules(trigger_question_version_id, order_index)
  where is_deleted = false;

create table public.sm_question_logic_rule_targets (
  id uuid primary key default gen_random_uuid(),
  rule_id uuid not null references public.sm_question_logic_rules(id) on delete restrict,
  target_question_version_id uuid not null references public.sm_question_versions(id) on delete restrict,
  order_index integer not null default 0,
  is_deleted boolean not null default false,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sm_question_logic_rule_targets_order_ck check (order_index >= 0)
);

create unique index sm_question_logic_rule_targets_active_unique
  on public.sm_question_logic_rule_targets(rule_id, target_question_version_id)
  where is_deleted = false;
create index sm_question_logic_rule_targets_target_idx
  on public.sm_question_logic_rule_targets(target_question_version_id);

create table public.sm_modules (
  id uuid primary key default gen_random_uuid(),
  stable_code text not null,
  created_by_user_id uuid references public.users(id) on delete set null,
  updated_by_user_id uuid references public.users(id) on delete set null,
  is_deleted boolean not null default false,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sm_modules_stable_code_ck check (
    stable_code = lower(btrim(stable_code))
    and stable_code ~ '^[a-z0-9][a-z0-9_-]*$'
  )
);

create unique index sm_modules_stable_code_active_unique
  on public.sm_modules(stable_code)
  where is_deleted = false;
create index sm_modules_created_by_idx on public.sm_modules(created_by_user_id);
create index sm_modules_updated_by_idx on public.sm_modules(updated_by_user_id);
create index sm_modules_active_updated_idx
  on public.sm_modules(updated_at desc)
  where is_deleted = false;

create table public.sm_module_versions (
  id uuid primary key default gen_random_uuid(),
  module_id uuid not null references public.sm_modules(id) on delete restrict,
  version_number integer not null,
  status public.sm_content_version_status not null default 'draft',
  name text not null,
  description text not null default '',
  published_at timestamptz,
  created_by_user_id uuid references public.users(id) on delete set null,
  is_deleted boolean not null default false,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sm_module_versions_version_ck check (version_number >= 1),
  constraint sm_module_versions_name_ck check (btrim(name) <> ''),
  constraint sm_module_versions_publish_state_ck check (
    (status = 'draft' and published_at is null)
    or (status = 'published' and published_at is not null)
  ),
  constraint sm_module_versions_module_version_unique unique (module_id, version_number)
);

create index sm_module_versions_module_status_idx
  on public.sm_module_versions(module_id, status, version_number desc)
  where is_deleted = false;
create index sm_module_versions_created_by_idx on public.sm_module_versions(created_by_user_id);

create table public.sm_module_version_questions (
  id uuid primary key default gen_random_uuid(),
  module_version_id uuid not null references public.sm_module_versions(id) on delete restrict,
  question_version_id uuid not null references public.sm_question_versions(id) on delete restrict,
  order_index integer not null default 0,
  is_deleted boolean not null default false,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sm_module_version_questions_order_ck check (order_index >= 0)
);

create unique index sm_module_version_questions_question_active_unique
  on public.sm_module_version_questions(module_version_id, question_version_id)
  where is_deleted = false;
create unique index sm_module_version_questions_order_active_unique
  on public.sm_module_version_questions(module_version_id, order_index)
  where is_deleted = false;
create index sm_module_version_questions_question_idx
  on public.sm_module_version_questions(question_version_id);

create table public.sm_questionnaire_templates (
  id uuid primary key default gen_random_uuid(),
  stable_code text not null,
  status public.sm_questionnaire_template_status not null default 'active',
  created_by_user_id uuid references public.users(id) on delete set null,
  updated_by_user_id uuid references public.users(id) on delete set null,
  is_deleted boolean not null default false,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sm_questionnaire_templates_stable_code_ck check (
    stable_code = lower(btrim(stable_code))
    and stable_code ~ '^[a-z0-9][a-z0-9_-]*$'
  )
);

create unique index sm_questionnaire_templates_stable_code_active_unique
  on public.sm_questionnaire_templates(stable_code)
  where is_deleted = false;
create index sm_questionnaire_templates_status_updated_idx
  on public.sm_questionnaire_templates(status, updated_at desc)
  where is_deleted = false;
create index sm_questionnaire_templates_created_by_idx
  on public.sm_questionnaire_templates(created_by_user_id);
create index sm_questionnaire_templates_updated_by_idx
  on public.sm_questionnaire_templates(updated_by_user_id);

create table public.sm_questionnaire_versions (
  id uuid primary key default gen_random_uuid(),
  questionnaire_template_id uuid not null references public.sm_questionnaire_templates(id) on delete restrict,
  version_number integer not null,
  status public.sm_questionnaire_version_status not null default 'draft',
  name text not null,
  description text not null default '',
  once_per_market boolean not null default false,
  effective_from date,
  effective_to date,
  timezone text not null default 'Europe/Vienna',
  published_at timestamptz,
  published_by_user_id uuid references public.users(id) on delete set null,
  content_hash text,
  is_deleted boolean not null default false,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sm_questionnaire_versions_version_ck check (version_number >= 1),
  constraint sm_questionnaire_versions_name_ck check (btrim(name) <> ''),
  constraint sm_questionnaire_versions_timezone_ck check (btrim(timezone) <> ''),
  constraint sm_questionnaire_versions_dates_ck check (
    effective_from is null or effective_to is null or effective_to >= effective_from
  ),
  constraint sm_questionnaire_versions_publish_state_ck check (
    (status = 'draft' and published_at is null and content_hash is null)
    or (
      status = 'published'
      and published_at is not null
      and content_hash is not null
      and btrim(content_hash) <> ''
    )
  ),
  constraint sm_questionnaire_versions_id_template_unique unique (
    id,
    questionnaire_template_id
  ),
  constraint sm_questionnaire_versions_template_version_unique unique (
    questionnaire_template_id,
    version_number
  )
);

create unique index sm_questionnaire_versions_one_draft_unique
  on public.sm_questionnaire_versions(questionnaire_template_id)
  where is_deleted = false and status = 'draft';
create index sm_questionnaire_versions_template_published_idx
  on public.sm_questionnaire_versions(questionnaire_template_id, version_number desc)
  where is_deleted = false and status = 'published';
create index sm_questionnaire_versions_effective_idx
  on public.sm_questionnaire_versions(effective_from, effective_to)
  where is_deleted = false and status = 'published';
create index sm_questionnaire_versions_published_by_idx
  on public.sm_questionnaire_versions(published_by_user_id);

create table public.sm_questionnaire_version_modules (
  id uuid primary key default gen_random_uuid(),
  questionnaire_version_id uuid not null references public.sm_questionnaire_versions(id) on delete restrict,
  module_version_id uuid not null references public.sm_module_versions(id) on delete restrict,
  order_index integer not null default 0,
  is_deleted boolean not null default false,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sm_questionnaire_version_modules_order_ck check (order_index >= 0)
);

create unique index sm_questionnaire_version_modules_module_active_unique
  on public.sm_questionnaire_version_modules(questionnaire_version_id, module_version_id)
  where is_deleted = false;
create unique index sm_questionnaire_version_modules_order_active_unique
  on public.sm_questionnaire_version_modules(questionnaire_version_id, order_index)
  where is_deleted = false;
create index sm_questionnaire_version_modules_module_idx
  on public.sm_questionnaire_version_modules(module_version_id);

create table public.sm_questionnaire_submissions (
  id uuid primary key default gen_random_uuid(),
  assignment_id uuid,
  questionnaire_template_id uuid not null references public.sm_questionnaire_templates(id) on delete restrict,
  questionnaire_version_id uuid not null references public.sm_questionnaire_versions(id) on delete restrict,
  sm_user_id uuid not null references public.users(id) on delete restrict,
  sm_market_id uuid not null references public.sm_markets(id) on delete restrict,
  supersedes_submission_id uuid references public.sm_questionnaire_submissions(id) on delete restrict,
  revision_number integer not null default 1,
  is_current boolean not null default true,
  status public.sm_submission_status not null default 'draft',
  client_submission_token text not null,
  timezone text not null default 'Europe/Vienna',
  once_per_market_snapshot boolean not null default false,
  questionnaire_name_snapshot text not null,
  questionnaire_version_snapshot integer not null,
  sm_name_snapshot text not null,
  market_name_snapshot text not null,
  market_address_snapshot text not null default '',
  market_postal_code_snapshot text not null default '',
  market_city_snapshot text not null default '',
  visit_started_at timestamptz,
  visit_completed_at timestamptz,
  submitted_at timestamptz,
  reporting_available_at timestamptz,
  cancelled_at timestamptz,
  cancellation_reason text,
  last_saved_at timestamptz not null default now(),
  resolved_question_count integer not null default 0,
  answered_question_count integer not null default 0,
  earned_points numeric(14, 4) not null default 0,
  possible_points numeric(14, 4) not null default 0,
  invalidated_at timestamptz,
  invalidated_by_user_id uuid references public.users(id) on delete set null,
  invalidation_reason text,
  is_deleted boolean not null default false,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sm_questionnaire_submissions_revision_ck check (revision_number >= 1),
  constraint sm_questionnaire_submissions_token_ck check (btrim(client_submission_token) <> ''),
  constraint sm_questionnaire_submissions_snapshot_ck check (
    btrim(questionnaire_name_snapshot) <> ''
    and questionnaire_version_snapshot >= 1
    and btrim(sm_name_snapshot) <> ''
    and btrim(market_name_snapshot) <> ''
  ),
  constraint sm_questionnaire_submissions_counts_ck check (
    resolved_question_count >= 0
    and answered_question_count >= 0
    and answered_question_count <= resolved_question_count
  ),
  constraint sm_questionnaire_submissions_points_ck check (
    earned_points >= 0 and possible_points >= 0 and earned_points <= possible_points
  ),
  constraint sm_questionnaire_submissions_visit_time_ck check (
    visit_started_at is null or visit_completed_at is null or visit_completed_at >= visit_started_at
  ),
  constraint sm_questionnaire_submissions_submit_state_ck check (
    status <> 'submitted' or (submitted_at is not null and reporting_available_at is not null)
  ),
  constraint sm_questionnaire_submissions_invalidation_ck check (
    status <> 'invalidated'
    or (
      invalidated_at is not null
      and invalidation_reason is not null
      and btrim(invalidation_reason) <> ''
    )
  ),
  constraint sm_questionnaire_submissions_cancellation_ck check (
    status <> 'cancelled'
    or (
      cancelled_at is not null
      and cancellation_reason is not null
      and btrim(cancellation_reason) <> ''
    )
  ),
  constraint sm_questionnaire_submissions_template_version_fk foreign key (
    questionnaire_version_id,
    questionnaire_template_id
  ) references public.sm_questionnaire_versions(id, questionnaire_template_id) on delete restrict
);

comment on column public.sm_questionnaire_submissions.assignment_id is
  'Reserved SM Einsatz UUID. A foreign key is added when the separate sm_assignments domain is introduced.';

create unique index sm_questionnaire_submissions_client_token_active_unique
  on public.sm_questionnaire_submissions(sm_user_id, client_submission_token)
  where is_deleted = false;
create unique index sm_questionnaire_submissions_current_assignment_unique
  on public.sm_questionnaire_submissions(assignment_id)
  where is_deleted = false and is_current = true and assignment_id is not null;
create unique index sm_questionnaire_submissions_once_per_market_unique
  on public.sm_questionnaire_submissions(questionnaire_template_id, sm_market_id)
  where is_deleted = false
    and is_current = true
    and status = 'submitted'
    and once_per_market_snapshot = true;
create index sm_questionnaire_submissions_sm_status_idx
  on public.sm_questionnaire_submissions(sm_user_id, status, submitted_at desc)
  where is_deleted = false;
create index sm_questionnaire_submissions_market_status_idx
  on public.sm_questionnaire_submissions(sm_market_id, status, submitted_at desc)
  where is_deleted = false;
create index sm_questionnaire_submissions_version_idx
  on public.sm_questionnaire_submissions(questionnaire_version_id, submitted_at desc);
create index sm_questionnaire_submissions_supersedes_idx
  on public.sm_questionnaire_submissions(supersedes_submission_id);
create index sm_questionnaire_submissions_invalidated_by_idx
  on public.sm_questionnaire_submissions(invalidated_by_user_id);

create table public.sm_questionnaire_submission_sections (
  id uuid primary key default gen_random_uuid(),
  submission_id uuid not null references public.sm_questionnaire_submissions(id) on delete restrict,
  module_version_id uuid not null references public.sm_module_versions(id) on delete restrict,
  module_code_snapshot text not null,
  module_name_snapshot text not null,
  module_description_snapshot text not null default '',
  order_index integer not null default 0,
  is_deleted boolean not null default false,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sm_questionnaire_submission_sections_snapshot_ck check (
    btrim(module_code_snapshot) <> '' and btrim(module_name_snapshot) <> ''
  ),
  constraint sm_questionnaire_submission_sections_order_ck check (order_index >= 0),
  constraint sm_questionnaire_submission_sections_id_submission_unique unique (id, submission_id)
);

create unique index sm_questionnaire_submission_sections_module_active_unique
  on public.sm_questionnaire_submission_sections(submission_id, module_version_id)
  where is_deleted = false;
create unique index sm_questionnaire_submission_sections_order_active_unique
  on public.sm_questionnaire_submission_sections(submission_id, order_index)
  where is_deleted = false;
create index sm_questionnaire_submission_sections_module_idx
  on public.sm_questionnaire_submission_sections(module_version_id);

create table public.sm_questionnaire_submission_questions (
  id uuid primary key default gen_random_uuid(),
  submission_id uuid not null references public.sm_questionnaire_submissions(id) on delete restrict,
  submission_section_id uuid not null references public.sm_questionnaire_submission_sections(id) on delete restrict,
  question_version_id uuid not null references public.sm_question_versions(id) on delete restrict,
  question_code_snapshot text not null,
  question_type_snapshot public.sm_question_type not null,
  question_text_snapshot text not null,
  required_snapshot boolean not null,
  metric_role_snapshot public.sm_metric_role not null,
  oos_category_snapshot public.sm_oos_category,
  max_points_snapshot numeric(10, 4) not null default 0,
  config_snapshot jsonb not null default '{}'::jsonb,
  metric_config_snapshot jsonb not null default '{}'::jsonb,
  answer_options_snapshot jsonb not null default '[]'::jsonb,
  logic_rules_snapshot jsonb not null default '[]'::jsonb,
  is_applicable boolean not null default true,
  applicability_reason text,
  order_index integer not null default 0,
  is_deleted boolean not null default false,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sm_questionnaire_submission_questions_snapshot_ck check (
    btrim(question_code_snapshot) <> '' and btrim(question_text_snapshot) <> ''
  ),
  constraint sm_questionnaire_submission_questions_order_ck check (order_index >= 0),
  constraint sm_questionnaire_submission_questions_points_ck check (max_points_snapshot >= 0),
  constraint sm_questionnaire_submission_questions_config_ck check (
    jsonb_typeof(config_snapshot) = 'object'
    and jsonb_typeof(metric_config_snapshot) = 'object'
    and jsonb_typeof(answer_options_snapshot) = 'array'
    and jsonb_typeof(logic_rules_snapshot) = 'array'
  ),
  constraint sm_questionnaire_submission_questions_applicability_ck check (
    is_applicable
    or (
      applicability_reason is not null
      and btrim(applicability_reason) <> ''
    )
  ),
  constraint sm_questionnaire_submission_questions_id_submission_unique unique (id, submission_id),
  constraint sm_questionnaire_submission_questions_section_submission_fk foreign key (
    submission_section_id,
    submission_id
  ) references public.sm_questionnaire_submission_sections(id, submission_id) on delete restrict
);

create unique index sm_questionnaire_submission_questions_question_active_unique
  on public.sm_questionnaire_submission_questions(submission_id, question_version_id)
  where is_deleted = false;
create unique index sm_questionnaire_submission_questions_section_order_active_unique
  on public.sm_questionnaire_submission_questions(submission_section_id, order_index)
  where is_deleted = false;
create index sm_questionnaire_submission_questions_submission_idx
  on public.sm_questionnaire_submission_questions(submission_id, submission_section_id, order_index);
create index sm_questionnaire_submission_questions_question_idx
  on public.sm_questionnaire_submission_questions(question_version_id);
create index sm_questionnaire_submission_questions_metric_idx
  on public.sm_questionnaire_submission_questions(metric_role_snapshot, oos_category_snapshot)
  where is_deleted = false and is_applicable = true and metric_role_snapshot <> 'none';

create table public.sm_question_answers (
  id uuid primary key default gen_random_uuid(),
  submission_id uuid not null references public.sm_questionnaire_submissions(id) on delete restrict,
  submission_question_id uuid not null references public.sm_questionnaire_submission_questions(id) on delete restrict,
  supersedes_answer_id uuid references public.sm_question_answers(id) on delete restrict,
  answer_version integer not null default 1,
  is_current boolean not null default true,
  answer_state public.sm_answer_state not null default 'unanswered',
  value_text text,
  value_number numeric(18, 6),
  value_json jsonb,
  earned_points numeric(10, 4) not null default 0,
  possible_points numeric(10, 4) not null default 0,
  metric_outcome_code text,
  applicability_reason text,
  answered_by_user_id uuid references public.users(id) on delete set null,
  answered_at timestamptz,
  invalidated_at timestamptz,
  invalidated_by_user_id uuid references public.users(id) on delete set null,
  invalidation_reason text,
  is_deleted boolean not null default false,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sm_question_answers_version_ck check (answer_version >= 1),
  constraint sm_question_answers_points_ck check (
    earned_points >= 0 and possible_points >= 0 and earned_points <= possible_points
  ),
  constraint sm_question_answers_answered_ck check (
    answer_state <> 'answered' or answered_at is not null
  ),
  constraint sm_question_answers_not_applicable_ck check (
    answer_state <> 'not_applicable'
    or (
      earned_points = 0
      and possible_points = 0
      and applicability_reason is not null
      and btrim(applicability_reason) <> ''
    )
  ),
  constraint sm_question_answers_invalidated_ck check (
    answer_state <> 'invalidated'
    or (
      invalidated_at is not null
      and invalidation_reason is not null
      and btrim(invalidation_reason) <> ''
    )
  ),
  constraint sm_question_answers_id_submission_unique unique (id, submission_id),
  constraint sm_question_answers_question_submission_fk foreign key (
    submission_question_id,
    submission_id
  ) references public.sm_questionnaire_submission_questions(id, submission_id) on delete restrict
);

create unique index sm_question_answers_current_question_unique
  on public.sm_question_answers(submission_question_id)
  where is_deleted = false and is_current = true;
create unique index sm_question_answers_question_version_unique
  on public.sm_question_answers(submission_question_id, answer_version);
create index sm_question_answers_submission_state_idx
  on public.sm_question_answers(submission_id, answer_state, updated_at desc)
  where is_deleted = false and is_current = true;
create index sm_question_answers_supersedes_idx on public.sm_question_answers(supersedes_answer_id);
create index sm_question_answers_answered_by_idx on public.sm_question_answers(answered_by_user_id);
create index sm_question_answers_invalidated_by_idx on public.sm_question_answers(invalidated_by_user_id);

create table public.sm_question_answer_options (
  id uuid primary key default gen_random_uuid(),
  answer_id uuid not null references public.sm_question_answers(id) on delete restrict,
  answer_option_version_id uuid references public.sm_answer_option_versions(id) on delete restrict,
  option_code_snapshot text not null,
  option_label_snapshot text not null,
  earned_points_snapshot numeric(10, 4) not null default 0,
  possible_points_snapshot numeric(10, 4) not null default 0,
  metric_outcome_code_snapshot text,
  order_index integer not null default 0,
  is_deleted boolean not null default false,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sm_question_answer_options_snapshot_ck check (
    btrim(option_code_snapshot) <> '' and btrim(option_label_snapshot) <> ''
  ),
  constraint sm_question_answer_options_points_ck check (
    earned_points_snapshot >= 0
    and possible_points_snapshot >= 0
    and earned_points_snapshot <= possible_points_snapshot
  ),
  constraint sm_question_answer_options_order_ck check (order_index >= 0)
);

create unique index sm_question_answer_options_code_active_unique
  on public.sm_question_answer_options(answer_id, option_code_snapshot)
  where is_deleted = false;
create unique index sm_question_answer_options_order_active_unique
  on public.sm_question_answer_options(answer_id, order_index)
  where is_deleted = false;
create index sm_question_answer_options_option_version_idx
  on public.sm_question_answer_options(answer_option_version_id);

create table public.sm_question_answer_matrix_cells (
  id uuid primary key default gen_random_uuid(),
  answer_id uuid not null references public.sm_question_answers(id) on delete restrict,
  row_code text not null,
  column_code text not null,
  value_text text,
  value_number numeric(18, 6),
  selected boolean,
  order_index integer not null default 0,
  is_deleted boolean not null default false,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sm_question_answer_matrix_cells_codes_ck check (
    btrim(row_code) <> '' and btrim(column_code) <> ''
  ),
  constraint sm_question_answer_matrix_cells_order_ck check (order_index >= 0)
);

create unique index sm_question_answer_matrix_cells_active_unique
  on public.sm_question_answer_matrix_cells(answer_id, row_code, column_code)
  where is_deleted = false;
create index sm_question_answer_matrix_cells_answer_order_idx
  on public.sm_question_answer_matrix_cells(answer_id, order_index);

create table public.sm_question_answer_files (
  id uuid primary key default gen_random_uuid(),
  answer_id uuid not null references public.sm_question_answers(id) on delete restrict,
  storage_bucket text not null,
  storage_path text not null,
  original_file_name text,
  mime_type text,
  byte_size integer,
  width_px integer,
  height_px integer,
  sha256 text,
  uploaded_at timestamptz not null default now(),
  is_deleted boolean not null default false,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sm_question_answer_files_storage_ck check (
    btrim(storage_bucket) <> '' and btrim(storage_path) <> ''
  ),
  constraint sm_question_answer_files_size_ck check (byte_size is null or byte_size >= 0),
  constraint sm_question_answer_files_dimensions_ck check (
    (width_px is null or width_px > 0) and (height_px is null or height_px > 0)
  )
);

create unique index sm_question_answer_files_path_active_unique
  on public.sm_question_answer_files(answer_id, storage_bucket, storage_path)
  where is_deleted = false;
create index sm_question_answer_files_answer_idx on public.sm_question_answer_files(answer_id, uploaded_at);
create index sm_question_answer_files_sha_idx
  on public.sm_question_answer_files(sha256)
  where is_deleted = false and sha256 is not null;

create table public.sm_question_answer_events (
  id uuid primary key default gen_random_uuid(),
  answer_id uuid not null references public.sm_question_answers(id) on delete restrict,
  submission_id uuid not null references public.sm_questionnaire_submissions(id) on delete restrict,
  event_type public.sm_answer_event_type not null,
  answer_version integer not null,
  payload jsonb not null default '{}'::jsonb,
  actor_user_id uuid references public.users(id) on delete set null,
  created_at timestamptz not null default now(),
  constraint sm_question_answer_events_version_ck check (answer_version >= 1),
  constraint sm_question_answer_events_payload_ck check (jsonb_typeof(payload) = 'object'),
  constraint sm_question_answer_events_answer_submission_fk foreign key (
    answer_id,
    submission_id
  ) references public.sm_question_answers(id, submission_id) on delete restrict
);

create index sm_question_answer_events_answer_created_idx
  on public.sm_question_answer_events(answer_id, created_at);
create index sm_question_answer_events_submission_created_idx
  on public.sm_question_answer_events(submission_id, created_at);
create index sm_question_answer_events_actor_idx on public.sm_question_answer_events(actor_user_id);

create table public.sm_answer_change_requests (
  id uuid primary key default gen_random_uuid(),
  submission_id uuid not null references public.sm_questionnaire_submissions(id) on delete restrict,
  submission_question_id uuid not null references public.sm_questionnaire_submission_questions(id) on delete restrict,
  original_answer_id uuid references public.sm_question_answers(id) on delete restrict,
  sm_user_id uuid not null references public.users(id) on delete restrict,
  sm_market_id uuid not null references public.sm_markets(id) on delete restrict,
  question_text_snapshot text not null,
  original_answer_snapshot jsonb not null default '{}'::jsonb,
  requested_answer_payload jsonb not null default '{}'::jsonb,
  requested_answer_summary text not null default '',
  request_reason text not null,
  status public.sm_change_request_status not null default 'pending',
  reviewed_by_user_id uuid references public.users(id) on delete set null,
  reviewed_at timestamptz,
  admin_note text,
  is_deleted boolean not null default false,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sm_answer_change_requests_question_ck check (btrim(question_text_snapshot) <> ''),
  constraint sm_answer_change_requests_reason_ck check (btrim(request_reason) <> ''),
  constraint sm_answer_change_requests_payload_ck check (
    jsonb_typeof(original_answer_snapshot) = 'object'
    and jsonb_typeof(requested_answer_payload) = 'object'
  ),
  constraint sm_answer_change_requests_review_ck check (
    status = 'pending'
    or status = 'cancelled'
    or (reviewed_by_user_id is not null and reviewed_at is not null)
  ),
  constraint sm_answer_change_requests_question_submission_fk foreign key (
    submission_question_id,
    submission_id
  ) references public.sm_questionnaire_submission_questions(id, submission_id) on delete restrict,
  constraint sm_answer_change_requests_answer_submission_fk foreign key (
    original_answer_id,
    submission_id
  ) references public.sm_question_answers(id, submission_id) on delete restrict
);

create unique index sm_answer_change_requests_pending_question_unique
  on public.sm_answer_change_requests(submission_question_id)
  where is_deleted = false and status = 'pending';
create index sm_answer_change_requests_sm_status_idx
  on public.sm_answer_change_requests(sm_user_id, status, created_at desc)
  where is_deleted = false;
create index sm_answer_change_requests_market_idx
  on public.sm_answer_change_requests(sm_market_id, created_at desc);
create index sm_answer_change_requests_submission_idx
  on public.sm_answer_change_requests(submission_id, created_at desc);
create index sm_answer_change_requests_original_answer_idx
  on public.sm_answer_change_requests(original_answer_id);
create index sm_answer_change_requests_reviewer_idx
  on public.sm_answer_change_requests(reviewed_by_user_id);

create table public.sm_questionnaire_submission_delete_requests (
  id uuid primary key default gen_random_uuid(),
  submission_id uuid not null references public.sm_questionnaire_submissions(id) on delete restrict,
  sm_user_id uuid not null references public.users(id) on delete restrict,
  sm_market_id uuid not null references public.sm_markets(id) on delete restrict,
  questionnaire_name_snapshot text not null,
  questionnaire_version_snapshot integer not null,
  market_name_snapshot text not null,
  submitted_at_snapshot timestamptz,
  request_reason text not null,
  status public.sm_change_request_status not null default 'pending',
  reviewed_by_user_id uuid references public.users(id) on delete set null,
  reviewed_at timestamptz,
  admin_note text,
  is_deleted boolean not null default false,
  deleted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sm_questionnaire_submission_delete_requests_snapshot_ck check (
    btrim(questionnaire_name_snapshot) <> ''
    and questionnaire_version_snapshot >= 1
    and btrim(market_name_snapshot) <> ''
  ),
  constraint sm_questionnaire_submission_delete_requests_reason_ck check (btrim(request_reason) <> ''),
  constraint sm_questionnaire_submission_delete_requests_review_ck check (
    status = 'pending'
    or status = 'cancelled'
    or (reviewed_by_user_id is not null and reviewed_at is not null)
  )
);

create unique index sm_questionnaire_submission_delete_requests_pending_unique
  on public.sm_questionnaire_submission_delete_requests(submission_id)
  where is_deleted = false and status = 'pending';
create index sm_questionnaire_submission_delete_requests_sm_status_idx
  on public.sm_questionnaire_submission_delete_requests(sm_user_id, status, created_at desc)
  where is_deleted = false;
create index sm_questionnaire_submission_delete_requests_market_idx
  on public.sm_questionnaire_submission_delete_requests(sm_market_id, created_at desc);
create index sm_questionnaire_submission_delete_requests_reviewer_idx
  on public.sm_questionnaire_submission_delete_requests(reviewed_by_user_id);

comment on table public.sm_questions is 'Stable logical SM question identities. Editable wording and behavior live in immutable versions.';
comment on table public.sm_question_versions is 'Versioned SM question content, type, required flag, scoring role, OOS category, and type-specific configuration.';
comment on table public.sm_answer_option_versions is 'Versioned answer choices with stable codes, points, metric outcomes, and explicit not-applicable semantics.';
comment on table public.sm_question_logic_rules is 'Backend-authoritative SM conditional logic conditions attached to exact question versions.';
comment on table public.sm_question_logic_rule_targets is 'Questions shown or hidden by an SM conditional logic rule.';
comment on table public.sm_modules is 'Stable logical SM module identities used by the reusable question-module-questionnaire hierarchy.';
comment on table public.sm_module_versions is 'Versioned SM module labels and descriptions.';
comment on table public.sm_module_version_questions is 'Ordered exact question versions contained by an exact SM module version.';
comment on table public.sm_questionnaire_templates is 'Stable logical SM questionnaire identities and administrative lifecycle.';
comment on table public.sm_questionnaire_versions is 'Draft or published SM questionnaire versions. Assignment-driven; no GM campaign or schedule dependency.';
comment on table public.sm_questionnaire_version_modules is 'Ordered exact module versions composing an exact SM questionnaire version.';
comment on table public.sm_questionnaire_submissions is 'Versioned, idempotent SM questionnaire drafts/final submissions bound to SM user and SM market context.';
comment on table public.sm_questionnaire_submission_sections is 'Immutable module snapshots resolved into a concrete SM submission.';
comment on table public.sm_questionnaire_submission_questions is 'Immutable resolved question, option, scoring, metric, and conditional-rule snapshots for a concrete submission.';
comment on table public.sm_question_answers is 'Versioned answer source of truth with distinct unanswered, answered, not-applicable, and invalidated states.';
comment on table public.sm_question_answer_options is 'Selected answer option snapshots for single, multi, yes/no, and yes/no-multi SM questions.';
comment on table public.sm_question_answer_matrix_cells is 'Normalized matrix-cell answers for SM matrix questions.';
comment on table public.sm_question_answer_files is 'Storage metadata for SM photo/file answers; no GM photo-tag semantics are inherited.';
comment on table public.sm_question_answer_events is 'Append-only answer change timeline supporting autosave, retries, and audited corrections.';
comment on table public.sm_answer_change_requests is 'SM-only submitted-answer correction requests; GM request tables are not reused.';
comment on table public.sm_questionnaire_submission_delete_requests is 'SM-only questionnaire submission deletion requests that never delete the planned Einsatz or time record.';

-- Published graphs are immutable. Editing after publication must clone question,
-- module, and questionnaire versions instead of rewriting history.
create function public.sm_question_version_is_locked(p_question_version_id uuid)
returns boolean
language sql
stable
set search_path = ''
as $$
  select exists (
    select 1
    from public.sm_question_versions q
    where q.id = p_question_version_id
      and q.status = 'published'
      and q.is_deleted = false
  ) or exists (
    select 1
    from public.sm_module_version_questions mvq
    join public.sm_questionnaire_version_modules qvm
      on qvm.module_version_id = mvq.module_version_id
     and qvm.is_deleted = false
    join public.sm_questionnaire_versions qv
      on qv.id = qvm.questionnaire_version_id
     and qv.is_deleted = false
     and qv.status = 'published'
    where mvq.question_version_id = p_question_version_id
      and mvq.is_deleted = false
  );
$$;

create function public.sm_module_version_is_locked(p_module_version_id uuid)
returns boolean
language sql
stable
set search_path = ''
as $$
  select exists (
    select 1
    from public.sm_module_versions m
    where m.id = p_module_version_id
      and m.status = 'published'
      and m.is_deleted = false
  ) or exists (
    select 1
    from public.sm_questionnaire_version_modules qvm
    join public.sm_questionnaire_versions qv
      on qv.id = qvm.questionnaire_version_id
     and qv.is_deleted = false
     and qv.status = 'published'
    where qvm.module_version_id = p_module_version_id
      and qvm.is_deleted = false
  );
$$;

create function public.sm_guard_question_version_mutation()
returns trigger
language plpgsql
set search_path = ''
as $$
declare
  v_question_version_id uuid;
begin
  v_question_version_id := case when tg_op = 'INSERT' then new.id else old.id end;
  if tg_op <> 'INSERT' and public.sm_question_version_is_locked(v_question_version_id) then
    raise exception 'Published SM question versions are immutable';
  end if;
  if tg_op = 'UPDATE'
     and old.status = 'draft'
     and new.status = 'published'
     and new.question_type in ('single', 'yesno', 'yesnomulti', 'multiple', 'likert')
     and (
       select count(*)
       from public.sm_answer_option_versions option_row
       where option_row.question_version_id = old.id
         and option_row.is_deleted = false
     ) < 2 then
    raise exception 'Choice-based SM questions need at least two active answer options before publication';
  end if;
  return case when tg_op = 'DELETE' then old else new end;
end;
$$;

create function public.sm_guard_question_child_mutation()
returns trigger
language plpgsql
set search_path = ''
as $$
declare
  v_question_version_id uuid;
begin
  v_question_version_id := case when tg_op = 'DELETE' then old.question_version_id else new.question_version_id end;
  if public.sm_question_version_is_locked(v_question_version_id) then
    raise exception 'Children of published SM question versions are immutable';
  end if;
  return case when tg_op = 'DELETE' then old else new end;
end;
$$;

create function public.sm_guard_logic_rule_mutation()
returns trigger
language plpgsql
set search_path = ''
as $$
declare
  v_question_version_id uuid;
begin
  v_question_version_id := case
    when tg_op = 'DELETE' then old.trigger_question_version_id
    else new.trigger_question_version_id
  end;
  if public.sm_question_version_is_locked(v_question_version_id) then
    raise exception 'Rules of published SM question versions are immutable';
  end if;
  return case when tg_op = 'DELETE' then old else new end;
end;
$$;

create function public.sm_guard_logic_target_mutation()
returns trigger
language plpgsql
set search_path = ''
as $$
declare
  v_rule_id uuid;
  v_question_version_id uuid;
begin
  v_rule_id := case when tg_op = 'DELETE' then old.rule_id else new.rule_id end;
  select trigger_question_version_id into v_question_version_id
  from public.sm_question_logic_rules
  where id = v_rule_id;
  if tg_op <> 'DELETE' and new.target_question_version_id = v_question_version_id then
    raise exception 'An SM question logic rule cannot target its own trigger question';
  end if;
  if v_question_version_id is not null and public.sm_question_version_is_locked(v_question_version_id) then
    raise exception 'Logic targets of published SM question versions are immutable';
  end if;
  return case when tg_op = 'DELETE' then old else new end;
end;
$$;

create function public.sm_guard_module_version_mutation()
returns trigger
language plpgsql
set search_path = ''
as $$
declare
  v_module_version_id uuid;
begin
  v_module_version_id := case when tg_op = 'INSERT' then new.id else old.id end;
  if tg_op <> 'INSERT' and public.sm_module_version_is_locked(v_module_version_id) then
    raise exception 'Published SM module versions are immutable';
  end if;
  if tg_op = 'UPDATE' and old.status = 'draft' and new.status = 'published' then
    if not exists (
      select 1
      from public.sm_module_version_questions mvq
      where mvq.module_version_id = old.id
        and mvq.is_deleted = false
    ) then
      raise exception 'SM modules need at least one active question before publication';
    end if;
    if exists (
      select 1
      from public.sm_module_version_questions mvq
      join public.sm_question_versions qv on qv.id = mvq.question_version_id
      where mvq.module_version_id = old.id
        and mvq.is_deleted = false
        and (qv.is_deleted = true or qv.status <> 'published')
    ) then
      raise exception 'Every question version in an SM module must be published first';
    end if;
  end if;
  return case when tg_op = 'DELETE' then old else new end;
end;
$$;

create function public.sm_guard_module_question_mutation()
returns trigger
language plpgsql
set search_path = ''
as $$
declare
  v_module_version_id uuid;
begin
  v_module_version_id := case when tg_op = 'DELETE' then old.module_version_id else new.module_version_id end;
  if public.sm_module_version_is_locked(v_module_version_id) then
    raise exception 'Questions of published SM module versions are immutable';
  end if;
  return case when tg_op = 'DELETE' then old else new end;
end;
$$;

create function public.sm_guard_questionnaire_version_mutation()
returns trigger
language plpgsql
set search_path = ''
as $$
begin
  if tg_op <> 'INSERT' and old.status = 'published' then
    raise exception 'Published SM questionnaire versions are immutable';
  end if;
  if tg_op = 'UPDATE' and old.status = 'draft' and new.status = 'published' then
    if not exists (
      select 1
      from public.sm_questionnaire_version_modules qvm
      where qvm.questionnaire_version_id = old.id
        and qvm.is_deleted = false
    ) then
      raise exception 'SM questionnaires need at least one active module before publication';
    end if;
    if exists (
      select 1
      from public.sm_questionnaire_version_modules qvm
      join public.sm_module_versions mv on mv.id = qvm.module_version_id
      where qvm.questionnaire_version_id = old.id
        and qvm.is_deleted = false
        and (mv.is_deleted = true or mv.status <> 'published')
    ) then
      raise exception 'Every module version in an SM questionnaire must be published first';
    end if;
  end if;
  return case when tg_op = 'DELETE' then old else new end;
end;
$$;

create function public.sm_guard_questionnaire_module_mutation()
returns trigger
language plpgsql
set search_path = ''
as $$
declare
  v_questionnaire_version_id uuid;
  v_status public.sm_questionnaire_version_status;
begin
  v_questionnaire_version_id := case when tg_op = 'DELETE' then old.questionnaire_version_id else new.questionnaire_version_id end;
  select status into v_status
  from public.sm_questionnaire_versions
  where id = v_questionnaire_version_id;
  if v_status = 'published' then
    raise exception 'Modules of published SM questionnaire versions are immutable';
  end if;
  return case when tg_op = 'DELETE' then old else new end;
end;
$$;

create trigger sm_question_versions_immutable_trg
before update or delete on public.sm_question_versions
for each row execute function public.sm_guard_question_version_mutation();

create trigger sm_answer_option_versions_immutable_trg
before insert or update or delete on public.sm_answer_option_versions
for each row execute function public.sm_guard_question_child_mutation();

create trigger sm_question_logic_rules_immutable_trg
before insert or update or delete on public.sm_question_logic_rules
for each row execute function public.sm_guard_logic_rule_mutation();

create trigger sm_question_logic_rule_targets_immutable_trg
before insert or update or delete on public.sm_question_logic_rule_targets
for each row execute function public.sm_guard_logic_target_mutation();

create trigger sm_module_versions_immutable_trg
before update or delete on public.sm_module_versions
for each row execute function public.sm_guard_module_version_mutation();

create trigger sm_module_version_questions_immutable_trg
before insert or update or delete on public.sm_module_version_questions
for each row execute function public.sm_guard_module_question_mutation();

create trigger sm_questionnaire_versions_immutable_trg
before update or delete on public.sm_questionnaire_versions
for each row execute function public.sm_guard_questionnaire_version_mutation();

create trigger sm_questionnaire_version_modules_immutable_trg
before insert or update or delete on public.sm_questionnaire_version_modules
for each row execute function public.sm_guard_questionnaire_module_mutation();

-- All questionnaire tables are backend-only. No anon/authenticated Data API
-- access is granted; the Express API remains the authorization boundary.
do $$
declare
  v_table_name text;
begin
  foreach v_table_name in array array[
    'sm_questions',
    'sm_question_versions',
    'sm_answer_option_versions',
    'sm_question_logic_rules',
    'sm_question_logic_rule_targets',
    'sm_modules',
    'sm_module_versions',
    'sm_module_version_questions',
    'sm_questionnaire_templates',
    'sm_questionnaire_versions',
    'sm_questionnaire_version_modules',
    'sm_questionnaire_submissions',
    'sm_questionnaire_submission_sections',
    'sm_questionnaire_submission_questions',
    'sm_question_answers',
    'sm_question_answer_options',
    'sm_question_answer_matrix_cells',
    'sm_question_answer_files',
    'sm_question_answer_events',
    'sm_answer_change_requests',
    'sm_questionnaire_submission_delete_requests'
  ]
  loop
    execute format('alter table public.%I enable row level security', v_table_name);
    execute format('alter table public.%I force row level security', v_table_name);
    execute format('revoke all privileges on table public.%I from public, anon, authenticated', v_table_name);
    execute format('grant all privileges on table public.%I to service_role', v_table_name);
  end loop;
end;
$$;

revoke all on function public.sm_question_version_is_locked(uuid) from public, anon, authenticated;
revoke all on function public.sm_module_version_is_locked(uuid) from public, anon, authenticated;
revoke all on function public.sm_guard_question_version_mutation() from public, anon, authenticated;
revoke all on function public.sm_guard_question_child_mutation() from public, anon, authenticated;
revoke all on function public.sm_guard_logic_rule_mutation() from public, anon, authenticated;
revoke all on function public.sm_guard_logic_target_mutation() from public, anon, authenticated;
revoke all on function public.sm_guard_module_version_mutation() from public, anon, authenticated;
revoke all on function public.sm_guard_module_question_mutation() from public, anon, authenticated;
revoke all on function public.sm_guard_questionnaire_version_mutation() from public, anon, authenticated;
revoke all on function public.sm_guard_questionnaire_module_mutation() from public, anon, authenticated;

grant execute on function public.sm_question_version_is_locked(uuid) to service_role;
grant execute on function public.sm_module_version_is_locked(uuid) to service_role;
grant execute on function public.sm_guard_question_version_mutation() to service_role;
grant execute on function public.sm_guard_question_child_mutation() to service_role;
grant execute on function public.sm_guard_logic_rule_mutation() to service_role;
grant execute on function public.sm_guard_logic_target_mutation() to service_role;
grant execute on function public.sm_guard_module_version_mutation() to service_role;
grant execute on function public.sm_guard_module_question_mutation() to service_role;
grant execute on function public.sm_guard_questionnaire_version_mutation() to service_role;
grant execute on function public.sm_guard_questionnaire_module_mutation() to service_role;
