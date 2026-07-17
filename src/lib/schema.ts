import { sql } from "drizzle-orm";
import {
  bigint,
  boolean,
  check,
  date,
  integer,
  index,
  jsonb,
  numeric,
  pgEnum,
  pgTable,
  primaryKey,
  text,
  timestamp,
  uniqueIndex,
  uuid,
} from "drizzle-orm/pg-core";

export const userRoleEnum = pgEnum("user_role", ["admin", "gm", "sm", "kunde"]);
export const fragebogenSectionEnum = pgEnum("fragebogen_section", ["standard", "flex", "billa", "kuehler", "mhd"]);
export const fragebogenMainSectionEnum = pgEnum("fragebogen_main_section", ["standard", "flex", "billa"]);
export const campaignSectionEnum = pgEnum("campaign_section", ["standard", "flex", "billa", "kuehler", "mhd"]);
export const questionTypeEnum = pgEnum("fragebogen_question_type", [
  "single",
  "yesno",
  "yesnomulti",
  "multiple",
  "likert",
  "text",
  "numeric",
  "slider",
  "photo",
  "matrix",
]);
export const fragebogenStatusEnum = pgEnum("fragebogen_status", ["active", "scheduled", "inactive"]);
export const fragebogenScheduleTypeEnum = pgEnum("fragebogen_schedule_type", ["always", "scheduled"]);
export const questionRuleActionEnum = pgEnum("fragebogen_rule_action", ["hide", "show"]);
export const fragebogenScopeEnum = pgEnum("fragebogen_scope", ["main", "kuehler", "mhd"]);
export const visitSessionStatusEnum = pgEnum("visit_session_status", ["draft", "submitted", "cancelled"]);
export const visitSectionStatusEnum = pgEnum("visit_section_status", ["draft", "submitted"]);
export const visitAnswerStatusEnum = pgEnum("visit_answer_status", ["unanswered", "answered", "hidden_by_rule", "skipped", "invalid"]);
export const visitAnswerOptionRoleEnum = pgEnum("visit_answer_option_role", ["top", "sub"]);
export const visitAnswerEventTypeEnum = pgEnum("visit_answer_event_type", ["set", "clear", "status_change"]);
export const visitAnswerChangeRequestStatusEnum = pgEnum("visit_answer_change_request_status", ["pending", "approved", "rejected", "cancelled"]);
export const timeEntryChangeRequestStatusEnum = pgEnum("time_entry_change_request_status", ["pending", "approved", "rejected", "cancelled"]);
export const timeEntryChangeRequestSourceKindEnum = pgEnum("time_entry_change_request_source_kind", ["day_start", "day_end", "day_km", "marktbesuch", "pause", "zusatzzeit"]);
export const dsarRequestTypeEnum = pgEnum("dsar_request_type", [
  "access",
  "rectification",
  "erasure",
  "restriction",
  "portability",
  "objection",
  "mixed",
]);
export const dsarRequestStatusEnum = pgEnum("dsar_request_status", [
  "open",
  "identity_check",
  "collecting",
  "decision",
  "responded",
  "closed",
  "cancelled",
]);
export const marketTypeEnum = pgEnum("market_type", ["universum", "kuehler", "both"]);
export const timeTrackingActivityTypeEnum = pgEnum("time_tracking_activity_type", [
  "sonderaufgabe",
  "arztbesuch",
  "werkstatt",
  "homeoffice",
  "schulung",
  "lager",
  "heimfahrt",
  "hoteluebernachtung",
]);
export const timeTrackingEntryStatusEnum = pgEnum("time_tracking_entry_status", ["draft", "submitted", "cancelled"]);
export const timeTrackingEntryEventTypeEnum = pgEnum("time_tracking_entry_event_type", [
  "start_set",
  "end_set",
  "comment_set",
  "submitted",
  "cancelled",
]);
export const gmDaySessionStatusEnum = pgEnum("gm_day_session_status", [
  "draft",
  "started",
  "ended",
  "submitted",
  "cancelled",
]);
export const praemienWaveStatusEnum = pgEnum("praemien_wave_status", ["draft", "active", "archived"]);
export const praemienDistributionFreqRuleEnum = pgEnum("praemien_distribution_freq_rule", ["lt8", "gt8"]);
export const praemienRewardModelEnum = pgEnum("praemien_reward_model", ["global_thresholds", "pillar_targets", "pillar_tiers"]);
export const redMonthYearStatusEnum = pgEnum("red_month_year_status", ["draft", "active", "locked"]);
export const ippGmAdjustmentEventTypeEnum = pgEnum("ipp_gm_adjustment_event_type", ["set", "clear"]);

export const users = pgTable(
  "users",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    supabaseAuthId: text("supabase_auth_id").notNull(),
    email: text("email").notNull(),
    role: userRoleEnum("role").notNull(),

    firstName: text("first_name").notNull(),
    lastName: text("last_name").notNull(),
    phone: text("phone"),
    address: text("address"),
    city: text("city"),
    postalCode: text("postal_code"),
    region: text("region"),
    ipp: numeric("ipp", { precision: 4, scale: 1 }),
    isBillaGm: boolean("is_billa_gm").notNull().default(false),
    profilePhotoBucket: text("profile_photo_bucket"),
    profilePhotoPath: text("profile_photo_path"),
    profilePhotoUpdatedAt: timestamp("profile_photo_updated_at", { withTimezone: true }),

    isActive: boolean("is_active").notNull().default(true),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    anonymizedAt: timestamp("anonymized_at", { withTimezone: true }),
    anonymizedByUserId: uuid("anonymized_by_user_id"),

    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("users_supabase_auth_id_unique").on(table.supabaseAuthId),
    uniqueIndex("users_email_unique").on(table.email),
    index("users_role_idx").on(table.role),
    index("users_active_idx").on(table.isActive),
    index("users_billa_gm_idx").on(table.isBillaGm),
    index("users_anonymized_at_idx").on(table.anonymizedAt),
  ],
);

export type KundePermissionAction = "read" | "write" | "update";
export type KundePagePermissions = Record<string, KundePermissionAction[]>;

export const kundeUsers = pgTable(
  "kunde_users",
  {
    userId: uuid("user_id")
      .primaryKey()
      .references(() => users.id, { onDelete: "cascade" }),
    pagePermissions: jsonb("page_permissions").$type<KundePagePermissions>().notNull().default(sql`'{}'::jsonb`),
    createdByUserId: uuid("created_by_user_id").references(() => users.id, { onDelete: "set null" }),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    index("kunde_users_created_by_idx").on(table.createdByUserId),
    index("kunde_users_deleted_idx").on(table.isDeleted),
  ],
);

export const gmTextSettings = pgTable(
  "gm_text_settings",
  {
    userId: uuid("user_id")
      .primaryKey()
      .references(() => users.id, { onDelete: "cascade" }),
    textScalePercent: integer("text_scale_percent").notNull().default(0),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    check("gm_text_settings_percent_range_ck", sql`${table.textScalePercent} >= 0 and ${table.textScalePercent} <= 50`),
    index("gm_text_settings_deleted_idx").on(table.isDeleted),
  ],
);

export type GmKurtiMessageRole = "user" | "assistant";

export const gmKurtiMessages = pgTable(
  "gm_kurti_messages",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    gmUserId: uuid("gm_user_id")
      .notNull()
      .references(() => users.id, { onDelete: "cascade" }),
    role: text("role").$type<GmKurtiMessageRole>().notNull(),
    content: text("content").notNull(),
    expiresAt: timestamp("expires_at", { withTimezone: true }).notNull(),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    check("gm_kurti_messages_role_ck", sql`${table.role} in ('user', 'assistant')`),
    check("gm_kurti_messages_content_length_ck", sql`char_length(${table.content}) between 1 and 12000`),
    index("gm_kurti_messages_gm_created_idx").on(table.gmUserId, table.createdAt),
    index("gm_kurti_messages_expires_idx").on(table.expiresAt),
  ],
);

export type AdminKurtiMessageRole = "user" | "assistant";

export const adminKurtiMessages = pgTable(
  "admin_kurti_messages",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    adminUserId: uuid("admin_user_id")
      .notNull()
      .references(() => users.id, { onDelete: "cascade" }),
    role: text("role").$type<AdminKurtiMessageRole>().notNull(),
    content: text("content").notNull(),
    expiresAt: timestamp("expires_at", { withTimezone: true }).notNull(),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    check("admin_kurti_messages_role_ck", sql`${table.role} in ('user', 'assistant')`),
    check("admin_kurti_messages_content_length_ck", sql`char_length(${table.content}) between 1 and 60000`),
    index("admin_kurti_messages_admin_created_idx").on(table.adminUserId, table.createdAt),
    index("admin_kurti_messages_expires_idx").on(table.expiresAt),
  ],
);

export const adminKurtiWindowPreferences = pgTable(
  "admin_kurti_window_preferences",
  {
    adminUserId: uuid("admin_user_id")
      .primaryKey()
      .references(() => users.id, { onDelete: "cascade" }),
    panelX: integer("panel_x").notNull(),
    panelY: integer("panel_y").notNull(),
    panelWidth: integer("panel_width").notNull(),
    panelHeight: integer("panel_height").notNull(),
    bubbleX: integer("bubble_x").notNull(),
    bubbleY: integer("bubble_y").notNull(),
    bubbleDismissed: boolean("bubble_dismissed").notNull().default(false),
    isCollapsed: boolean("is_collapsed").notNull().default(false),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    check("admin_kurti_window_preferences_panel_position_ck", sql`${table.panelX} between 0 and 100000 and ${table.panelY} between 0 and 100000`),
    check("admin_kurti_window_preferences_panel_size_ck", sql`${table.panelWidth} between 280 and 10000 and ${table.panelHeight} between 300 and 10000`),
    check("admin_kurti_window_preferences_bubble_position_ck", sql`${table.bubbleX} between 0 and 100000 and ${table.bubbleY} between 0 and 100000`),
  ],
);

export const specialArthurFilter = pgTable(
  "special_arthur_filter",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    gmUserId: uuid("gm_user_id")
      .notNull()
      .references(() => users.id, { onDelete: "cascade" }),
    matchValue: text("match_value").notNull(),
    createdByUserId: uuid("created_by_user_id").references(() => users.id, { onDelete: "set null" }),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("special_arthur_filter_gm_value_active_unique")
      .on(table.gmUserId, table.matchValue)
      .where(sql`${table.isDeleted} = false`),
    index("special_arthur_filter_gm_active_idx").on(table.gmUserId, table.isDeleted),
    index("special_arthur_filter_value_idx").on(table.matchValue),
  ],
);

export const employeeAgreementAcceptances = pgTable(
  "employee_agreement_acceptances",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    userId: uuid("user_id")
      .notNull()
      .references(() => users.id, { onDelete: "cascade" }),
    agreementKey: text("agreement_key").notNull().default("spark_employee_agreement"),
    agreementVersion: text("agreement_version").notNull(),
    agreementTitle: text("agreement_title").notNull(),
    agreementHash: text("agreement_hash").notNull(),
    acceptedAt: timestamp("accepted_at", { withTimezone: true }).defaultNow().notNull(),
    acceptedIp: text("accepted_ip"),
    acceptedUserAgent: text("accepted_user_agent"),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("employee_agreement_acceptances_user_version_unique").on(
      table.userId,
      table.agreementKey,
      table.agreementVersion,
    ),
    index("employee_agreement_acceptances_user_idx").on(table.userId),
    index("employee_agreement_acceptances_key_version_idx").on(table.agreementKey, table.agreementVersion),
    index("employee_agreement_acceptances_accepted_at_idx").on(table.acceptedAt),
  ],
);

export const dsarRequests = pgTable(
  "dsar_requests",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    requestType: dsarRequestTypeEnum("request_type").notNull(),
    status: dsarRequestStatusEnum("status").notNull().default("open"),
    intakeChannel: text("intake_channel").notNull().default("email"),
    requesterName: text("requester_name").notNull(),
    requesterEmail: text("requester_email").notNull(),
    requesterUserId: uuid("requester_user_id").references(() => users.id, { onDelete: "set null" }),
    subjectUserId: uuid("subject_user_id").references(() => users.id, { onDelete: "set null" }),
    subjectNameSnapshot: text("subject_name_snapshot").notNull(),
    subjectEmailSnapshot: text("subject_email_snapshot").notNull(),
    subjectRoleSnapshot: text("subject_role_snapshot"),
    requestSummary: text("request_summary").notNull().default(""),
    assignedToUserId: uuid("assigned_to_user_id").references(() => users.id, { onDelete: "set null" }),
    receivedAt: timestamp("received_at", { withTimezone: true }).defaultNow().notNull(),
    dueAt: timestamp("due_at", { withTimezone: true }).notNull(),
    extendedUntil: timestamp("extended_until", { withTimezone: true }),
    extensionReason: text("extension_reason"),
    identityVerifiedAt: timestamp("identity_verified_at", { withTimezone: true }),
    identityVerifiedByUserId: uuid("identity_verified_by_user_id").references(() => users.id, { onDelete: "set null" }),
    decisionSummary: text("decision_summary"),
    legalBlockers: text("legal_blockers"),
    responseChannel: text("response_channel"),
    responseSentAt: timestamp("response_sent_at", { withTimezone: true }),
    responseSentByUserId: uuid("response_sent_by_user_id").references(() => users.id, { onDelete: "set null" }),
    exportPackageSummary: jsonb("export_package_summary").$type<Record<string, unknown>>(),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdByUserId: uuid("created_by_user_id").references(() => users.id, { onDelete: "set null" }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    index("dsar_requests_status_due_idx").on(table.status, table.dueAt),
    index("dsar_requests_subject_user_idx").on(table.subjectUserId),
    index("dsar_requests_requester_user_idx").on(table.requesterUserId),
    index("dsar_requests_received_at_idx").on(table.receivedAt),
    index("dsar_requests_deleted_idx").on(table.isDeleted),
  ],
);

export const dsarRequestEvents = pgTable(
  "dsar_request_events",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    requestId: uuid("request_id")
      .notNull()
      .references(() => dsarRequests.id, { onDelete: "cascade" }),
    actorUserId: uuid("actor_user_id").references(() => users.id, { onDelete: "set null" }),
    eventType: text("event_type").notNull(),
    message: text("message").notNull().default(""),
    metadata: jsonb("metadata").$type<Record<string, unknown>>(),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    index("dsar_request_events_request_idx").on(table.requestId, table.createdAt),
    index("dsar_request_events_actor_idx").on(table.actorUserId),
  ],
);

export const markets = pgTable(
  "markets",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    // Identity values are optional and must stay nullable.
    // Missing values are stored as NULL (never as empty string).
    standardMarketNumber: text("standard_market_number"),
    cokeMasterNumber: text("coke_master_number"),
    flexNumber: text("flex_number"),
    name: text("name").notNull(),
    dbName: text("db_name").notNull().default(""),
    address: text("address").notNull(),
    postalCode: text("postal_code").notNull(),
    city: text("city").notNull(),
    region: text("region").notNull(),
    emEh: text("em_eh").notNull().default(""),
    employee: text("employee").notNull().default(""),
    currentGmName: text("current_gm_name").notNull().default(""),
    visitFrequencyPerYear: integer("visit_frequency_per_year").notNull().default(0),
    infoFlag: boolean("info_flag").notNull().default(false),
    infoNote: text("info_note").notNull().default(""),
    universeMarket: boolean("universe_market").notNull().default(false),
    marketType: marketTypeEnum("market_type").notNull().default("universum"),
    kuehlerStammnr: text("kuehler_stammnr"),
    isActive: boolean("is_active").notNull().default(true),
    importSourceFileName: text("import_source_file_name").notNull().default(""),
    importedAt: timestamp("imported_at", { withTimezone: true }).defaultNow().notNull(),
    plannedToId: uuid("planned_to_id"),
    isDeleted: boolean("is_deleted").notNull().default(false),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("markets_standard_market_number_unique")
      .on(table.standardMarketNumber)
      .where(sql`${table.isDeleted} = false AND ${table.standardMarketNumber} IS NOT NULL`),
    uniqueIndex("markets_coke_master_number_unique")
      .on(table.cokeMasterNumber)
      .where(sql`${table.isDeleted} = false AND ${table.cokeMasterNumber} IS NOT NULL`),
    uniqueIndex("markets_flex_number_unique")
      .on(table.flexNumber)
      .where(sql`${table.isDeleted} = false AND ${table.flexNumber} IS NOT NULL`),
    index("markets_region_idx").on(table.region),
    index("markets_market_type_idx").on(table.marketType),
    index("markets_city_idx").on(table.city),
    index("markets_postal_code_idx").on(table.postalCode),
    index("markets_current_gm_name_idx").on(table.currentGmName),
    index("markets_active_idx").on(table.isActive),
    index("markets_deleted_idx").on(table.isDeleted),
  ],
);

export const marketKuehlerUnits = pgTable(
  "market_kuehler_units",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    marketId: uuid("market_id")
      .notNull()
      .references(() => markets.id, { onDelete: "cascade" }),
    name: text("name").notNull().default(""),
    employee: text("employee").notNull().default(""),
    kuehlerInternalId: text("kuehler_internal_id"),
    kuehlerBd: text("kuehler_bd"),
    kuehlerAnzahlKsAmStandort: integer("kuehler_anzahl_ks_am_standort"),
    kuehlerSerialNumber: text("kuehler_serial_number"),
    kuehlerModel: text("kuehler_model"),
    importSourceFileName: text("import_source_file_name").notNull().default(""),
    importedAt: timestamp("imported_at", { withTimezone: true }).defaultNow().notNull(),
    isDeleted: boolean("is_deleted").notNull().default(false),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    index("market_kuehler_units_market_deleted_idx").on(table.marketId, table.isDeleted),
    index("market_kuehler_units_internal_id_idx").on(table.kuehlerInternalId),
    uniqueIndex("market_kuehler_units_internal_id_active_unique")
      .on(table.kuehlerInternalId)
      .where(sql`${table.isDeleted} = false AND ${table.kuehlerInternalId} IS NOT NULL`),
    index("market_kuehler_units_deleted_idx").on(table.isDeleted),
  ],
);

export const lager = pgTable(
  "lager",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    address: text("address").notNull(),
    postalCode: text("postal_code").notNull(),
    city: text("city").notNull(),
    gmUserId: uuid("gm_user_id").references(() => users.id, { onDelete: "set null" }),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    index("lager_gm_user_idx").on(table.gmUserId),
    index("lager_deleted_idx").on(table.isDeleted),
  ],
);

export const lagerGmAssignments = pgTable(
  "lager_gm_assignments",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    lagerId: uuid("lager_id")
      .notNull()
      .references(() => lager.id, { onDelete: "cascade" }),
    gmUserId: uuid("gm_user_id")
      .notNull()
      .references(() => users.id, { onDelete: "cascade" }),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("lager_gm_assignments_lager_gm_active_unique")
      .on(table.lagerId, table.gmUserId)
      .where(sql`${table.isDeleted} = false`),
    index("lager_gm_assignments_lager_deleted_idx").on(table.lagerId, table.isDeleted),
    index("lager_gm_assignments_gm_deleted_idx").on(table.gmUserId, table.isDeleted),
    index("lager_gm_assignments_deleted_idx").on(table.isDeleted),
  ],
);

export const authAuditLogs = pgTable(
  "auth_audit_logs",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    actorUserId: uuid("actor_user_id").references(() => users.id, { onDelete: "set null" }),
    targetUserId: uuid("target_user_id").references(() => users.id, { onDelete: "set null" }),
    eventType: text("event_type").notNull(),
    details: text("details"),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [index("auth_audit_logs_event_idx").on(table.eventType)],
);

export const campaigns = pgTable("campaigns", {
  id: uuid("id").defaultRandom().primaryKey(),
  name: text("name").notNull(),
  section: campaignSectionEnum("section").notNull(),
  assignedGmUserId: uuid("assigned_gm_user_id").references(() => users.id, { onDelete: "set null" }),
  currentFragebogenId: uuid("current_fragebogen_id"),
  status: fragebogenStatusEnum("status").notNull().default("inactive"),
  scheduleType: fragebogenScheduleTypeEnum("schedule_type").notNull().default("always"),
  startDate: date("start_date"),
  endDate: date("end_date"),
  isDeleted: boolean("is_deleted").notNull().default(false),
  deletedAt: timestamp("deleted_at", { withTimezone: true }),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
}, (table) => [
  check(
    "campaigns_schedule_dates_ck",
    sql`(${table.scheduleType} = 'always') OR (${table.startDate} IS NOT NULL AND ${table.endDate} IS NOT NULL AND ${table.startDate} <= ${table.endDate})`,
  ),
  check(
    "campaigns_assigned_gm_flex_only_ck",
    sql`${table.assignedGmUserId} IS NULL OR ${table.section} = 'flex'`,
  ),
  index("campaigns_section_idx").on(table.section),
  index("campaigns_status_idx").on(table.status),
  index("campaigns_deleted_idx").on(table.isDeleted),
  index("campaigns_current_fragebogen_idx").on(table.currentFragebogenId),
  index("campaigns_assigned_gm_active_idx")
    .on(table.assignedGmUserId, table.section, table.status)
    .where(sql`${table.isDeleted} = false AND ${table.assignedGmUserId} IS NOT NULL`),
]);

export const campaignMarketAssignments = pgTable(
  "campaign_market_assignments",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    campaignId: uuid("campaign_id")
      .notNull()
      .references(() => campaigns.id, { onDelete: "cascade" }),
    marketId: uuid("market_id")
      .notNull()
      .references(() => markets.id, { onDelete: "cascade" }),
    assignedAt: timestamp("assigned_at", { withTimezone: true }).defaultNow().notNull(),
    gmUserId: uuid("gm_user_id").references(() => users.id, { onDelete: "set null" }),
    assignmentSlot: integer("assignment_slot").notNull().default(1),
    visitTargetCount: integer("visit_target_count").notNull().default(1),
    currentVisitsCount: integer("current_visits_count").notNull().default(0),
    assignedByUserId: uuid("assigned_by_user_id").references(() => users.id, { onDelete: "set null" }),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    check("campaign_market_assignments_assignment_slot_ck", sql`${table.assignmentSlot} >= 1`),
    check("campaign_market_assignments_visit_target_count_ck", sql`${table.visitTargetCount} >= 1`),
    check("campaign_market_assignments_current_visits_count_ck", sql`${table.currentVisitsCount} >= 0`),
    uniqueIndex("campaign_market_assignments_campaign_market_gm_active_unique")
      .on(table.campaignId, table.marketId, table.gmUserId, table.assignmentSlot)
      .where(sql`${table.isDeleted} = false and ${table.gmUserId} is not null`),
    uniqueIndex("campaign_market_assignments_campaign_market_unassigned_active_unique")
      .on(table.campaignId, table.marketId, table.assignmentSlot)
      .where(sql`${table.isDeleted} = false and ${table.gmUserId} is null`),
    index("campaign_market_assignments_campaign_market_active_idx")
      .on(table.campaignId, table.marketId)
      .where(sql`${table.isDeleted} = false`),
    index("campaign_market_assignments_market_idx").on(table.marketId),
    index("campaign_market_assignments_gm_deleted_idx").on(table.gmUserId, table.isDeleted),
    index("campaign_market_assignments_campaign_deleted_idx").on(table.campaignId, table.isDeleted),
    index("campaign_market_assignments_deleted_idx").on(table.isDeleted),
  ],
);

export const campaignFragebogenHistory = pgTable(
  "campaign_fragebogen_history",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    campaignId: uuid("campaign_id")
      .notNull()
      .references(() => campaigns.id, { onDelete: "cascade" }),
    fromFragebogenId: uuid("from_fragebogen_id"),
    toFragebogenId: uuid("to_fragebogen_id").notNull(),
    changedAt: timestamp("changed_at", { withTimezone: true }).defaultNow().notNull(),
    changedByUserId: uuid("changed_by_user_id").references(() => users.id, { onDelete: "set null" }),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    index("campaign_fragebogen_history_campaign_idx").on(table.campaignId),
    index("campaign_fragebogen_history_campaign_deleted_idx").on(table.campaignId, table.isDeleted),
    index("campaign_fragebogen_history_changed_at_idx").on(table.changedAt),
    index("campaign_fragebogen_history_deleted_idx").on(table.isDeleted),
  ],
);

export const campaignMarketAssignmentHistory = pgTable(
  "campaign_market_assignment_history",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    marketId: uuid("market_id")
      .notNull()
      .references(() => markets.id, { onDelete: "cascade" }),
    section: campaignSectionEnum("section").notNull(),
    fromCampaignId: uuid("from_campaign_id")
      .notNull()
      .references(() => campaigns.id, { onDelete: "cascade" }),
    toCampaignId: uuid("to_campaign_id")
      .notNull()
      .references(() => campaigns.id, { onDelete: "cascade" }),
    fromGmUserId: uuid("from_gm_user_id").references(() => users.id, { onDelete: "set null" }),
    toGmUserId: uuid("to_gm_user_id").references(() => users.id, { onDelete: "set null" }),
    migratedByUserId: uuid("migrated_by_user_id").references(() => users.id, { onDelete: "set null" }),
    migratedAt: timestamp("migrated_at", { withTimezone: true }).defaultNow().notNull(),
    reason: text("reason"),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    index("campaign_market_assignment_history_market_section_migrated_idx").on(
      table.marketId,
      table.section,
      table.migratedAt,
    ),
    index("campaign_market_assignment_history_from_campaign_idx").on(table.fromCampaignId),
    index("campaign_market_assignment_history_to_campaign_idx").on(table.toCampaignId),
  ],
);

export const redMonthCalendarConfig = pgTable(
  "red_month_calendar_config",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    anchorStart: date("anchor_start").notNull(),
    cycleWeeks: integer("cycle_weeks").array().notNull(),
    timezone: text("timezone").notNull().default("Europe/Vienna"),
    isActive: boolean("is_active").notNull().default(true),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    check("red_month_calendar_config_cycle_weeks_not_empty_ck", sql`cardinality(${table.cycleWeeks}) > 0`),
    uniqueIndex("red_month_calendar_config_single_active_unique")
      .on(table.isActive)
      .where(sql`${table.isActive} = true and ${table.isDeleted} = false`),
    index("red_month_calendar_config_active_idx").on(table.isActive, table.isDeleted),
  ],
);

export const redMonthYears = pgTable(
  "red_month_years",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    redYear: integer("red_year").notNull(),
    anchorStart: date("anchor_start").notNull(),
    cycleWeeks: integer("cycle_weeks").array().notNull(),
    periodCount: integer("period_count").notNull().default(13),
    timezone: text("timezone").notNull().default("Europe/Vienna"),
    status: redMonthYearStatusEnum("status").notNull().default("draft"),
    createdByUserId: uuid("created_by_user_id").references(() => users.id, { onDelete: "set null" }),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    check("red_month_years_cycle_weeks_not_empty_ck", sql`cardinality(${table.cycleWeeks}) > 0`),
    check("red_month_years_period_count_positive_ck", sql`${table.periodCount} >= 1`),
    check("red_month_years_status_ck", sql`${table.status} in ('draft','active','locked')`),
    uniqueIndex("red_month_years_year_active_unique")
      .on(table.redYear)
      .where(sql`${table.isDeleted} = false`),
    index("red_month_years_status_idx").on(table.status, table.isDeleted),
    index("red_month_years_anchor_idx").on(table.anchorStart),
    index("red_month_years_created_by_idx").on(table.createdByUserId),
  ],
);

export const redMonthPeriods = pgTable(
  "red_month_periods",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    redMonthYearId: uuid("red_month_year_id")
      .notNull()
      .references(() => redMonthYears.id, { onDelete: "cascade" }),
    redYear: integer("red_year").notNull(),
    periodIndex: integer("period_index").notNull(),
    label: text("label").notNull(),
    startDate: date("start_date").notNull(),
    endDate: date("end_date").notNull(),
    lookupEndDate: date("lookup_end_date").notNull(),
    cycleIndex: integer("cycle_index").notNull(),
    cycleWeeks: integer("cycle_weeks").notNull(),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    check("red_month_periods_period_index_positive_ck", sql`${table.periodIndex} >= 1`),
    check("red_month_periods_cycle_weeks_positive_ck", sql`${table.cycleWeeks} >= 1`),
    check("red_month_periods_date_order_ck", sql`${table.startDate} <= ${table.endDate} and ${table.endDate} <= ${table.lookupEndDate}`),
    uniqueIndex("red_month_periods_year_period_unique")
      .on(table.redMonthYearId, table.periodIndex)
      .where(sql`${table.isDeleted} = false`),
    uniqueIndex("red_month_periods_year_label_unique")
      .on(table.redMonthYearId, table.label)
      .where(sql`${table.isDeleted} = false`),
    uniqueIndex("red_month_periods_start_date_unique")
      .on(table.startDate)
      .where(sql`${table.isDeleted} = false`),
    index("red_month_periods_lookup_idx").on(table.startDate, table.lookupEndDate),
    index("red_month_periods_red_year_idx").on(table.redYear, table.periodIndex),
    index("red_month_periods_year_id_idx").on(table.redMonthYearId),
  ],
);

export const ippMarketRedmonthResults = pgTable(
  "ipp_market_redmonth_results",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    marketId: uuid("market_id")
      .notNull()
      .references(() => markets.id, { onDelete: "cascade" }),
    redPeriodId: uuid("red_period_id").references(() => redMonthPeriods.id, { onDelete: "set null" }),
    redPeriodStart: date("red_period_start").notNull(),
    redPeriodEnd: date("red_period_end").notNull(),
    redPeriodLabel: text("red_period_label").notNull(),
    redPeriodYear: integer("red_period_year").notNull(),
    marketIpp: numeric("market_ipp", { precision: 12, scale: 4 }).notNull().default("0"),
    sourceSubmissionCount: integer("source_submission_count").notNull().default(0),
    contributingQuestionCount: integer("contributing_question_count").notNull().default(0),
    questionRowsSnapshot: jsonb("question_rows_snapshot").$type<Array<Record<string, unknown>>>().notNull().default(sql`'[]'::jsonb`),
    snapshotVersion: integer("snapshot_version").notNull().default(1),
    isFinalized: boolean("is_finalized").notNull().default(true),
    computedAt: timestamp("computed_at", { withTimezone: true }).defaultNow().notNull(),
    finalizedAt: timestamp("finalized_at", { withTimezone: true }).defaultNow().notNull(),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("ipp_market_redmonth_results_market_period_active_unique")
      .on(table.marketId, table.redPeriodStart)
      .where(sql`${table.isDeleted} = false`),
    index("ipp_market_redmonth_results_period_idx").on(table.redPeriodStart, table.redPeriodEnd),
    index("ipp_market_redmonth_results_year_idx").on(table.redPeriodYear, table.isDeleted),
    index("ipp_market_redmonth_results_market_year_idx").on(table.marketId, table.redPeriodYear),
    index("ipp_market_redmonth_results_period_market_active_idx")
      .on(table.redPeriodStart, table.marketId)
      .where(sql`${table.isDeleted} = false`),
    index("ipp_market_redmonth_results_red_period_market_idx")
      .on(table.redPeriodId, table.marketId)
      .where(sql`${table.isDeleted} = false`),
    index("ipp_market_redmonth_results_deleted_idx").on(table.isDeleted),
  ],
);

export const ippRecalcQueue = pgTable(
  "ipp_recalc_queue",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    marketId: uuid("market_id")
      .notNull()
      .references(() => markets.id, { onDelete: "cascade" }),
    redPeriodId: uuid("red_period_id").references(() => redMonthPeriods.id, { onDelete: "set null" }),
    redPeriodStart: date("red_period_start").notNull(),
    redPeriodEnd: date("red_period_end").notNull(),
    reason: text("reason").notNull().default("unspecified"),
    status: text("status").notNull().default("pending"),
    attempts: integer("attempts").notNull().default(0),
    lastError: text("last_error"),
    queuedAt: timestamp("queued_at", { withTimezone: true }).defaultNow().notNull(),
    startedAt: timestamp("started_at", { withTimezone: true }),
    finishedAt: timestamp("finished_at", { withTimezone: true }),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    check(
      "ipp_recalc_queue_status_ck",
      sql`${table.status} in ('pending','processing','done','failed')`,
    ),
    uniqueIndex("ipp_recalc_queue_market_period_active_unique")
      .on(table.marketId, table.redPeriodStart)
      .where(sql`${table.isDeleted} = false and ${table.status} in ('pending','processing')`),
    index("ipp_recalc_queue_status_idx").on(table.status, table.queuedAt),
    index("ipp_recalc_queue_market_period_idx").on(table.marketId, table.redPeriodStart),
    index("ipp_recalc_queue_red_period_market_idx")
      .on(table.redPeriodId, table.marketId)
      .where(sql`${table.isDeleted} = false and ${table.status} in ('pending','processing')`),
    index("ipp_recalc_queue_deleted_idx").on(table.isDeleted),
  ],
);

export const gmKpiCache = pgTable(
  "gm_kpi_cache",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    gmUserId: uuid("gm_user_id")
      .notNull()
      .references(() => users.id, { onDelete: "cascade" }),
    ippAllTimeAvg: numeric("ipp_all_time_avg", { precision: 12, scale: 4 }).notNull().default("0"),
    ippSampleCount: integer("ipp_sample_count").notNull().default(0),
    bonusCumulativeEur: numeric("bonus_cumulative_eur", { precision: 14, scale: 2 }).notNull().default("0"),
    lastComputedAt: timestamp("last_computed_at", { withTimezone: true }).defaultNow().notNull(),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("gm_kpi_cache_gm_user_active_unique")
      .on(table.gmUserId)
      .where(sql`${table.isDeleted} = false`),
    index("gm_kpi_cache_gm_user_idx").on(table.gmUserId),
    index("gm_kpi_cache_last_computed_idx").on(table.lastComputedAt),
    index("gm_kpi_cache_deleted_idx").on(table.isDeleted),
  ],
);

export const ippGmRedmonthAdjustmentEvents = pgTable(
  "ipp_gm_redmonth_adjustment_events",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    revisionNumber: bigint("revision_number", { mode: "number" }).generatedAlwaysAsIdentity().notNull(),
    requestId: uuid("request_id").notNull(),
    gmUserId: uuid("gm_user_id")
      .notNull()
      .references(() => users.id, { onDelete: "restrict" }),
    redPeriodId: uuid("red_period_id")
      .notNull()
      .references(() => redMonthPeriods.id, { onDelete: "restrict" }),
    eventType: ippGmAdjustmentEventTypeEnum("event_type").notNull(),
    correctedIpp: numeric("corrected_ipp", { precision: 12, scale: 4 }),
    baseCalculatedIpp: numeric("base_calculated_ipp", { precision: 12, scale: 4 }).notNull(),
    baseSampleCount: integer("base_sample_count").notNull(),
    baseFingerprint: text("base_fingerprint").notNull(),
    reason: text("reason").notNull(),
    createdByUserId: uuid("created_by_user_id")
      .notNull()
      .references(() => users.id, { onDelete: "restrict" }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("ipp_gm_redmonth_adjustment_events_revision_unique").on(table.revisionNumber),
    uniqueIndex("ipp_gm_redmonth_adjustment_events_request_unique").on(table.requestId),
    index("ipp_gm_redmonth_adjustment_events_latest_idx").on(
      table.gmUserId,
      table.redPeriodId,
      table.revisionNumber,
    ),
    index("ipp_gm_redmonth_adjustment_events_period_idx").on(table.redPeriodId, table.gmUserId),
    index("ipp_gm_redmonth_adjustment_events_created_by_idx").on(table.createdByUserId),
    check("ipp_gm_redmonth_adjustment_events_base_sample_count_ck", sql`${table.baseSampleCount} >= 0`),
    check(
      "ipp_gm_redmonth_adjustment_events_value_ck",
      sql`(${table.eventType} = 'set' and ${table.correctedIpp} is not null and ${table.correctedIpp} >= 0) or (${table.eventType} = 'clear' and ${table.correctedIpp} is null)`,
    ),
    check("ipp_gm_redmonth_adjustment_events_reason_ck", sql`char_length(btrim(${table.reason})) >= 8`),
  ],
);

export const photoTags = pgTable(
  "photo_tags",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    label: text("label").notNull(),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("photo_tags_label_active_unique")
      .on(table.label)
      .where(sql`${table.isDeleted} = false`),
  ],
);

export const questionBankShared = pgTable(
  "question_bank_shared",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    questionType: questionTypeEnum("question_type").notNull(),
    text: text("text").notNull().default(""),
    required: boolean("required").notNull().default(true),
    redSurvey: boolean("red_survey"),
    singleChoiceAvailability: boolean("single_choice_availability"),
    singleChoiceAvailabilityType: text("single_choice_availability_type"),
    chains: text("chains").array(),
    config: jsonb("config").$type<Record<string, unknown>>().notNull().default(sql`'{}'::jsonb`),
    rules: jsonb("rules").$type<Array<Record<string, unknown>>>().notNull().default(sql`'[]'::jsonb`),
    scoring: jsonb("scoring").$type<Record<string, unknown>>().notNull().default(sql`'{}'::jsonb`),
    isSpezial: boolean("is_spezial").notNull().default(false),
    isDeleted: boolean("is_deleted").notNull().default(false),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    index("question_bank_shared_type_idx").on(table.questionType),
    index("question_bank_shared_spezial_idx").on(table.isSpezial, table.isDeleted),
    index("question_bank_shared_deleted_idx").on(table.isDeleted),
  ],
);

export const questionRules = pgTable(
  "question_rules",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    questionId: uuid("question_id")
      .notNull()
      .references(() => questionBankShared.id, { onDelete: "cascade" }),
    triggerQuestionId: uuid("trigger_question_id").references(() => questionBankShared.id, {
      onDelete: "set null",
    }),
    operator: text("operator").notNull(),
    triggerValue: text("trigger_value"),
    triggerValueMax: text("trigger_value_max"),
    action: questionRuleActionEnum("action").notNull(),
    orderIndex: integer("order_index").notNull().default(0),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    index("question_rules_question_idx").on(table.questionId),
    index("question_rules_deleted_idx").on(table.isDeleted),
  ],
);

export const questionRuleTargets = pgTable(
  "question_rule_targets",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    ruleId: uuid("rule_id")
      .notNull()
      .references(() => questionRules.id, { onDelete: "cascade" }),
    targetQuestionId: uuid("target_question_id")
      .notNull()
      .references(() => questionBankShared.id, { onDelete: "cascade" }),
    orderIndex: integer("order_index").notNull().default(0),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("question_rule_targets_rule_target_active_unique")
      .on(table.ruleId, table.targetQuestionId)
      .where(sql`${table.isDeleted} = false`),
    index("question_rule_targets_target_idx").on(table.targetQuestionId),
    index("question_rule_targets_deleted_idx").on(table.isDeleted),
  ],
);

export const questionScoring = pgTable(
  "question_scoring",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    questionId: uuid("question_id")
      .notNull()
      .references(() => questionBankShared.id, { onDelete: "cascade" }),
    scoreKey: text("score_key").notNull(),
    ipp: numeric("ipp", { precision: 8, scale: 2 }),
    zweitplatzierung: numeric("zweitplatzierung", { precision: 8, scale: 2 }),
    mitbewerberabfrage: numeric("mitbewerberabfrage", { precision: 8, scale: 2 }),
    boni: numeric("boni", { precision: 8, scale: 2 }),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("question_scoring_question_key_unique")
      .on(table.questionId, table.scoreKey)
      .where(sql`${table.isDeleted} = false`),
    index("question_scoring_boni_active_idx")
      .on(table.questionId, table.boni)
      .where(sql`${table.isDeleted} = false and ${table.boni} is not null`),
    index("question_scoring_zweitplatzierung_active_idx")
      .on(table.questionId, table.zweitplatzierung)
      .where(sql`${table.isDeleted} = false and ${table.zweitplatzierung} is not null`),
    index("question_scoring_mitbewerberabfrage_active_idx")
      .on(table.questionId, table.mitbewerberabfrage)
      .where(sql`${table.isDeleted} = false and ${table.mitbewerberabfrage} is not null`),
    index("question_scoring_ipp_question_key_active_idx")
      .on(table.questionId, table.scoreKey)
      .where(sql`${table.isDeleted} = false and ${table.ipp} is not null`),
    index("question_scoring_deleted_idx").on(table.isDeleted),
  ],
);

export const praemienWaves = pgTable(
  "praemien_waves",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    name: text("name").notNull(),
    year: integer("year").notNull(),
    quarter: integer("quarter").notNull(),
    startDate: date("start_date").notNull(),
    endDate: date("end_date").notNull(),
    status: praemienWaveStatusEnum("status").notNull().default("draft"),
    description: text("description").notNull().default(""),
    timezone: text("timezone").notNull().default("Europe/Vienna"),
    rewardModel: praemienRewardModelEnum("reward_model").notNull().default("global_thresholds"),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    check("praemien_waves_quarter_ck", sql`${table.quarter} in (1, 2, 3, 4)`),
    check("praemien_waves_date_range_ck", sql`${table.startDate} <= ${table.endDate}`),
    uniqueIndex("praemien_waves_year_quarter_active_unique")
      .on(table.year, table.quarter)
      .where(sql`${table.isDeleted} = false and ${table.status} = 'active'`),
    index("praemien_waves_status_idx").on(table.status, table.isDeleted),
    index("praemien_waves_period_idx").on(table.year, table.quarter),
  ],
);

export const praemienWaveThresholds = pgTable(
  "praemien_wave_thresholds",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    waveId: uuid("wave_id")
      .notNull()
      .references(() => praemienWaves.id, { onDelete: "cascade" }),
    label: text("label").notNull(),
    orderIndex: integer("order_index").notNull().default(0),
    minPoints: numeric("min_points", { precision: 12, scale: 2 }).notNull().default("0"),
    rewardEur: numeric("reward_eur", { precision: 12, scale: 2 }).notNull().default("0"),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    check("praemien_wave_thresholds_min_points_ck", sql`${table.minPoints} >= 0`),
    check("praemien_wave_thresholds_reward_eur_ck", sql`${table.rewardEur} >= 0`),
    uniqueIndex("praemien_wave_thresholds_wave_order_active_unique")
      .on(table.waveId, table.orderIndex)
      .where(sql`${table.isDeleted} = false`),
    index("praemien_wave_thresholds_wave_idx").on(table.waveId, table.isDeleted),
  ],
);

export const praemienWavePillars = pgTable(
  "praemien_wave_pillars",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    waveId: uuid("wave_id")
      .notNull()
      .references(() => praemienWaves.id, { onDelete: "cascade" }),
    name: text("name").notNull(),
    description: text("description").notNull().default(""),
    color: text("color").notNull().default("#DC2626"),
    orderIndex: integer("order_index").notNull().default(0),
    isManual: boolean("is_manual").notNull().default(false),
    payoutMode: text("payout_mode").notNull().default("highest_tier"),
    maxRewardEur: numeric("max_reward_eur", { precision: 12, scale: 2 }).notNull().default("0"),
    targetPoints: numeric("target_points", { precision: 14, scale: 4 }),
    rewardEur: numeric("reward_eur", { precision: 12, scale: 2 }).notNull().default("0"),
    carryAnswersForWave: boolean("carry_answers_for_wave").notNull().default(false),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    check("praemien_wave_pillars_target_points_ck", sql`${table.targetPoints} is null or ${table.targetPoints} > 0`),
    check("praemien_wave_pillars_reward_eur_ck", sql`${table.rewardEur} >= 0`),
    check("praemien_wave_pillars_max_reward_eur_ck", sql`${table.maxRewardEur} >= 0`),
    check("praemien_wave_pillars_payout_mode_ck", sql`${table.payoutMode} in ('highest_tier', 'sum_earned_tiers')`),
    uniqueIndex("praemien_wave_pillars_wave_order_active_unique")
      .on(table.waveId, table.orderIndex)
      .where(sql`${table.isDeleted} = false`),
    uniqueIndex("praemien_wave_pillars_wave_name_active_unique")
      .on(table.waveId, table.name)
      .where(sql`${table.isDeleted} = false`),
    index("praemien_wave_pillars_wave_idx").on(table.waveId, table.isDeleted),
  ],
);

export const praemienWavePillarMetrics = pgTable(
  "praemien_wave_pillar_metrics",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    waveId: uuid("wave_id")
      .notNull()
      .references(() => praemienWaves.id, { onDelete: "cascade" }),
    pillarId: uuid("pillar_id")
      .notNull()
      .references(() => praemienWavePillars.id, { onDelete: "cascade" }),
    key: text("key").notNull(),
    label: text("label").notNull(),
    unit: text("unit").notNull().default("points"),
    valueSource: text("value_source").notNull().default("contribution_points"),
    sourceKey: text("source_key"),
    orderIndex: integer("order_index").notNull().default(0),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    check("praemien_wave_pillar_metrics_unit_ck", sql`${table.unit} in ('points', 'percent', 'count', 'currency')`),
    check(
      "praemien_wave_pillar_metrics_value_source_ck",
      sql`${table.valueSource} in ('contribution_points', 'contribution_percent', 'quality_zeiterfassung', 'quality_reporting', 'quality_accuracy', 'quality_average', 'flex_total_points', 'flex_component')`,
    ),
    uniqueIndex("praemien_wave_pillar_metrics_pillar_key_active_unique")
      .on(table.pillarId, table.key)
      .where(sql`${table.isDeleted} = false`),
    uniqueIndex("praemien_wave_pillar_metrics_pillar_order_active_unique")
      .on(table.pillarId, table.orderIndex)
      .where(sql`${table.isDeleted} = false`),
    index("praemien_wave_pillar_metrics_wave_idx").on(table.waveId, table.isDeleted),
    index("praemien_wave_pillar_metrics_pillar_idx").on(table.pillarId, table.isDeleted),
  ],
);

export const praemienWavePillarTiers = pgTable(
  "praemien_wave_pillar_tiers",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    waveId: uuid("wave_id")
      .notNull()
      .references(() => praemienWaves.id, { onDelete: "cascade" }),
    pillarId: uuid("pillar_id")
      .notNull()
      .references(() => praemienWavePillars.id, { onDelete: "cascade" }),
    label: text("label").notNull(),
    orderIndex: integer("order_index").notNull().default(0),
    rewardEur: numeric("reward_eur", { precision: 12, scale: 2 }).notNull().default("0"),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    check("praemien_wave_pillar_tiers_reward_eur_ck", sql`${table.rewardEur} >= 0`),
    uniqueIndex("praemien_wave_pillar_tiers_pillar_order_active_unique")
      .on(table.pillarId, table.orderIndex)
      .where(sql`${table.isDeleted} = false`),
    uniqueIndex("praemien_wave_pillar_tiers_pillar_label_active_unique")
      .on(table.pillarId, table.label)
      .where(sql`${table.isDeleted} = false`),
    index("praemien_wave_pillar_tiers_wave_idx").on(table.waveId, table.isDeleted),
    index("praemien_wave_pillar_tiers_pillar_idx").on(table.pillarId, table.isDeleted),
  ],
);

export const praemienWavePillarTierConditions = pgTable(
  "praemien_wave_pillar_tier_conditions",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    waveId: uuid("wave_id")
      .notNull()
      .references(() => praemienWaves.id, { onDelete: "cascade" }),
    pillarId: uuid("pillar_id")
      .notNull()
      .references(() => praemienWavePillars.id, { onDelete: "cascade" }),
    tierId: uuid("tier_id")
      .notNull()
      .references(() => praemienWavePillarTiers.id, { onDelete: "cascade" }),
    metricId: uuid("metric_id")
      .notNull()
      .references(() => praemienWavePillarMetrics.id, { onDelete: "cascade" }),
    operator: text("operator").notNull().default("gte"),
    thresholdValue: numeric("threshold_value", { precision: 14, scale: 4 }).notNull(),
    orderIndex: integer("order_index").notNull().default(0),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    check("praemien_wave_pillar_tier_conditions_operator_ck", sql`${table.operator} in ('gte', 'lte', 'eq')`),
    uniqueIndex("praemien_wave_pillar_tier_conditions_tier_order_active_unique")
      .on(table.tierId, table.orderIndex)
      .where(sql`${table.isDeleted} = false`),
    index("praemien_wave_pillar_tier_conditions_wave_idx").on(table.waveId, table.isDeleted),
    index("praemien_wave_pillar_tier_conditions_pillar_idx").on(table.pillarId, table.isDeleted),
    index("praemien_wave_pillar_tier_conditions_tier_idx").on(table.tierId, table.isDeleted),
    index("praemien_wave_pillar_tier_conditions_metric_idx").on(table.metricId, table.isDeleted),
  ],
);

export const praemienWaveSources = pgTable(
  "praemien_wave_sources",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    waveId: uuid("wave_id")
      .notNull()
      .references(() => praemienWaves.id, { onDelete: "cascade" }),
    pillarId: uuid("pillar_id")
      .notNull()
      .references(() => praemienWavePillars.id, { onDelete: "cascade" }),
    sectionType: fragebogenSectionEnum("section_type").notNull(),
    fragebogenId: uuid("fragebogen_id"),
    fragebogenName: text("fragebogen_name").notNull().default(""),
    moduleId: uuid("module_id"),
    moduleName: text("module_name").notNull().default(""),
    questionId: uuid("question_id").notNull(),
    questionText: text("question_text").notNull().default(""),
    scoreKey: text("score_key").notNull(),
    displayLabel: text("display_label").notNull().default(""),
    isFactorMode: boolean("is_factor_mode").notNull().default(false),
    boniValue: numeric("boni_value", { precision: 12, scale: 2 }).notNull().default("0"),
    distributionFreqRule: praemienDistributionFreqRuleEnum("distribution_freq_rule"),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("praemien_wave_sources_wave_question_score_active_unique")
      .on(table.waveId, table.questionId, table.scoreKey)
      .where(sql`${table.isDeleted} = false`),
    index("praemien_wave_sources_wave_idx").on(table.waveId, table.isDeleted),
    index("praemien_wave_sources_pillar_idx").on(table.pillarId, table.isDeleted),
  ],
);

export const praemienWaveQualityScores = pgTable(
  "praemien_wave_quality_scores",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    waveId: uuid("wave_id")
      .notNull()
      .references(() => praemienWaves.id, { onDelete: "cascade" }),
    gmUserId: uuid("gm_user_id")
      .notNull()
      .references(() => users.id, { onDelete: "cascade" }),
    zeiterfassung: integer("zeiterfassung").notNull().default(0),
    reporting: integer("reporting").notNull().default(0),
    accuracy: integer("accuracy").notNull().default(0),
    totalPoints: integer("total_points").notNull().default(0),
    note: text("note"),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    check("praemien_wave_quality_scores_zeiterfassung_ck", sql`${table.zeiterfassung} between 0 and 100`),
    check("praemien_wave_quality_scores_reporting_ck", sql`${table.reporting} between 0 and 100`),
    check("praemien_wave_quality_scores_accuracy_ck", sql`${table.accuracy} between 0 and 100`),
    check("praemien_wave_quality_scores_total_points_ck", sql`${table.totalPoints} between 0 and 100`),
    uniqueIndex("praemien_wave_quality_scores_wave_gm_active_unique")
      .on(table.waveId, table.gmUserId)
      .where(sql`${table.isDeleted} = false`),
    index("praemien_wave_quality_scores_wave_idx").on(table.waveId, table.isDeleted),
  ],
);

export const praemienWaveFlexScores = pgTable(
  "praemien_wave_flex_scores",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    waveId: uuid("wave_id")
      .notNull()
      .references(() => praemienWaves.id, { onDelete: "cascade" }),
    gmUserId: uuid("gm_user_id")
      .notNull()
      .references(() => users.id, { onDelete: "cascade" }),
    totalPoints: integer("total_points").notNull().default(0),
    componentValues: jsonb("component_values").$type<Record<string, number>>().notNull().default({}),
    note: text("note"),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    check("praemien_wave_flex_scores_total_points_ck", sql`${table.totalPoints} between 0 and 100`),
    uniqueIndex("praemien_wave_flex_scores_wave_gm_active_unique")
      .on(table.waveId, table.gmUserId)
      .where(sql`${table.isDeleted} = false`),
    index("praemien_wave_flex_scores_wave_idx").on(table.waveId, table.isDeleted),
  ],
);

export const praemienGmWaveTotals = pgTable(
  "praemien_gm_wave_totals",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    waveId: uuid("wave_id")
      .notNull()
      .references(() => praemienWaves.id, { onDelete: "cascade" }),
    gmUserId: uuid("gm_user_id")
      .notNull()
      .references(() => users.id, { onDelete: "cascade" }),
    totalPoints: numeric("total_points", { precision: 14, scale: 4 }).notNull().default("0"),
    currentRewardEur: numeric("current_reward_eur", { precision: 12, scale: 2 }).notNull().default("0"),
    contributionCount: integer("contribution_count").notNull().default(0),
    lastContributionAt: timestamp("last_contribution_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("praemien_gm_wave_totals_wave_gm_unique").on(table.waveId, table.gmUserId),
    index("praemien_gm_wave_totals_gm_idx").on(table.gmUserId, table.updatedAt),
  ],
);

export const praemienGmWavePillarTotals = pgTable(
  "praemien_gm_wave_pillar_totals",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    waveId: uuid("wave_id")
      .notNull()
      .references(() => praemienWaves.id, { onDelete: "cascade" }),
    gmUserId: uuid("gm_user_id")
      .notNull()
      .references(() => users.id, { onDelete: "cascade" }),
    pillarId: uuid("pillar_id")
      .notNull()
      .references(() => praemienWavePillars.id, { onDelete: "cascade" }),
    points: numeric("points", { precision: 14, scale: 4 }).notNull().default("0"),
    targetPoints: numeric("target_points", { precision: 14, scale: 4 }),
    rewardEur: numeric("reward_eur", { precision: 12, scale: 2 }).notNull().default("0"),
    earnedRewardEur: numeric("earned_reward_eur", { precision: 12, scale: 2 }).notNull().default("0"),
    goalAchieved: boolean("goal_achieved").notNull().default(false),
    metricValues: jsonb("metric_values").$type<Record<string, number>>().notNull().default({}),
    achievedTierIds: jsonb("achieved_tier_ids").$type<string[]>().notNull().default([]),
    achievedTierLabels: jsonb("achieved_tier_labels").$type<string[]>().notNull().default([]),
    nextTierId: uuid("next_tier_id").references(() => praemienWavePillarTiers.id, { onDelete: "set null" }),
    nextTierLabel: text("next_tier_label"),
    calculatedAt: timestamp("calculated_at", { withTimezone: true }).defaultNow().notNull(),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    check("praemien_gm_wave_pillar_totals_points_ck", sql`${table.points} >= 0`),
    check("praemien_gm_wave_pillar_totals_target_points_ck", sql`${table.targetPoints} is null or ${table.targetPoints} > 0`),
    check("praemien_gm_wave_pillar_totals_reward_eur_ck", sql`${table.rewardEur} >= 0`),
    check("praemien_gm_wave_pillar_totals_earned_reward_eur_ck", sql`${table.earnedRewardEur} >= 0`),
    uniqueIndex("praemien_gm_wave_pillar_totals_wave_gm_pillar_unique")
      .on(table.waveId, table.gmUserId, table.pillarId),
    index("praemien_gm_wave_pillar_totals_wave_gm_idx").on(table.waveId, table.gmUserId),
    index("praemien_gm_wave_pillar_totals_gm_wave_idx").on(table.gmUserId, table.waveId),
    index("praemien_gm_wave_pillar_totals_pillar_gm_idx").on(table.pillarId, table.gmUserId),
  ],
);

export const praemienGmWaveContributions = pgTable(
  "praemien_gm_wave_contributions",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    waveId: uuid("wave_id")
      .notNull()
      .references(() => praemienWaves.id, { onDelete: "cascade" }),
    gmUserId: uuid("gm_user_id")
      .notNull()
      .references(() => users.id, { onDelete: "cascade" }),
    marketId: uuid("market_id")
      .notNull()
      .references(() => markets.id, { onDelete: "cascade" }),
    sourceId: uuid("source_id")
      .notNull()
      .references(() => praemienWaveSources.id, { onDelete: "cascade" }),
    pillarId: uuid("pillar_id")
      .notNull()
      .references(() => praemienWavePillars.id, { onDelete: "cascade" }),
    visitSessionId: uuid("visit_session_id"),
    redPeriodId: uuid("red_period_id").references(() => redMonthPeriods.id, { onDelete: "set null" }),
    redPeriodStart: date("red_period_start").notNull(),
    redPeriodEnd: date("red_period_end").notNull(),
    questionId: uuid("question_id").notNull(),
    scoreKey: text("score_key").notNull(),
    appliedValue: numeric("applied_value", { precision: 14, scale: 4 }).notNull().default("0"),
    submittedAt: timestamp("submitted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("praemien_gm_wave_contrib_dedupe_unique")
      .on(table.waveId, table.gmUserId, table.marketId, table.redPeriodStart, table.sourceId),
    index("praemien_gm_wave_contrib_wave_gm_idx").on(table.waveId, table.gmUserId, table.updatedAt),
    index("praemien_gm_wave_contrib_source_idx").on(table.sourceId, table.updatedAt),
    index("praemien_gm_wave_contrib_red_period_idx").on(table.redPeriodId, table.gmUserId),
  ],
);

export const questionMatrix = pgTable(
  "question_matrix",
  {
    questionId: uuid("question_id")
      .primaryKey()
      .references(() => questionBankShared.id, { onDelete: "cascade" }),
    matrixSubtype: text("matrix_subtype").notNull().default("toggle"),
    rows: jsonb("rows").$type<string[]>().notNull().default(sql`'[]'::jsonb`),
    columns: jsonb("columns").$type<string[]>().notNull().default(sql`'[]'::jsonb`),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [index("question_matrix_deleted_idx").on(table.isDeleted)],
);

export const questionAttachments = pgTable(
  "question_attachments",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    questionId: uuid("question_id")
      .notNull()
      .references(() => questionBankShared.id, { onDelete: "cascade" }),
    payload: text("payload").notNull(),
    orderIndex: integer("order_index").notNull().default(0),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    index("question_attachments_question_idx").on(table.questionId),
    index("question_attachments_deleted_idx").on(table.isDeleted),
  ],
);

export const questionPhotoTags = pgTable(
  "question_photo_tags",
  {
    questionId: uuid("question_id")
      .notNull()
      .references(() => questionBankShared.id, { onDelete: "cascade" }),
    photoTagId: uuid("photo_tag_id")
      .notNull()
      .references(() => photoTags.id, { onDelete: "cascade" }),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.questionId, table.photoTagId] }),
    index("question_photo_tags_tag_idx").on(table.photoTagId),
    index("question_photo_tags_deleted_idx").on(table.isDeleted),
  ],
);

export const moduleMain = pgTable("module_main", {
  id: uuid("id").defaultRandom().primaryKey(),
  name: text("name").notNull(),
  description: text("description").notNull().default(""),
  sectionKeywords: fragebogenMainSectionEnum("section_keywords").array().notNull().default(sql`'{standard}'::fragebogen_main_section[]`),
  isDeleted: boolean("is_deleted").notNull().default(false),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
});

export const moduleMainQuestion = pgTable(
  "module_main_question",
  {
    moduleId: uuid("module_id")
      .notNull()
      .references(() => moduleMain.id, { onDelete: "cascade" }),
    questionId: uuid("question_id")
      .notNull()
      .references(() => questionBankShared.id, { onDelete: "cascade" }),
    orderIndex: integer("order_index").notNull().default(0),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.moduleId, table.questionId] }),
    index("module_main_question_question_idx").on(table.questionId),
    index("module_main_question_deleted_idx").on(table.isDeleted),
  ],
);

export const fragebogenMain = pgTable("fragebogen_main", {
  id: uuid("id").defaultRandom().primaryKey(),
  name: text("name").notNull(),
  description: text("description").notNull().default(""),
  sectionKeywords: fragebogenMainSectionEnum("section_keywords").array().notNull().default(sql`'{standard}'::fragebogen_main_section[]`),
  nurEinmalAusfuellbar: boolean("nur_einmal_ausfuellbar").notNull().default(false),
  status: fragebogenStatusEnum("status").notNull().default("inactive"),
  scheduleType: fragebogenScheduleTypeEnum("schedule_type").notNull().default("always"),
  startDate: date("start_date"),
  endDate: date("end_date"),
  isDeleted: boolean("is_deleted").notNull().default(false),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
}, (table) => [
  check(
    "fragebogen_main_schedule_dates_ck",
    sql`(${table.scheduleType} = 'always') OR (${table.startDate} IS NOT NULL AND ${table.endDate} IS NOT NULL AND ${table.startDate} <= ${table.endDate})`,
  ),
]);

export const fragebogenMainModule = pgTable(
  "fragebogen_main_module",
  {
    fragebogenId: uuid("fragebogen_id")
      .notNull()
      .references(() => fragebogenMain.id, { onDelete: "cascade" }),
    moduleId: uuid("module_id")
      .notNull()
      .references(() => moduleMain.id, { onDelete: "cascade" }),
    orderIndex: integer("order_index").notNull().default(0),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.fragebogenId, table.moduleId] }),
    index("fragebogen_main_module_module_idx").on(table.moduleId),
    index("fragebogen_main_module_deleted_idx").on(table.isDeleted),
  ],
);

export const fragebogenMainSpezialQuestion = pgTable(
  "fragebogen_main_spezial_question",
  {
    fragebogenId: uuid("fragebogen_id")
      .notNull()
      .references(() => fragebogenMain.id, { onDelete: "cascade" }),
    questionId: uuid("question_id")
      .notNull()
      .references(() => questionBankShared.id, { onDelete: "cascade" }),
    orderIndex: integer("order_index").notNull().default(0),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.fragebogenId, table.questionId] }),
    index("fragebogen_main_spezial_question_question_idx").on(table.questionId),
    index("fragebogen_main_spezial_question_active_order_idx").on(
      table.fragebogenId,
      table.isDeleted,
      table.orderIndex,
    ),
    index("fragebogen_main_spezial_question_deleted_idx").on(table.isDeleted),
  ],
);

export const fragebogenMainSpezialItems = pgTable(
  "fragebogen_main_spezial_items",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    fragebogenId: uuid("fragebogen_id")
      .notNull()
      .references(() => fragebogenMain.id, { onDelete: "cascade" }),
    questionType: questionTypeEnum("question_type").notNull(),
    text: text("text").notNull().default(""),
    required: boolean("required").notNull().default(true),
    redSurvey: boolean("red_survey"),
    singleChoiceAvailability: boolean("single_choice_availability"),
    singleChoiceAvailabilityType: text("single_choice_availability_type"),
    chains: text("chains").array(),
    config: jsonb("config").$type<Record<string, unknown>>().notNull().default(sql`'{}'::jsonb`),
    rules: jsonb("rules").$type<Array<Record<string, unknown>>>().notNull().default(sql`'[]'::jsonb`),
    scoring: jsonb("scoring").$type<Record<string, unknown>>().notNull().default(sql`'{}'::jsonb`),
    orderIndex: integer("order_index").notNull().default(0),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    index("fragebogen_main_spezial_items_fb_idx").on(table.fragebogenId),
    index("fragebogen_main_spezial_items_order_idx").on(table.fragebogenId, table.orderIndex),
    index("fragebogen_main_spezial_items_deleted_idx").on(table.isDeleted),
  ],
);

export const questionAnswerHistory = pgTable(
  "question_answer_history",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    questionId: uuid("question_id").references(() => questionBankShared.id, { onDelete: "set null" }),
    fragebogenId: uuid("fragebogen_id").references(() => fragebogenMain.id, { onDelete: "set null" }),
    spezialItemId: uuid("spezial_item_id").references(() => fragebogenMainSpezialItems.id, { onDelete: "set null" }),
    changeKind: text("change_kind").notNull(),
    previousQuestionType: questionTypeEnum("previous_question_type"),
    nextQuestionType: questionTypeEnum("next_question_type"),
    previousAnswerState: jsonb("previous_answer_state").$type<Record<string, unknown>>().notNull().default(sql`'{}'::jsonb`),
    nextAnswerState: jsonb("next_answer_state").$type<Record<string, unknown>>().notNull().default(sql`'{}'::jsonb`),
    changedAt: timestamp("changed_at", { withTimezone: true }).defaultNow().notNull(),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    check("question_answer_history_change_kind_ck", sql`${table.changeKind} in ('type_change','answer_edit')`),
    index("question_answer_history_question_changed_idx").on(table.questionId, table.changedAt),
    index("question_answer_history_fragebogen_changed_idx").on(table.fragebogenId, table.changedAt),
    index("question_answer_history_spezial_changed_idx").on(table.spezialItemId, table.changedAt),
    index("question_answer_history_deleted_idx").on(table.isDeleted),
  ],
);

export const moduleKuehler = pgTable("module_kuehler", {
  id: uuid("id").defaultRandom().primaryKey(),
  name: text("name").notNull(),
  description: text("description").notNull().default(""),
  isDeleted: boolean("is_deleted").notNull().default(false),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
});

export const moduleKuehlerQuestion = pgTable(
  "module_kuehler_question",
  {
    moduleId: uuid("module_id")
      .notNull()
      .references(() => moduleKuehler.id, { onDelete: "cascade" }),
    questionId: uuid("question_id")
      .notNull()
      .references(() => questionBankShared.id, { onDelete: "cascade" }),
    orderIndex: integer("order_index").notNull().default(0),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.moduleId, table.questionId] }),
    index("module_kuehler_question_question_idx").on(table.questionId),
    index("module_kuehler_question_deleted_idx").on(table.isDeleted),
  ],
);

export const fragebogenKuehler = pgTable("fragebogen_kuehler", {
  id: uuid("id").defaultRandom().primaryKey(),
  name: text("name").notNull(),
  description: text("description").notNull().default(""),
  nurEinmalAusfuellbar: boolean("nur_einmal_ausfuellbar").notNull().default(false),
  status: fragebogenStatusEnum("status").notNull().default("inactive"),
  scheduleType: fragebogenScheduleTypeEnum("schedule_type").notNull().default("always"),
  startDate: date("start_date"),
  endDate: date("end_date"),
  isDeleted: boolean("is_deleted").notNull().default(false),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
}, (table) => [
  check(
    "fragebogen_kuehler_schedule_dates_ck",
    sql`(${table.scheduleType} = 'always') OR (${table.startDate} IS NOT NULL AND ${table.endDate} IS NOT NULL AND ${table.startDate} <= ${table.endDate})`,
  ),
]);

export const fragebogenKuehlerModule = pgTable(
  "fragebogen_kuehler_module",
  {
    fragebogenId: uuid("fragebogen_id")
      .notNull()
      .references(() => fragebogenKuehler.id, { onDelete: "cascade" }),
    moduleId: uuid("module_id")
      .notNull()
      .references(() => moduleKuehler.id, { onDelete: "cascade" }),
    orderIndex: integer("order_index").notNull().default(0),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.fragebogenId, table.moduleId] }),
    index("fragebogen_kuehler_module_module_idx").on(table.moduleId),
    index("fragebogen_kuehler_module_deleted_idx").on(table.isDeleted),
  ],
);

export const fragebogenKuehlerSpezialQuestion = pgTable(
  "fragebogen_kuehler_spezial_question",
  {
    fragebogenId: uuid("fragebogen_id")
      .notNull()
      .references(() => fragebogenKuehler.id, { onDelete: "cascade" }),
    questionId: uuid("question_id")
      .notNull()
      .references(() => questionBankShared.id, { onDelete: "cascade" }),
    orderIndex: integer("order_index").notNull().default(0),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.fragebogenId, table.questionId] }),
    index("fragebogen_kuehler_spezial_question_question_idx").on(table.questionId),
    index("fragebogen_kuehler_spezial_question_active_order_idx").on(
      table.fragebogenId,
      table.isDeleted,
      table.orderIndex,
    ),
    index("fragebogen_kuehler_spezial_question_deleted_idx").on(table.isDeleted),
  ],
);

export const moduleMhd = pgTable("module_mhd", {
  id: uuid("id").defaultRandom().primaryKey(),
  name: text("name").notNull(),
  description: text("description").notNull().default(""),
  isDeleted: boolean("is_deleted").notNull().default(false),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
});

export const moduleMhdQuestion = pgTable(
  "module_mhd_question",
  {
    moduleId: uuid("module_id")
      .notNull()
      .references(() => moduleMhd.id, { onDelete: "cascade" }),
    questionId: uuid("question_id")
      .notNull()
      .references(() => questionBankShared.id, { onDelete: "cascade" }),
    orderIndex: integer("order_index").notNull().default(0),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.moduleId, table.questionId] }),
    index("module_mhd_question_question_idx").on(table.questionId),
    index("module_mhd_question_deleted_idx").on(table.isDeleted),
  ],
);

export const moduleQuestionChains = pgTable(
  "module_question_chains",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    scope: fragebogenScopeEnum("scope").notNull(),
    moduleId: uuid("module_id").notNull(),
    questionId: uuid("question_id")
      .notNull()
      .references(() => questionBankShared.id, { onDelete: "cascade" }),
    chainDbName: text("chain_db_name").notNull(),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("module_question_chains_scope_module_question_chain_active_unique")
      .on(table.scope, table.moduleId, table.questionId, table.chainDbName)
      .where(sql`${table.isDeleted} = false`),
    index("module_question_chains_scope_module_question_deleted_idx").on(
      table.scope,
      table.moduleId,
      table.questionId,
      table.isDeleted,
    ),
  ],
);

export const fragebogenMhd = pgTable("fragebogen_mhd", {
  id: uuid("id").defaultRandom().primaryKey(),
  name: text("name").notNull(),
  description: text("description").notNull().default(""),
  nurEinmalAusfuellbar: boolean("nur_einmal_ausfuellbar").notNull().default(false),
  status: fragebogenStatusEnum("status").notNull().default("inactive"),
  scheduleType: fragebogenScheduleTypeEnum("schedule_type").notNull().default("always"),
  startDate: date("start_date"),
  endDate: date("end_date"),
  isDeleted: boolean("is_deleted").notNull().default(false),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
}, (table) => [
  check(
    "fragebogen_mhd_schedule_dates_ck",
    sql`(${table.scheduleType} = 'always') OR (${table.startDate} IS NOT NULL AND ${table.endDate} IS NOT NULL AND ${table.startDate} <= ${table.endDate})`,
  ),
]);

export const fragebogenMhdModule = pgTable(
  "fragebogen_mhd_module",
  {
    fragebogenId: uuid("fragebogen_id")
      .notNull()
      .references(() => fragebogenMhd.id, { onDelete: "cascade" }),
    moduleId: uuid("module_id")
      .notNull()
      .references(() => moduleMhd.id, { onDelete: "cascade" }),
    orderIndex: integer("order_index").notNull().default(0),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.fragebogenId, table.moduleId] }),
    index("fragebogen_mhd_module_module_idx").on(table.moduleId),
    index("fragebogen_mhd_module_deleted_idx").on(table.isDeleted),
  ],
);

export const fragebogenMhdSpezialQuestion = pgTable(
  "fragebogen_mhd_spezial_question",
  {
    fragebogenId: uuid("fragebogen_id")
      .notNull()
      .references(() => fragebogenMhd.id, { onDelete: "cascade" }),
    questionId: uuid("question_id")
      .notNull()
      .references(() => questionBankShared.id, { onDelete: "cascade" }),
    orderIndex: integer("order_index").notNull().default(0),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.fragebogenId, table.questionId] }),
    index("fragebogen_mhd_spezial_question_question_idx").on(table.questionId),
    index("fragebogen_mhd_spezial_question_active_order_idx").on(
      table.fragebogenId,
      table.isDeleted,
      table.orderIndex,
    ),
    index("fragebogen_mhd_spezial_question_deleted_idx").on(table.isDeleted),
  ],
);

export const visitSessions = pgTable(
  "visit_sessions",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    gmUserId: uuid("gm_user_id")
      .notNull()
      .references(() => users.id, { onDelete: "cascade" }),
    marketId: uuid("market_id")
      .notNull()
      .references(() => markets.id, { onDelete: "cascade" }),
    kuehlerUnitId: uuid("kuehler_unit_id").references(() => marketKuehlerUnits.id, { onDelete: "set null" }),
    status: visitSessionStatusEnum("status").notNull().default("draft"),
    startedAt: timestamp("started_at", { withTimezone: true }).defaultNow().notNull(),
    submittedAt: timestamp("submitted_at", { withTimezone: true }),
    cancelledAt: timestamp("cancelled_at", { withTimezone: true }),
    lastSavedAt: timestamp("last_saved_at", { withTimezone: true }).defaultNow().notNull(),
    clientSessionToken: text("client_session_token"),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    index("visit_sessions_gm_status_idx").on(table.gmUserId, table.status),
    index("visit_sessions_market_idx").on(table.marketId),
    index("visit_sessions_kuehler_unit_idx").on(table.kuehlerUnitId),
    index("visit_sessions_gm_market_submitted_idx")
      .on(table.gmUserId, table.marketId, table.submittedAt)
      .where(sql`${table.isDeleted} = false AND ${table.status} = 'submitted' AND ${table.submittedAt} IS NOT NULL`),
    index("visit_sessions_market_submitted_active_idx")
      .on(table.marketId, table.submittedAt, table.createdAt)
      .where(sql`${table.isDeleted} = false AND ${table.status} = 'submitted' AND ${table.submittedAt} IS NOT NULL`),
    index("visit_sessions_submitted_period_market_gm_idx")
      .on(table.submittedAt, table.marketId, table.gmUserId)
      .where(sql`${table.isDeleted} = false AND ${table.status} = 'submitted' AND ${table.submittedAt} IS NOT NULL`),
    index("visit_sessions_gm_status_started_idx")
      .on(table.gmUserId, table.status, table.startedAt)
      .where(sql`${table.isDeleted} = false`),
    index("visit_sessions_deleted_idx").on(table.isDeleted),
    uniqueIndex("visit_sessions_client_token_active_unique")
      .on(table.gmUserId, table.clientSessionToken)
      .where(sql`${table.isDeleted} = false AND ${table.clientSessionToken} IS NOT NULL`),
  ],
);

export const timeTrackingEntries = pgTable(
  "time_tracking_entries",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    gmUserId: uuid("gm_user_id")
      .notNull()
      .references(() => users.id, { onDelete: "cascade" }),
    marketId: uuid("market_id").references(() => markets.id, { onDelete: "set null" }),
    activityType: timeTrackingActivityTypeEnum("activity_type").notNull(),
    clientEntryToken: text("client_entry_token"),
    startAt: timestamp("start_at", { withTimezone: true }),
    endAt: timestamp("end_at", { withTimezone: true }),
    comment: text("comment"),
    doctorConfirmationBucket: text("doctor_confirmation_bucket"),
    doctorConfirmationPath: text("doctor_confirmation_path"),
    doctorConfirmationFileName: text("doctor_confirmation_file_name"),
    doctorConfirmationMimeType: text("doctor_confirmation_mime_type"),
    doctorConfirmationByteSize: integer("doctor_confirmation_byte_size"),
    doctorConfirmationUploadedAt: timestamp("doctor_confirmation_uploaded_at", { withTimezone: true }),
    status: timeTrackingEntryStatusEnum("status").notNull().default("draft"),
    submittedAt: timestamp("submitted_at", { withTimezone: true }),
    cancelledAt: timestamp("cancelled_at", { withTimezone: true }),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    check(
      "time_tracking_entries_submitted_complete_ck",
      sql`(${table.status} <> 'submitted') OR (${table.startAt} IS NOT NULL AND ${table.endAt} IS NOT NULL AND ${table.endAt} >= ${table.startAt})`,
    ),
    index("time_tracking_entries_gm_status_start_idx").on(table.gmUserId, table.status, table.startAt),
    index("time_tracking_entries_activity_idx").on(table.activityType),
    index("time_tracking_entries_deleted_idx").on(table.isDeleted),
    uniqueIndex("time_tracking_entries_draft_token_unique")
      .on(table.gmUserId, table.activityType, table.clientEntryToken)
      .where(
        sql`${table.isDeleted} = false and ${table.status} = 'draft' and ${table.clientEntryToken} is not null`,
      ),
  ],
);

export const timeTrackingEntryEvents = pgTable(
  "time_tracking_entry_events",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    entryId: uuid("entry_id")
      .notNull()
      .references(() => timeTrackingEntries.id, { onDelete: "cascade" }),
    gmUserId: uuid("gm_user_id").references(() => users.id, { onDelete: "set null" }),
    eventType: timeTrackingEntryEventTypeEnum("event_type").notNull(),
    payload: jsonb("payload").$type<Record<string, unknown>>().notNull().default(sql`'{}'::jsonb`),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    index("time_tracking_entry_events_entry_idx").on(table.entryId, table.createdAt),
    index("time_tracking_entry_events_type_idx").on(table.eventType),
  ],
);

export const gmDaySessions = pgTable(
  "gm_day_sessions",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    gmUserId: uuid("gm_user_id")
      .notNull()
      .references(() => users.id, { onDelete: "cascade" }),
    workDate: date("work_date").notNull(),
    timezone: text("timezone").notNull().default("Europe/Vienna"),
    status: gmDaySessionStatusEnum("status").notNull().default("draft"),
    dayStartedAt: timestamp("day_started_at", { withTimezone: true }),
    dayEndedAt: timestamp("day_ended_at", { withTimezone: true }),
    startKm: integer("start_km"),
    endKm: integer("end_km"),
    startKmDeferred: boolean("start_km_deferred").notNull().default(false),
    endKmDeferred: boolean("end_km_deferred").notNull().default(false),
    isStartKmCompleted: boolean("is_start_km_completed").notNull().default(false),
    isEndKmCompleted: boolean("is_end_km_completed").notNull().default(false),
    comment: text("comment"),
    submittedAt: timestamp("submitted_at", { withTimezone: true }),
    cancelledAt: timestamp("cancelled_at", { withTimezone: true }),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    check(
      "gm_day_sessions_end_after_start_ck",
      sql`${table.dayEndedAt} IS NULL OR ${table.dayStartedAt} IS NULL OR ${table.dayEndedAt} >= ${table.dayStartedAt}`,
    ),
    check(
      "gm_day_sessions_submit_requires_completion_ck",
      sql`(${table.status} <> 'submitted') OR (
        ${table.dayStartedAt} IS NOT NULL
        AND ${table.dayEndedAt} IS NOT NULL
        AND ${table.isStartKmCompleted} = true
        AND ${table.isEndKmCompleted} = true
      )`,
    ),
    uniqueIndex("gm_day_sessions_gm_work_date_active_unique")
      .on(table.gmUserId, table.workDate)
      .where(sql`${table.isDeleted} = false`),
    uniqueIndex("gm_day_sessions_one_open_per_gm_unique")
      .on(table.gmUserId)
      .where(sql`${table.isDeleted} = false and ${table.status} in ('started','ended')`),
    index("gm_day_sessions_gm_status_idx").on(table.gmUserId, table.status, table.workDate),
    index("gm_day_sessions_work_date_idx").on(table.workDate),
    index("gm_day_sessions_deleted_idx").on(table.isDeleted),
  ],
);

export const gmDaySessionPauses = pgTable(
  "gm_day_session_pauses",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    daySessionId: uuid("day_session_id")
      .notNull()
      .references(() => gmDaySessions.id, { onDelete: "cascade" }),
    gmUserId: uuid("gm_user_id")
      .notNull()
      .references(() => users.id, { onDelete: "cascade" }),
    pauseStartedAt: timestamp("pause_started_at", { withTimezone: true }).notNull(),
    pauseEndedAt: timestamp("pause_ended_at", { withTimezone: true }),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    check(
      "gm_day_session_pauses_end_after_start_ck",
      sql`${table.pauseEndedAt} IS NULL OR ${table.pauseEndedAt} >= ${table.pauseStartedAt}`,
    ),
    index("gm_day_session_pauses_gm_session_started_idx")
      .on(table.gmUserId, table.daySessionId, table.pauseStartedAt),
    index("gm_day_session_pauses_deleted_idx").on(table.isDeleted),
    uniqueIndex("gm_day_session_pauses_one_open_per_gm_unique")
      .on(table.gmUserId)
      .where(sql`${table.isDeleted} = false AND ${table.pauseEndedAt} IS NULL`),
  ],
);

export const visitSessionSections = pgTable(
  "visit_session_sections",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    visitSessionId: uuid("visit_session_id")
      .notNull()
      .references(() => visitSessions.id, { onDelete: "cascade" }),
    campaignId: uuid("campaign_id")
      .notNull()
      .references(() => campaigns.id, { onDelete: "cascade" }),
    section: campaignSectionEnum("section").notNull(),
    fragebogenId: uuid("fragebogen_id"),
    fragebogenNameSnapshot: text("fragebogen_name_snapshot").notNull().default(""),
    orderIndex: integer("order_index").notNull().default(0),
    status: visitSectionStatusEnum("status").notNull().default("draft"),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    index("visit_session_sections_session_idx").on(table.visitSessionId, table.orderIndex),
    index("visit_session_sections_campaign_idx").on(table.campaignId),
    index("visit_session_sections_campaign_session_active_idx")
      .on(table.campaignId, table.visitSessionId)
      .where(sql`${table.isDeleted} = false`),
    index("visit_session_sections_deleted_idx").on(table.isDeleted),
  ],
);

export const visitSessionQuestions = pgTable(
  "visit_session_questions",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    visitSessionSectionId: uuid("visit_session_section_id")
      .notNull()
      .references(() => visitSessionSections.id, { onDelete: "cascade" }),
    questionId: uuid("question_id")
      .notNull()
      .references(() => questionBankShared.id, { onDelete: "restrict" }),
    moduleId: uuid("module_id"),
    moduleNameSnapshot: text("module_name_snapshot").notNull().default(""),
    questionType: questionTypeEnum("question_type").notNull(),
    questionTextSnapshot: text("question_text_snapshot").notNull().default(""),
    questionConfigSnapshot: jsonb("question_config_snapshot").$type<Record<string, unknown>>().notNull().default(sql`'{}'::jsonb`),
    questionRulesSnapshot: jsonb("question_rules_snapshot").$type<Array<Record<string, unknown>>>().notNull().default(sql`'[]'::jsonb`),
    questionChainsSnapshot: text("question_chains_snapshot").array().notNull().default(sql`'{}'::text[]`),
    appliesToMarketChainSnapshot: boolean("applies_to_market_chain_snapshot").notNull().default(true),
    requiredSnapshot: boolean("required_snapshot").notNull().default(true),
    redSurveySnapshot: boolean("red_survey_snapshot"),
    singleChoiceAvailabilitySnapshot: boolean("single_choice_availability_snapshot"),
    singleChoiceAvailabilityTypeSnapshot: text("single_choice_availability_type_snapshot"),
    orderIndex: integer("order_index").notNull().default(0),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    index("visit_session_questions_section_idx").on(table.visitSessionSectionId, table.orderIndex),
    index("visit_session_questions_question_idx").on(table.questionId),
    index("visit_session_questions_applicability_idx").on(
      table.visitSessionSectionId,
      table.appliesToMarketChainSnapshot,
      table.isDeleted,
    ),
    index("visit_session_questions_red_survey_snapshot_idx")
      .on(table.visitSessionSectionId)
      .where(sql`${table.isDeleted} = false AND ${table.redSurveySnapshot} = true`),
    index("visit_session_questions_deleted_idx").on(table.isDeleted),
  ],
);

export const visitAnswers = pgTable(
  "visit_answers",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    visitSessionId: uuid("visit_session_id")
      .notNull()
      .references(() => visitSessions.id, { onDelete: "cascade" }),
    visitSessionSectionId: uuid("visit_session_section_id")
      .notNull()
      .references(() => visitSessionSections.id, { onDelete: "cascade" }),
    visitSessionQuestionId: uuid("visit_session_question_id")
      .notNull()
      .references(() => visitSessionQuestions.id, { onDelete: "cascade" }),
    questionId: uuid("question_id")
      .notNull()
      .references(() => questionBankShared.id, { onDelete: "restrict" }),
    questionType: questionTypeEnum("question_type").notNull(),
    answerStatus: visitAnswerStatusEnum("answer_status").notNull().default("unanswered"),
    valueText: text("value_text"),
    valueNumber: numeric("value_number", { precision: 16, scale: 4 }),
    valueJson: jsonb("value_json").$type<Record<string, unknown>>(),
    isValid: boolean("is_valid").notNull().default(true),
    validationError: text("validation_error"),
    answeredAt: timestamp("answered_at", { withTimezone: true }),
    changedAt: timestamp("changed_at", { withTimezone: true }).defaultNow().notNull(),
    version: integer("version").notNull().default(1),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("visit_answers_question_active_unique")
      .on(table.visitSessionQuestionId)
      .where(sql`${table.isDeleted} = false`),
    index("visit_answers_session_idx").on(table.visitSessionId),
    index("visit_answers_section_idx").on(table.visitSessionSectionId),
    index("visit_answers_question_session_active_idx")
      .on(table.questionId, table.visitSessionId)
      .where(sql`${table.isDeleted} = false`),
    index("visit_answers_session_question_changed_active_idx")
      .on(table.visitSessionId, table.questionId, table.changedAt, table.updatedAt, table.createdAt)
      .where(sql`${table.isDeleted} = false`),
    index("visit_answers_deleted_idx").on(table.isDeleted),
  ],
);

export const visitAnswerOptions = pgTable(
  "visit_answer_options",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    visitAnswerId: uuid("visit_answer_id")
      .notNull()
      .references(() => visitAnswers.id, { onDelete: "cascade" }),
    optionRole: visitAnswerOptionRoleEnum("option_role").notNull().default("sub"),
    optionValue: text("option_value").notNull(),
    orderIndex: integer("order_index").notNull().default(0),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    index("visit_answer_options_answer_idx").on(table.visitAnswerId, table.orderIndex),
    index("visit_answer_options_answer_order_active_idx")
      .on(table.visitAnswerId, table.orderIndex)
      .where(sql`${table.isDeleted} = false`),
    index("visit_answer_options_deleted_idx").on(table.isDeleted),
  ],
);

export const visitAnswerMatrixCells = pgTable(
  "visit_answer_matrix_cells",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    visitAnswerId: uuid("visit_answer_id")
      .notNull()
      .references(() => visitAnswers.id, { onDelete: "cascade" }),
    rowKey: text("row_key").notNull(),
    columnKey: text("column_key").notNull(),
    cellValueText: text("cell_value_text"),
    cellValueDate: date("cell_value_date"),
    cellSelected: boolean("cell_selected"),
    orderIndex: integer("order_index").notNull().default(0),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("visit_answer_matrix_cells_active_unique")
      .on(table.visitAnswerId, table.rowKey, table.columnKey)
      .where(sql`${table.isDeleted} = false`),
    index("visit_answer_matrix_cells_answer_idx").on(table.visitAnswerId, table.orderIndex),
    index("visit_answer_matrix_cells_deleted_idx").on(table.isDeleted),
  ],
);

export const visitQuestionComments = pgTable(
  "visit_question_comments",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    visitSessionQuestionId: uuid("visit_session_question_id")
      .notNull()
      .references(() => visitSessionQuestions.id, { onDelete: "cascade" }),
    commentText: text("comment_text").notNull().default(""),
    commentedAt: timestamp("commented_at", { withTimezone: true }).defaultNow().notNull(),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("visit_question_comments_active_unique")
      .on(table.visitSessionQuestionId)
      .where(sql`${table.isDeleted} = false`),
    index("visit_question_comments_deleted_idx").on(table.isDeleted),
  ],
);

export const visitAnswerPhotos = pgTable(
  "visit_answer_photos",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    visitAnswerId: uuid("visit_answer_id")
      .notNull()
      .references(() => visitAnswers.id, { onDelete: "cascade" }),
    storageBucket: text("storage_bucket").notNull(),
    storagePath: text("storage_path").notNull(),
    mimeType: text("mime_type"),
    byteSize: integer("byte_size"),
    widthPx: integer("width_px"),
    heightPx: integer("height_px"),
    sha256: text("sha256"),
    uploadedAt: timestamp("uploaded_at", { withTimezone: true }).defaultNow().notNull(),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("visit_answer_photos_path_active_unique")
      .on(table.visitAnswerId, table.storagePath)
      .where(sql`${table.isDeleted} = false`),
    uniqueIndex("visit_answer_photos_sha_path_active_unique")
      .on(table.visitAnswerId, table.sha256, table.storagePath)
      .where(sql`${table.isDeleted} = false AND ${table.sha256} IS NOT NULL`),
    index("visit_answer_photos_answer_idx").on(table.visitAnswerId),
    index("visit_answer_photos_answer_deleted_idx").on(table.visitAnswerId, table.isDeleted),
    index("visit_answer_photos_deleted_idx").on(table.isDeleted),
  ],
);

export const visitAnswerPhotoTags = pgTable(
  "visit_answer_photo_tags",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    visitAnswerPhotoId: uuid("visit_answer_photo_id")
      .notNull()
      .references(() => visitAnswerPhotos.id, { onDelete: "cascade" }),
    photoTagId: uuid("photo_tag_id")
      .references(() => photoTags.id, { onDelete: "set null" }),
    photoTagLabelSnapshot: text("photo_tag_label_snapshot").notNull().default(""),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("visit_answer_photo_tags_active_unique")
      .on(table.visitAnswerPhotoId, table.photoTagLabelSnapshot)
      .where(sql`${table.isDeleted} = false`),
    index("visit_answer_photo_tags_photo_idx").on(table.visitAnswerPhotoId),
    index("visit_answer_photo_tags_photo_deleted_idx").on(table.visitAnswerPhotoId, table.isDeleted),
    index("visit_answer_photo_tags_deleted_idx").on(table.isDeleted),
  ],
);

export const visitAnswerChangeRequests = pgTable(
  "visit_answer_change_requests",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    visitSessionId: uuid("visit_session_id")
      .notNull()
      .references(() => visitSessions.id, { onDelete: "cascade" }),
    visitSessionSectionId: uuid("visit_session_section_id")
      .notNull()
      .references(() => visitSessionSections.id, { onDelete: "cascade" }),
    visitSessionQuestionId: uuid("visit_session_question_id")
      .notNull()
      .references(() => visitSessionQuestions.id, { onDelete: "cascade" }),
    visitAnswerId: uuid("visit_answer_id").references(() => visitAnswers.id, { onDelete: "set null" }),
    questionId: uuid("question_id")
      .notNull()
      .references(() => questionBankShared.id, { onDelete: "restrict" }),
    gmUserId: uuid("gm_user_id")
      .notNull()
      .references(() => users.id, { onDelete: "cascade" }),
    marketId: uuid("market_id")
      .notNull()
      .references(() => markets.id, { onDelete: "cascade" }),
    questionType: questionTypeEnum("question_type").notNull(),
    questionTextSnapshot: text("question_text_snapshot").notNull().default(""),
    currentAnswerSnapshot: jsonb("current_answer_snapshot").$type<Record<string, unknown>>().notNull().default(sql`'{}'::jsonb`),
    requestedAnswerPayload: jsonb("requested_answer_payload").$type<Record<string, unknown>>().notNull().default(sql`'{}'::jsonb`),
    requestedAnswerSummary: text("requested_answer_summary").notNull().default(""),
    requestNote: text("request_note"),
    status: visitAnswerChangeRequestStatusEnum("status").notNull().default("pending"),
    reviewedByUserId: uuid("reviewed_by_user_id").references(() => users.id, { onDelete: "set null" }),
    reviewedAt: timestamp("reviewed_at", { withTimezone: true }),
    adminNote: text("admin_note"),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("visit_answer_change_requests_pending_question_unique")
      .on(table.gmUserId, table.visitSessionQuestionId)
      .where(sql`${table.isDeleted} = false AND ${table.status} = 'pending'`),
    index("visit_answer_change_requests_gm_status_idx").on(table.gmUserId, table.status, table.createdAt),
    index("visit_answer_change_requests_session_idx").on(table.visitSessionId, table.createdAt),
    index("visit_answer_change_requests_market_idx").on(table.marketId, table.createdAt),
    index("visit_answer_change_requests_deleted_idx").on(table.isDeleted),
  ],
);

export const visitSessionDeleteRequests = pgTable(
  "visit_session_delete_requests",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    visitSessionId: uuid("visit_session_id")
      .notNull()
      .references(() => visitSessions.id, { onDelete: "cascade" }),
    gmUserId: uuid("gm_user_id")
      .notNull()
      .references(() => users.id, { onDelete: "cascade" }),
    marketId: uuid("market_id")
      .notNull()
      .references(() => markets.id, { onDelete: "cascade" }),
    marketNameSnapshot: text("market_name_snapshot").notNull().default(""),
    marketAddressSnapshot: text("market_address_snapshot").notNull().default(""),
    marketPostalCodeSnapshot: text("market_postal_code_snapshot").notNull().default(""),
    marketCitySnapshot: text("market_city_snapshot").notNull().default(""),
    campaignSummarySnapshot: text("campaign_summary_snapshot").notNull().default(""),
    sectionSummarySnapshot: text("section_summary_snapshot").notNull().default(""),
    sessionStartedAtSnapshot: timestamp("session_started_at_snapshot", { withTimezone: true }),
    sessionSubmittedAtSnapshot: timestamp("session_submitted_at_snapshot", { withTimezone: true }),
    requestNote: text("request_note"),
    status: visitAnswerChangeRequestStatusEnum("status").notNull().default("pending"),
    reviewedByUserId: uuid("reviewed_by_user_id").references(() => users.id, { onDelete: "set null" }),
    reviewedAt: timestamp("reviewed_at", { withTimezone: true }),
    adminNote: text("admin_note"),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("visit_session_delete_requests_pending_session_unique")
      .on(table.visitSessionId)
      .where(sql`${table.isDeleted} = false AND ${table.status} = 'pending'`),
    index("visit_session_delete_requests_gm_status_idx").on(table.gmUserId, table.status, table.createdAt),
    index("visit_session_delete_requests_session_idx").on(table.visitSessionId, table.createdAt),
    index("visit_session_delete_requests_market_idx").on(table.marketId, table.createdAt),
    index("visit_session_delete_requests_deleted_idx").on(table.isDeleted),
  ],
);

export const timeEntryChangeRequests = pgTable(
  "time_entry_change_requests",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    daySessionId: uuid("day_session_id")
      .notNull()
      .references(() => gmDaySessions.id, { onDelete: "cascade" }),
    gmUserId: uuid("gm_user_id")
      .notNull()
      .references(() => users.id, { onDelete: "cascade" }),
    sourceKind: timeEntryChangeRequestSourceKindEnum("source_kind").notNull(),
    sourceId: uuid("source_id").notNull(),
    workDate: date("work_date").notNull(),
    timezone: text("timezone").notNull().default("Europe/Vienna"),
    titleSnapshot: text("title_snapshot").notNull().default(""),
    subtitleSnapshot: text("subtitle_snapshot"),
    requestedActivityType: timeTrackingActivityTypeEnum("requested_activity_type"),
    originalStartAt: timestamp("original_start_at", { withTimezone: true }).notNull(),
    originalEndAt: timestamp("original_end_at", { withTimezone: true }).notNull(),
    requestedStartAt: timestamp("requested_start_at", { withTimezone: true }).notNull(),
    requestedEndAt: timestamp("requested_end_at", { withTimezone: true }).notNull(),
    originalStartKm: integer("original_start_km"),
    originalEndKm: integer("original_end_km"),
    requestedStartKm: integer("requested_start_km"),
    requestedEndKm: integer("requested_end_km"),
    requestNote: text("request_note"),
    status: timeEntryChangeRequestStatusEnum("status").notNull().default("pending"),
    reviewedByUserId: uuid("reviewed_by_user_id").references(() => users.id, { onDelete: "set null" }),
    reviewedAt: timestamp("reviewed_at", { withTimezone: true }),
    appliedAt: timestamp("applied_at", { withTimezone: true }),
    adminNote: text("admin_note"),
    isDeleted: boolean("is_deleted").notNull().default(false),
    deletedAt: timestamp("deleted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("time_entry_change_requests_pending_source_unique")
      .on(table.gmUserId, table.sourceKind, table.sourceId)
      .where(sql`${table.isDeleted} = false AND ${table.status} = 'pending'`),
    index("time_entry_change_requests_gm_status_idx").on(table.gmUserId, table.status, table.createdAt),
    index("time_entry_change_requests_day_session_idx").on(table.daySessionId, table.createdAt),
    index("time_entry_change_requests_source_idx").on(table.sourceKind, table.sourceId),
    index("time_entry_change_requests_work_date_idx").on(table.workDate),
    index("time_entry_change_requests_deleted_idx").on(table.isDeleted),
  ],
);

export const visitAnswerEvents = pgTable(
  "visit_answer_events",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    visitAnswerId: uuid("visit_answer_id")
      .notNull()
      .references(() => visitAnswers.id, { onDelete: "cascade" }),
    eventType: visitAnswerEventTypeEnum("event_type").notNull(),
    payload: jsonb("payload").$type<Record<string, unknown>>().notNull().default(sql`'{}'::jsonb`),
    actorUserId: uuid("actor_user_id").references(() => users.id, { onDelete: "set null" }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    index("visit_answer_events_answer_idx").on(table.visitAnswerId, table.createdAt),
  ],
);

export type UserRole = (typeof userRoleEnum.enumValues)[number];
export type UserRow = typeof users.$inferSelect;
export type NewUserRow = typeof users.$inferInsert;
export type KundeUserRow = typeof kundeUsers.$inferSelect;
export type NewKundeUserRow = typeof kundeUsers.$inferInsert;
export type MarketRow = typeof markets.$inferSelect;
export type NewMarketRow = typeof markets.$inferInsert;
export type TimeEntryChangeRequestRow = typeof timeEntryChangeRequests.$inferSelect;
export type NewTimeEntryChangeRequestRow = typeof timeEntryChangeRequests.$inferInsert;
export type VisitSessionDeleteRequestRow = typeof visitSessionDeleteRequests.$inferSelect;
export type NewVisitSessionDeleteRequestRow = typeof visitSessionDeleteRequests.$inferInsert;
export type MarketKuehlerUnitRow = typeof marketKuehlerUnits.$inferSelect;
export type NewMarketKuehlerUnitRow = typeof marketKuehlerUnits.$inferInsert;
export type LagerRow = typeof lager.$inferSelect;
export type NewLagerRow = typeof lager.$inferInsert;
export type CampaignRow = typeof campaigns.$inferSelect;
export type NewCampaignRow = typeof campaigns.$inferInsert;
export type CampaignMarketAssignmentHistoryRow = typeof campaignMarketAssignmentHistory.$inferSelect;
export type NewCampaignMarketAssignmentHistoryRow = typeof campaignMarketAssignmentHistory.$inferInsert;
export type QuestionBankRow = typeof questionBankShared.$inferSelect;
export type NewQuestionBankRow = typeof questionBankShared.$inferInsert;
