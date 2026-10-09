import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import test from "node:test";
import { createSMDurcharbeitFixture } from "../tests/SMDurcharbeit-fixture.js";

test("additive monthly migrations preserve preexisting Standard and dated SMDurcharbeit history", async () => {
  const tables = ["users", "sm_markets", "sm_questionnaire_templates", "sm_questionnaire_versions", "sm_modules", "sm_module_versions", "sm_questions", "sm_question_versions", "sm_assignments", "sm_questionnaire_submissions", "sm_questionnaire_submission_sections", "sm_questionnaire_submission_questions", "sm_question_answers", "sm_question_answer_options", "sm_question_answer_matrix_cells", "sm_question_answer_files", "sm_question_answer_events", "sm_assignment_time_submissions"];
  const before = new Map<string, Array<Record<string, unknown>>>();
  let constraints: Array<{ name: string; definition: string }> = [];
  const f = await createSMDurcharbeitFixture({ beforeMonthlyMigrations: async pg => {
    const admin = randomUUID(), employee = randomUUID(), market = randomUUID();
    await pg.query("insert into users(id,first_name,last_name,role) values($1,'Sentinel','Admin','sm_admin'),($2,'Sentinel','SM','sm')", [admin, employee]);
    await pg.query("insert into sm_markets(id,name,chain,address,postal_code,city,region,internal_market_id) values($1,'Historical sentinel','Spar','Fixture only 1','1010','Wien','Ost','SENTINEL')", [market]);
    for (const scope of ["standard", "smdurcharbeit"] as const) {
      const template = randomUUID(), version = randomUUID(), module = randomUUID(), moduleVersion = randomUUID(), assignment = randomUUID(), submission = randomUUID(), section = randomUUID();
      await pg.query("insert into sm_questionnaire_templates(id,stable_code) values($1,$2)", [template, `${scope}_sentinel`]);
      await pg.query("insert into sm_questionnaire_versions(id,questionnaire_template_id,version_number,name,status,published_at,content_hash) values($1,$2,1,$3,'published','2026-09-01T10:00Z','synthetic-sentinel-hash')", [version, template, `${scope} historical`]);
      await pg.query("insert into sm_modules(id,stable_code) values($1,$2)", [module, `${scope}_module`]);
      await pg.query("insert into sm_module_versions(id,module_id,version_number,name,status,published_at) values($1,$2,1,$3,'published','2026-09-01T10:00Z')", [moduleVersion, module, `${scope} module`]);
      await pg.query("insert into sm_assignments(id,idempotency_key,source_type,status,original_work_date,original_sm_user_id,original_sm_market_id,original_market_internal_id,original_planned_minutes,created_by_user_id,updated_by_user_id) values($1,$2,'single','completed','2026-09-15',$3,$4,'SENTINEL',45,$5,$5)", [assignment, randomUUID(), employee, market, admin]);
      await pg.query("insert into sm_questionnaire_submissions(id,assignment_id,questionnaire_template_id,questionnaire_version_id,sm_user_id,sm_market_id,status,client_submission_token,questionnaire_name_snapshot,questionnaire_version_snapshot,sm_name_snapshot,market_name_snapshot,submitted_at,reporting_available_at,visit_started_at,visit_completed_at) values($1,$2,$3,$4,$5,$6,'submitted',$7,$8,1,'Historical SM','Historical market','2026-09-15T09:15Z','2026-09-15T09:15Z','2026-09-15T09:00Z','2026-09-15T09:15Z')", [submission, assignment, template, version, employee, market, randomUUID(), `${scope} frozen snapshot`]);
      await pg.query("insert into sm_questionnaire_submission_sections(id,submission_id,module_version_id,module_code_snapshot,module_name_snapshot) values($1,$2,$3,$4,$5)", [section, submission, moduleVersion, `${scope}_module`, `${scope} frozen module`]);
      for (const [index, type] of ["photo", "multiple", "matrix"].entries()) {
        const question = randomUUID(), qVersion = randomUUID(), snapshot = randomUUID(), answer = randomUUID();
        await pg.query("insert into sm_questions(id,stable_code) values($1,$2)", [question, `${scope}_${type}`]);
        await pg.query("insert into sm_question_versions(id,question_id,version_number,status,published_at,question_type,question_text,config) values($1,$2,1,'published','2026-09-01T10:00Z',$3,$4,$5::jsonb)", [qVersion, question, type, `${type} historical`, { subheading: "Immutable historical configuration", min: 0 }]);
        await pg.query("insert into sm_questionnaire_submission_questions(id,submission_id,submission_section_id,question_version_id,question_code_snapshot,question_type_snapshot,question_text_snapshot,required_snapshot,metric_role_snapshot,config_snapshot,order_index) values($1,$2,$3,$4,$5,$6,$7,false,'none',$8::jsonb,$9)", [snapshot, submission, section, qVersion, `${scope}_${type}`, type, `${type} frozen question`, { instruction: "Original sentinel" }, index]);
        await pg.query("insert into sm_question_answers(id,submission_id,submission_question_id,answer_state,value_json,answered_by_user_id,answered_at) values($1,$2,$3,'answered',$4::jsonb,$5,'2026-09-15T09:10Z')", [answer, submission, snapshot, { kind: type === "multiple" ? "multi" : type, comment: "Frozen historical comment" }, employee]);
        if (type === "photo") await pg.query("insert into sm_question_answer_files(answer_id,storage_bucket,storage_path,original_file_name,mime_type,byte_size,uploaded_at) values($1,'synthetic-history-only',$2,'historical.png','image/png',123,'2026-09-15T09:05Z')", [answer, `sentinel/${scope}/historical.png`]);
        if (type === "multiple") await pg.query("insert into sm_question_answer_options(answer_id,option_code_snapshot,option_label_snapshot) values($1,'original','Frozen option')", [answer]);
        if (type === "matrix") await pg.query("insert into sm_question_answer_matrix_cells(answer_id,row_code,column_code,selected) values($1,'original_row','original_column',true)", [answer]);
        await pg.query("insert into sm_question_answer_events(answer_id,submission_id,event_type,answer_version,payload,actor_user_id) values($1,$2,'set',1,$3::jsonb,$4)", [answer, submission, { original: true }, employee]);
      }
      await pg.query("insert into sm_assignment_time_submissions(assignment_id,revision_number,actual_minutes,submitted_by_user_id) values($1,1,15,$2)", [assignment, employee]);
    }
    for (const table of tables) before.set(table, (await pg.query<{ row: Record<string, unknown> }>(`select to_jsonb(t) as row from "${table}" t order by id`)).rows.map(row => row.row));
    constraints = (await pg.query<{ name: string; definition: string }>("select conname as name,pg_get_constraintdef(oid) as definition from pg_constraint where connamespace='public'::regnamespace order by conname")).rows;
  } });
  try {
    for (const [table, baseline] of before) {
      const result = (await f.pg.query<{ row: Record<string, unknown> }>(`select to_jsonb(t) as row from "${table}" t where id = any($1::uuid[]) order by id`, [baseline.map(row => row.id)])).rows.map(row => row.row);
      if (table === "sm_questionnaire_submissions") for (const row of result) {
        assert.equal(row.smdurcharbeit_visit_id, null); assert.equal(row.smdurcharbeit_target_id, null);
        delete row.smdurcharbeit_visit_id; delete row.smdurcharbeit_target_id;
      }
      const digest = (rows: unknown) => createHash("sha256").update(JSON.stringify(rows)).digest("hex");
      assert.equal(digest(result), digest(baseline), `${table}: IDs, snapshots, timestamps and values must remain identical`);
    }
    const after = (await f.pg.query<{ name: string; definition: string }>("select conname as name,pg_get_constraintdef(oid) as definition from pg_constraint where connamespace='public'::regnamespace order by conname")).rows;
    for (const original of constraints) assert.ok(after.some(row => row.name === original.name && row.definition === original.definition), `Preserve existing constraint ${original.name}`);
    await assert.rejects(f.pg.query("update sm_assignments set original_planned_minutes=0"), /check constraint|immutable/);
    const nullLinks = (await f.pg.query<{ count: number }>("select count(*)::int as count from sm_questionnaire_submissions where smdurcharbeit_visit_id is null and smdurcharbeit_target_id is null")).rows[0];
    assert.equal(nullLinks?.count, 2);
  } finally { await f.pg.close(); }
});
