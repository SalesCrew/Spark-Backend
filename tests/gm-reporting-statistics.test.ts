import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { sql } from "drizzle-orm";
import { supplementalFixture } from "./gm-supplemental-fixture.js";
import { isolatedModule } from "./isolated-module.js";
import { modelDatabase } from "../src/lib/praemien-workspace.js";
import { regularBonusGmCondition, regularGmReportingDays } from "../src/lib/gm-reporting-scope.js";

test("Kurti visit and answer statistics exclude current and historical Zusatz work before grouping and numeric averages", async () => {
  const f = await supplementalFixture();
  try {
    const gm = randomUUID(), extra = randomUUID(), converted = randomUUID(), question = randomUUID(), market = randomUUID(), campaign = randomUUID();
    await f.pg.query("insert into users(id,role,first_name,last_name,is_supplemental_gm) values($1,'gm','Regular','Test',false),($2,'gm','Extra','Test',true),($3,'gm','Converted','Test',true)", [gm, extra, converted]);
    await f.pg.query("insert into markets(id,name,db_name,region) values($1,'Synthetic market','Spar','Nord')", [market]);
    await f.pg.query("insert into campaigns(id,name,section) values($1,'Synthetic campaign','standard')", [campaign]);
    const seed = async (owner: string, value: number) => {
      const visit = randomUUID(), section = randomUUID(), visitQuestion = randomUUID(), answer = randomUUID();
      await f.pg.query("insert into visit_sessions(id,gm_user_id,market_id,status,started_at,submitted_at) values($1,$2,$3,'submitted','2026-09-15T10:00:00Z','2026-09-15T10:30:00Z')", [visit, owner, market]);
      await f.pg.query("insert into visit_session_sections(id,visit_session_id,section,campaign_id) values($1,$2,'standard',$3)", [section, visit, campaign]);
      await f.pg.query("insert into visit_session_questions(id,visit_session_section_id,question_id,question_type,question_text_snapshot) values($1,$2,$3,'numeric','Synthetic units')", [visitQuestion, section, question]);
      await f.pg.query("insert into visit_answers(id,visit_session_id,visit_session_section_id,visit_session_question_id,question_id,question_type,answer_status,value_number,value_text,is_valid) values($1,$2,$3,$4,$5,'numeric','answered',$6,'Synthetic',true)", [answer, visit, section, visitQuestion, question, value]);
      await f.pg.query("insert into visit_answer_options(id,visit_answer_id,option_value) values($1,$2,'Synthetic choice')", [randomUUID(), answer]);
    };
    await seed(gm, 10);
    await seed(extra, 1000);
    await seed(converted, 1000);
    await f.pg.query("update users set is_supplemental_gm=false where id=$1", [converted]);
    await seed(converted, 20);
    const pgSql = async (parts: TemplateStringsArray, ...values: unknown[]) => {
      const query = parts.reduce((result, part, index) => result + (index ? `$${index}` : "") + part, "");
      return (await f.pg.query(query, values.map(value => Array.isArray(value) ? `{${value.join(",")}}` : value))).rows;
    };
    const analytics = await isolatedModule<typeof import("../src/lib/admin-kurti-operational-analytics.js")>(
      new URL("../src/lib/admin-kurti-operational-analytics.ts", import.meta.url), { "./db.js": { sql: pgSql } },
    );
    const visitResult = await analytics.executeAdminKurtiOperationalAnalytics("get_visit_analytics", {
      gmUserIds: [], marketId: null, campaignId: null, section: null, chain: null, region: null,
      status: "submitted", from: "2026-09-01", to: "2026-09-30", granularity: "month", groupBy: "total", limitGroups: 24,
    });
    assert.equal((visitResult.rows as any[])[0].visit_count, 2);
    const answerResult = await analytics.executeAdminKurtiOperationalAnalytics("get_answer_distribution_analytics", {
      questionId: question, gmUserId: null, marketId: null, campaignId: null, section: null, visitStatus: "submitted",
      includeInvalid: false, from: "2026-09-01", to: "2026-09-30", granularity: "month", limitValues: 10,
    });
    assert.equal((answerResult.summary as any).answer_rows, 2);
    assert.equal((answerResult.summary as any).average_numeric_value, 15);
    assert.equal((answerResult.optionDistribution as any[])[0].value, 2);
    assert.equal((answerResult.timeSeries as any[])[0].average_numeric_value, 15);
    assert.equal((await f.pg.query<{ count: number }>("select count(*)::int as count from visit_answers")).rows[0]!.count, 4);
  } finally { await f.pg.close(); }
});

test("legacy bonus filters preserve excluded records during ordinary bulk replacements and retain separation after conversion", async () => {
  const f = await supplementalFixture();
  try {
    const regular = randomUUID(), extra = randomUUID(), wave = randomUUID();
    await f.pg.query("insert into users(id,role,first_name,last_name,is_supplemental_gm) values($1,'gm','Regular','Test',false),($2,'gm','Extra','Test',true)", [regular, extra]);
    await f.pg.query("insert into praemien_waves(id,name,year,quarter,start_date,end_date,status) values($1,'Synthetic',2026,3,'2026-07-01','2026-09-30','active')", [wave]);
    await f.pg.query("insert into praemien_wave_quality_scores(id,wave_id,gm_user_id,zeiterfassung) values($1,$3,$4,90),($2,$3,$5,99)", [randomUUID(), randomUUID(), wave, regular, extra]);
    const database = modelDatabase(f.database);
    const eligible = regularBonusGmCondition(sql`praemien_wave_quality_scores.gm_user_id`, sql`${wave}::uuid`);
    const rows = await database.query<{ gmId: string }>(sql`select gm_user_id as "gmId" from praemien_wave_quality_scores where ${eligible}`);
    assert.deepEqual(rows.map(row => row.gmId), [regular]);
    await database.query(sql`update praemien_wave_quality_scores set is_deleted=true where ${eligible}`);
    assert.equal((await f.pg.query<{ is_deleted: boolean }>("select is_deleted from praemien_wave_quality_scores where gm_user_id=$1", [extra])).rows[0]!.is_deleted, false);
    await f.pg.query("insert into visit_sessions(id,gm_user_id,market_id,status,submitted_at) values($1,$2,$3,'submitted','2026-09-01T10:00:00Z')", [randomUUID(), extra, randomUUID()]);
    await f.pg.query("update users set is_supplemental_gm=false where id=$1", [extra]);
    const converted = await database.query<{ gmId: string }>(sql`select gm_user_id as "gmId" from praemien_wave_quality_scores where ${eligible}`);
    assert.equal(converted.some(row => row.gmId === extra), false);
  } finally { await f.pg.close(); }
});


test("workday statistics exclude current Zusatz accounts and complete mixed days while preserving regular days after conversion", async () => {
  const f = await supplementalFixture();
  try {
    const gm = randomUUID(), extra = randomUUID();
    await f.pg.query("insert into users(id,role,is_supplemental_gm) values($1,'gm',false),($2,'gm',true)", [gm, extra]);
    await f.pg.query("insert into visit_sessions(id,gm_user_id,market_id,status,started_at,submitted_at) values($1,$2,$3,'submitted','2026-09-01T10:00:00Z','2026-09-01T11:00:00Z')", [randomUUID(), extra, randomUUID()]);
    const sessions = [
      { gmId: gm, date: "2026-09-01", minutes: 300 },
      { gmId: extra, date: "2026-09-01", minutes: 300 },
      { gmId: extra, date: "2026-09-02", minutes: 300 },
    ];
    assert.deepEqual(await regularGmReportingDays(modelDatabase(f.database), sessions), [sessions[0]]);
    await f.pg.query("update users set is_supplemental_gm=false where id=$1", [extra]);
    assert.deepEqual(await regularGmReportingDays(modelDatabase(f.database), sessions), [sessions[0], sessions[2]]);
    assert.equal((await f.pg.query<{ extra: boolean }>("select is_supplemental_gm_snapshot as extra from visit_sessions where gm_user_id=$1", [extra])).rows[0]!.extra, true);
  } finally { await f.pg.close(); }
});
