import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { supplementalFixture } from "./gm-supplemental-fixture.js";
import { isolatedModule } from "./isolated-module.js";
import * as schema from "../src/lib/schema.js";
import * as visibility from "../src/gm-historical-visibility.shared.js";

test("actual effective RED IPP excludes before answer/owner ranking, bypasses mixed archives/corrections, preserves clean archives and inclusive personal results", async () => {
  const f = await supplementalFixture();
  try {
    const pgSql = async (parts: TemplateStringsArray, ...values: unknown[]) => {
      const text = parts.reduce((out, part, index) => out + (index ? `$${index}` : "") + part, "");
      return (await f.pg.query(text, values.map(value => Array.isArray(value) ? `{${value.join(",")}}` : value))).rows;
    };
    const ipp = await isolatedModule<typeof import("../src/lib/ipp.js")>(new URL("../src/lib/ipp.ts", import.meta.url), {
      "./db.js": { db: f.database, sql: pgSql }, "./schema.js": schema,
      "./red-monat.js": { addDays() { throw new Error("Unexpected date path"); }, startOfDay() { throw new Error("Unexpected date path"); } },
    });
    const service = await isolatedModule<typeof import("../src/lib/ipp-gm-effective.js")>(new URL("../src/lib/ipp-gm-effective.ts", import.meta.url), {
      "./db.js": { db: f.database, sql: pgSql }, "./schema.js": schema, "./ipp.js": ipp,
      "../gm-historical-visibility.shared.js": visibility,
      "./red-month-periods.js": { resolveCurrentRedPeriod: async () => ({}), redMonthPeriodToYmd: () => ({ startYmd: "2026-10-01", endYmd: "2026-10-31" }) },
    });
    const gm = randomUUID(), extraGm = randomUUID(), market = randomUUID(), cleanMarket = randomUUID(), period = randomUUID(), question = randomUUID();
    await f.pg.query(`insert into users(id,role,first_name,last_name,is_active) values($1,'gm','Normal','Test',true),($2,'gm','Zusatz','Test',true)`, [gm, extraGm]);
    await f.pg.query(`update users set is_supplemental_gm=true where id=$1`, [extraGm]);
    await f.pg.query(`insert into red_month_periods(id,red_year,period_index,label,start_date,end_date,is_deleted) values($1,2026,9,'RED Test','2026-09-01','2026-09-30',false)`, [period]);
    await f.pg.query(`insert into question_scoring(question_id,score_key,ipp,is_deleted) values($1,'Ja',2,false),($1,'Extra',20,false)`, [question]);
    for (const [owner, selected, when, whichMarket] of [[gm, "Ja", "2026-09-10T10:00:00Z", market], [gm, "Ja", "2026-09-11T10:00:00Z", cleanMarket], [extraGm, "Extra", "2026-09-29T10:00:00Z", market]] as const) {
      const visit = randomUUID(), section = randomUUID(), q = randomUUID(), answer = randomUUID();
      await f.pg.query(`insert into visit_sessions(id,gm_user_id,market_id,status,submitted_at) values($1,$2,$3,'submitted',$4)`, [visit, owner, whichMarket, when]);
      await f.pg.query(`insert into visit_session_sections(id,visit_session_id,section) values($1,$2,'standard')`, [section, visit]);
      await f.pg.query(`insert into visit_session_questions(id,visit_session_section_id,question_id) values($1,$2,$3)`, [q, section, question]);
      await f.pg.query(`insert into visit_answers(id,visit_session_id,visit_session_section_id,visit_session_question_id,question_id,question_type,value_text) values($1,$2,$3,$4,$5,'single','${selected}')`, [answer, visit, section, q, question]);
    }
    for (const [whichMarket, value] of [[market, 999], [cleanMarket, 77]] as const) {
      await f.pg.query(`insert into ipp_market_redmonth_results(id,red_period_id,red_period_start,red_period_end,market_id,market_ipp,source_submission_count,contributing_question_count,is_finalized,is_deleted) values($1,$2,'2026-09-01','2026-09-30',$3,$4,1,1,true,false)`, [randomUUID(), period, whichMarket, value]);
    }
    await f.pg.query(`insert into ipp_gm_redmonth_adjustment_events(id,gm_user_id,red_period_id,revision_number,event_type,corrected_ipp,base_calculated_ipp,base_sample_count,base_fingerprint,reason,created_by_user_id,created_at) values($1,$2,$3,1,'set',444,2,1,'synthetic','synthetic',$4,now())`, [randomUUID(), gm, period, f.adminId]);
    const filtered = await service.loadEffectiveGmIppPeriods({ redPeriodIds: [period], excludeSupplementalGms: true });
    assert.equal(filtered.find(row => row.gmUserId === gm)?.effectiveIpp, 39.5, "2 from eligible source + 77 from untouched clean archive, divided by two");
    assert.equal(filtered.some(row => row.gmUserId === extraGm), false);
    const inclusive = await service.loadEffectiveGmIppPeriods({ redPeriodIds: [period] });
    assert.equal(inclusive.find(row => row.gmUserId === gm)?.effectiveIpp, 444, "personal/inclusive correction unchanged");
    await f.pg.query(`update users set is_supplemental_gm=false where id=$1`, [extraGm]);
    assert.equal((await service.loadEffectiveGmIppPeriods({ redPeriodIds: [period], excludeSupplementalGms: true })).some(row => row.gmUserId === extraGm), false);
    assert.equal((await f.pg.query<{ market_ipp: string }>(`select market_ipp from ipp_market_redmonth_results where market_id=$1`, [market])).rows[0]?.market_ipp, "999.0000", "archive is never rewritten");
  } finally { await f.pg.close(); }
});
