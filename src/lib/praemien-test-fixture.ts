// Local-only synthetic database. Does not import db.ts, env.ts, auth or Supabase.
import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { readFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { modelDatabase, mutateWorkspace } from "./praemien-workspace.js";
import { modelTemplate } from "../praemien-model.shared.js";

export async function praemienFixture() {
  const pg = new PGlite();
  await pg.exec(`
    CREATE ROLE anon; CREATE ROLE authenticated; CREATE ROLE service_role;
    CREATE TABLE users(id uuid primary key,first_name text,last_name text,role text,is_active boolean);
    CREATE TABLE praemien_waves(id uuid primary key default gen_random_uuid(),name text,year int,quarter int,status text,start_date date,end_date date,reward_model text,created_at timestamptz default now(),updated_at timestamptz default now(),is_deleted boolean default false);
    CREATE TABLE markets(id uuid primary key,visit_frequency_per_year int,db_name text);
    CREATE TABLE question_bank_shared(id uuid primary key,text text,question_type text,config jsonb default '{}',is_spezial boolean default false,is_deleted boolean default false,updated_at timestamptz default now());
    CREATE TABLE question_scoring(id uuid primary key default gen_random_uuid(),question_id uuid,score_key text,boni numeric,is_deleted boolean default false);
    CREATE TABLE visit_sessions(id uuid primary key,gm_user_id uuid,market_id uuid,status text,submitted_at timestamptz,is_deleted boolean default false);
    CREATE TABLE visit_session_questions(id uuid primary key,applies_to_market_chain_snapshot boolean default true,is_deleted boolean default false);
    CREATE TABLE visit_session_sections(id uuid primary key,section text,is_deleted boolean default false);
    CREATE TABLE visit_answers(id uuid primary key,visit_session_id uuid,visit_session_question_id uuid,visit_session_section_id uuid,question_id uuid,value_number numeric,value_text text,is_valid boolean default true,answer_status text default 'answered',is_deleted boolean default false);
    CREATE TABLE visit_answer_options(id uuid primary key default gen_random_uuid(),visit_answer_id uuid,option_value text,is_deleted boolean default false);
  `);
  await pg.exec(`CREATE TABLE campaigns(id uuid primary key,section text,current_fragebogen_id uuid,status text,schedule_type text,start_date date,end_date date,is_deleted boolean default false);`);
  for (const scope of ["main", "kuehler", "mhd", "durcharbeit"]) {
    await pg.exec(`
      CREATE TABLE fragebogen_${scope}(id uuid primary key,status text default 'inactive',section_keywords text[] default '{standard,flex,billa}',is_deleted boolean default false);
      CREATE TABLE module_${scope}(id uuid primary key,is_deleted boolean default false);
      CREATE TABLE fragebogen_${scope}_module(fragebogen_id uuid,module_id uuid,is_deleted boolean default false);
      CREATE TABLE module_${scope}_question(module_id uuid,question_id uuid,is_deleted boolean default false);
      CREATE TABLE fragebogen_${scope}_spezial_question(fragebogen_id uuid,question_id uuid,is_deleted boolean default false);
    `);
  }
  await pg.exec(
    await readFile(
      new URL(
        "../../supabase/migrations/20260928081414_praemien_wave_workspace.sql",
        import.meta.url,
      ),
      "utf8",
    ),
  );
  const database = modelDatabase(drizzle(pg));
  const ids = {
    admin: randomUUID(),
    gm: randomUUID(),
    other: randomUUID(),
    inactive: randomUUID(),
    wave: randomUUID(),
    question: randomUUID(),
    market: randomUUID(),
    catalogModule: randomUUID(),
    catalogFragebogen: randomUUID(),
    catalogCampaign: randomUUID(),
  };
  await pg.query(
    `insert into users values($1,'Lokal','Admin','admin',true),($2,'GM Test','Nord','gm',true),($3,'GM Test','Süd','gm',true),($4,'GM Test','Historie','gm',false)`,
    [ids.admin, ids.gm, ids.other, ids.inactive],
  );
  await pg.query(
    `insert into praemien_waves(id,name,year,quarter,status,start_date,end_date,reward_model) values($1,'Isolierter Test Q3',2026,3,'draft','2026-07-01','2026-09-30','pillar_tiers')`,
    [ids.wave],
  );
  await pg.query(
    `insert into question_bank_shared(id,text,question_type) values($1,'Permanente Racks – Testfrage','numeric')`,
    [ids.question],
  );
  await pg.query(
    `insert into question_scoring(question_id,score_key,boni) values($1,'__value__',1)`,
    [ids.question],
  );
  await pg.query("insert into fragebogen_main(id) values($1)", [ids.catalogFragebogen]);
  await pg.query("insert into module_main(id) values($1)", [ids.catalogModule]);
  await pg.query("insert into fragebogen_main_module(fragebogen_id,module_id) values($1,$2)", [ids.catalogFragebogen, ids.catalogModule]);
  await pg.query("insert into module_main_question(module_id,question_id) values($1,$2)", [ids.catalogModule, ids.question]);
  await pg.query("insert into campaigns(id,section,current_fragebogen_id,status,schedule_type) values($1,'standard',$2,'active','always')", [ids.catalogCampaign, ids.catalogFragebogen]);
  await pg.query(`insert into markets values($1,8,'Sparmarkt')`, [ids.market]);
  const model = modelTemplate("q2");
  model.provenance +=
    " ISOLIERTER TEST: Qualitätsstufen und GM-Werte sind synthetisch, keine fachliche Freigabe.";
  const quality = model.pillars.find((p) => p.key === "quality")!;
  quality.tiers = quality.metrics.flatMap((m, i) => [
    {
      key: `quality_${i}`,
      label: `${m.label} 80%`,
      group: m.key,
      rewardEur: i === 0 ? 110 : 55,
      conditions: [{ metricKey: m.key, operator: "gte" as const, value: 80 }],
    },
  ]);
  let workspace = await mutateWorkspace(
    database,
    ids.wave,
    0,
    { id: ids.admin, name: "Lokal Admin" },
    { type: "rules", model },
  );
  const values = workspace.results.flatMap((gm) =>
    gm.pillars.flatMap((p) =>
      p.metrics
        .filter(
          (m) =>
            model.pillars
              .find((x) => x.key === p.key)!
              .metrics.find((x) => x.key === m.key)!.method === "manual",
        )
        .map((m) => ({
          gmId: gm.gmId,
          pillarKey: p.key,
          metricKey: m.key,
          value: m.key === "new_coolers" ? 3 : m.key === "returned" ? 0 : 85,
          target: null,
          note: "Synthetischer lokaler Testwert",
        })),
    ),
  );
  workspace = await mutateWorkspace(
    database,
    ids.wave,
    workspace.revision,
    { id: ids.admin, name: "Lokal Admin" },
    { type: "values", entries: values },
  );
  return { pg, database, ids, workspace, model };
}
