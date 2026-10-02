// Synthetic extension of the existing local Prämien fixture, not an app seed.
import { randomUUID } from "node:crypto";
import type { praemienFixture } from "./praemien-test-fixture.js";
export async function installDashboardFixture(
  f: Awaited<ReturnType<typeof praemienFixture>>,
) {
  await f.pg.exec(`
    alter table users add column region text default 'Nord';
    alter table markets add column name text default 'Sparmarkt Test',add column address text default 'Testgasse 1',add column city text default 'Wien',add column postal_code text default '1010',add column region text default 'Nord',add column flex_number text default 'S-TEST',add column standard_market_number text,add column coke_master_number text,add column is_deleted boolean default false;
    alter table visit_sessions add column started_at timestamptz;
    alter table visit_session_sections add column visit_session_id uuid;
    alter table visit_session_questions add column visit_session_section_id uuid,add column question_id uuid,add column single_choice_availability_snapshot boolean default false,add column single_choice_availability_type_snapshot text,add column red_survey_snapshot boolean default false,add column question_text_snapshot text default '',add column module_name_snapshot text default '';
    alter table visit_answers add column changed_at timestamptz default now(),add column question_type text default 'single_choice',add column value_json jsonb;
    alter table visit_answer_options add column option_role text default 'top';
    alter table question_scoring add column ipp numeric,add column zweitplatzierung numeric,add column mitbewerberabfrage numeric;
  `);
  const qAvailability = randomUUID(),
    qPlacement = randomUUID(),
    qNumeric = randomUUID();
  await f.pg.query(
    `insert into question_bank_shared(id,text,question_type) values($1,'Füllstand Test','single_choice'),($2,'Coke Test','single_choice'),($3,'Mitbewerber Test','numeric')`,
    [qAvailability, qPlacement, qNumeric],
  );
  await f.pg.query(
    `insert into question_scoring(question_id,score_key,ipp,zweitplatzierung,mitbewerberabfrage) values($1,'Ja',2,2,null),($1,'Nein',0,0,null),($2,'__value__',null,null,0.5)`,
    [qPlacement, qNumeric],
  );
  const seedVisit = async (options: {
    when: string;
    category: string;
    gm?: string;
    market?: string;
    deleted?: boolean;
    status?: string;
    mixed?: boolean;
    invalid?: boolean;
    hidden?: boolean;
    weight?: string;
  }) => {
    const session = randomUUID(),
      section = randomUUID();
    await f.pg.query(
      `insert into visit_sessions(id,gm_user_id,market_id,status,submitted_at,started_at,is_deleted) values($1,$2,$3,$4,$5::timestamptz,$5::timestamptz-interval '45 minutes',$6)`,
      [
        session,
        options.gm ?? f.ids.gm,
        options.market ?? f.ids.market,
        options.status ?? "submitted",
        options.when,
        options.deleted ?? false,
      ],
    );
    await f.pg.query(
      `insert into visit_session_sections(id,section,visit_session_id) values($1,'standard',$2)`,
      [section, session],
    );
    for (const [question, category, type, available] of [
      [qAvailability, options.category, "single_choice", true],
      [qPlacement, options.weight ?? "Ja", "single_choice", false],
      [qNumeric, null, "numeric", false],
    ] as const) {
      const instance = randomUUID(),
        answer = randomUUID();
      await f.pg.query(
        `insert into visit_session_questions(id,visit_session_section_id,question_id,single_choice_availability_snapshot,single_choice_availability_type_snapshot,red_survey_snapshot,applies_to_market_chain_snapshot) values($1,$2,$3,$4,'Cooler',true,$5)`,
        [instance, section, question, available, !options.hidden],
      );
      await f.pg.query(
        `insert into visit_answers(id,visit_session_id,visit_session_section_id,visit_session_question_id,question_id,value_text,value_number,question_type,changed_at,is_valid) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
        [
          answer,
          session,
          section,
          instance,
          question,
          category,
          type === "numeric" ? 6 : null,
          type,
          options.when,
          !options.invalid,
        ],
      );
      // The same option also exists in normalized storage: score must not double.
      if (question === qPlacement)
        await f.pg.query(
          `insert into visit_answer_options(visit_answer_id,option_value) values($1,$2)`,
          [answer, options.weight ?? "Ja"],
        );
    }
    if (options.mixed) {
      const flex = randomUUID(),
        instance = randomUUID(),
        answer = randomUUID();
      await f.pg.query(
        `insert into visit_session_sections(id,section,visit_session_id) values($1,'flex',$2)`,
        [flex, session],
      );
      await f.pg.query(
        `insert into visit_session_questions(id,visit_session_section_id,question_id,single_choice_availability_snapshot,single_choice_availability_type_snapshot,red_survey_snapshot) values($1,$2,$3,true,'Cooler',true)`,
        [instance, flex, qAvailability],
      );
      await f.pg.query(
        `insert into visit_answers(id,visit_session_id,visit_session_section_id,visit_session_question_id,question_id,value_text,changed_at) values($1,$2,$3,$4,$5,$6,$7)`,
        [
          answer,
          session,
          flex,
          instance,
          qAvailability,
          options.category,
          options.when,
        ],
      );
    }
    return session;
  };
  // September: 2 Top / 1 Mediocre / 1 Bad = 50/25/25, average 62.5.
  await seedVisit({
    when: "2026-09-05T08:00:00+02:00",
    category: "Top",
    mixed: true,
  });
  await seedVisit({
    when: "2026-09-06T08:00:00+02:00",
    category: "Voll",
    gm: f.ids.other,
  });
  await seedVisit({ when: "2026-09-14T08:00:00+02:00", category: "Mittel" });
  await seedVisit({
    when: "2026-09-21T08:00:00+02:00",
    category: "Leer",
    gm: f.ids.inactive,
  });
  await seedVisit({ when: "2026-08-18T08:00:00+02:00", category: "Leer" });
  await seedVisit({ when: "2026-08-19T08:00:00+02:00", category: "Top" });
  await seedVisit({
    when: "2026-09-12T08:00:00+02:00",
    category: "Top",
    deleted: true,
  });
  await seedVisit({
    when: "2026-09-13T08:00:00+02:00",
    category: "Top",
    status: "draft",
  });
  await seedVisit({ when: "2026-10-01T00:00:00+02:00", category: "Top" });
  // Genuine synthetic 4/4/5 local calendar, including future intervals to test
  // current selection. UUIDs never correspond to production periods.
  const calendar = [];
  for (const year of [2024, 2025, 2026, 2027]) {
    let start = new Date(Date.UTC(year, 0, 5));
    for (let index = 0; index < 12; index++) {
      const weeks = [4, 4, 5][index % 3]!,
        end = new Date(start.getTime() + weeks * 7 * 86400000 - 86400000);
      const startYmd = start.toISOString().slice(0, 10),
        endYmd = end.toISOString().slice(0, 10),
        id = randomUUID();
      calendar.push({
        id,
        redPeriodId: id,
        redMonthYearId: null,
        label: `RED ${String(index + 1).padStart(2, "0")} · ${year}`,
        periodIndex: index + 1,
        periodIndexFromAnchor: index,
        start: startYmd,
        end: endYmd,
        year,
        status: "active",
        isCurrent: startYmd <= "2026-09-28" && endYmd >= "2026-09-28",
        daysUntilEnd: 0,
      });
      start = new Date(end.getTime() + 86400000);
    }
  }
  return { seedVisit, calendar, qAvailability, qPlacement, qNumeric };
}
