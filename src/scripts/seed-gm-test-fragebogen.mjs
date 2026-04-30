import postgres from "postgres";

const databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) {
  throw new Error("DATABASE_URL is missing");
}

const sql = postgres(databaseUrl, { prepare: false });

function nowRunKey() {
  const d = new Date();
  const p2 = (n) => String(n).padStart(2, "0");
  return `GM_TEST_${d.getFullYear()}${p2(d.getMonth() + 1)}${p2(d.getDate())}_${p2(d.getHours())}${p2(d.getMinutes())}${p2(d.getSeconds())}`;
}

async function main() {
  const runKey = nowRunKey();
  const sections = ["standard", "flex", "billa", "kuehler", "mhd"];

  await sql.begin(async (tx) => {
    const [photoTag] = await tx`
      insert into photo_tags (label)
      values (${`${runKey} | FotoTag`})
      returning id
    `;
    const photoTagId = photoTag.id;

    for (const section of sections) {
      let moduleId;
      let fragebogenId;

      if (section === "standard" || section === "flex" || section === "billa") {
        const [moduleRow] = await tx.unsafe(
          `
            insert into module_main (name, description, section_keywords)
            values ($1, $2, ARRAY[$3]::fragebogen_main_section[])
            returning id
          `,
          [`${runKey} | Modul | ${section.toUpperCase()}`, "Auto test module with all question types", section],
        );
        moduleId = moduleRow.id;

        const [fbRow] = await tx.unsafe(
          `
            insert into fragebogen_main
              (name, description, section_keywords, nur_einmal_ausfuellbar, status, schedule_type)
            values
              ($1, $2, ARRAY[$3]::fragebogen_main_section[], false, 'inactive', 'always')
            returning id
          `,
          [`${runKey} | Fragebogen | ${section.toUpperCase()}`, "Auto test fragebogen with all question types", section],
        );
        fragebogenId = fbRow.id;

        await tx`
          insert into fragebogen_main_module (fragebogen_id, module_id, order_index)
          values (${fragebogenId}, ${moduleId}, 0)
        `;
      } else if (section === "kuehler") {
        const [moduleRow] = await tx`
          insert into module_kuehler (name, description)
          values (${`${runKey} | Modul | KUEHLER`}, ${"Auto test module with all question types"})
          returning id
        `;
        moduleId = moduleRow.id;

        const [fbRow] = await tx`
          insert into fragebogen_kuehler
            (name, description, nur_einmal_ausfuellbar, status, schedule_type)
          values
            (${`${runKey} | Fragebogen | KUEHLER`}, ${"Auto test fragebogen with all question types"}, false, 'inactive', 'always')
          returning id
        `;
        fragebogenId = fbRow.id;

        await tx`
          insert into fragebogen_kuehler_module (fragebogen_id, module_id, order_index)
          values (${fragebogenId}, ${moduleId}, 0)
        `;
      } else {
        const [moduleRow] = await tx`
          insert into module_mhd (name, description)
          values (${`${runKey} | Modul | MHD`}, ${"Auto test module with all question types"})
          returning id
        `;
        moduleId = moduleRow.id;

        const [fbRow] = await tx`
          insert into fragebogen_mhd
            (name, description, nur_einmal_ausfuellbar, status, schedule_type)
          values
            (${`${runKey} | Fragebogen | MHD`}, ${"Auto test fragebogen with all question types"}, false, 'inactive', 'always')
          returning id
        `;
        fragebogenId = fbRow.id;

        await tx`
          insert into fragebogen_mhd_module (fragebogen_id, module_id, order_index)
          values (${fragebogenId}, ${moduleId}, 0)
        `;
      }

      async function createQuestion(type, text, required, config) {
        const [row] = await tx`
          insert into question_bank_shared (question_type, text, required, config)
          values (${type}, ${text}, ${required}, ${JSON.stringify(config)}::jsonb)
          returning id
        `;
        return row.id;
      }

      const prefix = `${runKey} | ${section.toUpperCase()} |`;
      const qSingle = await createQuestion("single", `${prefix} Q single`, true, { options: ["Marke A", "Marke B", "Marke C"] });
      const qYesno = await createQuestion("yesno", `${prefix} Q yesno`, true, {});
      const qYesnomulti = await createQuestion("yesnomulti", `${prefix} Q yesnomulti`, true, {
        options: ["Neuplatzierung", "POS-Material", "Zusatzdisplay"],
      });
      const qMultiple = await createQuestion("multiple", `${prefix} Q multiple`, true, {
        options: ["OOS", "Preisschild fehlt", "Falsch platziert"],
      });
      const qLikert = await createQuestion("likert", `${prefix} Q likert`, true, {
        labels: ["Sehr schlecht", "Schlecht", "Neutral", "Gut", "Sehr gut"],
        min: 1,
        max: 5,
      });
      const qText = await createQuestion("text", `${prefix} Q text`, false, {
        minLength: 0,
        maxLength: 300,
        placeholder: "Kommentar",
      });
      const qNumeric = await createQuestion("numeric", `${prefix} Q numeric`, true, {
        min: 0,
        max: 999,
        decimals: false,
        unit: "Stk",
      });
      const qSlider = await createQuestion("slider", `${prefix} Q slider`, true, {
        min: 0,
        max: 100,
        step: 1,
        unit: "%",
      });
      const qPhoto = await createQuestion("photo", `${prefix} Q photo`, true, {
        tagsEnabled: true,
        tagIds: [photoTagId],
        instruction: "Bitte 1 Foto aufnehmen",
      });
      const qMatrix = await createQuestion("matrix", `${prefix} Q matrix`, true, {
        matrixSubtype: "toggle",
      });

      await tx`
        insert into question_matrix (question_id, matrix_subtype, rows, columns)
        values (
          ${qMatrix},
          'toggle',
          ${JSON.stringify(["Regal 1", "Regal 2", "Zweitplatzierung"])}::jsonb,
          ${JSON.stringify(["Vorhanden", "Sauber", "Korrekt befüllt"])}::jsonb
        )
      `;

      await tx`
        insert into question_photo_tags (question_id, photo_tag_id)
        values (${qPhoto}, ${photoTagId})
      `;

      const scoringRows = [
        [qSingle, "Marke A", 1, 0],
        [qYesno, "ja", 1, 0],
        [qYesnomulti, "Neuplatzierung", 0.5, 0],
        [qMultiple, "OOS", -1, 0],
        [qLikert, "__value__", 1, 0],
        [qText, "__value__", 0, 0],
        [qNumeric, "__value__", 1, 0],
        [qSlider, "__value__", 1, 0],
        [qPhoto, "__value__", 0, 0],
        [qMatrix, "__value__", 1, 0],
      ];
      for (const [questionId, scoreKey, ipp, boni] of scoringRows) {
        await tx`
          insert into question_scoring (question_id, score_key, ipp, boni)
          values (${questionId}, ${scoreKey}, ${ipp}, ${boni})
        `;
      }

      const orderedQuestionIds = [qSingle, qYesno, qYesnomulti, qMultiple, qLikert, qText, qNumeric, qSlider, qPhoto, qMatrix];
      for (let i = 0; i < orderedQuestionIds.length; i += 1) {
        const questionId = orderedQuestionIds[i];
        if (section === "standard" || section === "flex" || section === "billa") {
          await tx`
            insert into module_main_question (module_id, question_id, order_index)
            values (${moduleId}, ${questionId}, ${i})
          `;
        } else if (section === "kuehler") {
          await tx`
            insert into module_kuehler_question (module_id, question_id, order_index)
            values (${moduleId}, ${questionId}, ${i})
          `;
        } else {
          await tx`
            insert into module_mhd_question (module_id, question_id, order_index)
            values (${moduleId}, ${questionId}, ${i})
          `;
        }
      }
    }
  });

  console.log(`SEED_OK RUN_KEY=${runKey}`);
}

main()
  .catch((error) => {
    console.error("SEED_FAILED", error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await sql.end({ timeout: 5 });
  });

