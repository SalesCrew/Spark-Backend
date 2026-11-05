import test from "node:test";
import assert from "node:assert/strict";
import express from "express";
import request from "supertest";
import { createRequire } from "node:module";
import { isolatedModule } from "./isolated-module.js";
import { praemienFixture } from "../src/lib/praemien-test-fixture.js";
import { installDashboardFixture } from "../src/lib/gm-dashboard-test-fixture.js";
import { createGmDashboardRouter } from "../src/routes/gm-dashboard.js";
import { createPraemienDashboardReadRouter } from "../src/routes/praemien-dashboard-read.js";
import { loadDashboard } from "../src/lib/gm-dashboard.js";

test("actual dashboard XLSX generation uses the same regular cohort for all four cards, answer audit and bonus sheet", async () => {
  const f = await praemienFixture();
  try {
    await installDashboardFixture(f);
    await f.pg.query("update users set is_supplemental_gm=true where id=$1", [f.ids.other]);
    const app = express();
    app.use(express.json());
    app.use("/dashboard", createGmDashboardRouter(f.database));
    app.use("/bonus", createPraemienDashboardReadRouter(f.database));
    const scope = { region: null, gmId: null, chain: null, marketId: null, stc: null };
    const intervals = [{ id: "sep", label: "September", shortLabel: "Sep", start: "2026-09-01", end: "2026-09-30" }];
    const data = await loadDashboard(f.database, intervals, scope, true);
    const require = createRequire(new URL("../../package.json", import.meta.url));
    const XLSX = require("xlsx-js-style");
    const workbook = await isolatedModule<any>(new URL("../../src/lib/exports/workbook.ts", import.meta.url), {});
    const availability = await isolatedModule<any>(new URL("../../src/lib/availability.shared.ts", import.meta.url), {});
    const availabilityExport = await isolatedModule<any>(new URL("../../src/lib/exports/availabilityExport.ts", import.meta.url), {
      "@/lib/availability.shared": availability, "./workbook": workbook,
    });
    const dashboardData = await isolatedModule<any>(new URL("../../src/lib/gm-dashboard/data.ts", import.meta.url), {});
    const chains = await isolatedModule<any>(new URL("../../src/lib/gm-dashboard/chain-groups.ts", import.meta.url), {});
    let downloaded: Buffer | undefined;
    const exporter = await isolatedModule<any>(new URL("../../src/lib/gm-dashboard/export.ts", import.meta.url), {
      "@/lib/api/backend": {
        requestGmDashboard: async (_path: string, body: object) => {
          const response = await request(app).post("/dashboard/query").send(body);
          assert.equal(response.status, 200);
          return response.body;
        },
        requestPraemienWorkspace: async (path: string) => {
          const response = await request(app).get("/bonus" + path);
          assert.equal(response.status, 200);
          return response.body;
        },
      },
      "@/lib/exports/availabilityExport": availabilityExport,
      "./data": dashboardData, "./chain-groups": chains,
      "@/lib/availability.shared": availability,
      "@/lib/exports/workbook": {
        ...workbook,
        buildAndDownloadWorkbook: async ({ build }: any) => {
          const wb = XLSX.utils.book_new();
          await build({ XLSX, wb });
          downloaded = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });
        },
      },
    });
    await exporter.exportRealGmDashboard({
      datasets: ["IPP", "Fuellstand", "Platzierungen", "Aktivitaet"].map(title => ({
        title, data, selectedIntervalId: "sep", comparisonIntervalId: null,
      })),
      bonus: { wave: { id: f.ids.wave } }, exportedBy: "Synthetic Admin",
    });
    assert.ok(downloaded);
    const result = XLSX.read(downloaded, { type: "buffer" });
    assert.ok(result.SheetNames.includes("Boni"));
    const bonusRows = XLSX.utils.sheet_to_json(result.Sheets.Boni, { header: 1 });
    assert.equal(JSON.stringify(bonusRows).includes(f.ids.other), false);
    assert.equal(JSON.stringify(bonusRows).includes(f.ids.gm), true);
    for (const title of ["IPP", "Platzierungen", "Aktivitaet"]) {
      const rows = XLSX.utils.sheet_to_json(result.Sheets[title], { header: 1 }) as unknown[][];
      const headers = rows.find(row => row.includes("Besuche"))!;
      const values = rows.find(row => row[0] === "September")!;
      assert.equal(values[headers.indexOf("Besuche")], 3);
    }
    assert.equal(JSON.stringify(XLSX.utils.sheet_to_json(result.Sheets["Verfügbarkeit Antworten"], { header: 1 })).includes(f.ids.other), false);
  } finally { await f.pg.close(); }
});
