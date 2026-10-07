// Disposable synthetic fixture for the actual campaign and dashboard HTTP routes.
// Never imports app/index.ts, env.ts, db.ts, auth or external services.
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { is, SQL } from "drizzle-orm";
import { getTableConfig, PgDialect, PgTable } from "drizzle-orm/pg-core";
import express from "express";
import * as schema from "../src/lib/schema.js";
import * as conditional from "../src/lib/conditional-visibility.js";
import * as exportDetails from "../src/lib/campaign-visit-export.js";
import * as exportIndex from "../src/routes/campaign-visit-export-index.js";
import { modelDatabase } from "../src/lib/praemien-workspace.js";
import { createGmDashboardRouter } from "../src/routes/gm-dashboard.js";
import { isolatedModule } from "./isolated-module.js";

export async function availabilityHttpFixture() {
  if (process.env.DATABASE_URL || process.env.SUPABASE_URL || process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NODE_ENV === "production") {
    throw new Error("Availability fixture forbids production configuration");
  }
  const pg = new PGlite(), database = drizzle(pg, { schema }), dialect = new PgDialect();
  for (const value of Object.values(schema)) {
    if (typeof value !== "function" || !("enumName" in value) || !("enumValues" in value)) continue;
    const enumValues = value.enumValues as string[];
    await pg.exec('create type "' + value.enumName + '" as enum (' + enumValues.map(v => "'" + v.replaceAll("'", "''") + "'").join(",") + ')');
  }
  // Read-path fixture uses actual column definitions; no production migration,
  // foreign-key mutation, or persisted database is involved.
  for (const table of Object.values(schema).filter(value => is(value, PgTable))) {
    const config = getTableConfig(table);
    const columns = config.columns.map(column => {
      const type = column.getSQLType();
      const value = column.default;
      const defaultSql = is(value, SQL) ? dialect.sqlToQuery(value).sql
        : typeof value === "string" ? "'" + value.replaceAll("'", "''") + "'"
        : typeof value === "number" || typeof value === "boolean" ? String(value) : null;
      return '"' + column.name + '" ' + type + (defaultSql ? " default " + defaultSql : "");
    });
    await pg.exec('create table "' + config.name + '" (' + columns.join(",") + ')');
  }
  const ids = { admin: randomUUID(), gm: randomUUID(), campaign: randomUUID(), secondCampaign: randomUUID(),
    question: randomUUID(), market: randomUUID(), plus: randomUUID(), corso: randomUUID(), spar: randomUUID() };
  await database.insert(schema.users).values([
    { id: ids.admin, firstName: "Synthetic", lastName: "Admin", role: "admin", isActive: true, email: "synthetic-admin@example.test" },
    { id: ids.gm, firstName: "Synthetic", lastName: "GM", role: "gm", isActive: true, email: "synthetic-gm@example.test", region: "Nord" },
  ]);
  await database.insert(schema.markets).values([
    { id: ids.market, dbName: "Billa", name: "Synthetic Billa", region: "Nord", visitFrequencyPerYear: 12 },
    { id: ids.plus, dbName: "Billa+", name: "Synthetic Plus", region: "Nord", visitFrequencyPerYear: 8 },
    { id: ids.corso, dbName: "Billa Corso", name: "Synthetic Corso", region: "Nord", visitFrequencyPerYear: 6 },
    { id: ids.spar, dbName: "SPAR", name: "Synthetic SPAR", region: "Nord", visitFrequencyPerYear: 8 },
  ]);
  await database.insert(schema.campaigns).values([
    { id: ids.campaign, name: "Synthetic Standard", section: "standard", status: "active" },
    { id: ids.secondCampaign, name: "Synthetic Flex", section: "flex", status: "active" },
  ]);
  const seed = async (input: { category: string; market?: string; start?: string; submitted?: string; invalid?: boolean; duplicate?: boolean }) => {
    const session = randomUUID(), section = randomUUID(), question = randomUUID(), answer = randomUUID();
    const submittedAt = new Date(input.submitted ?? "2026-09-15T10:00:00+02:00");
    await database.insert(schema.visitSessions).values({ id: session, gmUserId: ids.gm, marketId: input.market ?? ids.market,
      status: "submitted", startedAt: new Date(input.start ?? "2026-09-15T09:00:00+02:00"), submittedAt });
    await database.insert(schema.visitSessionSections).values({ id: section, visitSessionId: session, campaignId: ids.campaign, section: "standard", status: "submitted" });
    await database.insert(schema.visitSessionQuestions).values({ id: question, visitSessionSectionId: section, questionId: ids.question,
      questionType: "single", questionTextSnapshot: "Historical Cooler", moduleNameSnapshot: "Verfügbarkeit", singleChoiceAvailabilitySnapshot: true,
      singleChoiceAvailabilityTypeSnapshot: "Cooler", appliesToMarketChainSnapshot: true });
    await database.insert(schema.visitAnswers).values({ id: answer, visitSessionId: session, visitSessionSectionId: section,
      visitSessionQuestionId: question, questionId: ids.question, questionType: "single", valueText: input.category,
      answerStatus: "answered", isValid: !input.invalid, changedAt: submittedAt });
    if (input.duplicate) {
      const flex = randomUUID(), q = randomUUID();
      await database.insert(schema.visitSessionSections).values({ id: flex, visitSessionId: session, campaignId: ids.secondCampaign, section: "flex", status: "submitted", orderIndex: 1 });
      await database.insert(schema.visitSessionQuestions).values({ id: q, visitSessionSectionId: flex, questionId: ids.question,
        questionType: "single", questionTextSnapshot: "Historical Cooler", moduleNameSnapshot: "Verfügbarkeit", singleChoiceAvailabilitySnapshot: true,
        singleChoiceAvailabilityTypeSnapshot: "Cooler", appliesToMarketChainSnapshot: true });
      await database.insert(schema.visitAnswers).values({ visitSessionId: session, visitSessionSectionId: flex, visitSessionQuestionId: q,
        questionId: ids.question, questionType: "single", valueText: "Mediocre", answerStatus: "answered", isValid: true,
        changedAt: new Date(submittedAt.getTime() + 60000), version: 2 });
    }
    return { session, section, question, answer };
  };
  const visits = [
    await seed({ category: "Top", start: "2026-09-20T23:50:00+02:00", submitted: "2026-09-21T08:00:00+02:00" }),
    await seed({ category: "Top", duplicate: true }),
    await seed({ category: "(3) = mittelmäßig, Verfügbarkeit gewährleistet" }),
    await seed({ category: "Bad", market: ids.plus }),
    await seed({ category: "Top", invalid: true }),
    await seed({ category: "Top", market: ids.corso, start: "2026-09-22T09:00:00+02:00", submitted: "2026-09-22T10:00:00+02:00" }),
    await seed({ category: "Bad", market: ids.spar }),
  ];
  // No current question/catalog/assignments exist: all answers are historical.
  const asPostgres: any = new Proxy(database, { get(object, key) {
    if (key === "execute") return async (query: unknown) => (await object.execute(query as SQL)).rows;
    const value = Reflect.get(object, key); return typeof value === "function" ? value.bind(object) : value;
  } });
  const pass = (_req: express.Request, _res: express.Response, next: express.NextFunction) => next();
  const forbidden = () => { throw new Error("Write/external I/O is forbidden in availability verification"); };
  const source = await readFile(new URL("../src/routes/campaigns.ts", import.meta.url), "utf8");
  const replacements: Record<string, unknown> = {};
  for (const match of source.matchAll(/from\s+["']([^"']+)["']/g)) if (match[1]!.startsWith(".")) replacements[match[1]!] = new Proxy({}, { get: () => forbidden });
  Object.assign(replacements, {
    "../lib/db.js": { db: asPostgres }, "../lib/schema.js": schema,
    "../lib/admin-role.js": { isFullAdminRole: (role: string) => role === "admin" },
    "../middleware/auth.js": { requireAuth: () => pass }, "../lib/kunde-access.js": { requireKundeAdminPermission: pass },
    "../lib/logger.js": { logger: { warn() {}, error() {}, info() {} }, logAction() {}, startActionTimer: () => 0 },
    "./campaign-extension.js": { createCampaignExtensionRouter: () => express.Router() },
    "./campaign-visit-export-index.js": exportIndex,
    "../lib/praemien-workspace.js": { modelDatabase }, "../lib/conditional-visibility.js": conditional,
    "../lib/campaign-visit-export.js": exportDetails,
  });
  const route = await isolatedModule<typeof import("../src/routes/campaigns.js")>(new URL("../src/routes/campaigns.ts", import.meta.url), replacements);
  const app = express(); app.use(express.json());
  app.use((req, res, next) => {
    if (req.headers.authorization !== "Bearer synthetic-availability") { res.sendStatus(401); return; }
    if (req.method !== "GET" && req.path !== "/admin/gm-dashboard/query" && !/^\/admin\/campaigns\/[^/]+\/market-visits\/export-details$/.test(req.path)) { res.sendStatus(405); return; }
    next();
  });
  app.use("/admin/gm-dashboard", createGmDashboardRouter(modelDatabase(database)));
  app.use("/admin", route.adminCampaignsRouter);
  app.use((error: Error, _req: express.Request, res: express.Response, _next: express.NextFunction) => res.status(500).json({ error: error.message }));
  await pg.exec("set default_transaction_read_only=on");
  return { app, pg, ids, visits, database };
}
