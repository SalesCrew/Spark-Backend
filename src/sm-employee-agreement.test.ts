import assert from "node:assert/strict";
import test, { after } from "node:test";
import express from "express";
import request from "supertest";
import { PgDialect } from "drizzle-orm/pg-core";
import type { SQL } from "drizzle-orm";

Object.assign(process.env, {
  NODE_ENV: "test", BYPASS_AUTH_FOR_TESTS: "1", BYPASS_AUTH_ROLE: "sm",
  BYPASS_AUTH_USER_ID: "11111111-1111-4111-8111-111111111111",
  DATABASE_URL: "postgres://test:test@127.0.0.1:1/test", SUPABASE_URL: "http://127.0.0.1:1",
  SUPABASE_ANON_KEY: "test-only", SUPABASE_SERVICE_ROLE_KEY: "test-only", JWT_SECRET: "test-only-placeholder-secret",
});
const { agreementPayload, employeeAgreementRouter } = await import("./routes/employee-agreement.js");
const { db, sql: client } = await import("./lib/db.js");
after(async () => { await client.end(); });
const app = express();
app.use(express.json());
app.use("/employee-agreement", employeeAgreementRouter);
const dialect = new PgDialect();
const gmHash = "a1b2bfd8277473fe36f056958e77a393fe93ab28ee0a2bc88610d2ae16379caa";

type Acceptance = { userId: string; agreementKey: string; agreementVersion: string; agreementTitle: string; agreementHash: string; acceptedAt: Date };
function mockAcceptances(context: { mock: { method: typeof test.mock.method } }, rows: Acceptance[]) {
  context.mock.method(db, "select", () => {
    let params: unknown[] = [];
    const chain = {
      from: () => chain,
      where: (query: SQL) => { params = dialect.sqlToQuery(query).params; return chain; },
      orderBy: () => chain,
      limit: async () => rows.filter((row) => params.includes(row.userId) && params.includes(row.agreementKey) && params.includes(row.agreementVersion)),
    };
    return chain;
  });
  context.mock.method(db, "insert", () => ({ values: (row: Acceptance) => ({ returning: async () => {
    const saved = { ...row, acceptedAt: new Date("2026-08-31T12:00:00Z") };
    rows.push(saved);
    return [saved];
  } }) }));
}

test("GM agreement key, version, complete wording and hash remain unchanged", () => {
  const gm = agreementPayload("gm");
  assert.equal(gm.key, "spark_employee_agreement");
  assert.equal(gm.version, "2026-07-11-v5");
  assert.equal(gm.hash, gmHash);
});

test("SM gets its own stable version and relevant times, OOS, offline and correction information", () => {
  const sm = agreementPayload("sm");
  assert.equal(sm.key, "spark_sm_employee_agreement");
  assert.equal(sm.version, "2026-08-31-sm-v1");
  assert.notEqual(sm.hash, gmHash);
  const text = JSON.stringify(sm.sections);
  for (const term of ["Fahrtzeit", "aktiviert", "abgeschlossenen", "Out-of-Stock", "offline", "Start- und Endzeitpunkte", "Lesestatus", "datenschutz@merch.at"]) assert.ok(text.includes(term), term);
  assert.doesNotMatch(text, /IPP|Prämienberechnung|RED-Jahres|Frag Kurti/);
});

test("old shared acceptance is preserved but cannot silently accept the new SM document", async (context) => {
  process.env.BYPASS_AUTH_ROLE = "sm";
  const gm = agreementPayload("gm");
  const rows: Acceptance[] = [{ userId: process.env.BYPASS_AUTH_USER_ID!, agreementKey: gm.key, agreementVersion: gm.version, agreementTitle: gm.title, agreementHash: gm.hash, acceptedAt: new Date("2026-07-11T12:00:00Z") }];
  mockAcceptances(context, rows);
  const response = await request(app).get("/employee-agreement/current").expect(200);
  assert.equal(response.body.accepted, false);
  assert.equal(response.body.agreement.key, "spark_sm_employee_agreement");
  assert.equal(rows.length, 1);
  const mismatch = await request(app).post("/employee-agreement/accept").send({ version: gm.version, role: "gm" }).expect(409);
  assert.equal(mismatch.body.agreement.key, "spark_sm_employee_agreement");
  assert.equal(rows.length, 1);
});

test("SM acceptance saves the displayed SM hash/version and is idempotent", async (context) => {
  process.env.BYPASS_AUTH_ROLE = "sm";
  const rows: Acceptance[] = [];
  mockAcceptances(context, rows);
  const sm = agreementPayload("sm");
  await request(app).post("/employee-agreement/accept").send({ version: sm.version }).expect(200);
  await request(app).post("/employee-agreement/accept").send({ version: sm.version }).expect(200);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].agreementKey, sm.key);
  assert.equal(rows[0].agreementHash, sm.hash);
  assert.equal(rows[0].userId, process.env.BYPASS_AUTH_USER_ID);
  const current = await request(app).get("/employee-agreement/current").expect(200);
  assert.equal(current.body.accepted, true);
});

test("GM acceptance continues to use the original agreement, not the SM version", async (context) => {
  process.env.BYPASS_AUTH_ROLE = "gm";
  const rows: Acceptance[] = [];
  mockAcceptances(context, rows);
  const gm = agreementPayload("gm");
  await request(app).post("/employee-agreement/accept").send({ version: gm.version }).expect(200);
  assert.equal(rows[0].agreementKey, gm.key);
  assert.equal(rows[0].agreementHash, gmHash);
  const response = await request(app).get("/employee-agreement/current").expect(200);
  assert.equal(response.body.accepted, true);
  assert.equal(response.body.agreement.hash, gmHash);
});
