// Explicitly local synthetic preview: no env.ts, db.ts, Supabase or job imports.
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import express from "express";
import cors from "cors";
import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { modelDatabase } from "../lib/praemien-workspace.js";
import { readModuleCatalogState } from "../lib/module-catalog-state.js";
import { createModuleCatalogStateRouter } from "../routes/module-catalog-state.js";

if (process.env.NODE_ENV === "production") throw new Error("Local preview only");
const pg = new PGlite();
await pg.exec("create role anon; create role authenticated; create role service_role bypassrls; create table module_main(id uuid primary key,name text,description text,revision int default 1,is_deleted boolean default false,created_at timestamptz default now());");
await pg.exec(await readFile(new URL("../../supabase/migrations/20260929140742_module_catalog_state.sql", import.meta.url), "utf8"));
for (const name of ["Aktuelles Modul", "Älteres Modul", "Weiteres Modul"]) await pg.query("insert into module_main(id,name,description) values($1,$2,'Synthetischer lokaler Test')", [randomUUID(), name]);
const database = modelDatabase(drizzle(pg));
const app = express();
app.use(cors({ origin: "http://localhost:3018" }));
app.use(express.json());
app.use((req, res, next) => {
  if (req.method === "OPTIONS") { res.sendStatus(204); return; }
  if (req.headers.authorization !== "Bearer local-module-preview") { res.sendStatus(401); return; }
  next();
});
app.use("/admin", createModuleCatalogStateRouter(database));
async function modules() {
  const states = await readModuleCatalogState(database, "main");
  const rows = (await pg.query<{ id: string; name: string; description: string; revision: number; created_at: Date }>("select * from module_main order by created_at")).rows;
  return rows.map((row) => ({ id: row.id, name: row.name, description: row.description, revision: row.revision, createdAt: new Date(row.created_at).toISOString(), usedInCount: 1, catalogInactive: states.get(row.id) ?? false, sectionKeywords: ["standard"], questions: [] }));
}
app.get("/admin/modules/main", async (_req, res) => res.json({ modules: await modules() }));
app.patch("/admin/modules/main/:id", async (req, res) => {
  // Synthetic content-save stand-in; the status route above is production code.
  const row = (await pg.query("update module_main set name=$1,description=$2,revision=revision+1 where id=$3 and revision=$4 returning id", [req.body.name, req.body.description, req.params.id, req.body.revision])).rows[0];
  if (!row) { res.status(409).json({ error: "Revision conflict" }); return; }
  res.json({ module: (await modules()).find((module) => module.id === req.params.id) });
});
app.get("/admin/campaigns", (_req, res) => res.json({ campaigns: [] }));
app.get("/admin/praemien/question-usage", (_req, res) => res.json({ usage: [] }));
app.post("/telemetry/events", (_req, res) => res.sendStatus(204));
app.get("/admin/photos/tags", (_req, res) => res.json({ tags: [] }));
app.listen(4018, "127.0.0.1", () => console.log("Synthetic module preview listening on http://localhost:4018; no production connections"));
