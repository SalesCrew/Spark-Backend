import { sql } from "drizzle-orm";
import type { ModelDatabase } from "./praemien-workspace.js";

export const moduleCatalogTables = {
  main: "module_main",
  kuehler: "module_kuehler",
  mhd: "module_mhd",
  durcharbeit: "module_durcharbeit",
} as const;
export type ModuleCatalogScope = keyof typeof moduleCatalogTables;

async function isReady(database: ModelDatabase) {
  const [row] = await database.query<{ ready: boolean }>(sql`
    select to_regclass('public.module_catalog_state') is not null as ready
  `);
  return row?.ready === true;
}

// Missing migration must not break existing catalog reads. Never hide modules
// from questionnaires or visit execution: this metadata is presentation only.
export async function readModuleCatalogState(database: ModelDatabase, scope: ModuleCatalogScope) {
  if (!(await isReady(database))) return new Map<string, boolean>();
  const rows = await database.query<{ moduleId: string; inactive: boolean }>(sql`
    select module_id as "moduleId", inactive from public.module_catalog_state where scope=${scope}
  `);
  return new Map(rows.map((row) => [row.moduleId, row.inactive]));
}

export async function setModuleCatalogState(database: ModelDatabase, scope: ModuleCatalogScope, moduleId: string, inactive: boolean) {
  if (!(await isReady(database))) return { status: 503 as const };
  // The whitelist is the only source for identifiers. The upsert touches just
  // catalog metadata, never questions, links, module revision or visit rows.
  const [row] = await database.query<{ id: string; catalogInactive: boolean }>(sql`
    insert into public.module_catalog_state(scope,module_id,inactive,updated_at)
    select ${scope},id,${inactive},now() from ${sql.identifier(moduleCatalogTables[scope])}
    where id=${moduleId}::uuid and is_deleted=false
    on conflict(scope,module_id) do update set inactive=excluded.inactive,updated_at=excluded.updated_at
    returning module_id as id,inactive as "catalogInactive"
  `);
  return row ? { status: 200 as const, module: row } : { status: 404 as const };
}
