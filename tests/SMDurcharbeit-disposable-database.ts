import { randomUUID } from "node:crypto";
import { readFile, realpath } from "node:fs/promises";
import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { drizzle as postgresDrizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import * as schema from "../src/lib/schema.js";

/** Never accepts a URL or credentials. Native tests require our private disposable socket marker. */
export async function createSMDurcharbeitDisposableDatabase() {
  const socket = process.env.SMDURCHARBEIT_SYNTHETIC_PG_SOCKET_DIR;
  if (!socket) {
    const pg = new PGlite();
    return { pg: pg as Pick<PGlite, "query" | "exec" | "close">, database: drizzle(pg, { schema }), nativePostgres: false };
  }
  const root = await realpath(socket);
  if (!/^\/tmp\/coke-smdurcharbeit-pg-tools\.[A-Za-z0-9]+\/socket$/.test(socket) || !/^\/(?:private\/)?tmp\/coke-smdurcharbeit-pg-tools\.[A-Za-z0-9]+\/socket$/.test(root)
    || await readFile(`${root}/SMDurcharbeit-disposable-marker`, "utf8") !== "SMDurcharbeit synthetic PostgreSQL 16 fixture only\n") {
    throw new Error("Only a marked private disposable SMDurcharbeit PostgreSQL cluster is allowed.");
  }
  const connect = (database: string, max = 8) => postgres({ host: root, port: 55437, database, username: "synthetic_fixture", password: "synthetic-disposable-only", ssl: false, max, connect_timeout: 5, idle_timeout: 5, onnotice: () => {} });
  const admin = connect("postgres"), name = `smd_fixture_${randomUUID().replaceAll("-", "")}`;
  await admin.unsafe(`create database "${name}"`);
  const client = connect(name), raw = connect(name, 1);
  const pg = {
    query: async (text: string, parameters: unknown[] = []) => ({ rows: [...await raw.unsafe(text, parameters as postgres.ParameterOrJSON<never>[])] }),
    exec: async (text: string) => { await raw.unsafe(text).simple(); },
    close: async () => { await Promise.all([client.end(), raw.end()]); try { await admin.unsafe(`drop database "${name}"`); } finally { await admin.end(); } },
  } as unknown as Pick<PGlite, "query" | "exec" | "close">;
  // The fixture exposes the common Drizzle query/transaction API to existing synthetic tests.
  return { pg, database: postgresDrizzle(client, { schema }) as unknown as ReturnType<typeof drizzle<typeof schema>>, nativePostgres: true };
}
