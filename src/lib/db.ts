import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import { env } from "../config/env.js";
import * as schema from "./schema.js";

const sql = postgres(env.DATABASE_URL, {
  max: env.DB_POOL_MAX,
  idle_timeout: 20,
  connect_timeout: 15,
  prepare: false,
});

export const db = drizzle(sql, { schema });
export { sql };
