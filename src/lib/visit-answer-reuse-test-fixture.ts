// Synthetic PostgreSQL only. Never imports the configured production connector.
import { SQL } from "drizzle-orm";
import { drizzle } from "drizzle-orm/pglite";
import { getTableConfig, PgDialect, type PgTable } from "drizzle-orm/pg-core";
import { praemienFixture } from "./praemien-test-fixture.js";
import {
  praemienWavePillars, praemienWaveSources, praemienWaves,
  visitSessions, visitAnswers, visitAnswerOptions, visitAnswerMatrixCells,
  visitAnswerPhotos, visitAnswerPhotoTags, visitQuestionComments,
} from "./schema.js";

export async function visitAnswerReuseFixture() {
  const f = await praemienFixture();
  try {
    const quote = (value: string) => `"${value.replaceAll('"', '""')}"`;
    const literal = (value: unknown) => {
      if (value instanceof SQL) return new PgDialect().sqlToQuery(value).sql;
      if (typeof value === "boolean" || typeof value === "number") return String(value);
      const text = typeof value === "string" ? value : JSON.stringify(value);
      return `'${text.replaceAll("'", "''")}'`;
    };
    // Mirror the production columns for Drizzle SELECT/RETURNING and inserts.
    // FK/auth/RLS are intentionally outside this isolated query/copy fixture.
    const tables: PgTable[] = [
      praemienWaves, praemienWavePillars, praemienWaveSources,
      visitSessions, visitAnswers, visitAnswerOptions, visitAnswerMatrixCells,
      visitAnswerPhotos, visitAnswerPhotoTags, visitQuestionComments,
    ];
    for (const table of tables) {
      const config = getTableConfig(table);
      await f.pg.exec(`create table if not exists ${quote(config.name)} ()`);
      for (const column of config.columns) {
        const type = "enumValues" in column && column.enumValues?.length
          ? "text" : column.getSQLType();
        const defaultClause = column.default === undefined ? "" : ` default ${literal(column.default)}`;
        await f.pg.exec(`alter table ${quote(config.name)} add column if not exists ${quote(column.name)} ${type}${defaultClause}`);
        if (defaultClause) {
          await f.pg.exec(`alter table ${quote(config.name)} alter column ${quote(column.name)} set${defaultClause}`);
        }
      }
    }
    return { ...f, visitDb: drizzle(f.pg) };
  } catch (error) {
    await f.pg.close();
    throw error;
  }
}
