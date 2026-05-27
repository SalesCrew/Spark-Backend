import { sql } from "./lib/db.js";

type CleanupTarget = {
  key: string;
  table: string;
  where: string;
  sampleSelect: string;
};

function escapeLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function buildIlikeAny(column: string, patterns: string[]): string {
  return `(${patterns.map((pattern) => `${column} ILIKE ${escapeLiteral(pattern)}`).join(" OR ")})`;
}

async function main() {
  const apply = process.argv.includes("--apply");

  const campaignPatterns = [
    "Campaign Hard Delete %",
    "Campaign Assign %",
    "Campaign Persist GM %",
    "Campaign Aggregate %",
    "Campaign Split %",
    "Campaign Mixed Payload %",
    "Visit % visit-%",
    "%visit-token-%",
    "Submit Bonus Campaign %",
    "Visit Comment Campaign %",
    "Visit Dup % Campaign %",
    "Redmonth % visit-%",
  ];

  const marketPatterns = [
    "Visit % visit-%",
    "Submit Bonus Markt %",
    "Visit Comment Markt %",
    "Visit Dup % Markt %",
    "Visit Token % visit-%",
    "Redmonth % visit-%",
    "Campaign Visit Markt %",
    "Progress Markt %",
  ];

  const sessionTokenPatterns = [
    "bonus-session-%",
    "submit-route-bonus-%",
    "token-%",
    "idem-%",
    "idem-mismatch-%",
  ];

  const trackingTokenPatterns = [
    "tt-%",
    "tt-block-%",
  ];

  const waveNamePatterns = [
    "Submit Bonus Wave %",
    "Praemien %",
  ];

  const tagLabelPatterns = [
    "Tag it-%",
    "VisitTag-%",
    "Visit Comment Tag %",
    "Redmonth Tag visit-%",
    "DupPhotoTag-%",
  ];

  const targets: CleanupTarget[] = [
    {
      key: "campaigns_by_name_pattern",
      table: "campaigns",
      where: buildIlikeAny("name", campaignPatterns),
      sampleSelect: "id, name, created_at",
    },
    {
      key: "markets_by_name_pattern",
      table: "markets",
      where: buildIlikeAny("name", marketPatterns),
      sampleSelect: "id, name, created_at",
    },
    {
      key: "users_by_example_email",
      table: "users",
      where: buildIlikeAny("email", ["%@example.com"]),
      sampleSelect: "id, email, first_name, last_name, created_at",
    },
    {
      key: "visit_sessions_by_token_pattern",
      table: "visit_sessions",
      where: `client_session_token IS NOT NULL AND ${buildIlikeAny("client_session_token", sessionTokenPatterns)}`,
      sampleSelect: "id, client_session_token, created_at",
    },
    {
      key: "time_tracking_entries_by_token_pattern",
      table: "time_tracking_entries",
      where: `client_entry_token IS NOT NULL AND ${buildIlikeAny("client_entry_token", trackingTokenPatterns)}`,
      sampleSelect: "id, client_entry_token, created_at",
    },
    {
      key: "praemien_waves_by_name_pattern",
      table: "praemien_waves",
      where: buildIlikeAny("name", waveNamePatterns),
      sampleSelect: "id, name, created_at",
    },
    {
      key: "photo_tags_by_label_pattern",
      table: "photo_tags",
      where: buildIlikeAny("label", tagLabelPatterns),
      sampleSelect: "id, label, created_at",
    },
  ];

  console.log(apply ? "== APPLY MODE ==" : "== DRY RUN MODE ==");
  console.log("Checking cleanup targets...");

  for (const target of targets) {
    const countRows = await sql.unsafe<{ count: number }[]>(
      `SELECT count(*)::int AS count FROM ${target.table} WHERE ${target.where}`,
    );
    const count = Number(countRows[0]?.count ?? 0);
    console.log(`\n[${target.key}] ${count} row(s)`);
    if (count > 0) {
      const samples = await sql.unsafe<Record<string, unknown>[]>(
        `SELECT ${target.sampleSelect} FROM ${target.table} WHERE ${target.where} ORDER BY created_at DESC NULLS LAST LIMIT 5`,
      );
      for (const sample of samples) {
        console.log(`  - ${JSON.stringify(sample)}`);
      }
    }
  }

  if (!apply) {
    console.log("\nDry run complete. Re-run with --apply to hard-delete listed rows.");
    return;
  }

  console.log("\nDeleting matched rows in transaction...");
  await sql.begin(async (tx) => {
    for (const target of targets) {
      const deletedRows = await tx.unsafe<{ id: string }[]>(
        `DELETE FROM ${target.table} WHERE ${target.where} RETURNING id`,
      );
      console.log(`[DELETE ${target.key}] ${deletedRows.length} row(s)`);
    }
  });
  console.log("Cleanup completed.");
}

void main().catch((error) => {
  console.error("Cleanup failed:", error);
  process.exitCode = 1;
});
