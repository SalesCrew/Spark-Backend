import { spawnSync } from "node:child_process";
import { dirname, delimiter } from "node:path";
import { fileURLToPath } from "node:url";

// Explicit fixtures replace all configuration, database, auth, storage and provider I/O.
// Do not inherit application credentials or run the application/job entry point.
const result = spawnSync(process.execPath, [
  "--import", "tsx", "--test", "--test-concurrency=1",
  "src/kurti-safeguards.test.ts", "src/kurti-identity.integration.test.ts",
  "src/kurti-request-deadline.test.ts", "src/kurti-operating-policy.test.ts",
  "src/sm-kurti.integration.test.ts", "src/admin-kurti-charts.test.ts",
  "src/admin-kurti-model-context.test.ts", "src/admin-kurti-visualizations.test.ts",
  "src/admin-kurti-exports.test.ts",
], {
  cwd: fileURLToPath(new URL("../", import.meta.url)),
  env: { PATH: [dirname(process.execPath), "/usr/bin", "/bin"].join(delimiter), NODE_ENV: "test", TZ: "Europe/Vienna" },
  stdio: "inherit",
});
if (result.error) console.error("Unable to start isolated Kurti verification:", result.error.message);
process.exitCode = result.status ?? 1;
