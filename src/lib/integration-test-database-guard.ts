const KNOWN_PRODUCTION_PROJECT_REFS = ["quqefecmqeienxmeueqa"] as const;

function normalized(value: string | undefined) {
  return value?.trim().replace(/\/$/, "") ?? "";
}

function pointsAtKnownProductionProject(...values: string[]) {
  const joined = values.join("\n").toLowerCase();
  return KNOWN_PRODUCTION_PROJECT_REFS.some((projectRef) => joined.includes(projectRef));
}

function fail(reason: string): never {
  throw new Error(
    [
      "Mutating integration tests are blocked.",
      reason,
      "Use a separate Supabase test project and set all of:",
      "NODE_ENV=test",
      "ALLOW_MUTATING_INTEGRATION_TESTS=1",
      "TEST_SUPABASE_URL=<isolated test project URL>",
      "SUPABASE_URL=<the same isolated test project URL>",
      "TEST_DATABASE_URL=<isolated test database URL>",
      "DATABASE_URL=<the same isolated test database URL>",
    ].join("\n"),
  );
}

export function assertSafeMutatingIntegrationTestEnvironment() {
  const nodeEnv = normalized(process.env.NODE_ENV);
  const allowMutations = normalized(process.env.ALLOW_MUTATING_INTEGRATION_TESTS);
  const supabaseUrl = normalized(process.env.SUPABASE_URL);
  const testSupabaseUrl = normalized(process.env.TEST_SUPABASE_URL);
  const databaseUrl = normalized(process.env.DATABASE_URL);
  const testDatabaseUrl = normalized(process.env.TEST_DATABASE_URL);

  if (nodeEnv !== "test") {
    fail("NODE_ENV must be exactly 'test'.");
  }

  if (allowMutations !== "1") {
    fail("ALLOW_MUTATING_INTEGRATION_TESTS must be explicitly set to '1'.");
  }

  if (!testSupabaseUrl || !testDatabaseUrl) {
    fail("Both TEST_SUPABASE_URL and TEST_DATABASE_URL are required.");
  }

  if (supabaseUrl !== testSupabaseUrl || databaseUrl !== testDatabaseUrl) {
    fail("The configured Supabase and database URLs must exactly match their TEST_* counterparts.");
  }

  if (pointsAtKnownProductionProject(supabaseUrl, testSupabaseUrl, databaseUrl, testDatabaseUrl)) {
    fail("The configured connection points at the Coke Spark production project.");
  }
}
