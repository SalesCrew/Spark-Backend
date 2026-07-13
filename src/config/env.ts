import dotenv from "dotenv";
import { z } from "zod";
import { configureLogger, logger } from "../lib/logger.js";

dotenv.config();

const envSchema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  PORT: z.coerce.number().int().positive().default(4000),
  SUPABASE_URL: z.string().url(),
  SUPABASE_ANON_KEY: z.string().min(1),
  SUPABASE_SERVICE_ROLE_KEY: z.string().min(1),
  DATABASE_URL: z.string().url(),
  DB_POOL_MAX: z.coerce.number().int().min(1).max(20).default(4),
  JWT_SECRET: z.string().min(16),
  CORS_ORIGIN: z.string().default("http://localhost:3000"),
  OPENAI_API_KEY: z.preprocess(
    (value) => (typeof value === "string" && value.trim().length === 0 ? undefined : value),
    z.string().min(1).optional(),
  ),
  OPENAI_KURTI_MODEL: z.string().min(1).default("gpt-5.6-luna"),
  OPENAI_KURTI_MAX_OUTPUT_TOKENS: z.coerce.number().int().min(200).max(4000).default(900),
  OPENAI_KURTI_TIMEOUT_MS: z.coerce.number().int().min(5_000).max(120_000).default(45_000),
  IPP_FINALIZER_ENABLED: z.preprocess((value) => {
    if (typeof value === "string") {
      const normalized = value.trim().toLowerCase();
      return ["1", "true", "yes", "on"].includes(normalized);
    }
    return value;
  }, z.boolean().default(false)),
  IPP_FINALIZER_INTERVAL_MS: z.coerce.number().int().positive().default(5 * 60 * 1000),
  LOG_LEVEL: z.enum(["debug", "info", "warn", "error"]).default("info"),
  LOG_INCLUDE_STACK: z.preprocess((value) => {
    if (typeof value === "string") {
      const normalized = value.trim().toLowerCase();
      return ["1", "true", "yes", "on"].includes(normalized);
    }
    return value;
  }, z.boolean().default(false)),
  LOG_HEALTH_REQUESTS: z.preprocess((value) => {
    if (typeof value === "string") {
      const normalized = value.trim().toLowerCase();
      return ["1", "true", "yes", "on"].includes(normalized);
    }
    return value;
  }, z.boolean().default(false)),
});

const parsedEnv = envSchema.safeParse(process.env);
if (!parsedEnv.success) {
  logger.error("env_validation_failed", {
    issues: parsedEnv.error.issues.map((issue) => ({
      code: issue.code,
      path: issue.path.join("."),
      message: issue.message,
    })),
  });
  throw parsedEnv.error;
}

configureLogger({
  level: parsedEnv.data.LOG_LEVEL,
  includeStack: parsedEnv.data.LOG_INCLUDE_STACK,
});

export const env = parsedEnv.data;
