import { Router } from "express";
import { z } from "zod";
import { logAction, logger, startActionTimer } from "../lib/logger.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";

const clientTelemetryRouter = Router();
clientTelemetryRouter.use(requireAuth(["admin", "gm", "sm", "kunde"]));

const telemetrySchema = z
  .object({
    event: z.string().trim().min(3).max(140),
    action: z.string().trim().min(3).max(160),
    result: z.enum(["success", "failure", "rejected", "started", "cancelled", "skipped"]),
    statusCode: z.number().int().min(100).max(599).optional(),
    durationMs: z.number().int().min(0).max(300_000).optional(),
    details: z.record(z.string(), z.unknown()).optional(),
  })
  .strict();

const WINDOW_MS = 10_000;
const MAX_KEYS = 400;
const duplicateBuckets = new Map<string, { count: number; timer: NodeJS.Timeout }>();

function isSensitiveKey(key: string): boolean {
  const normalized = key.toLowerCase();
  return (
    normalized.includes("token") ||
    normalized.includes("password") ||
    normalized.includes("secret") ||
    normalized.includes("cookie") ||
    normalized.includes("authorization")
  );
}

function sanitizeDetails(value: unknown, depth = 0): unknown {
  if (value == null) return value;
  if (depth >= 4) return "[truncated]";
  if (typeof value === "string") return value.length > 400 ? `${value.slice(0, 400)}...` : value;
  if (typeof value === "number" || typeof value === "boolean") return value;
  if (Array.isArray(value)) return value.slice(0, 20).map((entry) => sanitizeDetails(entry, depth + 1));
  if (typeof value === "object") {
    const output: Record<string, unknown> = {};
    const entries = Object.entries(value as Record<string, unknown>).slice(0, 30);
    for (const [key, childValue] of entries) {
      if (isSensitiveKey(key)) {
        output[key] = "[redacted]";
        continue;
      }
      output[key] = sanitizeDetails(childValue, depth + 1);
    }
    return output;
  }
  return String(value);
}

function dedupeKey(input: z.infer<typeof telemetrySchema>, req: AuthedRequest): string {
  const userId = req.authUser?.appUserId ?? "unknown";
  const status = input.statusCode != null ? String(input.statusCode) : "-";
  return `${userId}|${input.event}|${input.action}|${input.result}|${status}`;
}

function trackDuplicate(key: string): boolean {
  const bucket = duplicateBuckets.get(key);
  if (!bucket) {
    const timer = setTimeout(() => {
      const latest = duplicateBuckets.get(key);
      if (!latest) return;
      if (latest.count > 0) {
        logger.info("client_telemetry_dedup_summary", {
          action: "client_telemetry_ingest",
          result: "deduped",
          dedupeKey: key,
          suppressedCount: latest.count,
          windowMs: WINDOW_MS,
        });
      }
      duplicateBuckets.delete(key);
    }, WINDOW_MS);
    duplicateBuckets.set(key, { count: 0, timer });
    if (duplicateBuckets.size > MAX_KEYS) {
      const firstKey = duplicateBuckets.keys().next().value;
      if (typeof firstKey === "string") {
        const first = duplicateBuckets.get(firstKey);
        if (first) clearTimeout(first.timer);
        duplicateBuckets.delete(firstKey);
      }
    }
    return false;
  }
  bucket.count += 1;
  return true;
}

clientTelemetryRouter.post("/events", async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const parsed = telemetrySchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      logAction("warn", "client_telemetry_invalid_payload", {
        req,
        action: "client_telemetry_ingest",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Invalid telemetry payload." });
      return;
    }

    const duplicate = trackDuplicate(dedupeKey(parsed.data, req));
    if (duplicate) {
      res.status(202).json({ accepted: true, deduped: true });
      return;
    }

    const sanitizedDetails = parsed.data.details ? sanitizeDetails(parsed.data.details) : undefined;
    const level =
      parsed.data.result === "failure" || (parsed.data.statusCode != null && parsed.data.statusCode >= 500)
        ? "warn"
        : "info";

    logAction(level, parsed.data.event, {
      req,
      action: parsed.data.action,
      result: parsed.data.result,
      ...(parsed.data.statusCode != null ? { statusCode: parsed.data.statusCode } : {}),
      ...(parsed.data.statusCode != null
        ? {
            requestClass:
              parsed.data.statusCode >= 500 ? "server_error" : parsed.data.statusCode >= 400 ? "client_error" : "success",
          }
        : {}),
      startedAtNs,
      details: {
        source: "frontend",
        ...(parsed.data.durationMs != null ? { durationMs: parsed.data.durationMs } : {}),
        ...(sanitizedDetails ? { details: sanitizedDetails } : {}),
      },
    });

    res.status(202).json({ accepted: true, deduped: false });
  } catch (error) {
    logAction("error", "client_telemetry_ingest_failed", {
      req,
      action: "client_telemetry_ingest",
      result: "failure",
      statusCode: 500,
      requestClass: "server_error",
      startedAtNs,
      error,
    });
    next(error);
  }
});

export { clientTelemetryRouter };
