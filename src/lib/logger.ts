import type { Request } from "express";

type LogLevel = "debug" | "info" | "warn" | "error";

type LogPayload = Record<string, unknown>;
type HighVolumeMetric = "markets" | "campaigns" | "modules" | "fragebogen" | "users";

const LOG_LEVEL_PRIORITY: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

const REDACTED_KEYS = new Set([
  "authorization",
  "cookie",
  "set-cookie",
  "password",
  "pass",
  "token",
  "refresh_token",
  "access_token",
  "refreshtoken",
  "accesstoken",
  "jwt_secret",
  "supabase_service_role_key",
  "apikey",
  "api_key",
  "secret",
]);

const MAX_REDACTION_DEPTH = 6;
const LOGGED_ERROR_MARKER = "__backendLogged";
const AGGREGATION_WINDOW_MS = 15_000;

const ANSI = {
  reset: "\x1b[0m",
  bold: "\x1b[1m",
  dim: "\x1b[90m",
  info: "\x1b[36m",
  warn: "\x1b[33m",
  error: "\x1b[31m",
  debug: "\x1b[90m",
};

const LEVEL_COLOR: Record<LogLevel, string> = {
  debug: ANSI.debug,
  info: ANSI.info,
  warn: ANSI.warn,
  error: ANSI.error,
};

const runtimeLoggerConfig: {
  level: LogLevel;
  includeStack: boolean;
} = {
  level: "info",
  includeStack: process.env.NODE_ENV !== "production",
};

type RequestAuthSnapshot = {
  appUserId?: string;
  role?: string;
  supabaseAuthId?: string;
};

type RequestWithLoggingContext = Request & {
  requestId?: string;
  requestStartedAtNs?: bigint;
  authUser?: RequestAuthSnapshot;
};

type ActionLogInput = {
  req?: Request | null;
  action: string;
  result: string;
  startedAtNs?: bigint;
  statusCode?: number;
  requestClass?: string;
  details?: LogPayload;
  error?: unknown;
};

type HighVolumeAggregateInput = {
  metric: HighVolumeMetric;
  loaded: number;
  softDeleted?: number;
  scope?: string;
  requestId?: string | null;
  appUserId?: string | null;
  role?: string | null;
};

type HighVolumeAggregateBucket = {
  metric: HighVolumeMetric;
  scope: string;
  requests: number;
  loadedTotal: number;
  softDeletedLatest: number;
  lastRequestId: string | null;
  lastAppUserId: string | null;
  lastRole: string | null;
  timer: NodeJS.Timeout | null;
};

function parseLogLevel(raw: string | undefined): LogLevel {
  if (!raw) return "info";
  const normalized = raw.trim().toLowerCase();
  if (normalized === "debug" || normalized === "info" || normalized === "warn" || normalized === "error") {
    return normalized;
  }
  return "info";
}

function shouldIncludeStack(): boolean {
  return runtimeLoggerConfig.includeStack;
}

function isLogLevelEnabled(level: LogLevel): boolean {
  return LOG_LEVEL_PRIORITY[level] >= LOG_LEVEL_PRIORITY[runtimeLoggerConfig.level];
}

function sanitizeRequestPath(rawPath: string): string {
  if (!rawPath) return "/";
  try {
    const parsed = new URL(rawPath, "http://local.backend");
    return parsed.pathname || "/";
  } catch {
    const [pathname] = rawPath.split("?");
    return pathname || "/";
  }
}

function shouldSuppressRequestCompletionLog(path: string, method: string, statusCode: number): boolean {
  if (statusCode >= 400) return false;
  if (method.toUpperCase() !== "GET") return false;
  const normalizedPath = sanitizeRequestPath(path);
  if (normalizedPath === "/markets") return true;
  if (normalizedPath === "/admin/campaigns") return true;
  if (normalizedPath === "/admin/users") return true;
  if (/^\/admin\/modules\/(main|kuehler|mhd)$/i.test(normalizedPath)) return true;
  if (/^\/admin\/fragebogen\/(main|kuehler|mhd)$/i.test(normalizedPath)) return true;
  if (normalizedPath === "/markets/gm/assigned-active") return true;
  if (normalizedPath === "/markets/gm/kuehler-mhd-progress") return true;
  if (normalizedPath === "/markets/gm/visit-start") return true;
  if (normalizedPath === "/time-tracking/entries/draft/active") return true;
  return false;
}

function redactValue(value: unknown, keyHint?: string, depth = 0): unknown {
  if (keyHint && REDACTED_KEYS.has(keyHint.toLowerCase())) {
    return "[REDACTED]";
  }
  if (value == null) return value;
  if (depth >= MAX_REDACTION_DEPTH) return "[TRUNCATED]";
  if (Array.isArray(value)) {
    return value.map((entry) => redactValue(entry, undefined, depth + 1));
  }
  if (typeof value === "object") {
    const output: Record<string, unknown> = {};
    for (const [childKey, childValue] of Object.entries(value as Record<string, unknown>)) {
      output[childKey] = redactValue(childValue, childKey, depth + 1);
    }
    return output;
  }
  if (typeof value === "string" && value.length > 2_000) {
    return `${value.slice(0, 2_000)}…`;
  }
  return value;
}

function sanitizeLogPayload(payload: LogPayload = {}): LogPayload {
  return redactValue(payload) as LogPayload;
}

function formatInlineValue(value: unknown): string {
  if (value == null) return "-";
  if (typeof value === "string") {
    return value.length > 260 ? `${value.slice(0, 260)}...` : value;
  }
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  try {
    const serialized = JSON.stringify(value);
    if (!serialized) return String(value);
    return serialized.length > 260 ? `${serialized.slice(0, 260)}...` : serialized;
  } catch {
    return String(value);
  }
}

function formatLogBlock(level: LogLevel, event: string, payload: LogPayload, ts: string): string {
  const color = LEVEL_COLOR[level];
  const lines: string[] = [];
  const remaining: Record<string, unknown> = { ...payload };
  const pull = (key: string): unknown => {
    const value = remaining[key];
    delete remaining[key];
    return value;
  };

  const method = pull("method");
  const path = pull("path");
  const requestId = pull("requestId");
  const appUserId = pull("appUserId");
  const role = pull("role");
  const supabaseAuthId = pull("supabaseAuthId");
  const ip = pull("ip");
  const userAgent = pull("userAgent");
  const statusCode = pull("statusCode");
  const durationMs = pull("durationMs");
  const requestClass = pull("requestClass");
  const error = pull("error");

  lines.push(`${color}${ANSI.bold}[${level.toUpperCase()}] ${event}${ANSI.reset} ${ANSI.dim}${ts}${ANSI.reset}`);

  if (method || path) {
    lines.push(`REQUEST: ${formatInlineValue(method)} ${formatInlineValue(path)}`.trim());
  }
  if (requestId != null) {
    lines.push(`UID: ${formatInlineValue(requestId)}`);
  }
  lines.push(`USER: id=${formatInlineValue(appUserId)} role=${formatInlineValue(role)} authId=${formatInlineValue(supabaseAuthId)}`);
  lines.push(`SOURCE: ip=${formatInlineValue(ip)} ua=${formatInlineValue(userAgent)}`);

  const actionParts: string[] = [];
  if (remaining.action != null) actionParts.push(`action=${formatInlineValue(pull("action"))}`);
  if (remaining.metric != null) actionParts.push(`metric=${formatInlineValue(pull("metric"))}`);
  if (remaining.scope != null) actionParts.push(`scope=${formatInlineValue(pull("scope"))}`);
  lines.push(`ACTION: ${actionParts.length > 0 ? actionParts.join(" ") : "-"}`);

  const resultParts: string[] = [];
  if (remaining.result != null) resultParts.push(`result=${formatInlineValue(pull("result"))}`);
  if (statusCode != null) resultParts.push(`status=${formatInlineValue(statusCode)}`);
  if (requestClass != null) resultParts.push(`class=${formatInlineValue(requestClass)}`);
  if (remaining.requests != null) resultParts.push(`requests=${formatInlineValue(pull("requests"))}`);
  if (remaining.loadedTotal != null) resultParts.push(`loadedTotal=${formatInlineValue(pull("loadedTotal"))}`);
  if (remaining.loadedAvg != null) resultParts.push(`loadedAvg=${formatInlineValue(pull("loadedAvg"))}`);
  if (remaining.softDeletedLatest != null) {
    resultParts.push(`softDeletedLatest=${formatInlineValue(pull("softDeletedLatest"))}`);
  }
  lines.push(`RESULT: ${resultParts.length > 0 ? resultParts.join(" ") : "-"}`);

  const timingParts: string[] = [];
  if (durationMs != null) timingParts.push(`durationMs=${formatInlineValue(durationMs)}`);
  if (remaining.windowMs != null) timingParts.push(`windowMs=${formatInlineValue(pull("windowMs"))}`);
  lines.push(`TIMING: ${timingParts.length > 0 ? timingParts.join(" ") : "-"}`);

  if (error != null) {
    if (error && typeof error === "object") {
      const errObject = error as Record<string, unknown>;
      const errName = errObject.name;
      const errCode = errObject.code;
      const errMessage = errObject.message;
      const errStack = errObject.stack;
      lines.push(`ERROR: name=${formatInlineValue(errName)} code=${formatInlineValue(errCode)} message=${formatInlineValue(errMessage)}`);
      if (typeof errStack === "string" && errStack.length > 0) {
        const stackLines = errStack.split("\n").slice(0, 6);
        for (const stackLine of stackLines) {
          lines.push(`  | ${stackLine}`);
        }
      }
    } else {
      lines.push(`ERROR: ${formatInlineValue(error)}`);
    }
  }

  const detailKeys = Object.keys(remaining);
  if (detailKeys.length > 0) {
    lines.push("DETAILS:");
    for (const key of detailKeys) {
      lines.push(`  - ${key}: ${formatInlineValue(remaining[key])}`);
    }
  }

  lines.push("------------------------------------------------------------");
  return lines.join("\n");
}

function writeLog(level: LogLevel, event: string, payload: LogPayload = {}): void {
  if (!isLogLevelEnabled(level)) return;
  const ts = new Date().toISOString();
  const sanitizedPayload = sanitizeLogPayload(payload);
  const rendered = formatLogBlock(level, event, sanitizedPayload, ts);
  if (level === "error") {
    console.error(rendered);
    return;
  }
  if (level === "warn") {
    console.warn(rendered);
    return;
  }
  console.log(rendered);
}

function startActionTimer(): bigint {
  return process.hrtime.bigint();
}

function getDurationMs(startedAtNs?: bigint): number | undefined {
  if (!startedAtNs) return undefined;
  return Number((process.hrtime.bigint() - startedAtNs) / BigInt(1_000_000));
}

function logAction(level: LogLevel, event: string, input: ActionLogInput): void {
  const payload: LogPayload = {
    ...(input.req ? getRequestLogMeta(input.req) : {}),
    action: input.action,
    result: input.result,
    ...(input.statusCode != null ? { statusCode: input.statusCode } : {}),
    ...(input.requestClass ? { requestClass: input.requestClass } : {}),
    ...(input.startedAtNs ? { durationMs: getDurationMs(input.startedAtNs) } : {}),
    ...(input.error ? { error: serializeError(input.error) } : {}),
    ...(input.details ?? {}),
  };
  writeLog(level, event, payload);
}

function serializeError(error: unknown): LogPayload {
  if (error instanceof Error) {
    const payload: LogPayload = {
      name: error.name,
      message: error.message,
    };
    const withCode = error as Error & { code?: string | number };
    if (withCode.code != null) payload.code = String(withCode.code);
    if (shouldIncludeStack() && error.stack) payload.stack = error.stack;
    return payload;
  }
  return {
    message: typeof error === "string" ? error : String(error),
  };
}

function configureLogger(input: { level?: LogLevel; includeStack?: boolean }): void {
  if (input.level) {
    runtimeLoggerConfig.level = parseLogLevel(input.level);
  }
  if (typeof input.includeStack === "boolean") {
    runtimeLoggerConfig.includeStack = input.includeStack;
  }
}

const highVolumeBuckets = new Map<string, HighVolumeAggregateBucket>();

function flushHighVolumeAggregate(key: string): void {
  const bucket = highVolumeBuckets.get(key);
  if (!bucket) return;
  highVolumeBuckets.delete(key);
  const loadedAvg = bucket.requests > 0 ? Number((bucket.loadedTotal / bucket.requests).toFixed(2)) : 0;
  logger.info("high_volume_load_summary", {
    metric: bucket.metric,
    scope: bucket.scope,
    requests: bucket.requests,
    loadedTotal: bucket.loadedTotal,
    loadedAvg,
    softDeletedLatest: bucket.softDeletedLatest,
    requestId: bucket.lastRequestId,
    appUserId: bucket.lastAppUserId,
    role: bucket.lastRole,
    windowMs: AGGREGATION_WINDOW_MS,
  });
}

function aggregateHighVolumeLoad(input: HighVolumeAggregateInput): void {
  const scope = input.scope?.trim().length ? input.scope.trim() : "all";
  const key = `${input.metric}:${scope}`;
  let bucket = highVolumeBuckets.get(key);
  if (!bucket) {
    bucket = {
      metric: input.metric,
      scope,
      requests: 0,
      loadedTotal: 0,
      softDeletedLatest: 0,
      lastRequestId: null,
      lastAppUserId: null,
      lastRole: null,
      timer: null,
    };
    highVolumeBuckets.set(key, bucket);
    bucket.timer = setTimeout(() => flushHighVolumeAggregate(key), AGGREGATION_WINDOW_MS);
  }
  bucket.requests += 1;
  bucket.loadedTotal += Number.isFinite(input.loaded) ? input.loaded : 0;
  if (Number.isFinite(input.softDeleted ?? 0)) {
    bucket.softDeletedLatest = input.softDeleted ?? 0;
  }
  bucket.lastRequestId = input.requestId ?? null;
  bucket.lastAppUserId = input.appUserId ?? null;
  bucket.lastRole = input.role ?? null;
}

function markErrorAsLogged(error: unknown): void {
  if (!error || (typeof error !== "object" && typeof error !== "function")) return;
  try {
    Object.defineProperty(error, LOGGED_ERROR_MARKER, {
      value: true,
      configurable: true,
      enumerable: false,
      writable: true,
    });
  } catch {
    // noop
  }
}

function isErrorAlreadyLogged(error: unknown): boolean {
  if (!error || (typeof error !== "object" && typeof error !== "function")) return false;
  const withMarker = error as { [LOGGED_ERROR_MARKER]?: unknown };
  return withMarker[LOGGED_ERROR_MARKER] === true;
}

function getRequestLogMeta(req: Request): LogPayload {
  const typedReq = req as RequestWithLoggingContext;
  const authUser = typedReq.authUser;
  return {
    requestId: typedReq.requestId ?? null,
    method: req.method,
    path: sanitizeRequestPath(req.originalUrl || req.url),
    ip: req.ip ?? null,
    userAgent: req.get("user-agent") ?? null,
    appUserId: authUser?.appUserId ?? null,
    role: authUser?.role ?? null,
    supabaseAuthId: authUser?.supabaseAuthId ?? null,
  };
}

const logger = {
  debug: (event: string, payload?: LogPayload) => writeLog("debug", event, payload),
  info: (event: string, payload?: LogPayload) => writeLog("info", event, payload),
  warn: (event: string, payload?: LogPayload) => writeLog("warn", event, payload),
  error: (event: string, payload?: LogPayload) => writeLog("error", event, payload),
};

export {
  aggregateHighVolumeLoad,
  configureLogger,
  getRequestLogMeta,
  getDurationMs,
  isErrorAlreadyLogged,
  logger,
  logAction,
  markErrorAsLogged,
  serializeError,
  startActionTimer,
  shouldSuppressRequestCompletionLog,
};
export type { RequestWithLoggingContext };
