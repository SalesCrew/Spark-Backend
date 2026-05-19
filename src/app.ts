import cors from "cors";
import express from "express";
import { randomUUID } from "node:crypto";
import { env } from "./config/env.js";
import {
  getRequestLogMeta,
  isErrorAlreadyLogged,
  logger,
  markErrorAsLogged,
  shouldSuppressRequestCompletionLog,
  type RequestWithLoggingContext,
  serializeError,
} from "./lib/logger.js";
import { adminUsersRouter } from "./routes/admin-users.js";
import { adminCampaignsRouter } from "./routes/campaigns.js";
import { adminZeiterfassungRouter } from "./routes/admin-zeiterfassung.js";
import { adminLagerRouter } from "./routes/admin-lager.js";
import { authRouter } from "./routes/auth.js";
import { clientTelemetryRouter } from "./routes/client-telemetry.js";
import { daySessionRouter } from "./routes/day-session.js";
import { adminFragebogenRouter } from "./routes/fragebogen.js";
import { gmVisitSessionsRouter } from "./routes/gm-visit-sessions.js";
import { adminIppRouter } from "./routes/ipp.js";
import { adminMarketsRouter, marketsRouter } from "./routes/markets.js";
import { adminPraemienRouter } from "./routes/praemien.js";
import { adminRedMonthRouter, redMonthRouter } from "./routes/red-month.js";
import { timeTrackingRouter } from "./routes/time-tracking.js";

function resolveErrorStatusCode(error: unknown): number {
  const withStatus = error as { status?: unknown; statusCode?: unknown };
  const raw = typeof withStatus.statusCode === "number"
    ? withStatus.statusCode
    : typeof withStatus.status === "number"
      ? withStatus.status
      : 500;
  if (!Number.isFinite(raw)) return 500;
  const normalized = Math.trunc(raw);
  if (normalized < 400 || normalized > 599) return 500;
  return normalized;
}

function createApp() {
  const app = express();
  const normalizeOrigin = (value: string) => value.trim().replace(/\/+$/, "");
  const allowedOrigins = env.CORS_ORIGIN.split(",")
    .map((entry) => normalizeOrigin(entry))
    .filter((entry) => entry.length > 0);

  app.use(
    cors({
      origin: (origin, callback) => {
        if (!origin) {
          callback(null, true);
          return;
        }
        const normalizedOrigin = normalizeOrigin(origin);
        const allowed = allowedOrigins.some((entry) => entry === normalizedOrigin);
        callback(null, allowed);
      },
      credentials: true,
    }),
  );

  app.use((req, res, next) => {
    const typedReq = req as RequestWithLoggingContext;
    const incomingRequestId = req.get("x-request-id")?.trim();
    typedReq.requestId = incomingRequestId && incomingRequestId.length > 0
      ? incomingRequestId.slice(0, 128)
      : randomUUID();
    typedReq.requestStartedAtNs = process.hrtime.bigint();
    res.setHeader("x-request-id", typedReq.requestId);

    const isHealthRequest = req.method === "GET" && req.path === "/health";
    const onClose = () => {
      if (res.writableEnded) return;
      if (isHealthRequest && !env.LOG_HEALTH_REQUESTS) return;
      const startNs = typedReq.requestStartedAtNs ?? process.hrtime.bigint();
      const durationMs = Number((process.hrtime.bigint() - startNs) / BigInt(1_000_000));
      logger.warn("http_request_aborted", {
        ...getRequestLogMeta(req),
        action: "http_request",
        result: "aborted",
        statusCode: res.statusCode,
        requestClass: "aborted",
        durationMs,
      });
    };
    const onFinish = () => {
      res.removeListener("close", onClose);
      if (isHealthRequest && !env.LOG_HEALTH_REQUESTS) return;
      const startNs = typedReq.requestStartedAtNs ?? process.hrtime.bigint();
      const durationMs = Number((process.hrtime.bigint() - startNs) / BigInt(1_000_000));
      const payload = {
        ...getRequestLogMeta(req),
        action: "http_request",
        result: res.statusCode >= 400 ? "failure" : "success",
        statusCode: res.statusCode,
        durationMs,
        requestClass: res.statusCode >= 500 ? "server_error" : res.statusCode >= 400 ? "client_error" : "success",
      };
      if (shouldSuppressRequestCompletionLog(req.originalUrl || req.url, req.method, res.statusCode)) {
        return;
      }
      if (res.statusCode >= 500) {
        logger.error("http_request_completed", payload);
        return;
      }
      if (res.statusCode >= 400) {
        logger.warn("http_request_completed", payload);
        return;
      }
      logger.info("http_request_completed", payload);
    };

    res.on("close", onClose);
    res.on("finish", onFinish);
    next();
  });

  app.use(express.json({ limit: "1000mb" }));

  app.use((err: unknown, req: express.Request, _res: express.Response, next: express.NextFunction) => {
    const parseError = err as { type?: unknown };
    if (parseError?.type === "entity.parse.failed") {
      logger.warn("http_request_invalid_json", {
        ...getRequestLogMeta(req),
        action: "http_request",
        result: "invalid_json",
        statusCode: 400,
        requestClass: "client_error",
        error: serializeError(err),
      });
      markErrorAsLogged(err);
      next(err);
      return;
    }
    next(err);
  });

  app.get("/health", (_req, res) => {
    res.status(200).json({
      status: "ok",
      service: "coke-spark-backend",
      timestamp: new Date().toISOString(),
    });
  });

  // Keep route registration order explicit for predictable middleware flow.
  app.use("/auth", authRouter);
  app.use("/day-session", daySessionRouter);
  app.use("/markets", marketsRouter);
  app.use("/markets", gmVisitSessionsRouter);
  app.use("/time-tracking", timeTrackingRouter);
  app.use("/telemetry", clientTelemetryRouter);
  app.use("/red-month", redMonthRouter);
  app.use("/admin/users", adminUsersRouter);
  app.use("/admin/markets", adminMarketsRouter);
  app.use("/admin", adminLagerRouter);
  app.use("/admin", adminFragebogenRouter);
  app.use("/admin", adminCampaignsRouter);
  app.use("/admin", adminIppRouter);
  app.use("/admin/praemien", adminPraemienRouter);
  app.use("/admin", adminRedMonthRouter);
  app.use("/admin/zeiterfassung", adminZeiterfassungRouter);

  app.use((err: unknown, req: express.Request, res: express.Response, _next: express.NextFunction) => {
    const statusCode = resolveErrorStatusCode(err);
    if (!isErrorAlreadyLogged(err)) {
      const payload = {
        ...getRequestLogMeta(req),
        action: "http_request",
        result: "failure",
        statusCode,
        requestClass: statusCode >= 500 ? "server_error" : "client_error",
        error: serializeError(err),
      };
      if (statusCode >= 500) {
        logger.error("http_request_failed", payload);
      } else {
        logger.warn("http_request_failed", payload);
      }
    }
    res.status(statusCode).json({ error: "Ein interner Fehler ist aufgetreten. Bitte erneut versuchen." });
  });

  return app;
}

export { createApp };
