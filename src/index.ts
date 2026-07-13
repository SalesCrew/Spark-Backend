import { env } from "./config/env.js";
import { createApp } from "./app.js";
import { startIppFinalizerScheduler } from "./lib/ipp-finalizer.js";
import { logger, serializeError } from "./lib/logger.js";
import { startKurtiMemoryCleanupScheduler } from "./lib/kurti-memory.js";
import { startRedMonthCalendarScheduler } from "./lib/red-month-calendar.js";

const app = createApp();
const server = app.listen(env.PORT, () => {
  logger.info("backend_started", {
    action: "backend_bootstrap",
    result: "success",
    port: env.PORT,
    nodeEnv: env.NODE_ENV,
  });
  startRedMonthCalendarScheduler();
  startIppFinalizerScheduler();
  startKurtiMemoryCleanupScheduler();
});

server.on("error", (error) => {
  logger.error("backend_listen_failed", {
    action: "backend_listen",
    result: "failure",
    port: env.PORT,
    error: serializeError(error),
  });
});

process.on("unhandledRejection", (reason) => {
  logger.error("process_unhandled_rejection", {
    action: "process_runtime",
    result: "failure",
    error: serializeError(reason),
  });
});

process.on("uncaughtException", (error) => {
  logger.error("process_uncaught_exception", {
    action: "process_runtime",
    result: "failure",
    error: serializeError(error),
  });
});
