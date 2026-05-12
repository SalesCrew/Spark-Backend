import cors from "cors";
import express from "express";
import { env } from "./config/env.js";
import { adminUsersRouter } from "./routes/admin-users.js";
import { adminCampaignsRouter } from "./routes/campaigns.js";
import { adminZeiterfassungRouter } from "./routes/admin-zeiterfassung.js";
import { adminLagerRouter } from "./routes/admin-lager.js";
import { authRouter } from "./routes/auth.js";
import { daySessionRouter } from "./routes/day-session.js";
import { adminFragebogenRouter } from "./routes/fragebogen.js";
import { gmVisitSessionsRouter } from "./routes/gm-visit-sessions.js";
import { adminIppRouter } from "./routes/ipp.js";
import { adminMarketsRouter, marketsRouter } from "./routes/markets.js";
import { adminPraemienRouter } from "./routes/praemien.js";
import { adminRedMonthRouter, redMonthRouter } from "./routes/red-month.js";
import { timeTrackingRouter } from "./routes/time-tracking.js";

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
  app.use(express.json({ limit: "1000mb" }));

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

  app.use((err: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
    console.error(err);
    res.status(500).json({ error: "Ein interner Fehler ist aufgetreten. Bitte erneut versuchen." });
  });

  return app;
}

export { createApp };
