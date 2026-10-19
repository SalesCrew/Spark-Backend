import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { readFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import express, { type RequestHandler } from "express";
import cors from "cors";
import { PDFDocument, StandardFonts, rgb } from "pdf-lib";
import * as schema from "../src/lib/schema.js";
import { createBriefingService } from "../src/lib/gm-briefing-service.js";
import { createGmBriefingRouters } from "../src/routes/gm-briefings.js";
import { isolatedModule } from "./isolated-module.js";
import * as roles from "../src/lib/admin-role.js";
export async function syntheticBriefingPdf(pageCount = 3) {
  const pdf = await PDFDocument.create(),
    font = await pdf.embedFont(StandardFonts.HelveticaBold),
    regular = await pdf.embedFont(StandardFonts.Helvetica);
  for (let i = 0; i < pageCount; i++) {
    const page = pdf.addPage([595, 842]);
    page.drawRectangle({
      x: 0,
      y: 725,
      width: 595,
      height: 117,
      color: rgb(0.82, 0.07, 0.09),
    });
    page.drawText("WOCHENSCHWERPUNKTE", {
      x: 35,
      y: 790,
      size: 12,
      font,
      color: rgb(1, 1, 1),
    });
    page.drawText(
      [
        "Sichtbarkeit am POS",
        "Aktionsflaechen im Blick",
        "Gemeinsam zum Abschluss",
      ][i % 3]!,
      { x: 35, y: 750, size: 24, font, color: rgb(1, 1, 1) },
    );
    page.drawText(`Woche 43 / Seite ${i + 1}`, {
      x: 35,
      y: 685,
      size: 11,
      font: regular,
      color: rgb(0.45, 0.45, 0.45),
    });
    page.drawText("Ein klarer Fokus fuer die naechsten Marktbesuche.", {
      x: 35,
      y: 640,
      size: 15,
      font,
    });
    for (let n = 0; n < 3; n++) {
      page.drawRectangle({
        x: 35,
        y: 485 - n * 125,
        width: 525,
        height: 92,
        color: rgb(0.96, 0.96, 0.96),
      });
      page.drawText(
        `${n + 1}  ${["Platzierung pruefen", "Warenverfuegbarkeit sichern", "Ergebnis dokumentieren"][n]}`,
        { x: 55, y: 540 - n * 125, size: 17, font },
      );
      page.drawText("Synthetisches Beispiel - keine Kundendaten.", {
        x: 55,
        y: 515 - n * 125,
        size: 11,
        font: regular,
      });
    }
    page.drawText("Coke Spark / Vorschau", {
      x: 35,
      y: 40,
      size: 10,
      font: regular,
    });
  }
  return Buffer.from(await pdf.save());
}
export async function createBriefingFixture() {
  if (
    process.env.DATABASE_URL ||
    process.env.SUPABASE_URL ||
    process.env.SUPABASE_SERVICE_ROLE_KEY
  )
    throw new Error("Production configuration forbidden");
  const pg = new PGlite(),
    database = drizzle(pg, { schema });
  await pg.exec(
    `create role anon; create role authenticated; create schema storage; create table storage.buckets(id text primary key,name text,public boolean,file_size_limit bigint,allowed_mime_types text[]); create table storage.objects(id uuid,bucket_id text); alter table storage.objects enable row level security; create table users(id uuid primary key, first_name text,last_name text,role text); create table kunde_users(id uuid primary key,user_id uuid,page_permissions jsonb,is_deleted boolean default false);`,
  );
  await pg.exec(
    await readFile(
      new URL(
        "../supabase/migrations/20261019153844_gm_wochenschwerpunkte.sql",
        import.meta.url,
      ),
      "utf8",
    ),
  );
  const admin = randomUUID(),
    gm = randomUUID(),
    gm2 = randomUUID(),
    kunde = randomUUID(),
    sm = randomUUID();
  await pg.query(
    `insert into users(id,role) values ($1,'admin'),($2,'gm'),($3,'gm'),($4,'kunde'),($5,'sm')`,
    [admin, gm, gm2, kunde, sm],
  );
  await pg.query(
    `insert into kunde_users(id,user_id,page_permissions) values ($1,$2,'{"wochenschwerpunkte":["read"]}')`,
    [randomUUID(), kunde],
  );
  let current = new Date("2026-10-19T10:00:00Z"),
    failUpload = false;
  const objects = new Map<string, Buffer>();
  const storage = {
    async upload(path: string, bytes: Buffer) {
      if (failUpload) throw new Error("Synthetic upload failure");
      objects.set(path, Buffer.from(bytes));
    },
    async remove(path: string) {
      objects.delete(path);
    },
    async sign(path: string) {
      if (!objects.has(path)) throw new Error("Missing synthetic file");
      return `http://127.0.0.1:4037/synthetic-pdf/${encodeURIComponent(path)}`;
    },
  };
  const service = createBriefingService(
    database as unknown as Parameters<typeof createBriefingService>[0],
    storage,
    () => current,
  );
  const permissionModule = await isolatedModule<{
    requireKundeAdminPermission: RequestHandler;
  }>(new URL("../src/lib/kunde-access.ts", import.meta.url), {
    "./db.js": { db: database },
    "./schema.js": schema,
    "./admin-role.js": roles,
  });
  const auth =
    (allowed: string[]): RequestHandler =>
    (req, res, next) => {
      const token = req
          .header("authorization")
          ?.replace("Bearer synthetic-", ""),
        role = token === "gm2" ? "gm" : token;
      if (
        !role ||
        !roles.isRoleAllowedForEndpoint(
          role as schema.UserRole,
          allowed as schema.UserRole[],
        )
      ) {
        res.sendStatus(403);
        return;
      }
      Object.assign(req, {
        authUser: {
          appUserId:
            token === "gm"
              ? gm
              : token === "gm2"
                ? gm2
                : role === "kunde"
                  ? kunde
                  : role === "sm"
                    ? sm
                    : admin,
          role,
        },
      });
      next();
    };
  const routers = createGmBriefingRouters(
    service,
    auth,
    permissionModule.requireKundeAdminPermission,
  );
  const app = express();
  app.use(cors({ origin: ["http://127.0.0.1:3037", "http://localhost:3037"] }));
  app.use(express.json());
  app.use("/admin/gm-briefings", routers.admin);
  app.use("/gm/briefings", routers.gm);
  app.get("/synthetic-pdf/:path", (req, res) => {
    const bytes = objects.get(req.params.path);
    if (!bytes) {
      res.sendStatus(404);
      return;
    }
    res.type("application/pdf").set("Cache-Control", "no-store").send(bytes);
  });
  app.use((error: any, _req: unknown, res: express.Response, _next: unknown) =>
    res.status(error.status ?? 500).json({ error: error.message }),
  );
  return {
    app,
    pg,
    service,
    objects,
    admin,
    gm,
    gm2,
    kunde,
    setNow(value: string) {
      current = new Date(value);
    },
    failUploads(value: boolean) {
      failUpload = value;
    },
    close: () => pg.close(),
  };
}
