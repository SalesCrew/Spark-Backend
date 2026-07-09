import { and, asc, eq, inArray, ne, sql } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { requireKundeAdminPermission } from "../lib/kunde-access.js";
import { logAction, startActionTimer } from "../lib/logger.js";
import { requireAuth } from "../middleware/auth.js";
import { db } from "../lib/db.js";
import { lager, lagerGmAssignments, users } from "../lib/schema.js";

const createLagerSchema = z
  .object({
    address: z.string().min(1),
    postalCode: z.string().min(1),
    city: z.string().min(1),
    gmUserIds: z.array(z.string().uuid()).optional(),
    gmUserId: z.string().uuid().nullable().optional(),
  })
  .strict();
const updateLagerSchema = createLagerSchema;

const adminLagerRouter = Router();

adminLagerRouter.use(requireAuth(["admin", "kunde"]));
adminLagerRouter.use(requireKundeAdminPermission);
adminLagerRouter.use("/lager", (req, res, next) => {
  if (req.method.toUpperCase() === "GET") {
    next();
    return;
  }
  const startedAtNs = startActionTimer();
  res.on("finish", () => {
    const level = res.statusCode >= 500 ? "error" : res.statusCode >= 400 ? "warn" : "info";
    logAction(level, "admin_lager_action_completed", {
      req,
      action: "admin_lager_mutation",
      result: res.statusCode >= 400 ? "failure" : "success",
      statusCode: res.statusCode,
      requestClass: res.statusCode >= 500 ? "server_error" : res.statusCode >= 400 ? "client_error" : "success",
      startedAtNs,
      details: { route: req.path, method: req.method },
    });
  });
  next();
});

adminLagerRouter.get("/lager", async (_req, res, next) => {
  try {
    const rows = await db
      .select({
        id: lager.id,
        address: lager.address,
        postalCode: lager.postalCode,
        city: lager.city,
        gmUserId: lager.gmUserId,
        createdAt: lager.createdAt,
        updatedAt: lager.updatedAt,
        gmFirstName: users.firstName,
        gmLastName: users.lastName,
      })
      .from(lager)
      .leftJoin(users, eq(users.id, lager.gmUserId))
      .where(eq(lager.isDeleted, false))
      .orderBy(asc(lager.city), asc(lager.address));

    const lagerIds = rows.map((row) => row.id);
    const assignmentRows =
      lagerIds.length === 0
        ? []
        : await db
            .select({
              lagerId: lagerGmAssignments.lagerId,
              gmUserId: lagerGmAssignments.gmUserId,
              gmFirstName: users.firstName,
              gmLastName: users.lastName,
            })
            .from(lagerGmAssignments)
            .innerJoin(users, eq(users.id, lagerGmAssignments.gmUserId))
            .where(
              and(
                inArray(lagerGmAssignments.lagerId, lagerIds),
                eq(lagerGmAssignments.isDeleted, false),
                eq(users.role, "gm"),
                eq(users.isActive, true),
              ),
            )
            .orderBy(asc(lagerGmAssignments.lagerId), asc(users.firstName), asc(users.lastName));

    const gmUserIdsByLager = new Map<string, string[]>();
    const gmNamesByLager = new Map<string, string[]>();
    for (const assignment of assignmentRows) {
      const gmName =
        assignment.gmFirstName && assignment.gmLastName
          ? `${assignment.gmFirstName} ${assignment.gmLastName}`
          : null;
      if (!gmName) continue;
      const userIds = gmUserIdsByLager.get(assignment.lagerId) ?? [];
      userIds.push(assignment.gmUserId);
      gmUserIdsByLager.set(assignment.lagerId, userIds);
      const names = gmNamesByLager.get(assignment.lagerId) ?? [];
      names.push(gmName);
      gmNamesByLager.set(assignment.lagerId, names);
    }

    res.status(200).json({
      lagers: rows.map((row) => ({
        ...(() => {
          const gmUserIds = [...(gmUserIdsByLager.get(row.id) ?? [])];
          const gmNames = [...(gmNamesByLager.get(row.id) ?? [])];
          if (gmUserIds.length === 0 && row.gmUserId && row.gmFirstName && row.gmLastName) {
            gmUserIds.push(row.gmUserId);
            gmNames.push(`${row.gmFirstName} ${row.gmLastName}`);
          }
          return {
            gmUserIds,
            gmNames,
            gmUserId: gmUserIds[0] ?? null,
            gmName: gmNames[0] ?? null,
          };
        })(),
        id: row.id,
        address: row.address,
        postalCode: row.postalCode,
        city: row.city,
        createdAt: row.createdAt.toISOString(),
        updatedAt: row.updatedAt.toISOString(),
      })),
    });
  } catch (err) {
    next(err);
  }
});

adminLagerRouter.post("/lager", async (req, res, next) => {
  try {
    const parsed = createLagerSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Invalid lager create payload." });
      return;
    }

    const payload = parsed.data;
    const normalizedAddress = payload.address.trim();
    const normalizedPostalCode = payload.postalCode.trim();
    const normalizedCity = payload.city.trim();
    const normalizedGmUserIds = Array.from(
      new Set(
        (Array.isArray(payload.gmUserIds) && payload.gmUserIds.length > 0
          ? payload.gmUserIds
          : payload.gmUserId
            ? [payload.gmUserId]
            : []
        )
          .map((entry) => entry.trim())
          .filter((entry) => entry.length > 0),
      ),
    );
    let selectedGms: Array<{ id: string; firstName: string; lastName: string }> = [];
    if (normalizedGmUserIds.length > 0) {
      selectedGms = await db
        .select({ id: users.id, firstName: users.firstName, lastName: users.lastName })
        .from(users)
        .where(
          and(
            inArray(users.id, normalizedGmUserIds),
            eq(users.role, "gm"),
            eq(users.isActive, true),
          ),
        );
      if (selectedGms.length !== normalizedGmUserIds.length) {
        res.status(400).json({ error: "Ungültige GM-Zuweisung." });
        return;
      }
    }

    const [duplicate] = await db
      .select({ id: lager.id })
      .from(lager)
      .where(
        and(
          eq(lager.isDeleted, false),
          sql`lower(${lager.address}) = lower(${normalizedAddress})`,
          sql`lower(${lager.postalCode}) = lower(${normalizedPostalCode})`,
          sql`lower(${lager.city}) = lower(${normalizedCity})`,
        ),
      )
      .limit(1);
    if (duplicate) {
      res.status(409).json({ error: "Dieses Lager existiert bereits." });
      return;
    }

    const created = await db.transaction(async (tx) => {
      const [inserted] = await tx
        .insert(lager)
        .values({
          address: normalizedAddress,
          postalCode: normalizedPostalCode,
          city: normalizedCity,
          gmUserId: normalizedGmUserIds[0] ?? null,
          isDeleted: false,
        })
        .returning();
      if (!inserted) return null;
      if (normalizedGmUserIds.length > 0) {
        await tx.insert(lagerGmAssignments).values(
          normalizedGmUserIds.map((gmUserId) => ({
            lagerId: inserted.id,
            gmUserId,
            isDeleted: false,
          })),
        );
      }
      return inserted;
    });

    if (!created) {
      res.status(500).json({ error: "Failed to create lager." });
      return;
    }

    const gmNameById = new Map(selectedGms.map((gm) => [gm.id, `${gm.firstName} ${gm.lastName}`]));
    const gmNames = normalizedGmUserIds
      .map((gmUserId) => gmNameById.get(gmUserId))
      .filter((value): value is string => typeof value === "string" && value.length > 0);

    res.status(201).json({
      lager: {
        id: created.id,
        address: created.address,
        postalCode: created.postalCode,
        city: created.city,
        gmUserIds: normalizedGmUserIds,
        gmNames,
        gmUserId: normalizedGmUserIds[0] ?? null,
        gmName: gmNames[0] ?? null,
        createdAt: created.createdAt.toISOString(),
        updatedAt: created.updatedAt.toISOString(),
      },
    });
  } catch (err) {
    next(err);
  }
});

adminLagerRouter.patch("/lager/:id", async (req, res, next) => {
  try {
    const idParse = z.string().uuid().safeParse(req.params.id);
    if (!idParse.success) {
      res.status(400).json({ error: "Ungültige Lager-ID." });
      return;
    }

    const parsed = updateLagerSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Invalid lager update payload." });
      return;
    }

    const lagerId = idParse.data;
    const payload = parsed.data;
    const normalizedAddress = payload.address.trim();
    const normalizedPostalCode = payload.postalCode.trim();
    const normalizedCity = payload.city.trim();
    const normalizedGmUserIds = Array.from(
      new Set(
        (Array.isArray(payload.gmUserIds) && payload.gmUserIds.length > 0
          ? payload.gmUserIds
          : payload.gmUserId
            ? [payload.gmUserId]
            : []
        )
          .map((entry) => entry.trim())
          .filter((entry) => entry.length > 0),
      ),
    );

    const [existing] = await db
      .select({ id: lager.id })
      .from(lager)
      .where(and(eq(lager.id, lagerId), eq(lager.isDeleted, false)))
      .limit(1);
    if (!existing) {
      res.status(404).json({ error: "Lager nicht gefunden." });
      return;
    }

    let selectedGms: Array<{ id: string; firstName: string; lastName: string }> = [];
    if (normalizedGmUserIds.length > 0) {
      selectedGms = await db
        .select({ id: users.id, firstName: users.firstName, lastName: users.lastName })
        .from(users)
        .where(
          and(
            inArray(users.id, normalizedGmUserIds),
            eq(users.role, "gm"),
            eq(users.isActive, true),
          ),
        );
      if (selectedGms.length !== normalizedGmUserIds.length) {
        res.status(400).json({ error: "Ungültige GM-Zuweisung." });
        return;
      }
    }

    const [duplicate] = await db
      .select({ id: lager.id })
      .from(lager)
      .where(
        and(
          eq(lager.isDeleted, false),
          ne(lager.id, lagerId),
          sql`lower(${lager.address}) = lower(${normalizedAddress})`,
          sql`lower(${lager.postalCode}) = lower(${normalizedPostalCode})`,
          sql`lower(${lager.city}) = lower(${normalizedCity})`,
        ),
      )
      .limit(1);
    if (duplicate) {
      res.status(409).json({ error: "Dieses Lager existiert bereits." });
      return;
    }

    const now = new Date();
    const updated = await db.transaction(async (tx) => {
      const [row] = await tx
        .update(lager)
        .set({
          address: normalizedAddress,
          postalCode: normalizedPostalCode,
          city: normalizedCity,
          gmUserId: normalizedGmUserIds[0] ?? null,
          updatedAt: now,
        })
        .where(and(eq(lager.id, lagerId), eq(lager.isDeleted, false)))
        .returning();
      if (!row) return null;

      await tx
        .update(lagerGmAssignments)
        .set({
          isDeleted: true,
          deletedAt: now,
          updatedAt: now,
        })
        .where(and(eq(lagerGmAssignments.lagerId, lagerId), eq(lagerGmAssignments.isDeleted, false)));

      if (normalizedGmUserIds.length > 0) {
        await tx.insert(lagerGmAssignments).values(
          normalizedGmUserIds.map((gmUserId) => ({
            lagerId: row.id,
            gmUserId,
            isDeleted: false,
          })),
        );
      }

      return row;
    });

    if (!updated) {
      res.status(404).json({ error: "Lager nicht gefunden." });
      return;
    }

    const gmNameById = new Map(selectedGms.map((gm) => [gm.id, `${gm.firstName} ${gm.lastName}`]));
    const gmNames = normalizedGmUserIds
      .map((gmUserId) => gmNameById.get(gmUserId))
      .filter((value): value is string => typeof value === "string" && value.length > 0);

    res.status(200).json({
      lager: {
        id: updated.id,
        address: updated.address,
        postalCode: updated.postalCode,
        city: updated.city,
        gmUserIds: normalizedGmUserIds,
        gmNames,
        gmUserId: normalizedGmUserIds[0] ?? null,
        gmName: gmNames[0] ?? null,
        createdAt: updated.createdAt.toISOString(),
        updatedAt: updated.updatedAt.toISOString(),
      },
    });
  } catch (err) {
    next(err);
  }
});

export { adminLagerRouter };
