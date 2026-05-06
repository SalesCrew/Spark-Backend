import { and, asc, eq, sql } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { requireAuth } from "../middleware/auth.js";
import { db } from "../lib/db.js";
import { lager, users } from "../lib/schema.js";

const createLagerSchema = z
  .object({
    address: z.string().min(1),
    postalCode: z.string().min(1),
    city: z.string().min(1),
    gmUserId: z.string().uuid().nullable().optional(),
  })
  .strict();

const adminLagerRouter = Router();

adminLagerRouter.use(requireAuth(["admin"]));

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

    res.status(200).json({
      lagers: rows.map((row) => ({
        id: row.id,
        address: row.address,
        postalCode: row.postalCode,
        city: row.city,
        gmUserId: row.gmUserId,
        gmName: row.gmFirstName && row.gmLastName ? `${row.gmFirstName} ${row.gmLastName}` : null,
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
    const gmUserId = payload.gmUserId ?? null;
    if (gmUserId) {
      const [gm] = await db
        .select({ id: users.id })
        .from(users)
        .where(and(eq(users.id, gmUserId), eq(users.role, "gm"), eq(users.isActive, true)))
        .limit(1);
      if (!gm) {
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

    const [created] = await db
      .insert(lager)
      .values({
        address: normalizedAddress,
        postalCode: normalizedPostalCode,
        city: normalizedCity,
        gmUserId,
        isDeleted: false,
      })
      .returning();

    if (!created) {
      res.status(500).json({ error: "Failed to create lager." });
      return;
    }

    let gmName: string | null = null;
    if (created.gmUserId) {
      const [gm] = await db
        .select({ firstName: users.firstName, lastName: users.lastName })
        .from(users)
        .where(eq(users.id, created.gmUserId))
        .limit(1);
      gmName = gm?.firstName && gm?.lastName ? `${gm.firstName} ${gm.lastName}` : null;
    }

    res.status(201).json({
      lager: {
        id: created.id,
        address: created.address,
        postalCode: created.postalCode,
        city: created.city,
        gmUserId: created.gmUserId,
        gmName,
        createdAt: created.createdAt.toISOString(),
        updatedAt: created.updatedAt.toISOString(),
      },
    });
  } catch (err) {
    next(err);
  }
});

export { adminLagerRouter };
