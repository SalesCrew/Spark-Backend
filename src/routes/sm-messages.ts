import { and, asc, desc, eq, inArray, isNull, or, sql } from "drizzle-orm";
import { Router, type Response } from "express";
import { z } from "zod";
import { db } from "../lib/db.js";
import { smMessageRecipients, smMessages, users } from "../lib/schema.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";
import { smMessageVisibleUntil } from "../sm-message.shared.js";

const createMessageSchema = z.object({
  subject: z.string().trim().min(1).max(200),
  body: z.string().trim().min(1).max(12_000),
  recipientIds: z.array(z.string().uuid()).min(1).max(500),
  idempotencyKey: z.string().trim().min(8).max(200),
  visibleAfterReadDays: z.number().int().min(0).max(3650),
}).strict().superRefine((value, context) => {
  if (new Set(value.recipientIds).size !== value.recipientIds.length) {
    context.addIssue({ code: "custom", path: ["recipientIds"], message: "Empfänger dürfen nicht doppelt vorkommen." });
  }
});

type MessageApiError = Error & { statusCode: number; code: string };

function messageError(statusCode: number, code: string, message: string): MessageApiError {
  return Object.assign(new Error(message), { statusCode, code });
}

function sendKnownError(error: unknown, res: Response): boolean {
  const known = error as Partial<MessageApiError>;
  if (typeof known.statusCode !== "number" || typeof known.code !== "string") return false;
  res.status(known.statusCode).json({ error: known.message, code: known.code });
  return true;
}

function fullName(firstName: string, lastName: string): string {
  return `${firstName.trim()} ${lastName.trim()}`.trim();
}

async function loadAdminPayload() {
  const [messageRows, directoryRows] = await Promise.all([
    db.select().from(smMessages)
      .where(eq(smMessages.isDeleted, false))
      .orderBy(desc(smMessages.sentAt))
      .limit(250),
    db.select({
      id: users.id,
      firstName: users.firstName,
      lastName: users.lastName,
      email: users.email,
    }).from(users)
      .where(and(
        eq(users.role, "sm"),
        eq(users.isActive, true),
        isNull(users.deletedAt),
      ))
      .orderBy(asc(users.firstName), asc(users.lastName)),
  ]);

  const recipientRows = messageRows.length === 0
    ? []
    : await db.select().from(smMessageRecipients)
      .where(and(
        inArray(smMessageRecipients.messageId, messageRows.map((row) => row.id)),
        eq(smMessageRecipients.isDeleted, false),
      ))
      .orderBy(asc(smMessageRecipients.recipientNameSnapshot));

  const recipientsByMessageId = new Map<string, typeof recipientRows>();
  for (const row of recipientRows) {
    const rows = recipientsByMessageId.get(row.messageId) ?? [];
    rows.push(row);
    recipientsByMessageId.set(row.messageId, rows);
  }

  return {
    recipients: directoryRows.map((row) => ({
      id: row.id,
      name: fullName(row.firstName, row.lastName),
      email: row.email,
    })),
    messages: messageRows.map((row) => ({
      id: row.id,
      subject: row.subject,
      body: row.body,
      sender: row.senderNameSnapshot,
      sentAt: row.sentAt.toISOString(),
      visibleAfterReadDays: row.visibleAfterReadDays,
      recipients: (recipientsByMessageId.get(row.id) ?? []).map((recipient) => ({
        recipientId: recipient.smUserId,
        name: recipient.recipientNameSnapshot,
        email: recipient.recipientEmailSnapshot,
        deliveredAt: recipient.deliveredAt.toISOString(),
        readAt: recipient.readAt?.toISOString() ?? null,
      })),
    })),
  };
}

export const adminSmMessagesRouter = Router();
adminSmMessagesRouter.use(requireAuth(["admin", "sm_admin"]));

adminSmMessagesRouter.get("/", async (_req, res, next) => {
  try {
    res.status(200).json(await loadAdminPayload());
  } catch (error) {
    next(error);
  }
});

adminSmMessagesRouter.post("/", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = createMessageSchema.safeParse(req.body);
    if (!parsed.success) {
      throw messageError(400, "sm_message_invalid", "Betreff, Nachricht oder Empfänger sind ungültig.");
    }
    const actorUserId = req.authUser!.appUserId;
    const input = parsed.data;
    const result = await db.transaction(async (tx) => {
      await tx.execute(sql`set local lock_timeout = '5s'`);
      await tx.execute(sql`set local statement_timeout = '15s'`);
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_message:${input.idempotencyKey}`}, 0))`);

      const [existing] = await tx.select({ id: smMessages.id }).from(smMessages).where(and(
        eq(smMessages.idempotencyKey, input.idempotencyKey),
        eq(smMessages.isDeleted, false),
      )).limit(1);
      if (existing) return { messageId: existing.id, replayed: true };

      const [senderRows, recipientRows] = await Promise.all([
        tx.select({ firstName: users.firstName, lastName: users.lastName }).from(users).where(and(
          eq(users.id, actorUserId),
          eq(users.isActive, true),
          isNull(users.deletedAt),
        )).limit(1),
        tx.select({
          id: users.id,
          firstName: users.firstName,
          lastName: users.lastName,
          email: users.email,
        }).from(users).where(and(
          inArray(users.id, input.recipientIds),
          eq(users.role, "sm"),
          eq(users.isActive, true),
          isNull(users.deletedAt),
        )),
      ]);
      const sender = senderRows[0];
      if (!sender) throw messageError(403, "sm_message_sender_unavailable", "Der Absender ist nicht verfügbar.");
      if (recipientRows.length !== input.recipientIds.length) {
        throw messageError(400, "sm_message_recipient_unavailable", "Mindestens ein ausgewählter SM ist nicht mehr aktiv.");
      }

      const sentAt = new Date();
      const [created] = await tx.insert(smMessages).values({
        idempotencyKey: input.idempotencyKey,
        subject: input.subject,
        body: input.body,
        senderUserId: actorUserId,
        senderNameSnapshot: fullName(sender.firstName, sender.lastName),
        sentAt,
        visibleAfterReadDays: input.visibleAfterReadDays,
      }).returning({ id: smMessages.id });
      if (!created) throw new Error("SM message insert returned no row");

      await tx.insert(smMessageRecipients).values(recipientRows.map((recipient) => ({
        messageId: created.id,
        smUserId: recipient.id,
        recipientNameSnapshot: fullName(recipient.firstName, recipient.lastName),
        recipientEmailSnapshot: recipient.email,
        deliveredAt: sentAt,
      })));
      return { messageId: created.id, replayed: false };
    });

    res.status(result.replayed ? 200 : 201).json(result);
  } catch (error) {
    if (!sendKnownError(error, res)) next(error);
  }
});

export const smMessagesRouter = Router();
smMessagesRouter.use(requireAuth(["sm"]));

smMessagesRouter.get("/", async (req: AuthedRequest, res, next) => {
  try {
    const smUserId = req.authUser!.appUserId;
    const rows = await db.select({
      id: smMessages.id,
      subject: smMessages.subject,
      body: smMessages.body,
      sender: smMessages.senderNameSnapshot,
      sentAt: smMessages.sentAt,
      deliveredAt: smMessageRecipients.deliveredAt,
      readAt: smMessageRecipients.readAt,
      visibleAfterReadDays: smMessages.visibleAfterReadDays,
    }).from(smMessageRecipients)
      .innerJoin(smMessages, eq(smMessages.id, smMessageRecipients.messageId))
      .where(and(
        eq(smMessageRecipients.smUserId, smUserId),
        eq(smMessageRecipients.isDeleted, false),
        eq(smMessages.isDeleted, false),
        or(
          isNull(smMessageRecipients.readAt),
          isNull(smMessages.visibleAfterReadDays),
          sql`(
            ${smMessages.visibleAfterReadDays} > 0
            and ${smMessageRecipients.readAt} + (${smMessages.visibleAfterReadDays} * interval '1 day') > now()
          )`,
        ),
      ))
      .orderBy(desc(smMessages.sentAt))
      .limit(100);

    res.status(200).json({
      messages: rows.map((row) => ({
        id: row.id,
        subject: row.subject,
        body: row.body,
        sender: row.sender,
        sentAt: row.sentAt.toISOString(),
        deliveredAt: row.deliveredAt.toISOString(),
        readAt: row.readAt?.toISOString() ?? null,
        visibleAfterReadDays: row.visibleAfterReadDays,
        visibleUntil: smMessageVisibleUntil(row.readAt, row.visibleAfterReadDays)?.toISOString() ?? null,
      })),
    });
  } catch (error) {
    next(error);
  }
});

smMessagesRouter.post("/:messageId/read", async (req: AuthedRequest, res, next) => {
  try {
    const messageId = z.string().uuid().safeParse(req.params.messageId);
    if (!messageId.success) throw messageError(400, "sm_message_id_invalid", "Die Nachricht ist ungültig.");
    const smUserId = req.authUser!.appUserId;

    const [updated] = await db.update(smMessageRecipients)
      .set({ readAt: sql`now()`, updatedAt: sql`now()` })
      .where(and(
        eq(smMessageRecipients.messageId, messageId.data),
        eq(smMessageRecipients.smUserId, smUserId),
        eq(smMessageRecipients.isDeleted, false),
        isNull(smMessageRecipients.readAt),
        sql`exists (
          select 1 from ${smMessages}
          where ${smMessages.id} = ${smMessageRecipients.messageId}
            and ${smMessages.isDeleted} = false
        )`,
      ))
      .returning({ readAt: smMessageRecipients.readAt });

    if (updated?.readAt) {
      res.status(200).json({ messageId: messageId.data, readAt: updated.readAt.toISOString(), alreadyRead: false });
      return;
    }

    const [existing] = await db.select({ readAt: smMessageRecipients.readAt })
      .from(smMessageRecipients)
      .innerJoin(smMessages, eq(smMessages.id, smMessageRecipients.messageId))
      .where(and(
        eq(smMessageRecipients.messageId, messageId.data),
        eq(smMessageRecipients.smUserId, smUserId),
        eq(smMessageRecipients.isDeleted, false),
        eq(smMessages.isDeleted, false),
      ))
      .limit(1);
    if (!existing) throw messageError(404, "sm_message_not_found", "Die Nachricht wurde nicht gefunden.");
    if (!existing.readAt) throw new Error("SM message read transition did not persist");

    res.status(200).json({ messageId: messageId.data, readAt: existing.readAt.toISOString(), alreadyRead: true });
  } catch (error) {
    if (!sendKnownError(error, res)) next(error);
  }
});
