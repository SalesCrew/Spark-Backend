import { and, desc, eq, gt, lte } from "drizzle-orm";
import { db } from "./db.js";
import { logger, serializeError } from "./logger.js";
import { gmKurtiMessages, type GmKurtiMessageRole } from "./schema.js";

const KURTI_MEMORY_TTL_MS = 15 * 60 * 1000;
const KURTI_CLEANUP_INTERVAL_MS = 60 * 1000;

export type KurtiMemoryMessage = {
  id: string;
  role: GmKurtiMessageRole;
  content: string;
  createdAt: string;
  expiresAt: string;
};

function toPayload(row: typeof gmKurtiMessages.$inferSelect): KurtiMemoryMessage {
  return {
    id: row.id,
    role: row.role,
    content: row.content,
    createdAt: row.createdAt.toISOString(),
    expiresAt: row.expiresAt.toISOString(),
  };
}

export function getKurtiConversationExpiry(now = new Date()): Date {
  return new Date(now.getTime() + KURTI_MEMORY_TTL_MS);
}

export async function purgeExpiredKurtiMessages(now = new Date(), gmUserId?: string): Promise<number> {
  const deleted = await db
    .delete(gmKurtiMessages)
    .where(
      gmUserId
        ? and(eq(gmKurtiMessages.gmUserId, gmUserId), lte(gmKurtiMessages.expiresAt, now))
        : lte(gmKurtiMessages.expiresAt, now),
    )
    .returning({ id: gmKurtiMessages.id });
  return deleted.length;
}

export async function loadActiveKurtiMessages(
  gmUserId: string,
  now = new Date(),
): Promise<KurtiMemoryMessage[]> {
  await purgeExpiredKurtiMessages(now, gmUserId);
  const rows = await db
    .select()
    .from(gmKurtiMessages)
    .where(and(eq(gmKurtiMessages.gmUserId, gmUserId), gt(gmKurtiMessages.expiresAt, now)))
    .orderBy(desc(gmKurtiMessages.createdAt));

  return rows.reverse().map(toPayload);
}

export async function saveKurtiExchange(input: {
  gmUserId: string;
  userContent: string;
  assistantContent: string;
  now?: Date;
}): Promise<KurtiMemoryMessage[]> {
  const now = input.now ?? new Date();
  const expiresAt = getKurtiConversationExpiry(now);

  await db.transaction(async (tx) => {
    await tx
      .delete(gmKurtiMessages)
      .where(
        and(
          eq(gmKurtiMessages.gmUserId, input.gmUserId),
          lte(gmKurtiMessages.expiresAt, now),
        ),
      );

    await tx
      .update(gmKurtiMessages)
      .set({ expiresAt })
      .where(
        and(
          eq(gmKurtiMessages.gmUserId, input.gmUserId),
          gt(gmKurtiMessages.expiresAt, now),
        ),
      );

    await tx.insert(gmKurtiMessages).values([
      {
        gmUserId: input.gmUserId,
        role: "user",
        content: input.userContent,
        createdAt: now,
        expiresAt,
      },
      {
        gmUserId: input.gmUserId,
        role: "assistant",
        content: input.assistantContent,
        createdAt: new Date(now.getTime() + 1),
        expiresAt,
      },
    ]);
  });

  return loadActiveKurtiMessages(input.gmUserId, now);
}

export function startKurtiMemoryCleanupScheduler(): void {
  const runCleanup = async () => {
    try {
      const deletedCount = await purgeExpiredKurtiMessages();
      if (deletedCount > 0) {
        logger.info("kurti_memory_cleanup_completed", {
          action: "kurti_memory_cleanup",
          result: "success",
          deletedCount,
        });
      }
    } catch (error) {
      logger.warn("kurti_memory_cleanup_failed", {
        action: "kurti_memory_cleanup",
        result: "failure",
        error: serializeError(error),
      });
    }
  };

  void runCleanup();
  const timer = setInterval(() => void runCleanup(), KURTI_CLEANUP_INTERVAL_MS);
  timer.unref?.();
}
