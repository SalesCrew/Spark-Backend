import { and, desc, eq, gt, lte, sql } from "drizzle-orm";
import { parseAdminKurtiStoredContent, type AdminKurtiChartSpec } from "./admin-kurti-charts.js";
import { parseAdminKurtiStoredExports, type AdminKurtiExcelExport } from "./admin-kurti-exports.js";
import { parseAdminKurtiStoredVisualizations, type AdminKurtiVisualization } from "./admin-kurti-visualizations.js";
import { parseSmKurtiStoredExports, type SmKurtiExport } from "./sm-kurti-exports.js";
import { db } from "./db.js";
import { logger, serializeError } from "./logger.js";
import { adminKurtiMessages, type AdminKurtiMessageRole } from "./schema.js";

const SM_PREFIX = "<!--kurti-workspace:sm-->\n";
const GM_PREFIX = "<!--kurti-workspace:gm-->\n";
type Workspace = "gm" | "sm";
function workspaceCondition(workspace:Workspace) {
 return workspace === "sm" ? sql`left(${adminKurtiMessages.content}, ${SM_PREFIX.length}) = ${SM_PREFIX}` : sql`left(${adminKurtiMessages.content}, ${SM_PREFIX.length}) <> ${SM_PREFIX}`;
}
const ADMIN_KURTI_MEMORY_TTL_MS = 8 * 60 * 60 * 1000;
const ADMIN_KURTI_CLEANUP_INTERVAL_MS = 60 * 1000;

export type AdminKurtiMemoryMessage = {
  id: string;
  role: AdminKurtiMessageRole;
  content: string;
  charts: AdminKurtiChartSpec[];
  visualizations: AdminKurtiVisualization[];
  exports: AdminKurtiExcelExport[];
  smExports?: SmKurtiExport[];
  createdAt: string;
  expiresAt: string;
};

function toPayload(row: typeof adminKurtiMessages.$inferSelect): AdminKurtiMemoryMessage {
  const sm = parseSmKurtiStoredExports(row.content.startsWith(SM_PREFIX) ? row.content.slice(SM_PREFIX.length) : row.content.startsWith(GM_PREFIX) ? row.content.slice(GM_PREFIX.length) : row.content);
  const parsedExports = parseAdminKurtiStoredExports(sm.content);
  const parsedVisualizations = parseAdminKurtiStoredVisualizations(parsedExports.content);
  const parsedContent = parseAdminKurtiStoredContent(parsedVisualizations.content);
  return {
    id: row.id,
    role: row.role,
    content: parsedContent.content,
    charts: parsedContent.charts,
    visualizations: parsedVisualizations.visualizations,
    exports: parsedExports.exports,
    smExports: sm.smExports,
    createdAt: row.createdAt.toISOString(),
    expiresAt: row.expiresAt.toISOString(),
  };
}

export function getAdminKurtiConversationExpiry(now = new Date()): Date {
  return new Date(now.getTime() + ADMIN_KURTI_MEMORY_TTL_MS);
}

export async function purgeExpiredAdminKurtiMessages(now = new Date(), adminUserId?: string): Promise<number> {
  const deleted = await db
    .delete(adminKurtiMessages)
    .where(
      adminUserId
        ? and(eq(adminKurtiMessages.adminUserId, adminUserId), lte(adminKurtiMessages.expiresAt, now))
        : lte(adminKurtiMessages.expiresAt, now),
    )
    .returning({ id: adminKurtiMessages.id });
  return deleted.length;
}

export async function loadActiveAdminKurtiMessages(
  adminUserId: string,
  now = new Date(),
  workspace: Workspace = "gm",
): Promise<AdminKurtiMemoryMessage[]> {
  await purgeExpiredAdminKurtiMessages(now, adminUserId);
  const rows = await db
    .select()
    .from(adminKurtiMessages)
    .where(
      and(
        eq(adminKurtiMessages.adminUserId, adminUserId),
        gt(adminKurtiMessages.expiresAt, now),
        workspaceCondition(workspace),
      ),
    )
    .orderBy(desc(adminKurtiMessages.createdAt));

  return rows.reverse().map(toPayload);
}

export async function saveAdminKurtiExchange(input: {
  adminUserId: string;
  userContent: string;
  assistantContent: string;
  now?: Date;
  workspace?: Workspace;
}): Promise<AdminKurtiMemoryMessage[]> {
  const now = input.now ?? new Date();
  const workspace = input.workspace ?? "gm";
  const prefix = workspace === "sm" ? SM_PREFIX : GM_PREFIX;
  const expiresAt = getAdminKurtiConversationExpiry(now);

  await db.transaction(async (tx) => {
    await tx
      .delete(adminKurtiMessages)
      .where(
        and(
          eq(adminKurtiMessages.adminUserId, input.adminUserId),
          lte(adminKurtiMessages.expiresAt, now),
        ),
      );

    await tx
      .update(adminKurtiMessages)
      .set({ expiresAt })
      .where(
        and(
          eq(adminKurtiMessages.adminUserId, input.adminUserId),
          gt(adminKurtiMessages.expiresAt, now),
          workspaceCondition(workspace),
        ),
      );

    await tx.insert(adminKurtiMessages).values([
      {
        adminUserId: input.adminUserId,
        role: "user",
        content: prefix + input.userContent,
        createdAt: now,
        expiresAt,
      },
      {
        adminUserId: input.adminUserId,
        role: "assistant",
        content: prefix + input.assistantContent,
        createdAt: new Date(now.getTime() + 1),
        expiresAt,
      },
    ]);
  });

  return loadActiveAdminKurtiMessages(input.adminUserId, now, workspace);
}

export function startAdminKurtiMemoryCleanupScheduler(): void {
  const runCleanup = async () => {
    try {
      const deletedCount = await purgeExpiredAdminKurtiMessages();
      if (deletedCount > 0) {
        logger.info("admin_kurti_memory_cleanup_completed", {
          action: "admin_kurti_memory_cleanup",
          result: "success",
          deletedCount,
        });
      }
    } catch (error) {
      logger.warn("admin_kurti_memory_cleanup_failed", {
        action: "admin_kurti_memory_cleanup",
        result: "failure",
        error: serializeError(error),
      });
    }
  };

  void runCleanup();
  const timer = setInterval(() => void runCleanup(), ADMIN_KURTI_CLEANUP_INTERVAL_MS);
  timer.unref?.();
}
