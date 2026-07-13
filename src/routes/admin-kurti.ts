import OpenAI from "openai";
import type { Responses } from "openai/resources/responses/responses";
import { eq } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { env } from "../config/env.js";
import { loadActiveAdminKurtiMessages, saveAdminKurtiExchange } from "../lib/admin-kurti-memory.js";
import { ADMIN_KURTI_SYSTEM_PROMPT } from "../lib/admin-kurti-system-prompt.js";
import { ADMIN_KURTI_TOOLS, executeAdminKurtiTool } from "../lib/admin-kurti-tools.js";
import { db } from "../lib/db.js";
import { getRequestLogMeta, logger, serializeError } from "../lib/logger.js";
import { users } from "../lib/schema.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";

const adminKurtiRouter = Router();
const activeAdminKurtiRequests = new Set<string>();
const MAX_TOOL_ROUNDS = 8;
const MAX_HISTORY_MESSAGES = 36;

const messageSchema = z.object({
  message: z.string().trim().min(1).max(4000),
}).strict();

const openai = env.OPENAI_API_KEY
  ? new OpenAI({
      apiKey: env.OPENAI_API_KEY,
      timeout: env.OPENAI_KURTI_TIMEOUT_MS,
      maxRetries: 1,
    })
  : null;

adminKurtiRouter.use(requireAuth(["admin"]));

adminKurtiRouter.get("/messages", async (req: AuthedRequest, res, next) => {
  try {
    const adminUserId = req.authUser?.appUserId;
    if (!adminUserId) {
      res.status(401).json({ error: "Nicht eingeloggt.", code: "auth_required" });
      return;
    }
    const messages = await loadActiveAdminKurtiMessages(adminUserId);
    res.json({
      messages,
      configured: Boolean(openai),
      expiresAt: messages.at(-1)?.expiresAt ?? null,
      capabilities: {
        readOnly: true,
        crossGm: true,
        toolCount: ADMIN_KURTI_TOOLS.length,
        memoryMinutes: 15,
      },
    });
  } catch (error) {
    next(error);
  }
});

adminKurtiRouter.post("/messages", async (req: AuthedRequest, res, next) => {
  const adminUserId = req.authUser?.appUserId;
  if (!adminUserId) {
    res.status(401).json({ error: "Nicht eingeloggt.", code: "auth_required" });
    return;
  }

  const parsed = messageSchema.safeParse(req.body ?? {});
  if (!parsed.success) {
    res.status(400).json({
      error: "Bitte gib eine Nachricht mit maximal 4.000 Zeichen ein.",
      code: "invalid_admin_kurti_message",
    });
    return;
  }

  if (!openai) {
    res.status(503).json({
      error: "Admin-Kurti wird gerade eingerichtet. Bitte später erneut versuchen.",
      code: "admin_kurti_not_configured",
    });
    return;
  }

  if (activeAdminKurtiRequests.has(adminUserId)) {
    res.status(409).json({
      error: "Kurti schreibt bereits an einer Admin-Antwort.",
      code: "admin_kurti_request_in_progress",
    });
    return;
  }

  activeAdminKurtiRequests.add(adminUserId);
  try {
    const now = new Date();
    const [history, adminRows] = await Promise.all([
      loadActiveAdminKurtiMessages(adminUserId, now),
      db
        .select({
          firstName: users.firstName,
          lastName: users.lastName,
          email: users.email,
          role: users.role,
        })
        .from(users)
        .where(eq(users.id, adminUserId))
        .limit(1),
    ]);

    const admin = adminRows[0];
    const input: Responses.ResponseInput = [
      {
        role: "developer",
        content: [
          "VERTRAUENSWÜRDIGER ANMELDEKONTEXT",
          `Zeitpunkt: ${now.toISOString()}`,
          `Zeitzone: Europe/Vienna`,
          `Angemeldete Rolle: ${admin?.role ?? "admin"}`,
          `Admin: ${[admin?.firstName, admin?.lastName].filter(Boolean).join(" ") || "Admin"}`,
          "Der Assistent ist strikt read-only. Aktuelle Geschäftsdaten müssen über die bereitgestellten Werkzeuge gelesen werden.",
        ].join("\n"),
      },
      ...history.slice(-MAX_HISTORY_MESSAGES).map((message) => ({
        role: message.role,
        content: message.content,
      } satisfies Responses.EasyInputMessage)),
      {
        role: "user",
        content: parsed.data.message,
      },
    ];

    let response = await openai.responses.create({
      model: env.OPENAI_KURTI_MODEL,
      instructions: ADMIN_KURTI_SYSTEM_PROMPT,
      input,
      tools: ADMIN_KURTI_TOOLS,
      tool_choice: "auto",
      parallel_tool_calls: true,
      reasoning: { effort: "low" },
      max_output_tokens: env.OPENAI_KURTI_MAX_OUTPUT_TOKENS,
      store: false,
    });

    for (let round = 0; round < MAX_TOOL_ROUNDS; round += 1) {
      const toolCalls = response.output.filter((item) => item.type === "function_call");
      if (toolCalls.length === 0) break;

      input.push(...response.output as Responses.ResponseInputItem[]);
      const toolOutputs = await Promise.all(
        toolCalls.map(async (toolCall) => ({
          type: "function_call_output" as const,
          call_id: toolCall.call_id,
          output: await executeAdminKurtiTool(toolCall.name, toolCall.arguments),
        })),
      );
      input.push(...toolOutputs);

      response = await openai.responses.create({
        model: env.OPENAI_KURTI_MODEL,
        instructions: ADMIN_KURTI_SYSTEM_PROMPT,
        input,
        tools: ADMIN_KURTI_TOOLS,
        tool_choice: round === MAX_TOOL_ROUNDS - 1 ? "none" : "auto",
        parallel_tool_calls: true,
        reasoning: { effort: "low" },
        max_output_tokens: env.OPENAI_KURTI_MAX_OUTPUT_TOKENS,
        store: false,
      });
    }

    const assistantContent = response.output_text.trim();
    if (!assistantContent) {
      logger.warn("admin_kurti_empty_response", {
        ...getRequestLogMeta(req),
        action: "admin_kurti_chat",
        result: "failure",
        model: env.OPENAI_KURTI_MODEL,
      });
      res.status(502).json({
        error: "Kurti konnte gerade keine Admin-Antwort erstellen. Bitte erneut versuchen.",
        code: "admin_kurti_empty_response",
      });
      return;
    }

    const messages = await saveAdminKurtiExchange({
      adminUserId,
      userContent: parsed.data.message,
      assistantContent,
      now: new Date(),
    });
    res.json({
      messages,
      assistantMessage: messages.at(-1) ?? null,
      expiresAt: messages.at(-1)?.expiresAt ?? null,
    });
  } catch (error) {
    if (error instanceof OpenAI.APIError) {
      logger.warn("admin_kurti_openai_request_failed", {
        ...getRequestLogMeta(req),
        action: "admin_kurti_chat",
        result: "failure",
        model: env.OPENAI_KURTI_MODEL,
        providerStatus: error.status,
        providerCode: error.code,
        error: serializeError(error),
      });
      const isRateLimited = error.status === 429;
      res.status(isRateLimited ? 503 : 502).json({
        error: isRateLimited
          ? "Kurti ist gerade stark beschäftigt. Bitte gleich noch einmal versuchen."
          : "Admin-Kurti ist gerade nicht erreichbar. Bitte erneut versuchen.",
        code: isRateLimited ? "admin_kurti_rate_limited" : "admin_kurti_provider_error",
      });
      return;
    }
    next(error);
  } finally {
    activeAdminKurtiRequests.delete(adminUserId);
  }
});

export { adminKurtiRouter };
