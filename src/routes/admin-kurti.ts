import OpenAI from "openai";
import type { Responses } from "openai/resources/responses/responses";
import { eq } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { env } from "../config/env.js";
import {
  appendAdminKurtiCharts,
  parseAdminKurtiChartArguments,
  type AdminKurtiChartSpec,
} from "../lib/admin-kurti-charts.js";
import {
  appendAdminKurtiVisualizations,
  type AdminKurtiVisualization,
} from "../lib/admin-kurti-visualizations.js";
import {
  ADMIN_KURTI_VISUALIZATION_TOOL_NAMES,
  parseAdminKurtiVisualizationToolCall,
} from "../lib/admin-kurti-visualization-tools.js";
import { loadActiveAdminKurtiMessages, saveAdminKurtiExchange } from "../lib/admin-kurti-memory.js";
import { buildAdminKurtiModelHistory } from "../lib/admin-kurti-model-context.js";
import { ADMIN_KURTI_SYSTEM_PROMPT } from "../lib/admin-kurti-system-prompt.js";
import {
  addAdminKurtiToolGroups,
  parseAdminKurtiToolGroupArguments,
  selectAdminKurtiTools,
} from "../lib/admin-kurti-tool-routing.js";
import { ADMIN_KURTI_TOOLS, executeAdminKurtiTool } from "../lib/admin-kurti-tools.js";
import { db } from "../lib/db.js";
import { getRequestLogMeta, logger, serializeError } from "../lib/logger.js";
import { adminKurtiWindowPreferences, users } from "../lib/schema.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";

const adminKurtiRouter = Router();
const activeAdminKurtiRequests = new Set<string>();
const MAX_TOOL_ROUNDS = 6;

const messageSchema = z.object({
  message: z.string().trim().min(1).max(4000),
}).strict();

const layoutCoordinateSchema = z.number().int().min(0).max(100000);
const windowLayoutSchema = z.object({
  panel: z.object({
    x: layoutCoordinateSchema,
    y: layoutCoordinateSchema,
    width: z.number().int().min(280).max(10000),
    height: z.number().int().min(300).max(10000),
  }).strict(),
  bubble: z.object({
    x: layoutCoordinateSchema,
    y: layoutCoordinateSchema,
  }).strict(),
  bubbleDismissed: z.boolean(),
  isCollapsed: z.boolean().default(false),
}).strict();

function serializeWindowLayout(row: typeof adminKurtiWindowPreferences.$inferSelect) {
  return {
    panel: {
      x: row.panelX,
      y: row.panelY,
      width: row.panelWidth,
      height: row.panelHeight,
    },
    bubble: {
      x: row.bubbleX,
      y: row.bubbleY,
    },
    bubbleDismissed: row.bubbleDismissed,
    isCollapsed: row.isCollapsed,
    updatedAt: row.updatedAt.toISOString(),
  };
}

const openai = env.OPENAI_API_KEY
  ? new OpenAI({
      apiKey: env.OPENAI_API_KEY,
      timeout: env.OPENAI_KURTI_TIMEOUT_MS,
      maxRetries: 0,
    })
  : null;

function getRateLimitRetryDelayMs(error: { status?: number; message: string }): number | null {
  if (error.status !== 429) return null;
  const secondsMatch = error.message.match(/try again in\s+([\d.]+)s/i);
  if (!secondsMatch?.[1]) return 3_000;
  const seconds = Number(secondsMatch[1]);
  if (!Number.isFinite(seconds)) return 3_000;
  return Math.min(15_000, Math.max(1_000, Math.ceil(seconds * 1_000) + 350));
}

async function createAdminKurtiResponse(
  client: OpenAI,
  body: Responses.ResponseCreateParamsNonStreaming,
  logMeta: ReturnType<typeof getRequestLogMeta>,
) {
  try {
    return await client.responses.create(body);
  } catch (error) {
    if (!(error instanceof OpenAI.APIError)) throw error;
    const retryDelayMs = getRateLimitRetryDelayMs(error);
    if (retryDelayMs === null) throw error;
    logger.warn("admin_kurti_rate_limit_retry_scheduled", {
      ...logMeta,
      action: "admin_kurti_chat",
      result: "retry",
      retryDelayMs,
      providerStatus: error.status,
      providerCode: error.code,
    });
    await new Promise((resolve) => setTimeout(resolve, retryDelayMs));
    return client.responses.create(body);
  }
}

adminKurtiRouter.use(requireAuth(["admin"]));

adminKurtiRouter.get("/layout", async (req: AuthedRequest, res, next) => {
  try {
    const adminUserId = req.authUser?.appUserId;
    if (!adminUserId) {
      res.status(401).json({ error: "Nicht eingeloggt.", code: "auth_required" });
      return;
    }
    const rows = await db
      .select()
      .from(adminKurtiWindowPreferences)
      .where(eq(adminKurtiWindowPreferences.adminUserId, adminUserId))
      .limit(1);
    res.json({ layout: rows[0] ? serializeWindowLayout(rows[0]) : null });
  } catch (error) {
    next(error);
  }
});

adminKurtiRouter.put("/layout", async (req: AuthedRequest, res, next) => {
  try {
    const adminUserId = req.authUser?.appUserId;
    if (!adminUserId) {
      res.status(401).json({ error: "Nicht eingeloggt.", code: "auth_required" });
      return;
    }
    const parsed = windowLayoutSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({
        error: "Ungültige Admin-Kurti-Fensterposition.",
        code: "invalid_admin_kurti_layout",
      });
      return;
    }
    const now = new Date();
    const values = {
      adminUserId,
      panelX: parsed.data.panel.x,
      panelY: parsed.data.panel.y,
      panelWidth: parsed.data.panel.width,
      panelHeight: parsed.data.panel.height,
      bubbleX: parsed.data.bubble.x,
      bubbleY: parsed.data.bubble.y,
      bubbleDismissed: parsed.data.bubbleDismissed,
      isCollapsed: parsed.data.isCollapsed,
      updatedAt: now,
    };
    const rows = await db
      .insert(adminKurtiWindowPreferences)
      .values(values)
      .onConflictDoUpdate({
        target: adminKurtiWindowPreferences.adminUserId,
        set: {
          panelX: values.panelX,
          panelY: values.panelY,
          panelWidth: values.panelWidth,
          panelHeight: values.panelHeight,
          bubbleX: values.bubbleX,
          bubbleY: values.bubbleY,
          bubbleDismissed: values.bubbleDismissed,
          isCollapsed: values.isCollapsed,
          updatedAt: now,
        },
      })
      .returning();
    const savedLayout = rows[0];
    if (!savedLayout) {
      throw new Error("Admin-Kurti-Fensterposition konnte nicht gespeichert werden.");
    }
    res.json({ layout: serializeWindowLayout(savedLayout) });
  } catch (error) {
    next(error);
  }
});

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
        memoryMinutes: 480,
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
      ...buildAdminKurtiModelHistory(history),
      {
        role: "user",
        content: parsed.data.message,
      },
    ];
    const renderedCharts: AdminKurtiChartSpec[] = [];
    const renderedVisualizations: AdminKurtiVisualization[] = [];
    const routingText = [
      ...history.slice(-8).filter((message) => message.role === "user").map((message) => message.content.slice(0, 2_000)),
      parsed.data.message,
    ].join("\n");
    let activeTools = selectAdminKurtiTools(routingText);

    let response = await createAdminKurtiResponse(openai, {
      model: env.OPENAI_KURTI_MODEL,
      instructions: ADMIN_KURTI_SYSTEM_PROMPT,
      input,
      tools: activeTools,
      tool_choice: "auto",
      parallel_tool_calls: true,
      reasoning: { effort: "low" },
      include: ["reasoning.encrypted_content"],
      max_output_tokens: env.OPENAI_KURTI_MAX_OUTPUT_TOKENS,
      store: false,
    }, getRequestLogMeta(req));

    for (let round = 0; round < MAX_TOOL_ROUNDS; round += 1) {
      const toolCalls = response.output.filter((item) => item.type === "function_call");
      if (toolCalls.length === 0) break;

      input.push(...response.output as Responses.ResponseInputItem[]);
      const toolOutputs = await Promise.all(
        toolCalls.map(async (toolCall) => {
          let output: string;
          if (toolCall.name === "load_admin_tool_group") {
            try {
              const groups = parseAdminKurtiToolGroupArguments(toolCall.arguments);
              activeTools = addAdminKurtiToolGroups(activeTools, groups);
              output = JSON.stringify({
                tool: toolCall.name,
                loaded: true,
                groups,
                availableTools: activeTools.map((tool) => tool.name),
              });
            } catch (error) {
              output = JSON.stringify({
                tool: toolCall.name,
                error: "invalid_tool_group_request",
                message: error instanceof Error ? error.message : "The requested tool group is invalid.",
              });
            }
          } else if (ADMIN_KURTI_VISUALIZATION_TOOL_NAMES.has(toolCall.name)) {
            try {
              if (renderedVisualizations.length >= 8) {
                output = JSON.stringify({
                  tool: toolCall.name,
                  error: "visualization_limit_reached",
                  message: "At most eight visualizations can be rendered in one answer.",
                });
              } else {
                const visualization = parseAdminKurtiVisualizationToolCall(toolCall.name, toolCall.arguments);
                renderedVisualizations.push(visualization);
                output = JSON.stringify({
                  tool: toolCall.name,
                  accepted: true,
                  visualizationIndex: renderedVisualizations.length - 1,
                  kind: visualization.kind,
                  title: visualization.title,
                });
              }
            } catch (error) {
              output = JSON.stringify({
                tool: toolCall.name,
                error: "invalid_visualization_spec",
                message: error instanceof Error ? error.message : "The visualization specification is invalid.",
              });
            }
          } else if (toolCall.name === "render_admin_chart") {
            try {
              if (renderedCharts.length >= 3) {
                output = JSON.stringify({
                  tool: toolCall.name,
                  error: "chart_limit_reached",
                  message: "At most three charts can be rendered in one answer.",
                });
              } else {
                const chart = parseAdminKurtiChartArguments(toolCall.arguments);
                renderedCharts.push(chart);
                output = JSON.stringify({
                  tool: toolCall.name,
                  accepted: true,
                  chartIndex: renderedCharts.length - 1,
                  title: chart.title,
                });
              }
            } catch (error) {
              output = JSON.stringify({
                tool: toolCall.name,
                error: "invalid_chart_spec",
                message: error instanceof Error ? error.message : "The chart specification is invalid.",
              });
            }
          } else {
            output = await executeAdminKurtiTool(toolCall.name, toolCall.arguments);
          }
          return {
            type: "function_call_output" as const,
            call_id: toolCall.call_id,
            output,
          };
        }),
      );
      input.push(...toolOutputs);

      response = await createAdminKurtiResponse(openai, {
        model: env.OPENAI_KURTI_MODEL,
        instructions: ADMIN_KURTI_SYSTEM_PROMPT,
        input,
        tools: activeTools,
        tool_choice: round === MAX_TOOL_ROUNDS - 1 ? "none" : "auto",
        parallel_tool_calls: true,
        reasoning: { effort: "low" },
        include: ["reasoning.encrypted_content"],
        max_output_tokens: env.OPENAI_KURTI_MAX_OUTPUT_TOKENS,
        store: false,
      }, getRequestLogMeta(req));
    }

    const assistantText = response.output_text.trim();
    if (!assistantText && renderedCharts.length === 0 && renderedVisualizations.length === 0) {
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
    const contentWithLegacyCharts = appendAdminKurtiCharts(
      assistantText || "Hier ist die gewünschte Auswertung.",
      renderedCharts,
    );
    const assistantContent = appendAdminKurtiVisualizations(contentWithLegacyCharts, renderedVisualizations);

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
