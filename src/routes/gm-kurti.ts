import OpenAI from "openai";
import type { Responses } from "openai/resources/responses/responses";
import { and, eq } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { env } from "../config/env.js";
import { db } from "../lib/db.js";
import { EMPLOYEE_AGREEMENT_KEY, EMPLOYEE_AGREEMENT_VERSION } from "../lib/employee-agreement-meta.js";
import { buildKurtiGmContext } from "../lib/kurti-context.js";
import { KURTI_SYSTEM_PROMPT } from "../lib/kurti-system-prompt.js";
import { loadActiveKurtiMessages, saveKurtiExchange } from "../lib/kurti-memory.js";
import { getRequestLogMeta, logger, serializeError } from "../lib/logger.js";
import { employeeAgreementAcceptances } from "../lib/schema.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";

const gmKurtiRouter = Router();
const activeKurtiRequests = new Set<string>();
const messageSchema = z.object({
  message: z.string().trim().min(1).max(1500),
}).strict();

const openai = env.OPENAI_API_KEY
  ? new OpenAI({
      apiKey: env.OPENAI_API_KEY,
      timeout: env.OPENAI_KURTI_TIMEOUT_MS,
      maxRetries: 1,
    })
  : null;

gmKurtiRouter.use(requireAuth(["gm"]));
gmKurtiRouter.use(async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt.", code: "auth_required" });
      return;
    }

    const [acceptance] = await db
      .select({ id: employeeAgreementAcceptances.id })
      .from(employeeAgreementAcceptances)
      .where(
        and(
          eq(employeeAgreementAcceptances.userId, gmUserId),
          eq(employeeAgreementAcceptances.agreementKey, EMPLOYEE_AGREEMENT_KEY),
          eq(employeeAgreementAcceptances.agreementVersion, EMPLOYEE_AGREEMENT_VERSION),
        ),
      )
      .limit(1);

    if (!acceptance) {
      res.status(403).json({
        error: "Bitte bestätige zuerst die aktuelle Nutzungs- und Kontrollvereinbarung.",
        code: "kurti_agreement_required",
      });
      return;
    }

    next();
  } catch (error) {
    next(error);
  }
});

gmKurtiRouter.get("/messages", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt.", code: "auth_required" });
      return;
    }

    const messages = await loadActiveKurtiMessages(gmUserId);
    res.json({
      messages,
      configured: Boolean(openai),
      expiresAt: messages.at(-1)?.expiresAt ?? null,
    });
  } catch (error) {
    next(error);
  }
});

gmKurtiRouter.post("/messages", async (req: AuthedRequest, res, next) => {
  const gmUserId = req.authUser?.appUserId;
  if (!gmUserId) {
    res.status(401).json({ error: "Nicht eingeloggt.", code: "auth_required" });
    return;
  }

  const parsed = messageSchema.safeParse(req.body ?? {});
  if (!parsed.success) {
    res.status(400).json({
      error: "Bitte gib eine Nachricht mit maximal 1.500 Zeichen ein.",
      code: "invalid_kurti_message",
    });
    return;
  }

  if (!openai) {
    res.status(503).json({
      error: "Kurti wird gerade eingerichtet. Bitte später erneut versuchen.",
      code: "kurti_not_configured",
    });
    return;
  }

  if (activeKurtiRequests.has(gmUserId)) {
    res.status(409).json({
      error: "Kurti schreibt bereits an einer Antwort.",
      code: "kurti_request_in_progress",
    });
    return;
  }

  activeKurtiRequests.add(gmUserId);
  try {
    const now = new Date();
    const [history, gmContext] = await Promise.all([
      loadActiveKurtiMessages(gmUserId, now),
      buildKurtiGmContext(gmUserId, now),
    ]);

    const input: Responses.ResponseInput = [
      {
        role: "developer",
        content: `LIVE-DATEN DES AKTUELL ANGEMELDETEN GMS\n${JSON.stringify(gmContext)}`,
      },
      ...history.map((message) => ({
        role: message.role,
        content: message.content,
      } satisfies Responses.EasyInputMessage)),
      {
        role: "user",
        content: parsed.data.message,
      },
    ];

    const response = await openai.responses.create({
      model: env.OPENAI_KURTI_MODEL,
      instructions: KURTI_SYSTEM_PROMPT,
      input,
      reasoning: { effort: "low" },
      max_output_tokens: env.OPENAI_KURTI_MAX_OUTPUT_TOKENS,
      store: false,
    });
    const assistantContent = response.output_text.trim();
    if (!assistantContent) {
      logger.warn("kurti_empty_response", {
        ...getRequestLogMeta(req),
        action: "kurti_chat",
        result: "failure",
        model: env.OPENAI_KURTI_MODEL,
      });
      res.status(502).json({
        error: "Kurti konnte gerade keine Antwort erstellen. Bitte erneut versuchen.",
        code: "kurti_empty_response",
      });
      return;
    }

    const messages = await saveKurtiExchange({
      gmUserId,
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
      logger.warn("kurti_openai_request_failed", {
        ...getRequestLogMeta(req),
        action: "kurti_chat",
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
          : "Kurti ist gerade nicht erreichbar. Bitte erneut versuchen.",
        code: isRateLimited ? "kurti_rate_limited" : "kurti_provider_error",
      });
      return;
    }
    next(error);
  } finally {
    activeKurtiRequests.delete(gmUserId);
  }
});

export { gmKurtiRouter };
