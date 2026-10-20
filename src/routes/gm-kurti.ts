import { createKurtiRequestDeadline, KURTI_REQUEST_TIMEOUT_MS } from "../lib/kurti-request-deadline.js";
import { observeKurtiTurn } from "../lib/kurti-observability.js";
import { kurtiRestrictedRequestReply } from "../lib/kurti-operating-policy.js";
import { loadKurtiIdentityBoundary, kurtiWorkerIdentityInstructions, KURTI_MENTION_HELP, KURTI_MODEL } from "../lib/kurti-identity.js";
import OpenAI from "openai";
import type { Responses } from "openai/resources/responses/responses";
import { and, eq } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { env } from "../config/env.js";
import { db } from "../lib/db.js";
import { EMPLOYEE_AGREEMENT_KEY, EMPLOYEE_AGREEMENT_VERSION } from "../lib/employee-agreement-meta.js";
import { buildKurtiGmContext } from "../lib/kurti-context.js";
import { SM_KURTI_SYSTEM_PROMPT } from "../lib/sm-kurti-system-prompt.js";
import { buildKurtiSmContext } from "../lib/sm-kurti-context.js";
import { SM_EMPLOYEE_AGREEMENT } from "../lib/sm-employee-agreement.js";
import { KURTI_SYSTEM_PROMPT } from "../lib/kurti-system-prompt.js";
import { loadActiveKurtiMessages, saveKurtiExchange } from "../lib/kurti-memory.js";
import { getRequestLogMeta, logger } from "../lib/logger.js";
import { employeeAgreementAcceptances } from "../lib/schema.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";

export function createWorkerKurtiRouter(role: "gm" | "sm") {
const gmKurtiRouter = Router();
const activeKurtiRequests = new Set<string>();
const messageSchema = z.object({
  message: z.string().trim().min(1).max(1500),
}).strict();

const openai = env.OPENAI_API_KEY
  ? new OpenAI({
      apiKey: env.OPENAI_API_KEY,
      // Old deployment overrides must not keep the former short timeout.
      timeout: Math.max(KURTI_REQUEST_TIMEOUT_MS, env.OPENAI_KURTI_TIMEOUT_MS),
      maxRetries: 1,
    })
  : null;

gmKurtiRouter.use((_req, res, next) => { res.set("Cache-Control", "private, no-store"); next(); });
gmKurtiRouter.use(requireAuth([role]));
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
          eq(employeeAgreementAcceptances.agreementKey, (role === "sm" ? SM_EMPLOYEE_AGREEMENT.key : EMPLOYEE_AGREEMENT_KEY)),
          eq(employeeAgreementAcceptances.agreementVersion, (role === "sm" ? SM_EMPLOYEE_AGREEMENT.version : EMPLOYEE_AGREEMENT_VERSION)),
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

gmKurtiRouter.get("/people", async (req: AuthedRequest, res, next) => {
  try {const identity=await loadKurtiIdentityBoundary(req.authUser!);res.set("Cache-Control","private, no-store").json({people:identity.people});}catch(error){next(error);}
});

gmKurtiRouter.get("/messages", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt.", code: "auth_required" });
      return;
    }

    const [messages, identity] = await Promise.all([loadActiveKurtiMessages(gmUserId), loadKurtiIdentityBoundary(req.authUser!)]);
    res.json({
      messages,
      people: identity.people,
      configured: Boolean(openai) && env.OPENAI_KURTI_ENABLED !== false,
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

  const audit = observeKurtiTurn(req, res, { workspace: role, audience: "worker" });
  const parsed = messageSchema.safeParse(req.body ?? {});
  if (!parsed.success) {
    audit.outcome("invalid_message");
    res.status(400).json({
      error: "Bitte gib eine Nachricht mit maximal 1.500 Zeichen ein.",
      code: "invalid_kurti_message",
    });
    return;
  }

  if (env.OPENAI_KURTI_ENABLED === false) {
    audit.outcome("disabled");
    res.status(503).json({ error: "Kurti ist vorübergehend nicht verfügbar. Bitte später erneut versuchen.", code: "kurti_temporarily_unavailable" });
    return;
  }

  if (!openai) {
    audit.outcome("not_configured");
    res.status(503).json({
      error: "Kurti wird gerade eingerichtet. Bitte später erneut versuchen.",
      code: "kurti_not_configured",
    });
    return;
  }

  if (activeKurtiRequests.has(gmUserId)) {
    audit.outcome("busy");
    res.status(409).json({
      error: "Kurti schreibt bereits an einer Antwort.",
      code: "kurti_request_in_progress",
    });
    return;
  }

  activeKurtiRequests.add(gmUserId);
  const deadline = createKurtiRequestDeadline(res);
  try {
    const identity = await loadKurtiIdentityBoundary(req.authUser!);
    const prepared = identity.prepareWorkerMessage(parsed.data.message);
    const restrictedReply = kurtiRestrictedRequestReply(prepared.text);
    if (restrictedReply) {
      audit.outcome("policy_guard");
      const messages = await saveKurtiExchange({gmUserId,userContent:prepared.text,assistantContent:restrictedReply,identityProtected:true,now:new Date()});
      res.json({messages,people:identity.people,assistantMessage:messages.at(-1),expiresAt:messages.at(-1)?.expiresAt ?? null});return;
    }
    const now = new Date();
    const [history, gmContext] = await Promise.all([
      loadActiveKurtiMessages(gmUserId, now),
      role === "sm" ? buildKurtiSmContext(gmUserId, now) : buildKurtiGmContext(gmUserId, now),
    ]);

    const input: Responses.ResponseInput = [
      {
        role: "developer",
        content: `LIVE-DATEN DES AKTUELL ANGEMELDETEN ${role === "sm" ? "SM" : "GMS"}\n${JSON.stringify(identity.sanitize(gmContext))}`,
      },
      ...history.filter(message=>message.identityProtected && message.content !== KURTI_MENTION_HELP).map((message) => ({
        role: message.role,
        content: message.content,
      } satisfies Responses.EasyInputMessage)),
      {
        role: "user",
        content: prepared.text,
      },
    ];

    audit.providerCall();
    const response = await openai.responses.create(identity.providerRequest({
      model: KURTI_MODEL,
      instructions: kurtiWorkerIdentityInstructions(gmUserId) + "\n\n" + (role === "sm" ? SM_KURTI_SYSTEM_PROMPT : KURTI_SYSTEM_PROMPT),
      input,
      reasoning: { effort: "low" },
      max_output_tokens: env.OPENAI_KURTI_MAX_OUTPUT_TOKENS,
      store: false,
    }), { signal: deadline.signal });
    audit.providerResponse(response);
    deadline.signal.throwIfAborted();
    const assistantContent = identity.redactText(response.output_text.trim());
    if (!assistantContent) {
      audit.outcome("empty_response");
      logger.warn("kurti_empty_response", {
        ...getRequestLogMeta(req),
        action: "kurti_chat",
        result: "failure",
        model: KURTI_MODEL,
      });
      res.status(502).json({
        error: "Kurti konnte gerade keine Antwort erstellen. Bitte erneut versuchen.",
        code: "kurti_empty_response",
      });
      return;
    }

    const messages = await saveKurtiExchange({
      gmUserId,
      userContent: prepared.text,
      assistantContent,
      identityProtected: true,
      now: new Date(),
    });
    res.json({
      messages,
      people: identity.people,
      assistantMessage: messages.at(-1) ?? null,
      expiresAt: messages.at(-1)?.expiresAt ?? null,
    });
  } catch (error) {
    if (deadline.timedOut) {
      audit.outcome("timeout");
      if (!res.destroyed) res.status(504).json({error:"Die Anfrage hat das Zeitlimit erreicht. Bitte grenze sie ein und versuche es erneut.",code:"kurti_timeout"});
      return;
    }
    if (deadline.signal.aborted && res.destroyed) return;
    if(typeof error === "object" && error !== null && "code" in error && error.code === "kurti_person_not_allowed") {audit.outcome("forbidden");res.status(403).json({error:"Diese Person kann hier nicht ausgewählt werden.",code:"kurti_person_not_allowed"});return;}
    if (error instanceof OpenAI.APIError) {
      audit.outcome("provider_error");
      audit.providerError(error);
      logger.warn("kurti_openai_request_failed", {
        ...getRequestLogMeta(req),
        action: "kurti_chat",
        result: "failure",
        model: KURTI_MODEL,
        providerStatus: error.status,
        providerCode: error.code,
        error: { status: error.status, code: error.code },
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
    deadline.dispose();
    activeKurtiRequests.delete(gmUserId);
  }
});

return gmKurtiRouter;
}
export const gmKurtiRouter = createWorkerKurtiRouter("gm");
export const smKurtiRouter = createWorkerKurtiRouter("sm");
