import type { Request, Response } from "express";
import { performance } from "node:perf_hooks";
import { logger } from "./logger.js";

export const KURTI_SAFEGUARD_VERSION = "2026-10-20.1";
export type KurtiOutcome = "answered" | "mention_required" | "policy_guard" | "disabled" | "not_configured" | "invalid_message" | "busy" | "forbidden" | "timeout" | "cancelled" | "provider_error" | "empty_response" | "error";
type Scope = { workspace: "gm" | "sm"; audience: "admin" | "worker" };
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const providerId = /^req_[A-Za-z0-9_-]{1,160}$/;

/** Fixed-field operational evidence. Never accepts prompts, responses, tool arguments or identities. */
export function observeKurtiTurn(req: Request, res: Response, scope: Scope) {
  const started = performance.now();
  const requestId = (req as Request & { requestId?: unknown }).requestId;
  const providerRequestIds = new Set<string>();
  const recordProviderId = (id: unknown) => {
    if (typeof id === "string" && providerId.test(id) && providerRequestIds.size < 10) providerRequestIds.add(id);
  };
  let providerCalls = 0, toolCalls = 0, outcome: KurtiOutcome | undefined, finished = false;
  const finish = (cancelled = false) => {
    if (finished) return;
    finished = true;
    res.off("finish", onFinish);
    res.off("close", onClose);
    // A logging failure must not change an answer or prevent normal request cleanup.
    try {
      logger.info("kurti_turn_completed", {
        action: "kurti_chat", result: cancelled ? "cancelled" : outcome ?? (res.statusCode < 400 ? "answered" : "error"),
        ...scope, model: "gpt-6-luna", safeguardVersion: KURTI_SAFEGUARD_VERSION,
        requestId: typeof requestId === "string" && uuid.test(requestId) ? requestId : null,
        statusCode: cancelled ? 499 : res.statusCode, durationMs: Math.round(performance.now() - started),
        providerCalls, toolCalls, providerRequestIds: [...providerRequestIds],
      });
    } catch { /* Operational logging is best effort, independent of chat availability. */ }
  };
  const onFinish = () => finish();
  const onClose = () => { if (!res.writableEnded) finish(true); };
  res.once("finish", onFinish);
  res.once("close", onClose);
  return {
    outcome(value: KurtiOutcome) { outcome = value; },
    providerCall() { providerCalls += 1; },
    toolCall() { toolCalls += 1; },
    providerResponse(response: { _request_id?: string | null }) {
      recordProviderId(response._request_id);
    },
    providerError(error: { requestID?: string | null | undefined }) {
      recordProviderId(error.requestID);
    },
  };
}
