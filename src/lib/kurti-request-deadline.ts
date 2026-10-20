import type { Response } from "express";

export const KURTI_REQUEST_TIMEOUT_MS = 5 * 60 * 1000;

/** One budget for the complete conversation turn, including tool rounds and retries. */
export function createKurtiRequestDeadline(
  response: Pick<Response, "once" | "off" | "writableEnded">,
  // Railway closes non-streaming requests after five idle minutes. Reserve time for the error response.
  timeoutMs = KURTI_REQUEST_TIMEOUT_MS - 5_000,
) {
  const controller = new AbortController();
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, timeoutMs);
  timer.unref?.();
  const onClose = () => {
    if (!response.writableEnded) controller.abort();
  };
  response.once("close", onClose);
  return {
    signal: controller.signal,
    get timedOut() { return timedOut; },
    dispose() {
      clearTimeout(timer);
      response.off("close", onClose);
    },
  };
}
