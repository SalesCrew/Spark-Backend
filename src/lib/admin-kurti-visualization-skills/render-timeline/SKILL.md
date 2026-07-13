---
name: render-timeline
description: "Render a timeline for chronologically ordered events, visits, requests, campaign milestones, audit changes, state transitions, or day-session activities where sequence and context matter."
---

# Render a timeline

## Choose it

- Choose for discrete dated events and state changes, not numeric trend measurements.
- Use when event description, status, and order matter more than aggregate magnitude.
- Prefer a line for repeated numeric values and a table for dense exact event fields.

## Research and shape

1. Read events from the authoritative visit, request, time, or audit source.
2. Sort oldest-to-newest unless the admin explicitly asks for newest first.
3. Preserve event timestamps and distinguish event time from record-update time.
4. Use concise labels, short factual descriptions, and optional values only when they add context.
5. Assign statuses from documented workflow state: completed, active, pending, warning, or critical.
6. Limit to fifty relevant events and disclose filters or truncation.

## Quality gates

- Do not infer intent, fault, or causality from audit order alone.
- Do not reveal redacted payloads, tokens, auth IDs, or irrelevant personal data.
- Keep events from different entities separate unless the combined sequence is explicitly the question.
- Explain gaps where the source does not record intermediate actions.
- Summarize the current state, key transition, and unresolved next step after rendering.
