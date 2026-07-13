---
name: render-table
description: "Render an exact table for precise values, mixed text and numeric fields, long labels, audits, drilldowns, or comparisons where a graphical encoding would hide important detail."
---

# Render a table

## Choose it

- Choose when the admin needs exact values, names, dates, statuses, and mixed fields.
- Use as a companion to a chart when exact operational follow-up matters.
- Prefer a chart when shape, trend, rank, or composition is the primary question.

## Research and shape

1. Resolve readable row identities and define each column clearly.
2. Keep one row grain: one GM, market, visit, campaign, question, period, or event per row.
3. Align every `row.values` array exactly to the declared column order.
4. Format values for display before rendering; preserve `null` as an en dash or explicit “keine Daten,” not zero.
5. Align numeric columns right, compact statuses center or left, and descriptive text left.
6. Limit to the most relevant sixty rows and state sorting/filtering.

## Quality gates

- Do not expose internal UUIDs when a readable identity exists.
- Do not include secrets, auth identifiers, or sensitive raw payloads.
- Use row status only for a documented operational state, not subjective employee evaluation.
- Keep headers short and move definitions to subtitle or note.
- Explain the sort order, row limits, and material exceptions after rendering.
