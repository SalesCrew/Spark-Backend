---
name: render-metrics
description: "Render responsive metric cards for a concise KPI, total, average, delta, progress, status, or executive snapshot when a small set of headline values should be understood immediately."
---

# Render metric cards

## Choose it

- Choose for two to twelve headline values that summarize one coherent scope.
- Use as the first layer of a dashboard before a trend or exact table.
- Reject it for long rankings, dense history, or values that need distribution context.

## Research and shape

1. Define the population, period, and calculation for every item.
2. Provide numeric `value` whenever possible; use `displayValue` only for a genuine text status.
3. Use `delta` only against an explicit comparison basis and name that basis in `deltaLabel`.
4. Use `progress` only for a real 0–100 completion or target percentage, never as decorative fill.
5. Choose two columns for long labels or narrow displays, three for balanced summaries, and four only for short compact metrics; the UI reflows safely.
6. Assign status semantically: rising is not always positive, and falling is not always critical.

## Quality gates

- Keep units visible through `valueFormat` or `displayValue`.
- Do not mix unrelated scopes in one grid.
- Mark provisional, partial, cached, or analytical values in subtitle or note.
- Avoid false precision in averages and percentages.
- Follow with prose that connects the metrics rather than repeating each card.
