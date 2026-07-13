---
name: render-funnel-chart
description: "Render a funnel for ordered operational stages where records move through a defined process, such as assigned, started, submitted, reviewed, and approved visits or requests."
---

# Render a funnel

## Choose it

- Choose only when items have a real stage order and later stages are interpreted relative to earlier stages.
- Use for visit completion, campaign execution, request handling, or another documented pipeline.
- Reject it for unordered categories, time series, unrelated totals, and rankings.

## Research and shape

1. Use authoritative stage definitions from the relevant workflow.
2. Preserve process order exactly; never sort stages by value.
3. Set `variant: "funnel"` and use concise action labels.
4. Use counts from the same cohort and cutoff date.
5. Prefer non-increasing values. If records can legitimately enter later stages through another route, explain the increase instead of forcing monotonicity.
6. Keep three to eight stages.

## Quality gates

- Do not mix assigned targets with unrelated visit totals.
- Avoid double-counting records that can revisit a stage.
- State the cohort, timeframe, and whether open records may progress later.
- Compute conversion or drop-off percentages only from the displayed cohort.
- Explain the largest stage loss and its operational meaning without assigning blame.
