---
name: render-horizontal-bar-chart
description: "Render horizontal bars for rankings, top/bottom lists, long category labels, and single-metric comparisons across many GMs, markets, campaigns, questions, or other named entities."
---

# Render a horizontal bar chart

## Choose it

- Choose when rank order is the primary message.
- Choose when labels are too long for a vertical x-axis.
- Prefer one series; use multiple series only when each row compares the same few values.
- Reject it for temporal development, part-to-whole structure, or distribution bins.

## Research and shape

1. Resolve entities to readable names and stable identifiers before aggregation.
2. Calculate all values over the same timeframe and inclusion rules.
3. Sort descending for “highest/top” and ascending for “lowest/bottom”; preserve a requested business order.
4. Set `variant: "horizontal_bar"`.
5. Show roughly five to twenty rows. If the population is larger, render the requested top/bottom subset and provide an exact table when useful.
6. Preserve ties and do not invent tie-breakers.

## Quality gates

- Use a zero baseline and one unit.
- Identify exclusions, minimum sample rules, and partial periods in the note.
- Do not rank unstable averages without exposing sample count or a minimum-volume rule.
- Do not interpret rank as personal quality when the metric is operational or exposure-dependent.
- Explain the leader, spread, and any sample-size caveat after rendering.
