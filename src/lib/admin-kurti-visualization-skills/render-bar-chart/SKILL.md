---
name: render-bar-chart
description: "Render grouped vertical bars for concise comparisons across a small set of discrete categories, periods, GMs, markets, campaigns, or statuses with short labels and a shared unit."
---

# Render a vertical bar chart

## Choose it

- Choose for roughly two to twelve discrete categories with short labels.
- Choose grouped bars for at most four directly comparable series on one scale.
- Prefer horizontal bars for rankings, more than twelve categories, or long names.
- Prefer a line for dense ordered time series and stacked bars for genuine additive components.

## Research and shape

1. Define one consistent category population and aggregation rule.
2. Set `variant: "bar"` and each included series `display: "bar"`.
3. Start the numeric baseline at zero; do not exaggerate small differences.
4. Use the same unit for every series and category.
5. Order categories chronologically, logically, or by value; state a non-obvious order in the note.
6. Keep missing values `null`, not zero.

## Quality gates

- Shorten labels without losing identity; move exact long names into a companion table when necessary.
- Use red for the focal value and slate for a baseline or previous period.
- Do not place totals and percentages in the same grouped chart unless they share a meaningful scale.
- Add a reference line only for a same-unit target or benchmark.
- Explain the largest differences and the comparison basis after rendering.
