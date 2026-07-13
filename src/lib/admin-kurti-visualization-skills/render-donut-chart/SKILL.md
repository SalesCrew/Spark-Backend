---
name: render-donut-chart
description: "Render a donut chart for a small positive part-to-whole snapshot when the total or a short center label is meaningful, such as visit-type share, completion status, or photo-tag composition."
---

# Render a donut chart

## Choose it

- Choose for one snapshot with two to about seven mutually interpretable categories.
- Prefer donut over pie when the center should show the total, sample count, or concise unit label.
- Reject it for time series, rankings, negative values, overlapping populations, or many similarly sized categories.
- Prefer treemap when more than about eight positive parts must remain visible.

## Research and shape

1. Define the total population and timeframe before calculating parts.
2. Verify every item belongs to the same denominator and is non-negative.
3. Set `variant: "donut"` and place a useful unit such as “Besuche” or “Fotos” in `centerLabel`.
4. Use raw counts with `number` when the center shows a count; the renderer calculates shares for the legend.
5. Use `percent` only when the source already provides true percentages.
6. Order by business meaning or descending size and use stable tones.

## Quality gates

- Do not hide missing or uncategorized records; include an honest category or explain the exclusion.
- Do not combine categories into “Other” unless the aggregation is directly computed.
- Ensure rounded shares remain an honest approximation of the total.
- State the total sample and any filter that changes the denominator.
- Explain the dominant share and one meaningful secondary share; avoid narrating every slice.
