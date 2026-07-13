---
name: render-line-chart
description: "Render a line chart for chronological developments, trends, repeated period measurements, before/after paths, and multi-GM time series where order and continuity matter."
---

# Render a line chart

## Choose it

- Choose for daily, weekly, monthly, RED-month, quarterly, or yearly measurements in chronological order.
- Choose when slope, turning points, volatility, gaps, and relative trajectories matter.
- Prefer a line over bars when the x-axis is continuous or has more than about twelve ordered periods.
- Reject it for unordered categories, single snapshots, compositions, and numeric frequency distributions.

## Research and shape

1. Obtain one aligned value per series and period from the authoritative analytics tool.
2. Sort periods chronologically, never alphabetically.
3. Set `variant: "line"` and every series `display: "line"`.
4. Keep all series on the same unit and semantic scale.
5. Use `null` for missing observations; never connect a fabricated zero across a gap.
6. Prefer one to eight series. Use nine to twenty-four only for an explicit all-person comparison and keep names short.

## Labels and emphasis

- State the metric and comparison population in the title.
- Put the exact date range or RED period in `timeframe`.
- Use `duration_minutes` for minute totals, `duration_hours` for decimal hours, `decimal` for IPP, and `percent` only for true percentages.
- Add a reference line only for an authoritative target, contractual threshold, or clearly identified team average.
- Use red for the focal series, slate for comparison, then stable distinct tones for additional series.
- Avoid point labels in prose; the renderer already exposes precise hover titles.

## Integrity checks

- Verify every `point.values` array exactly matches declared series order and count.
- Verify aggregation granularity is identical across series.
- Do not infer causes from coincident movement.
- Mention sparse periods, partial current periods, or changed sample composition in `note`.
- Summarize direction, strongest change, and material exception after rendering without repeating the full dataset.
