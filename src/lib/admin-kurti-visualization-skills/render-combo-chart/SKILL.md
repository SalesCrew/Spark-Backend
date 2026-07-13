---
name: render-combo-chart
description: "Render a restrained line-and-bar combination when aligned series share one x-axis and one genuinely compatible numeric scale, such as actual visits with a same-unit target line."
---

# Render a combo chart

## Choose it

- Choose for one bar series plus one or two line series that share the same unit and period grain.
- Typical valid use: actual count bars with same-unit target or prior-period lines.
- Reject it for hours plus visits, percentages plus counts, or any request that would require a second y-axis.
- Prefer separate charts when the measures answer different questions.

## Research and shape

1. Align all series to identical periods and entity scope.
2. Set `variant: "combo"`; mark the magnitude series `display: "bar"` and comparator series `display: "line"` or `"area"` only when warranted.
3. Keep at most three series so the encoding remains obvious.
4. Use `null` for a missing target or observation; never carry values forward silently.
5. Use stable tones and a visible legend.

## Quality gates

- Confirm all values use one format and scale.
- Keep reference lines for thresholds not already represented as a series.
- State whether current-period bars are incomplete.
- Avoid redundant combinations where a grouped bar or a line chart communicates the result more directly.
- Explain the gap between actual and comparator, not merely their individual values.
