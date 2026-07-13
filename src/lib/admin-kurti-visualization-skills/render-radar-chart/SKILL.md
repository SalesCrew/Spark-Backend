---
name: render-radar-chart
description: "Render a radar chart for a compact multidimensional profile only when three to twelve dimensions use one common normalized scale and direct shape comparison is analytically meaningful."
---

# Render a radar chart

## Choose it

- Choose for a small normalized quality or operational profile across comparable dimensions.
- Use for one focal profile and at most a few comparison profiles.
- Reject raw mixed units such as hours, visits, kilometers, and percentages on one radar.
- Prefer bars for precise dimension ranking and metrics for a headline summary.

## Research and shape

1. Define every dimension and normalize it using the same documented range and direction.
2. Ensure higher values have the same interpretation across all axes; reverse a lower-is-better metric before rendering and document that transformation.
3. Set a common positive `maximum`, usually 100 for normalized percentages.
4. Keep axis order stable and group related dimensions adjacent.
5. Use three to eight axes when possible and no more than six series.

## Quality gates

- Do not invent composite scores or normalization rules.
- Do not interpret polygon area as an official total score.
- State sample period, eligibility, and normalization in subtitle or note.
- Use short axis labels and a companion table for exact definitions when needed.
- Explain the strongest and weakest dimensions plus one comparison difference, without turning the chart into a personnel judgment.
