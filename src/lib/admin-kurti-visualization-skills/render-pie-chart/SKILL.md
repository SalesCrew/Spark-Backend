---
name: render-pie-chart
description: "Render a classic pie chart only for a very small, simple part-to-whole snapshot with no useful center total and clearly distinguishable category shares."
---

# Render a pie chart

## Choose it

- Choose for two to five clear categories when angular share is the entire question.
- Prefer donut when a total, sample count, or label belongs in the center.
- Prefer bars when close values need accurate comparison.
- Prefer treemap for many categories and reject pie for negative or overlapping values.

## Research and shape

1. Define one denominator, one timeframe, and one mutually interpretable set of parts.
2. Set `variant: "pie"` and `centerLabel: null`.
3. Supply non-negative values only.
4. Order large, important categories consistently; never rearrange to imply a trend.
5. Use clear tones and avoid multiple nearly identical slices.

## Quality gates

- If more than five slices are needed, switch to donut, bars, or treemap based on the question.
- If values differ only slightly, use bars so the difference is not visually overstated or hidden.
- Expose uncategorized or excluded records in the note.
- Distinguish percentage of submitted records from percentage of assigned or eligible records.
- Explain denominator and the most material share after rendering.
