---
name: render-scatter-chart
description: "Render a scatter chart for the relationship between two aligned numeric measures across the same GMs, markets, visits, campaigns, questions, or periods, without implying causality."
---

# Render a scatter chart

## Choose it

- Choose when the question asks whether two numeric variables move together, form clusters, or contain outliers.
- Require aligned pairs for the same entity and compatible timeframe.
- Prefer bars for category comparison, line for time development, and box plot for group distributions.
- Use bubble only when a trustworthy third numeric variable materially improves interpretation.

## Research and shape

1. Resolve the sample population and calculate both measures using the same inclusion rules.
2. Remove a point only for a documented exclusion; never impute a missing x or y.
3. Set every point `size: null`.
4. Use one series for one population or a small number of meaningful cohort series.
5. Prefer at least eight points before describing a pattern; with fewer points, label the result exploratory.
6. Use honest axis formats and readable point labels.

## Quality gates

- Never describe correlation as cause, performance, or fault.
- Identify exposure effects: more visits can mechanically influence totals, and small samples can create unstable averages.
- Mention influential outliers and sample size without diagnosing people.
- Do not add a trend line because the renderer does not compute a validated regression.
- Explain direction, clustering, and limitations after rendering.
