---
name: choose-visualization
description: "Select the most truthful Admin Kurti visualization whenever a request explicitly or implicitly benefits from a chart, dashboard, ranking, comparison, development, distribution, relationship, matrix, timeline, anomaly view, KPI summary, or exact structured values."
---

# Choose the visualization

## Decision workflow

1. Resolve the entity, metric, unit, date range, comparison basis, and requested granularity before rendering.
2. Research with the authoritative data tool first. Never construct business values from memory or from truncated previews.
3. Preserve missing observations as `null`. Distinguish “no observation” from a measured zero.
4. Choose one primary visual question. Add another display only when it answers a different necessary question, such as overview + trend + exact values.
5. Load and obey the specific renderer skill before calling its render tool.

## Encoding map

- Use a line for change over ordered time.
- Use an area only when magnitude or accumulated volume matters and overlap stays readable.
- Use vertical bars for a small discrete category comparison.
- Use horizontal bars for a ranking or long category labels.
- Use stacked bars only when components genuinely add to the displayed total.
- Use a combo only for compatible series on one shared numeric scale.
- Use a donut or pie for a small part-to-whole snapshot; prefer donut when a total belongs in the center.
- Use a funnel for ordered process stages, not ordinary categories.
- Use scatter for two numeric variables and bubble only when a trustworthy third numeric variable controls size.
- Use a heatmap for a dense two-dimensional matrix.
- Use metrics for a compact executive summary, not for a long list.
- Use a table when exact values, mixed data types, or long labels matter more than shape.
- Use a timeline for dated events and state transitions.
- Use radar only for a small profile of dimensions on the same normalized scale.
- Use histogram for the frequency shape of raw numeric observations.
- Use box plot for medians, quartiles, spread, and outliers across groups.
- Use waterfall for an additive bridge from a start value through signed contributors to an end value.
- Use treemap for many positive parts of one whole when a donut would become crowded.

## Truthfulness gates

- Keep one unit per numeric axis. Never simulate a second axis.
- Do not render a relationship chart unless every point has aligned measurements from the same entity and compatible period.
- Do not render part-to-whole charts unless the categories are mutually interpretable parts of the stated total.
- Do not render radar for unrelated raw units.
- Label analytical or derived values as such; do not call them official KPIs without an authoritative definition.
- Use concise titles, an explicit timeframe, a source label, and a short note for material limitations.
- Prefer a readable subset plus an exact table over an unreadable all-series chart, except when the admin explicitly requests every GM or every period.

## Response contract

- If the admin asks for a visual and data exists, call a render tool; text alone is incomplete.
- Render after research, then explain the data basis and two or three meaningful observations in Markdown.
- Do not expose render JSON or repeat every plotted value in prose.
- Do not create decorative, redundant, or misleading displays.
