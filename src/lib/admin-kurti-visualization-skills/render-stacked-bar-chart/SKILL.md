---
name: render-stacked-bar-chart
description: "Render stacked bars when each category contains additive components that form a meaningful total, such as visit types per GM, status composition per campaign, or answer groups per period."
---

# Render a stacked bar chart

## Choose it

- Choose only when every stack segment is a component of the category total.
- Choose when both total size and internal composition matter.
- Reject it for independent metrics, overlapping populations, or values with incompatible units.
- Prefer donut for one category snapshot and grouped bars when direct component-to-component comparison is more important than totals.

## Research and shape

1. Prove that component definitions are mutually interpretable and use the same denominator.
2. Set `variant: "stacked_bar"` and every series `display: "bar"`.
3. Keep segment order and tone stable across every category.
4. Use raw additive values for total magnitude or true percentages that sum to approximately 100 per category for a share view.
5. Do not mix raw counts and normalized percentages.
6. Avoid negative stack components unless the business question explicitly represents positive and negative contributions; prefer waterfall for that case.

## Quality gates

- Limit stacks to about six components; merge only with an honest, clearly labelled “Other” category derived from data.
- Keep `point.values` aligned to declared series order.
- Preserve missing segments as `null`; do not assume a missing segment is zero.
- State whether the chart shows counts, hours, points, or percentage shares.
- Explain both the largest total and the most material composition difference.
