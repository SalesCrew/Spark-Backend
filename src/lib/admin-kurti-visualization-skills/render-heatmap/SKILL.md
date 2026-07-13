---
name: render-heatmap
description: "Render a heatmap for dense two-dimensional intensity, completeness, frequency, performance, or status matrices such as GM by day, market by RED month, or question by answer category."
---

# Render a heatmap

## Choose it

- Choose when both row and column dimensions matter and color should reveal clusters or gaps.
- Use for repeated periods across entities, weekday/hour patterns, completeness matrices, or category cross-tabs.
- Reject it for one-dimensional rankings, exact-text-heavy output, or cells with incompatible meanings.

## Research and shape

1. Define one cell metric, one unit, and consistent aggregation for the entire matrix.
2. Order columns chronologically or logically and rows by business order or a declared ranking.
3. Keep `xLabels` and every row’s positional `values` exactly aligned.
4. Preserve unobserved cells as `null`; do not color them as zero.
5. Use up to forty columns and thirty rows, but reduce or aggregate when labels become noise.
6. Keep color intensity directional: a higher number always means more of the named metric, not automatically “better.”

## Quality gates

- Explain whether zeros are measured zeros and blanks are missing.
- Do not combine percentages with counts in one matrix.
- Mark partial current periods and changed eligibility.
- Use a companion table for exact values when the matrix is dense.
- Explain clusters, persistent gaps, and one material exception rather than reading cells aloud.
