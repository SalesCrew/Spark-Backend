---
name: render-area-chart
description: "Render an area chart for non-negative magnitude, workload, volume, coverage, or accumulated change over ordered time when the filled extent communicates scale better than a line alone."
---

# Render an area chart

## Choose it

- Choose when the amount over time is the message: visits, hours, photos, completions, or cumulative totals.
- Prefer a line when only direction matters or values oscillate around an arbitrary non-zero baseline.
- Reject overlapping area charts with more than three series; the fills obscure each other.
- Reject area for mixed positive and negative contributions; use line or waterfall instead.

## Research and shape

1. Aggregate all periods using the same rule and aligned boundaries.
2. Set `variant: "area"`; use `display: "area"` for the focal magnitude and `display: "line"` only for a restrained comparator.
3. Keep values non-negative unless zero is a meaningful baseline.
4. Preserve missing periods as `null`; do not close an area through missing observations.
5. Limit to one dominant area and at most two light comparison lines.

## Labels and integrity

- Name the measured quantity and aggregation in the title or subtitle.
- Mark partial current periods and incomplete reporting explicitly.
- Use a target reference line only if the target has the same unit and aggregation.
- Ensure the visual does not imply that a non-zero truncated baseline is zero.
- Prefer weekly or monthly aggregation when daily points produce visual noise rather than insight.
- Explain the magnitude change and period coverage, not the color or geometry.
