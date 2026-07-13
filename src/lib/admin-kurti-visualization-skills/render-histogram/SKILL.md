---
name: render-histogram
description: "Render a histogram from raw numeric observations to show frequency shape, concentration, skew, gaps, and tails for IPP, work time, visit duration, answers, distances, or other continuous measures."
---

# Render a histogram

## Choose it

- Choose when the question is “how are numeric observations distributed?” rather than “which entity is highest?”.
- Use one series by default; compare at most three cohorts with identical units and scope.
- Prefer horizontal bars for named rankings and box plots for compact group-to-group spread comparison.

## Research and shape

1. Collect raw numeric observations from one defined population and timeframe.
2. Keep measured zeros; remove nulls rather than converting them.
3. Use `render_distribution_visualization` with `variant: "histogram"`.
4. Pass raw values and let the renderer calculate shared bins.
5. Leave `binCount` null for an automatic readable count or request four to twenty bins only for a justified resolution.
6. Use identical inclusion rules across comparison series.

## Quality gates

- State sample count and unit.
- Do not mix per-visit and per-GM observations in one histogram.
- Do not present frequency shape as a time trend.
- Treat extreme values as observations to inspect, not automatic errors.
- Mention small sample size, truncation, or selection bias.
- Explain center, spread, skew, gaps, or tails without claiming a formal statistical test.
