---
name: render-box-plot
description: "Render box plots from raw numeric observations to compare medians, quartiles, spread, and potential Tukey outliers across GMs, regions, campaigns, periods, visit types, or other cohorts."
---

# Render a box plot

## Choose it

- Choose when comparing distribution spread across two or more cohorts.
- Choose when median and variability matter more than every named observation.
- Prefer histogram for the detailed shape of one population and bars for exact cohort averages.

## Research and shape

1. Collect raw values per cohort using identical unit, period, and inclusion rules.
2. Use `render_distribution_visualization` with `variant: "box_plot"`.
3. Pass raw values; let the renderer calculate quartiles, interquartile range, whiskers, and outlier points.
4. Use at least five observations per cohort; label smaller samples exploratory.
5. Set `showOutliers` true unless privacy or visual density requires hiding the points; never hide the fact that they were omitted.
6. Keep cohort labels readable and limit to about twelve groups.

## Quality gates

- Describe outliers as potential statistical outliers, not mistakes or poor performance.
- Do not compare cohorts with materially different eligibility without stating it.
- Do not mix aggregation grains.
- Report sample count and whether current periods are partial.
- Explain median difference, spread, and overlap; avoid judging a person from a single point.
