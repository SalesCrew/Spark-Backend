---
name: render-treemap
description: "Render a treemap for many positive parts of one whole when compact area encoding communicates category weight better than an overcrowded donut, such as market, campaign, tag, or questionnaire contribution shares."
---

# Render a treemap

## Choose it

- Choose for roughly six to forty positive categories that share one denominator.
- Use when dominant and long-tail contributors matter more than exact rank differences.
- Prefer donut for two to seven simple shares and horizontal bars when precise ranking matters.
- Reject negative values, overlapping populations, and unrelated metrics.

## Research and shape

1. Define the whole, timeframe, and inclusion rules.
2. Use `render_treemap_visualization` with non-negative values from one unit.
3. Sort descending unless a documented category order matters.
4. Use stable tones for known groups; do not assign a unique semantic meaning to every color without a legend.
5. Keep readable labels and allow tiny tiles to rely on hover details.
6. Include uncategorized records or explain their exclusion.

## Quality gates

- Do not compare tile area as precisely as a bar length.
- Do not infer hierarchy when the data only contains a flat category list.
- State the total population and whether values are counts or shares.
- Avoid an artificial “Other” bucket unless it is calculated from explicit categories.
- Explain the dominant contributors and the long-tail concentration after rendering.
