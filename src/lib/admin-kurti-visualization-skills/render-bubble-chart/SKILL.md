---
name: render-bubble-chart
description: "Render a bubble chart when two aligned numeric axes plus one strictly positive, meaningful third numeric measure are required for each entity, such as IPP versus visits sized by evaluated answers."
---

# Render a bubble chart

## Choose it

- Choose only when x, y, and bubble size answer one coherent analytical question.
- Require the size measure to be numeric, positive, aligned to the same entity and period, and important enough to justify extra visual complexity.
- Reject categorical size, missing size, arbitrary visual weighting, and negative size.
- Prefer scatter when the third measure is optional or merely decorative.

## Research and shape

1. Calculate all three measures from authoritative tools using identical scope.
2. Set `size` for every point; do not mix sized and unsized points.
3. Keep the sample readable, usually eight to fifty points.
4. Use series only for meaningful cohorts, not for individual colors on every entity.
5. State the size metric clearly in subtitle or note because the schema exposes only x/y axis labels.

## Quality gates

- Verify extreme size values do not come from unit mistakes or duplicate aggregation.
- Explain that bubble area represents the third metric and does not imply importance.
- Do not infer causality from any apparent relationship.
- Identify small-sample instability and cohort filters.
- Prefer a companion table when exact three-measure values are operationally important.
