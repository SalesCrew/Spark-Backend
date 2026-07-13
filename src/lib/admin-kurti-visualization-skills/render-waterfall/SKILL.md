---
name: render-waterfall
description: "Render a waterfall for an additive bridge from a starting value through signed contributors to a computed ending value, such as target-to-actual variance or period-over-period change decomposition."
---

# Render a waterfall

## Choose it

- Choose when signed components mathematically explain movement from one total to another.
- Use for target variance, score movement, workload change, or another verified additive decomposition.
- Reject it when categories overlap, contributions are not additive, or the end cannot be reproduced from start plus steps.
- Prefer bars for independent categories and stacked bars for unsigned components of a total.

## Research and shape

1. Obtain the authoritative start value and every signed contribution.
2. Use `render_waterfall_visualization`; the final value is computed from start plus all steps.
3. Keep the sign truthful: positive raises the running total and negative lowers it.
4. Order contributions by business sequence or calculation order, not simply by absolute size unless the admin asks for contribution ranking.
5. Use one unit and at most twenty concise steps.

## Quality gates

- Verify the computed end matches the authoritative end before rendering.
- Do not create an unexplained residual merely to force reconciliation; label a real known residual honestly.
- State whether values are counts, hours, points, percentages, or currency.
- Distinguish causal contributors from arithmetic components.
- Explain the largest positive and negative contributions and the net movement.
