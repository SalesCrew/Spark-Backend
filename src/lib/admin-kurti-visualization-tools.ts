import type { Responses } from "openai/resources/responses/responses";
import {
  adminKurtiCompositionVisualizationSchema,
  adminKurtiDistributionVisualizationSchema,
  adminKurtiHeatmapVisualizationSchema,
  adminKurtiMetricsVisualizationSchema,
  adminKurtiRadarVisualizationSchema,
  adminKurtiScatterVisualizationSchema,
  adminKurtiSeriesVisualizationSchema,
  adminKurtiTableVisualizationSchema,
  adminKurtiTimelineVisualizationSchema,
  adminKurtiTreemapVisualizationSchema,
  adminKurtiWaterfallVisualizationSchema,
  type AdminKurtiVisualization,
} from "./admin-kurti-visualizations.js";

const TONES = ["red", "slate", "amber", "emerald", "blue", "violet", "cyan", "pink"] as const;
const VALUE_FORMATS = ["number", "decimal", "percent", "currency", "duration_minutes", "duration_hours"] as const;
const NULLABLE_STRING = { type: ["string", "null"] } as const;
const COMMON_PROPERTIES = {
  title: { type: "string", minLength: 1, maxLength: 120 },
  subtitle: { type: ["string", "null"], maxLength: 240 },
  sourceLabel: { type: ["string", "null"], maxLength: 120 },
  timeframe: { type: ["string", "null"], maxLength: 120 },
  note: { type: ["string", "null"], maxLength: 280 },
} as const;
const COMMON_REQUIRED = ["title", "subtitle", "sourceLabel", "timeframe", "note"];

function visualizationTool(
  name: string,
  description: string,
  properties: Record<string, unknown>,
  required: string[],
): Responses.FunctionTool {
  return {
    type: "function",
    name,
    description,
    strict: true,
    parameters: {
      type: "object",
      properties,
      required,
      additionalProperties: false,
    },
  };
}

export const ADMIN_KURTI_VISUALIZATION_TOOLS: Responses.FunctionTool[] = [
  visualizationTool(
    "render_series_visualization",
    "Renders time series, trends, multi-GM lines, bars, stacked bars, horizontal rankings, areas, or line/bar combinations. Values are positional: every point.values array must follow the exact order of series. Use null for missing observations. Supports up to 24 series and 92 periods, including daily multi-person work-time charts.",
    {
      ...COMMON_PROPERTIES,
      variant: { type: "string", enum: ["line", "area", "bar", "stacked_bar", "horizontal_bar", "combo"] },
      xLabel: { ...NULLABLE_STRING, maxLength: 80 },
      yLabel: { ...NULLABLE_STRING, maxLength: 80 },
      valueFormat: { type: "string", enum: VALUE_FORMATS },
      showLegend: { type: "boolean" },
      series: {
        type: "array", minItems: 1, maxItems: 24,
        items: {
          type: "object",
          properties: {
            key: { type: "string", minLength: 1, maxLength: 40, pattern: "^[A-Za-z0-9_-]+$" },
            label: { type: "string", minLength: 1, maxLength: 80 },
            display: { type: "string", enum: ["line", "area", "bar"] },
            tone: { type: "string", enum: TONES },
          },
          required: ["key", "label", "display", "tone"], additionalProperties: false,
        },
      },
      points: {
        type: "array", minItems: 1, maxItems: 92,
        items: {
          type: "object",
          properties: {
            label: { type: "string", minLength: 1, maxLength: 80 },
            values: { type: "array", minItems: 1, maxItems: 24, items: { type: ["number", "null"] } },
          },
          required: ["label", "values"], additionalProperties: false,
        },
      },
      referenceLines: {
        type: "array", maxItems: 5,
        items: {
          type: "object",
          properties: {
            label: { type: "string", minLength: 1, maxLength: 80 },
            value: { type: "number" },
          },
          required: ["label", "value"], additionalProperties: false,
        },
      },
    },
    [...COMMON_REQUIRED, "variant", "xLabel", "yLabel", "valueFormat", "showLegend", "series", "points", "referenceLines"],
  ),
  visualizationTool(
    "render_composition_visualization",
    "Renders a donut/pie for shares and composition or a funnel for ordered conversion/progress stages. Use only non-negative values from research tools. Prefer donut for category shares and funnel for decreasing process stages.",
    {
      ...COMMON_PROPERTIES,
      variant: { type: "string", enum: ["donut", "pie", "funnel"] },
      valueFormat: { type: "string", enum: VALUE_FORMATS },
      centerLabel: { ...NULLABLE_STRING, maxLength: 80 },
      items: {
        type: "array", minItems: 1, maxItems: 20,
        items: {
          type: "object",
          properties: {
            label: { type: "string", minLength: 1, maxLength: 80 },
            value: { type: "number", minimum: 0 },
            tone: { type: "string", enum: TONES },
          },
          required: ["label", "value", "tone"], additionalProperties: false,
        },
      },
    },
    [...COMMON_REQUIRED, "variant", "valueFormat", "centerLabel", "items"],
  ),
  visualizationTool(
    "render_scatter_visualization",
    "Renders scatter or bubble relationships such as work time vs visits, IPP vs visit volume, or target vs completion. Use size only when a real third metric exists; otherwise set size to null. Never imply causation from correlation.",
    {
      ...COMMON_PROPERTIES,
      xLabel: { type: "string", minLength: 1, maxLength: 80 },
      yLabel: { type: "string", minLength: 1, maxLength: 80 },
      xFormat: { type: "string", enum: VALUE_FORMATS },
      yFormat: { type: "string", enum: VALUE_FORMATS },
      series: {
        type: "array", minItems: 1, maxItems: 10,
        items: {
          type: "object",
          properties: {
            key: { type: "string", minLength: 1, maxLength: 40, pattern: "^[A-Za-z0-9_-]+$" },
            label: { type: "string", minLength: 1, maxLength: 80 },
            tone: { type: "string", enum: TONES },
          },
          required: ["key", "label", "tone"], additionalProperties: false,
        },
      },
      points: {
        type: "array", minItems: 2, maxItems: 120,
        items: {
          type: "object",
          properties: {
            label: { type: "string", minLength: 1, maxLength: 100 },
            seriesKey: { type: "string", minLength: 1, maxLength: 40 },
            x: { type: "number" },
            y: { type: "number" },
            size: { type: ["number", "null"], exclusiveMinimum: 0 },
          },
          required: ["label", "seriesKey", "x", "y", "size"], additionalProperties: false,
        },
      },
    },
    [...COMMON_REQUIRED, "xLabel", "yLabel", "xFormat", "yFormat", "series", "points"],
  ),
  visualizationTool(
    "render_heatmap_visualization",
    "Renders a heatmap for two-dimensional intensity or completeness: GM by day, market by RED month, question by answer category, weekday by hour, or status matrices. Values are positional and each row must match xLabels exactly; missing cells are null.",
    {
      ...COMMON_PROPERTIES,
      valueFormat: { type: "string", enum: VALUE_FORMATS },
      xLabels: { type: "array", minItems: 1, maxItems: 40, items: { type: "string", minLength: 1, maxLength: 60 } },
      rows: {
        type: "array", minItems: 1, maxItems: 30,
        items: {
          type: "object",
          properties: {
            label: { type: "string", minLength: 1, maxLength: 80 },
            values: { type: "array", minItems: 1, maxItems: 40, items: { type: ["number", "null"] } },
          },
          required: ["label", "values"], additionalProperties: false,
        },
      },
    },
    [...COMMON_REQUIRED, "valueFormat", "xLabels", "rows"],
  ),
  visualizationTool(
    "render_metrics_visualization",
    "Renders a clean KPI/progress card grid for totals, averages, deltas, completion, status, or compact executive summaries. Use displayValue only for text/status values; otherwise provide numeric value and its format. Progress is a real 0-100 percentage or null.",
    {
      ...COMMON_PROPERTIES,
      columns: { type: "string", enum: ["2", "3", "4"] },
      items: {
        type: "array", minItems: 1, maxItems: 12,
        items: {
          type: "object",
          properties: {
            label: { type: "string", minLength: 1, maxLength: 90 },
            value: { type: ["number", "null"] },
            displayValue: { type: ["string", "null"], maxLength: 80 },
            valueFormat: { type: "string", enum: VALUE_FORMATS },
            delta: { type: ["number", "null"] },
            deltaLabel: { type: ["string", "null"], maxLength: 100 },
            progress: { type: ["number", "null"], minimum: 0, maximum: 100 },
            status: { type: "string", enum: ["neutral", "positive", "warning", "critical"] },
          },
          required: ["label", "value", "displayValue", "valueFormat", "delta", "deltaLabel", "progress", "status"],
          additionalProperties: false,
        },
      },
    },
    [...COMMON_REQUIRED, "columns", "items"],
  ),
  visualizationTool(
    "render_table_visualization",
    "Renders a compact structured table when exact values and labels matter more than a chart. Values are positional and every row must match the column order. Use status only to highlight meaningful exceptions, not decoration.",
    {
      ...COMMON_PROPERTIES,
      columns: {
        type: "array", minItems: 1, maxItems: 10,
        items: {
          type: "object",
          properties: {
            key: { type: "string", minLength: 1, maxLength: 40, pattern: "^[A-Za-z0-9_-]+$" },
            label: { type: "string", minLength: 1, maxLength: 80 },
            align: { type: "string", enum: ["left", "center", "right"] },
          },
          required: ["key", "label", "align"], additionalProperties: false,
        },
      },
      rows: {
        type: "array", minItems: 1, maxItems: 60,
        items: {
          type: "object",
          properties: {
            label: { type: "string", minLength: 1, maxLength: 100 },
            values: { type: "array", minItems: 1, maxItems: 10, items: { type: ["string", "null"], maxLength: 240 } },
            status: { type: "string", enum: ["neutral", "positive", "warning", "critical"] },
          },
          required: ["label", "values", "status"], additionalProperties: false,
        },
      },
    },
    [...COMMON_REQUIRED, "columns", "rows"],
  ),
  visualizationTool(
    "render_timeline_visualization",
    "Renders a chronological event/status timeline for campaign changes, audit history, requests, visits, milestones, or a workday sequence. Order items chronologically and use warning/critical only when the underlying state supports it.",
    {
      ...COMMON_PROPERTIES,
      items: {
        type: "array", minItems: 1, maxItems: 50,
        items: {
          type: "object",
          properties: {
            date: { type: "string", minLength: 1, maxLength: 60 },
            label: { type: "string", minLength: 1, maxLength: 100 },
            description: { type: ["string", "null"], maxLength: 240 },
            value: { type: ["string", "null"], maxLength: 100 },
            status: { type: "string", enum: ["completed", "active", "pending", "warning", "critical"] },
          },
          required: ["date", "label", "description", "value", "status"], additionalProperties: false,
        },
      },
    },
    [...COMMON_REQUIRED, "items"],
  ),
  visualizationTool(
    "render_radar_visualization",
    "Renders a radar comparison across 3-12 comparable dimensions such as section scores, quality pillars, or normalized performance components. Use only metrics with compatible scales; set maximum to the real common maximum or null for the observed maximum.",
    {
      ...COMMON_PROPERTIES,
      valueFormat: { type: "string", enum: VALUE_FORMATS },
      maximum: { type: ["number", "null"], exclusiveMinimum: 0 },
      axes: { type: "array", minItems: 3, maxItems: 12, items: { type: "string", minLength: 1, maxLength: 60 } },
      series: {
        type: "array", minItems: 1, maxItems: 6,
        items: {
          type: "object",
          properties: {
            label: { type: "string", minLength: 1, maxLength: 80 },
            tone: { type: "string", enum: TONES },
            values: { type: "array", minItems: 3, maxItems: 12, items: { type: "number", minimum: 0 } },
          },
          required: ["label", "tone", "values"], additionalProperties: false,
        },
      },
    },
    [...COMMON_REQUIRED, "valueFormat", "maximum", "axes", "series"],
  ),
  visualizationTool(
    "render_distribution_visualization",
    "Renders a histogram or box plot from raw numeric observations. Use histogram for frequency shape and box_plot for median/quartile/spread comparisons. Pass raw values without manually calculating bins or quartiles. Histograms support at most three series; total observations across series must not exceed 600.",
    {
      ...COMMON_PROPERTIES,
      variant: { type: "string", enum: ["histogram", "box_plot"] },
      xLabel: { type: "string", minLength: 1, maxLength: 80 },
      valueFormat: { type: "string", enum: VALUE_FORMATS },
      binCount: { type: ["integer", "null"], minimum: 4, maximum: 20 },
      showOutliers: { type: "boolean" },
      series: {
        type: "array", minItems: 1, maxItems: 12,
        items: {
          type: "object",
          properties: {
            label: { type: "string", minLength: 1, maxLength: 80 },
            tone: { type: "string", enum: TONES },
            values: { type: "array", minItems: 1, maxItems: 120, items: { type: "number" } },
          },
          required: ["label", "tone", "values"], additionalProperties: false,
        },
      },
    },
    [...COMMON_REQUIRED, "variant", "xLabel", "valueFormat", "binCount", "showOutliers", "series"],
  ),
  visualizationTool(
    "render_waterfall_visualization",
    "Renders an additive waterfall bridge from one starting value through signed contribution steps to a computed ending value. Use only when start plus every signed step exactly explains the end; do not use for overlapping or independent categories.",
    {
      ...COMMON_PROPERTIES,
      valueFormat: { type: "string", enum: VALUE_FORMATS },
      startLabel: { type: "string", minLength: 1, maxLength: 80 },
      startValue: { type: "number" },
      steps: {
        type: "array", minItems: 1, maxItems: 20,
        items: {
          type: "object",
          properties: {
            label: { type: "string", minLength: 1, maxLength: 80 },
            value: { type: "number" },
          },
          required: ["label", "value"], additionalProperties: false,
        },
      },
      endLabel: { type: "string", minLength: 1, maxLength: 80 },
      showConnectors: { type: "boolean" },
    },
    [...COMMON_REQUIRED, "valueFormat", "startLabel", "startValue", "steps", "endLabel", "showConnectors"],
  ),
  visualizationTool(
    "render_treemap_visualization",
    "Renders a treemap for 2-40 non-negative parts of one whole when a donut would be too crowded. Use a single denominator and unit, preserve uncategorized values honestly, and prefer horizontal bars when exact ranking is more important than contribution area.",
    {
      ...COMMON_PROPERTIES,
      valueFormat: { type: "string", enum: VALUE_FORMATS },
      items: {
        type: "array", minItems: 2, maxItems: 40,
        items: {
          type: "object",
          properties: {
            label: { type: "string", minLength: 1, maxLength: 80 },
            value: { type: "number", minimum: 0 },
            tone: { type: "string", enum: TONES },
          },
          required: ["label", "value", "tone"], additionalProperties: false,
        },
      },
    },
    [...COMMON_REQUIRED, "valueFormat", "items"],
  ),
];

export const ADMIN_KURTI_VISUALIZATION_TOOL_NAMES = new Set(
  ADMIN_KURTI_VISUALIZATION_TOOLS.map((tool) => tool.name),
);

export function parseAdminKurtiVisualizationToolCall(name: string, rawArguments: string): AdminKurtiVisualization {
  const parsed = rawArguments.trim() ? JSON.parse(rawArguments) as Record<string, unknown> : {};
  switch (name) {
    case "render_series_visualization":
      return adminKurtiSeriesVisualizationSchema.parse({ ...parsed, kind: "series" });
    case "render_composition_visualization":
      return adminKurtiCompositionVisualizationSchema.parse({ ...parsed, kind: "composition" });
    case "render_scatter_visualization":
      return adminKurtiScatterVisualizationSchema.parse({ ...parsed, kind: "scatter" });
    case "render_heatmap_visualization":
      return adminKurtiHeatmapVisualizationSchema.parse({ ...parsed, kind: "heatmap" });
    case "render_metrics_visualization":
      return adminKurtiMetricsVisualizationSchema.parse({ ...parsed, kind: "metrics" });
    case "render_table_visualization":
      return adminKurtiTableVisualizationSchema.parse({ ...parsed, kind: "table" });
    case "render_timeline_visualization":
      return adminKurtiTimelineVisualizationSchema.parse({ ...parsed, kind: "timeline" });
    case "render_radar_visualization":
      return adminKurtiRadarVisualizationSchema.parse({ ...parsed, kind: "radar" });
    case "render_distribution_visualization":
      return adminKurtiDistributionVisualizationSchema.parse({ ...parsed, kind: "distribution" });
    case "render_waterfall_visualization":
      return adminKurtiWaterfallVisualizationSchema.parse({ ...parsed, kind: "waterfall" });
    case "render_treemap_visualization":
      return adminKurtiTreemapVisualizationSchema.parse({ ...parsed, kind: "treemap" });
    default:
      throw new Error(`Unknown Admin Kurti visualization tool: ${name}`);
  }
}
