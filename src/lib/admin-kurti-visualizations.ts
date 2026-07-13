import { z } from "zod";

const VISUALIZATION_MARKER_PATTERN = /\n?<!--admin-kurti-visualizations-v2:([A-Za-z0-9_-]+)-->\s*$/;
const toneSchema = z.enum(["red", "slate", "amber", "emerald", "blue", "violet", "cyan", "pink"]);
const valueFormatSchema = z.enum(["number", "decimal", "percent", "currency", "duration_minutes", "duration_hours"]);

const commonFields = {
  title: z.string().trim().min(1).max(120),
  subtitle: z.string().trim().max(240).nullable(),
  sourceLabel: z.string().trim().max(120).nullable(),
  timeframe: z.string().trim().max(120).nullable(),
  note: z.string().trim().max(280).nullable(),
};

const seriesDefinitionSchema = z.object({
  key: z.string().trim().min(1).max(40).regex(/^[A-Za-z0-9_-]+$/),
  label: z.string().trim().min(1).max(80),
  display: z.enum(["line", "area", "bar"]),
  tone: toneSchema,
}).strict();

const seriesPointSchema = z.object({
  label: z.string().trim().min(1).max(80),
  values: z.array(z.number().finite().nullable()).min(1).max(24),
}).strict();

const referenceLineSchema = z.object({
  label: z.string().trim().min(1).max(80),
  value: z.number().finite(),
}).strict();

export const adminKurtiSeriesVisualizationSchema = z.object({
  kind: z.literal("series"),
  ...commonFields,
  variant: z.enum(["line", "area", "bar", "stacked_bar", "horizontal_bar", "combo"]),
  xLabel: z.string().trim().max(80).nullable(),
  yLabel: z.string().trim().max(80).nullable(),
  valueFormat: valueFormatSchema,
  showLegend: z.boolean(),
  series: z.array(seriesDefinitionSchema).min(1).max(24),
  points: z.array(seriesPointSchema).min(1).max(92),
  referenceLines: z.array(referenceLineSchema).max(5),
}).strict().superRefine((visualization, context) => {
  const keys = visualization.series.map((series) => series.key);
  if (new Set(keys).size !== keys.length) {
    context.addIssue({ code: "custom", path: ["series"], message: "Series keys must be unique." });
  }
  visualization.points.forEach((point, index) => {
    if (point.values.length !== visualization.series.length) {
      context.addIssue({
        code: "custom",
        path: ["points", index, "values"],
        message: "Point values must match the declared series order and count.",
      });
    }
  });
  if (!visualization.points.some((point) => point.values.some((value) => value !== null))) {
    context.addIssue({ code: "custom", path: ["points"], message: "At least one numeric value is required." });
  }
});

const compositionItemSchema = z.object({
  label: z.string().trim().min(1).max(80),
  value: z.number().finite().nonnegative(),
  tone: toneSchema,
}).strict();

export const adminKurtiCompositionVisualizationSchema = z.object({
  kind: z.literal("composition"),
  ...commonFields,
  variant: z.enum(["donut", "pie", "funnel"]),
  valueFormat: valueFormatSchema,
  centerLabel: z.string().trim().max(80).nullable(),
  items: z.array(compositionItemSchema).min(1).max(20),
}).strict().superRefine((visualization, context) => {
  if (!visualization.items.some((item) => item.value > 0)) {
    context.addIssue({ code: "custom", path: ["items"], message: "At least one item must be greater than zero." });
  }
});

const scatterSeriesSchema = z.object({
  key: z.string().trim().min(1).max(40).regex(/^[A-Za-z0-9_-]+$/),
  label: z.string().trim().min(1).max(80),
  tone: toneSchema,
}).strict();

const scatterPointSchema = z.object({
  label: z.string().trim().min(1).max(100),
  seriesKey: z.string().trim().min(1).max(40),
  x: z.number().finite(),
  y: z.number().finite(),
  size: z.number().finite().positive().nullable(),
}).strict();

export const adminKurtiScatterVisualizationSchema = z.object({
  kind: z.literal("scatter"),
  ...commonFields,
  xLabel: z.string().trim().min(1).max(80),
  yLabel: z.string().trim().min(1).max(80),
  xFormat: valueFormatSchema,
  yFormat: valueFormatSchema,
  series: z.array(scatterSeriesSchema).min(1).max(10),
  points: z.array(scatterPointSchema).min(2).max(120),
}).strict().superRefine((visualization, context) => {
  const keys = visualization.series.map((series) => series.key);
  const allowed = new Set(keys);
  if (allowed.size !== keys.length) {
    context.addIssue({ code: "custom", path: ["series"], message: "Series keys must be unique." });
  }
  visualization.points.forEach((point, index) => {
    if (!allowed.has(point.seriesKey)) {
      context.addIssue({ code: "custom", path: ["points", index, "seriesKey"], message: "Unknown series key." });
    }
  });
});

const heatmapRowSchema = z.object({
  label: z.string().trim().min(1).max(80),
  values: z.array(z.number().finite().nullable()).min(1).max(40),
}).strict();

export const adminKurtiHeatmapVisualizationSchema = z.object({
  kind: z.literal("heatmap"),
  ...commonFields,
  valueFormat: valueFormatSchema,
  xLabels: z.array(z.string().trim().min(1).max(60)).min(1).max(40),
  rows: z.array(heatmapRowSchema).min(1).max(30),
}).strict().superRefine((visualization, context) => {
  visualization.rows.forEach((row, index) => {
    if (row.values.length !== visualization.xLabels.length) {
      context.addIssue({
        code: "custom",
        path: ["rows", index, "values"],
        message: "Each heatmap row must match the x-label count.",
      });
    }
  });
});

const metricItemSchema = z.object({
  label: z.string().trim().min(1).max(90),
  value: z.number().finite().nullable(),
  displayValue: z.string().trim().max(80).nullable(),
  valueFormat: valueFormatSchema,
  delta: z.number().finite().nullable(),
  deltaLabel: z.string().trim().max(100).nullable(),
  progress: z.number().finite().min(0).max(100).nullable(),
  status: z.enum(["neutral", "positive", "warning", "critical"]),
}).strict();

export const adminKurtiMetricsVisualizationSchema = z.object({
  kind: z.literal("metrics"),
  ...commonFields,
  columns: z.enum(["2", "3", "4"]),
  items: z.array(metricItemSchema).min(1).max(12),
}).strict();

const tableColumnSchema = z.object({
  key: z.string().trim().min(1).max(40).regex(/^[A-Za-z0-9_-]+$/),
  label: z.string().trim().min(1).max(80),
  align: z.enum(["left", "center", "right"]),
}).strict();

const tableRowSchema = z.object({
  label: z.string().trim().min(1).max(100),
  values: z.array(z.string().trim().max(240).nullable()).min(1).max(10),
  status: z.enum(["neutral", "positive", "warning", "critical"]),
}).strict();

export const adminKurtiTableVisualizationSchema = z.object({
  kind: z.literal("table"),
  ...commonFields,
  columns: z.array(tableColumnSchema).min(1).max(10),
  rows: z.array(tableRowSchema).min(1).max(60),
}).strict().superRefine((visualization, context) => {
  visualization.rows.forEach((row, index) => {
    if (row.values.length !== visualization.columns.length) {
      context.addIssue({
        code: "custom",
        path: ["rows", index, "values"],
        message: "Each table row must match the declared column order and count.",
      });
    }
  });
});

const timelineItemSchema = z.object({
  date: z.string().trim().min(1).max(60),
  label: z.string().trim().min(1).max(100),
  description: z.string().trim().max(240).nullable(),
  value: z.string().trim().max(100).nullable(),
  status: z.enum(["completed", "active", "pending", "warning", "critical"]),
}).strict();

export const adminKurtiTimelineVisualizationSchema = z.object({
  kind: z.literal("timeline"),
  ...commonFields,
  items: z.array(timelineItemSchema).min(1).max(50),
}).strict();

const radarSeriesSchema = z.object({
  label: z.string().trim().min(1).max(80),
  tone: toneSchema,
  values: z.array(z.number().finite().nonnegative()).min(3).max(12),
}).strict();

export const adminKurtiRadarVisualizationSchema = z.object({
  kind: z.literal("radar"),
  ...commonFields,
  valueFormat: valueFormatSchema,
  maximum: z.number().finite().positive().nullable(),
  axes: z.array(z.string().trim().min(1).max(60)).min(3).max(12),
  series: z.array(radarSeriesSchema).min(1).max(6),
}).strict().superRefine((visualization, context) => {
  visualization.series.forEach((series, index) => {
    if (series.values.length !== visualization.axes.length) {
      context.addIssue({
        code: "custom",
        path: ["series", index, "values"],
        message: "Each radar series must match the axis order and count.",
      });
    }
    if (visualization.maximum !== null && series.values.some((value) => value > visualization.maximum!)) {
      context.addIssue({
        code: "custom",
        path: ["series", index, "values"],
        message: "Radar values must not exceed the declared maximum.",
      });
    }
  });
});

export const adminKurtiVisualizationSchema = z.discriminatedUnion("kind", [
  adminKurtiSeriesVisualizationSchema,
  adminKurtiCompositionVisualizationSchema,
  adminKurtiScatterVisualizationSchema,
  adminKurtiHeatmapVisualizationSchema,
  adminKurtiMetricsVisualizationSchema,
  adminKurtiTableVisualizationSchema,
  adminKurtiTimelineVisualizationSchema,
  adminKurtiRadarVisualizationSchema,
]);

const adminKurtiVisualizationsSchema = z.array(adminKurtiVisualizationSchema).max(8);

export type AdminKurtiVisualization = z.infer<typeof adminKurtiVisualizationSchema>;
export type AdminKurtiSeriesVisualization = z.infer<typeof adminKurtiSeriesVisualizationSchema>;
export type AdminKurtiCompositionVisualization = z.infer<typeof adminKurtiCompositionVisualizationSchema>;
export type AdminKurtiScatterVisualization = z.infer<typeof adminKurtiScatterVisualizationSchema>;
export type AdminKurtiHeatmapVisualization = z.infer<typeof adminKurtiHeatmapVisualizationSchema>;
export type AdminKurtiMetricsVisualization = z.infer<typeof adminKurtiMetricsVisualizationSchema>;
export type AdminKurtiTableVisualization = z.infer<typeof adminKurtiTableVisualizationSchema>;
export type AdminKurtiTimelineVisualization = z.infer<typeof adminKurtiTimelineVisualizationSchema>;
export type AdminKurtiRadarVisualization = z.infer<typeof adminKurtiRadarVisualizationSchema>;

export function appendAdminKurtiVisualizations(content: string, visualizations: AdminKurtiVisualization[]): string {
  if (visualizations.length === 0) return content;
  const validated = adminKurtiVisualizationsSchema.parse(visualizations);
  const encoded = Buffer.from(JSON.stringify(validated), "utf8").toString("base64url");
  return `${content.trimEnd()}\n<!--admin-kurti-visualizations-v2:${encoded}-->`;
}

export function parseAdminKurtiStoredVisualizations(content: string): {
  content: string;
  visualizations: AdminKurtiVisualization[];
} {
  const match = content.match(VISUALIZATION_MARKER_PATTERN);
  if (!match || typeof match.index !== "number") return { content, visualizations: [] };
  const visibleContent = content.slice(0, match.index).trimEnd();
  try {
    const decoded = Buffer.from(match[1]!, "base64url").toString("utf8");
    const visualizations = adminKurtiVisualizationsSchema.parse(JSON.parse(decoded) as unknown);
    return { content: visibleContent, visualizations };
  } catch {
    return { content: visibleContent, visualizations: [] };
  }
}
