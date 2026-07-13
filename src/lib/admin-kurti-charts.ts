import { z } from "zod";

const CHART_MARKER_PATTERN = /\n?<!--admin-kurti-charts-v1:([A-Za-z0-9_-]+)-->\s*$/;

const chartSeriesSchema = z.object({
  key: z.string().trim().min(1).max(40).regex(/^[A-Za-z0-9_-]+$/),
  label: z.string().trim().min(1).max(80),
}).strict();

const chartValueSchema = z.object({
  seriesKey: z.string().trim().min(1).max(40).regex(/^[A-Za-z0-9_-]+$/),
  value: z.number().finite().nullable(),
}).strict();

const chartPointSchema = z.object({
  label: z.string().trim().min(1).max(80),
  values: z.array(chartValueSchema).min(1).max(4),
}).strict();

export const adminKurtiChartSpecSchema = z.object({
  type: z.enum(["line", "bar"]),
  title: z.string().trim().min(1).max(120),
  subtitle: z.string().trim().max(240).nullable(),
  xLabel: z.string().trim().max(80).nullable(),
  yLabel: z.string().trim().max(80).nullable(),
  valueFormat: z.enum(["number", "decimal", "percent", "currency"]),
  series: z.array(chartSeriesSchema).min(1).max(4),
  points: z.array(chartPointSchema).min(2).max(40),
}).strict().superRefine((chart, context) => {
  const seriesKeys = chart.series.map((series) => series.key);
  if (new Set(seriesKeys).size !== seriesKeys.length) {
    context.addIssue({
      code: "custom",
      path: ["series"],
      message: "Series keys must be unique.",
    });
  }

  const allowedKeys = new Set(seriesKeys);
  chart.points.forEach((point, pointIndex) => {
    const pointKeys = point.values.map((value) => value.seriesKey);
    if (new Set(pointKeys).size !== pointKeys.length) {
      context.addIssue({
        code: "custom",
        path: ["points", pointIndex, "values"],
        message: "Each series may occur only once per point.",
      });
    }
    if (pointKeys.length !== seriesKeys.length || pointKeys.some((key) => !allowedKeys.has(key))) {
      context.addIssue({
        code: "custom",
        path: ["points", pointIndex, "values"],
        message: "Every point must contain exactly one value for every declared series.",
      });
    }
  });

  if (!chart.points.some((point) => point.values.some((value) => value.value !== null))) {
    context.addIssue({
      code: "custom",
      path: ["points"],
      message: "A chart needs at least one numeric value.",
    });
  }
});

const adminKurtiChartsSchema = z.array(adminKurtiChartSpecSchema).max(3);

export type AdminKurtiChartSpec = z.infer<typeof adminKurtiChartSpecSchema>;

export function parseAdminKurtiChartArguments(rawArguments: string): AdminKurtiChartSpec {
  const parsed = rawArguments.trim() ? JSON.parse(rawArguments) as unknown : {};
  return adminKurtiChartSpecSchema.parse(parsed);
}

export function appendAdminKurtiCharts(content: string, charts: AdminKurtiChartSpec[]): string {
  if (charts.length === 0) return content;
  const validatedCharts = adminKurtiChartsSchema.parse(charts);
  const encoded = Buffer.from(JSON.stringify(validatedCharts), "utf8").toString("base64url");
  return `${content.trimEnd()}\n<!--admin-kurti-charts-v1:${encoded}-->`;
}

export function parseAdminKurtiStoredContent(content: string): {
  content: string;
  charts: AdminKurtiChartSpec[];
} {
  const match = content.match(CHART_MARKER_PATTERN);
  if (!match || typeof match.index !== "number") return { content, charts: [] };

  const visibleContent = content.slice(0, match.index).trimEnd();
  try {
    const decoded = Buffer.from(match[1]!, "base64url").toString("utf8");
    const charts = adminKurtiChartsSchema.parse(JSON.parse(decoded) as unknown);
    return { content: visibleContent, charts };
  } catch {
    return { content: visibleContent, charts: [] };
  }
}
