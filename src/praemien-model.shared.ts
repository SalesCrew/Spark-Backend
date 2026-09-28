import { calculateTieredPillarReward } from "./lib/praemien-rewards.js";

export type MetricUnit = "percent" | "points" | "count";
export type MetricMethod =
  | "manual"
  | "answer_sum"
  | "availability"
  | "sum"
  | "difference"
  | "average"
  | "ratio"
  | "steps";
export type ModelSource = {
  questionId: string;
  section: string;
  scoreKey: string;
  factor: boolean;
  weight: number;
  label: string;
  minFrequency: number;
  chains: string[];
  counting: "latest" | "once";
};
export type ModelMetric = {
  key: string;
  label: string;
  unit: MetricUnit;
  method: MetricMethod;
  inputs: string[];
  target: number | null;
  sources: ModelSource[];
  steps: { at: number; value: number }[];
};
export type ModelTier = {
  key: string;
  label: string;
  group: string;
  rewardEur: number;
  conditions: {
    metricKey: string;
    operator: "gte" | "lte" | "eq";
    value: number;
  }[];
};
export type ModelPillar = {
  key: string;
  name: string;
  kind: "displays" | "distribution" | "flex" | "quality" | "custom";
  color: string;
  maxRewardEur: number;
  payoutMode: "highest" | "groups";
  metrics: ModelMetric[];
  tiers: ModelTier[];
};
export type WaveModel = {
  version: 1;
  provenance: string;
  pillars: ModelPillar[];
};
export type MetricEntry = {
  gmId: string;
  pillarKey: string;
  metricKey: string;
  value: number | null;
  target: number | null;
  note: string;
  actorName?: string;
  updatedAt?: string;
};
export type MetricResult = {
  key: string;
  label: string;
  unit: MetricUnit;
  value: number | null;
  automatic: number | null;
  target: number | null;
  origin: "manual" | "automatic" | "pending";
  note: string;
  counted: number;
  excluded: number;
  actorName?: string | undefined;
  updatedAt?: string | undefined;
};
export type PillarResult = {
  key: string;
  name: string;
  color: string;
  earned: number;
  maximum: number;
  pending: boolean;
  metrics: MetricResult[];
  achieved: string[];
  next: string | null;
};
export type GmResult = {
  gmId: string;
  name: string;
  active: boolean;
  earned: number;
  maximum: number;
  pending: boolean;
  rank: number;
  pillars: PillarResult[];
};
export type Observation = {
  gmId: string;
  marketId: string;
  questionId: string;
  section: string;
  date: string;
  id: string;
  numeric: number | null;
  options: string[];
  frequency: number;
  chain: string;
};
export const money = (n: number) =>
  Math.round((n + Number.EPSILON) * 100) / 100;

export function validateModel(model: WaveModel): string[] {
  const errors: string[] = [];
  const unique = (keys: string[]) => new Set(keys).size === keys.length;
  if (!model.pillars.length || model.pillars.length > 12)
    errors.push("Bitte 1 bis 12 Säulen konfigurieren.");
  if (!unique(model.pillars.map((p) => p.key)))
    errors.push("Säulenschlüssel müssen eindeutig sein.");
  const sourceOwners = new Map<string, string>();
  for (const p of model.pillars) {
    if (!p.metrics.length || !unique(p.metrics.map((m) => m.key)))
      errors.push(`${p.name}: eindeutige Messgrößen erforderlich.`);
    if (!unique(p.tiers.map((t) => t.key)))
      errors.push(`${p.name}: Stufenschlüssel doppelt.`);
    const available = new Set<string>();
    for (const m of p.metrics) {
      if (m.inputs.some((key) => !available.has(key)))
        errors.push(
          `${p.name}/${m.label}: Eingaben müssen vorher definiert sein (keine Zyklen).`,
        );
      if (
        ["sum", "average"].includes(m.method) &&
        m.inputs.some(
          (key) => p.metrics.find((x) => x.key === key)?.unit !== m.unit,
        )
      )
        errors.push(`${m.label}: nur gleiche Einheiten addieren/mitteln.`);
      if (
        ["sum", "average", "steps", "ratio", "difference"].includes(m.method) &&
        !m.inputs.length
      )
        errors.push(`${m.label}: Eingaben fehlen.`);
      if (m.method === "difference" && m.inputs.length !== 2)
        errors.push(`${m.label}: genau zwei Eingaben erforderlich.`);
      if (m.method === "steps" && (m.inputs.length !== 1 || !m.steps.length))
        errors.push(`${m.label}: eine Eingabe und Schwellen erforderlich.`);
      if (
        m.method === "ratio" &&
        (m.inputs.length !== 1 || m.unit !== "percent")
      )
        errors.push(
          `${m.label}: Quote benötigt eine Eingabe und Prozent als Einheit.`,
        );
      if (m.method === "availability" && m.unit !== "percent")
        errors.push(`${m.label}: Verfügbarkeit ist eine Prozentquote.`);
      if (
        ["answer_sum", "availability"].includes(m.method) &&
        !m.sources.length
      )
        errors.push(`${m.label}: Fragequelle fehlt.`);
      if (m.method === "availability" && m.sources.length !== 1)
        errors.push(
          `${m.label}: Produktquoten einzeln definieren, anschließend mitteln.`,
        );
      if (!unique(m.steps.map((s) => String(s.at))))
        errors.push(`${m.label}: Schwellen doppelt.`);
      if (
        !unique(
          m.sources.map((s) => `${s.section}/${s.questionId}/${s.scoreKey}`),
        )
      )
        errors.push(`${m.label}: Quelle doppelt zugeordnet.`);
      for (const s of m.sources) {
        const identity = `${s.section}/${s.questionId}/${s.scoreKey}`;
        const owner = `${p.key}/${m.key}`;
        if (sourceOwners.has(identity) && sourceOwners.get(identity) !== owner)
          errors.push(
            `${m.label}: dieselbe Quelle ist bereits einer anderen Messgröße zugeordnet.`,
          );
        sourceOwners.set(identity, owner);
      }
      available.add(m.key);
    }
    for (const t of p.tiers) {
      if (
        !t.conditions.length ||
        t.conditions.some((c) => !available.has(c.metricKey))
      )
        errors.push(
          `${p.name}/${t.label}: Bedingung fehlt oder Messgröße unbekannt.`,
        );
      if (p.payoutMode === "groups" && !t.group.trim())
        errors.push(`${p.name}/${t.label}: Teilzielgruppe fehlt.`);
    }
    const groupMax = new Map<string, number>();
    for (const t of p.tiers)
      groupMax.set(
        p.payoutMode === "groups" ? t.group : "all",
        Math.max(
          groupMax.get(p.payoutMode === "groups" ? t.group : "all") ?? 0,
          t.rewardEur,
        ),
      );
    if (
      money([...groupMax.values()].reduce((s, x) => s + x, 0)) > p.maxRewardEur
    )
      errors.push(`${p.name}: Stufen überschreiten die Maximalprämie.`);
  }
  return errors;
}

const normalized = (s: string) =>
  s.normalize("NFKC").trim().toLocaleLowerCase("de-AT");
function sourceValue(row: Observation, source: ModelSource): number {
  if (source.factor)
    return row.numeric === null ? 0 : row.numeric * source.weight;
  return row.options.some((o) => normalized(o) === normalized(source.scoreKey))
    ? source.weight
    : 0;
}

function automaticValue(
  m: ModelMetric,
  gmId: string,
  observations: Observation[],
  values: Map<string, MetricResult>,
  target: number | null,
) {
  let counted = 0,
    excluded = 0;
  if (m.method === "manual") return { value: null, counted, excluded };
  if (m.method === "answer_sum" || m.method === "availability") {
    let total = 0;
    for (const s of m.sources) {
      const eligible = observations.filter(
        (r) =>
          r.gmId === gmId &&
          r.questionId === s.questionId &&
          r.section === s.section &&
          r.frequency >= s.minFrequency &&
          (!s.chains.length ||
            s.chains.some((c) => normalized(c) === normalized(r.chain))),
      );
      const byMarket = new Map<string, Observation>();
      for (const row of eligible) {
        const previous = byMarket.get(row.marketId);
        // Snapshot time, not edit time: editing an old visit cannot replace a newer visit.
        if (
          !previous ||
          (s.counting === "once"
            ? sourceValue(row, s) > sourceValue(previous, s)
            : `${row.date}/${row.id}` > `${previous.date}/${previous.id}`)
        )
          byMarket.set(row.marketId, row);
      }
      counted += byMarket.size;
      excluded += eligible.length - byMarket.size;
      // No observation is not a measured zero. A source missing for this GM
      // must remain open instead of quietly lowering or paying their bonus.
      if (
        !byMarket.size ||
        (s.factor && [...byMarket.values()].some((row) => row.numeric === null))
      )
        return { value: null, counted, excluded };
      total += [...byMarket.values()].reduce(
        (sum, row) =>
          sum +
          (m.method === "availability"
            ? sourceValue(row, s) > 0
              ? 1
              : 0
            : sourceValue(row, s)),
        0,
      );
    }
    if (m.method === "availability")
      return {
        value: target && target > 0 ? (total / target) * 100 : null,
        counted,
        excluded,
      };
    return { value: total, counted, excluded };
  }
  const inputs = m.inputs.map((k) => values.get(k)?.value ?? null);
  if (inputs.some((v) => v === null)) return { value: null, counted, excluded };
  const numbers = inputs as number[];
  if (m.method === "ratio")
    return {
      value: target && target > 0 ? (numbers[0]! / target) * 100 : null,
      counted,
      excluded,
    };
  if (m.method === "steps")
    return {
      value:
        m.steps
          .filter((s) => numbers[0]! >= s.at)
          .sort((a, b) => a.at - b.at)
          .at(-1)?.value ?? 0,
      counted,
      excluded,
    };
  if (m.method === "difference")
    return { value: numbers[0]! - numbers[1]!, counted, excluded };
  const sum = numbers.reduce((s, n) => s + n, 0);
  return {
    value: m.method === "average" ? sum / numbers.length : sum,
    counted,
    excluded,
  };
}

export function evaluateModel(
  model: WaveModel,
  roster: { gmId: string; name: string; active: boolean }[],
  entries: MetricEntry[],
  observations: Observation[],
): GmResult[] {
  const errors = validateModel(model);
  if (errors.length) throw new Error(errors.join(" "));
  const entryMap = new Map(
    entries.map((e) => [`${e.gmId}/${e.pillarKey}/${e.metricKey}`, e]),
  );
  const rows = roster
    .map((gm) => {
      const pillars = model.pillars.map((p) => {
        const results = new Map<string, MetricResult>();
        for (const m of p.metrics) {
          const entry = entryMap.get(`${gm.gmId}/${p.key}/${m.key}`);
          const target = entry?.target ?? m.target;
          const auto = automaticValue(
            m,
            gm.gmId,
            observations,
            results,
            target,
          );
          const value = entry?.value ?? auto.value;
          results.set(m.key, {
            key: m.key,
            label: m.label,
            unit: m.unit,
            value,
            automatic: auto.value,
            target,
            origin:
              entry?.value != null
                ? "manual"
                : value === null
                  ? "pending"
                  : "automatic",
            note: entry?.note ?? "",
            counted: auto.counted,
            excluded: auto.excluded,
            actorName: entry?.actorName,
            updatedAt: entry?.updatedAt,
          });
        }
        const metricValues = Object.fromEntries(
          [...results]
            .filter(([, r]) => r.value !== null)
            .map(([k, r]) => [k, r.value as number]),
        );
        const groups =
          p.payoutMode === "groups"
            ? [...new Set(p.tiers.map((t) => t.group))]
            : ["all"];
        let earned = 0;
        const achieved: string[] = [];
        let next: string | null = null;
        for (const group of groups) {
          const tiers = p.tiers.filter(
            (t) => p.payoutMode !== "groups" || t.group === group,
          );
          const payout = calculateTieredPillarReward({
            pillarId: p.key,
            points: 0,
            targetPoints: null,
            rewardEur: 0,
            maxRewardEur: p.maxRewardEur,
            metricValues,
            tiers: tiers.map((t, i) => ({
              tierId: t.key,
              label: t.label,
              orderIndex: i,
              rewardEur: t.rewardEur,
              conditions: t.conditions.map((c) => ({
                metricKey: c.metricKey,
                operator: c.operator,
                thresholdValue: c.value,
              })),
            })),
          });
          // Highest EURO tier, not whichever row was placed last in the editor.
          const best = payout.tierResults
            .filter((t) => t.achieved)
            .sort((a, b) => b.rewardEur - a.rewardEur)[0];
          earned += best?.rewardEur ?? 0;
          if (best) achieved.push(best.label);
          const nextCandidate = payout.tierResults
            .filter((t) => !t.achieved && t.rewardEur > (best?.rewardEur ?? 0))
            .sort((a, b) => a.rewardEur - b.rewardEur)[0];
          next ??= nextCandidate?.label ?? null;
        }
        const used = new Set(
          p.tiers.flatMap((t) => t.conditions.map((c) => c.metricKey)),
        );
        return {
          key: p.key,
          name: p.name,
          color: p.color,
          earned: money(Math.min(earned, p.maxRewardEur)),
          maximum: p.maxRewardEur,
          pending:
            p.tiers.length === 0 ||
            [...used].some((k) => results.get(k)?.value === null),
          metrics: [...results.values()],
          achieved,
          next,
        };
      });
      return {
        ...gm,
        earned: money(pillars.reduce((s, p) => s + p.earned, 0)),
        maximum: money(pillars.reduce((s, p) => s + p.maximum, 0)),
        pending: pillars.some((p) => p.pending),
        rank: 0,
        pillars,
      };
    })
    .sort((a, b) => b.earned - a.earned || a.name.localeCompare(b.name, "de"));
  let rank = 0,
    last = -1;
  rows.forEach((row, i) => {
    if (row.earned !== last) rank = i + 1;
    row.rank = rank;
    last = row.earned;
  });
  return rows;
}

export function modelTemplate(
  template: "empty" | "q1" | "q2" | "q3",
): WaveModel {
  const metric = (
    key: string,
    label: string,
    unit: MetricUnit = "percent",
  ): ModelMetric => ({
    key,
    label,
    unit,
    method: "manual",
    inputs: [],
    target: null,
    sources: [],
    steps: [],
  });
  const tier = (
    key: string,
    at: number,
    rewardEur: number,
    metricKey = "percent",
    group = "",
  ): ModelTier => ({
    key: `tier_${key}`,
    label: `Ab ${at} → ${rewardEur.toLocaleString("de-AT")} €`,
    group,
    rewardEur,
    conditions: [{ metricKey, operator: "gte", value: at }],
  });
  const pillar = (
    key: string,
    name: string,
    kind: ModelPillar["kind"],
    maximum: number,
    color: string,
    metrics: ModelMetric[],
    tiers: ModelTier[],
  ): ModelPillar => ({
    key,
    name,
    kind,
    maxRewardEur: maximum,
    color,
    payoutMode: "highest",
    metrics,
    tiers,
  });
  if (template === "empty")
    return {
      version: 1,
      provenance: "Leerer Entwurf – keine fachliche Freigabe",
      pillars: [
        pillar(
          "ziel",
          "Neues Ziel",
          "custom",
          0,
          "#dc2626",
          [metric("percent", "Zielerreichung")],
          [],
        ),
      ],
    };
  const flexMetrics =
    template === "q3"
      ? [metric("coolers", "Kühler"), metric("racks", "Permanente Racks")]
      : template === "q1"
        ? [
            metric("placements", "Neue Platzierungen", "count"),
            {
              ...metric("placement_points", "Platzierungspunkte", "points"),
              method: "steps" as const,
              inputs: ["placements"],
              steps: [
                { at: 18, value: 9 },
                { at: 22, value: 18 },
              ],
            },
            metric("scanning", "Kühler-Scanquote"),
            {
              ...metric("cooler_points", "Kühlerpunkte", "points"),
              method: "steps" as const,
              inputs: ["scanning"],
              steps: [
                { at: 65, value: 5 },
                { at: 75, value: 10 },
              ],
            },
            {
              ...metric("total", "Flexpunkte", "points"),
              method: "sum" as const,
              inputs: ["placement_points", "cooler_points"],
            },
          ]
        : [
            metric("new_coolers", "Neue Kühler", "count"),
            metric("returned", "Retouren", "count"),
            {
              ...metric("net", "Kühler netto", "count"),
              method: "difference" as const,
              inputs: ["new_coolers", "returned"],
            },
            {
              ...metric("cooler_points", "Kühlerpunkte", "points"),
              method: "steps" as const,
              inputs: ["net"],
              steps: [
                { at: 2, value: 5 },
                { at: 3, value: 10 },
              ],
            },
            metric("red_ir", "RED / IR"),
            {
              ...metric("red_points", "RED/IR-Punkte", "points"),
              method: "steps" as const,
              inputs: ["red_ir"],
              steps: [
                { at: 80, value: 5 },
                { at: 85, value: 10 },
              ],
            },
            {
              ...metric("total", "Flexpunkte", "points"),
              method: "sum" as const,
              inputs: ["cooler_points", "red_points"],
            },
          ];
  const flexTiers =
    template === "q3"
      ? []
      : [
          tier("half", template === "q1" ? 22 : 10, 82.5, "total"),
          tier("full", template === "q1" ? 26 : 15, 165, "total"),
        ].map((t) => ({
          ...t,
          conditions: [
            ...t.conditions,
            {
              metricKey: template === "q1" ? "placement_points" : "red_points",
              operator: "gte" as const,
              value: template === "q1" ? 9 : 5,
            },
            { metricKey: "cooler_points", operator: "gte" as const, value: 5 },
          ],
        }));
  const quality = pillar(
    "quality",
    "Qualität",
    "quality",
    220,
    "#7c3aed",
    [
      metric("time", "Zeitmanagement"),
      metric("reporting", "Reporting"),
      metric("tags", "Bildertags / Survey"),
    ],
    [],
  );
  quality.payoutMode = "groups"; // Thresholds intentionally unset: rating definition is still unconfirmed.
  return {
    version: 1,
    provenance: `${template.toUpperCase()}-Vorlage aus vorhandenen Unterlagen; Einrichtung/Freigabe erforderlich. Qualität ohne unbestätigte Stufen.`,
    pillars: [
      pillar(
        "displays",
        "Schütten / Displays",
        "displays",
        550,
        "#dc2626",
        [metric("percent", "Zielerreichung")],
        [tier("70", 70, 275), tier("80", 80, 440), tier("95", 95, 550)],
      ),
      pillar(
        "distribution",
        "Distribution",
        "distribution",
        165,
        "#2563eb",
        [metric("percent", "Zielerreichung")],
        [tier("80", 80, 82.5), tier("90", 90, 165)],
      ),
      pillar(
        "flex",
        "Flexziel",
        "flex",
        165,
        "#d97706",
        flexMetrics,
        flexTiers,
      ),
      quality,
    ],
  };
}
