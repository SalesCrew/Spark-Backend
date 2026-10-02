import { calculateTieredPillarReward } from "./lib/praemien-rewards.js";

export type MetricUnit = "percent" | "points" | "count" | "eur";
export type MetricMethod =
  | "manual"
  | "answer_sum"
  | "availability"
  | "weighted_sum"
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
  goal?: { halfAt: number; fullAt: number } | undefined;
  weights?: Record<string, number> | undefined;
  hint?: string | undefined;
  readOnly?: boolean | undefined;
  minValue?: number | undefined;
  maxValue?: number | undefined;
  integerOnly?: boolean | undefined;
  confirmation?: boolean | undefined;
  manualRewardCap?: number | undefined;
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
  payoutMode: "highest" | "groups" | "manual";
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
      if (m.goal && (!Number.isFinite(m.goal.halfAt) || !Number.isFinite(m.goal.fullAt) || m.goal.halfAt >= m.goal.fullAt))
        errors.push(`${m.label}: 100-%-Ziel muss über dem 50-%-Ziel liegen.`);
      if (m.minValue !== undefined && m.maxValue !== undefined && m.minValue > m.maxValue)
        errors.push(`${m.label}: Wertebereich ungültig.`);
      if (m.confirmation && (m.method !== "manual" || m.unit !== "count"))
        errors.push(`${m.label}: Bestätigung muss manuell erfasst werden.`);
      if (m.method === "weighted_sum" && (!m.inputs.length || m.unit !== "points" ||
        m.inputs.some(k => p.metrics.find(x => x.key === k)?.unit !== "count" || !Number.isFinite(m.weights?.[k]) || (m.weights?.[k] ?? 0) <= 0) ||
        Object.keys(m.weights ?? {}).some(k => !m.inputs.includes(k))))
        errors.push(`${m.label}: Stückzahlen und positive Punktegewichte für jede Eingabe erforderlich.`);
      if (m.manualRewardCap !== undefined && (m.unit !== "eur" || m.method !== "manual" || m.manualRewardCap <= 0))
        errors.push(`${m.label}: Manuelle Auszahlung benötigt Euro und eine positive Obergrenze.`);
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
    if (p.payoutMode === "manual") {
      const payable = p.metrics.filter(m => m.manualRewardCap !== undefined);
      if (p.metrics.some(m => m.unit === "eur" && m.method === "manual" && m.manualRewardCap === undefined)) errors.push(`${p.name}: Für jede Euro-Teilprämie eine maximale Auszahlung festlegen.`);
      if (!payable.length || p.tiers.length) errors.push(`${p.name}: Manuelle Auszahlung benötigt Euro-Teilprämien ohne automatische Stufen.`);
      if (money(payable.reduce((sum, m) => sum + (m.manualRewardCap ?? 0), 0)) > p.maxRewardEur)
        errors.push(`${p.name}: Teilprämien überschreiten die Maximalprämie.`);
    } else if (p.metrics.some(m => m.manualRewardCap !== undefined)) {
      errors.push(`${p.name}: Euro-Teilprämien benötigen den manuellen Auszahlungsmodus.`);
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
  const sum = numbers.reduce((s, n, i) => s + n * (m.method === "weighted_sum" ? m.weights?.[m.inputs[i]!] ?? 0 : 1), 0);
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
          const manualValue = m.readOnly ? null : entry?.value;
          const rawValue = manualValue ?? auto.value;
          const value = rawValue !== null && manualValue == null &&
            ((m.minValue !== undefined && rawValue < m.minValue) || (m.maxValue !== undefined && rawValue > m.maxValue) || (m.integerOnly && !Number.isInteger(rawValue))) ? null : rawValue;
          results.set(m.key, {
            key: m.key,
            label: m.label,
            unit: m.unit,
            value,
            automatic: auto.value,
            target,
            origin:
              manualValue != null
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
        let earned = p.payoutMode === "manual"
          ? p.metrics.filter(m => m.manualRewardCap !== undefined).reduce((sum, m) => sum + Math.max(0, Math.min(results.get(m.key)?.value ?? 0, m.manualRewardCap!)), 0)
          : 0;
        const achieved: string[] = [];
        let next: string | null = null;
        for (const group of p.payoutMode === "manual" ? [] : groups) {
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
          pending: p.payoutMode === "manual"
            ? p.metrics.some(m => m.manualRewardCap !== undefined && results.get(m.key)?.value === null)
            : p.tiers.length === 0 || [...used].some((k) => results.get(k)?.value === null),
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
  template: "empty" | "q1" | "q2" | "q3" | "xmas",
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
  if (template === "xmas") {
    // New draft only: historical templates and saved models keep their original rules.
    const model = modelTemplate("q3");
    model.provenance = "Kühler + X-Mas · Unterlagen vom 02.10.2026. Beide Teilziele mindestens 50 %; Auszahlung ab 25 / 30 Gesamtpunkten. Qualität: manuell festgelegte Eurobeträge.";
    const count = (key: string, label: string, hint = ""): ModelMetric => ({ ...metric(key, label, "count"), minValue: 0, integerOnly: true, hint });
    const derived = (key: string, label: string, unit: MetricUnit, method: MetricMethod, inputs: string[], extra: Partial<ModelMetric> = {}): ModelMetric => ({ ...metric(key, label, unit), method, inputs, readOnly: true, ...extra });
    const categories = [
      ["trucks", "Truck · Fahrerhaus + erster Anhänger", 3],
      ["trailers", "Weitere Anhänger · ohne ersten Truck-Anhänger", 1],
      ["bins", "Schütten", 1], ["fsdu", "FSDU", 1], ["sleds", "Schlittenschürzen", 1],
      ["pallets", "Palettenschürzen", 0.5], ["standees", "Standees", 0.5],
    ] as const;
    const flex = model.pillars.find(p => p.key === "flex")!;
    flex.name = "Flexziel · Kühler + X-Mas";
    flex.metrics = [
      count("new_coolers", "Qualifizierte Neuaufstellungen", "Alle Brands und Gerätetypen; mindestens 8 Wochen im Verkaufsraum. Wiedergefundene Geräte separat erfassen."),
      count("recovered", "Wiedergefundene, zuvor als Lost gemeldete Kühler", "Zählen als Neuaufstellung; nicht nochmals unter Neuaufstellungen erfassen."),
      count("returned", "Rückholungen · ohne Filialschließungen", "Nur abzugsfähige Rückholungen; Lost-Meldungen separat erfassen."),
      count("lost", "Lost-Meldungen · ohne Filialschließungen", "Filialschließungen zählen nicht als Abzug. Keine Rückholung doppelt erfassen."),
      derived("gross", "Neuaufstellungen inkl. Wiederfunde", "count", "sum", ["new_coolers", "recovered"]),
      derived("deductions", "Abzugsfähige Rückholungen + Lost", "count", "sum", ["returned", "lost"]),
      derived("net", "Kühler netto", "count", "difference", ["gross", "deductions"], { goal: { halfAt: 0, fullAt: 1 } }),
      derived("cooler_points", "Kühlerpunkte", "points", "steps", ["net"], { steps: [{ at: 0, value: 5 }, { at: 1, value: 10 }] }),
      ...categories.map(([key, label, weight]) => count(key, label, `Nur X-Mas-POS-Platzierungen, unabhängig von Brand/SKU · je ${weight.toLocaleString("de-AT")} Punkte.`)),
      derived("xmas_points", "X-Mas-Aktivierungen", "points", "weighted_sum", categories.map(([key]) => key), { weights: Object.fromEntries(categories.map(([key, , weight]) => [key, weight])), goal: { halfAt: 20, fullAt: 28 } }),
      derived("total", "Gesamtpunkte · Kühler + X-Mas", "points", "sum", ["cooler_points", "xmas_points"]),
      { ...metric("qualified", "Standzeit & Meldungen geprüft", "count"), confirmation: true, minValue: 0, maxValue: 1, hint: "Bestätigen: Neuaufstellungen mindestens 8 Wochen im Verkaufsraum; Neuaufstellungen, Rückholungen und Lost in Execution Manager erfasst und per E-Mail mit Agentur in CC gemeldet; Filialschließungen aus Abzügen ausgeschlossen; Wiederfunde korrekt als Neuaufstellung gezählt. Diese Nachweise werden manuell geprüft." },
    ];
    flex.tiers = [tier("half", 25, 82.5, "total"), tier("full", 30, 165, "total")].map(t => ({ ...t, label: `${t.rewardEur === 165 ? "100" : "50"} % Auszahlung · ab ${t.conditions[0]!.value} Punkten`, conditions: [...t.conditions, { metricKey: "net", operator: "gte", value: 0 }, { metricKey: "xmas_points", operator: "gte", value: 20 }, { metricKey: "qualified", operator: "eq", value: 1 }] }));
    const quality = model.pillars.find(p => p.key === "quality")!;
    quality.payoutMode = "manual";
    quality.metrics = [["reporting", "Merch Reporting", 55], ["tags", "Merch Survey / Bildertags", 55], ["time", "Merch Zeitmanagement", 110]].map(([key, label, cap]) => ({ ...metric(String(key), String(label), "eur"), manualRewardCap: Number(cap), minValue: 0, maxValue: Number(cap), hint: "Manuell festgelegte Auszahlung. Leer = Bewertung offen; 0 € = geprüft, keine Auszahlung. Keine unbestätigten Prozentgrenzen." }));
    return model;
  }
  const flexMetrics =
    template === "q3"
      ? [metric("coolers", "Kühler"), metric("racks", "Permanente Racks")]
      : template === "q1"
        ? [
            metric("placements", "Neue Platzierungen · je 1 Punkt", "points"),
            {
              ...metric("placement_points", "Platzierungspunkte", "points"),
              method: "sum" as const,
              inputs: ["placements"],
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
              value: template === "q1" ? 18 : 5,
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
