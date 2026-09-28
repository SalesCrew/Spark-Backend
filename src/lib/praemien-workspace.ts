import { sql, type SQL } from "drizzle-orm";
import { z } from "zod";
import {
  evaluateModel,
  modelTemplate,
  validateModel,
  type GmResult,
  type MetricEntry,
  type Observation,
  type WaveModel,
} from "../praemien-model.shared.js";

export interface ModelDatabase {
  query<T>(query: SQL): Promise<T[]>;
  transaction<T>(fn: (tx: ModelDatabase) => Promise<T>): Promise<T>;
}
export function modelDatabase(executor: {
  execute(query: SQL): PromiseLike<unknown>;
  transaction?: unknown;
}): ModelDatabase {
  return {
    async query<T>(query: SQL): Promise<T[]> {
      const data = await executor.execute(query);
      return (Array.isArray(data) ? data : (data as { rows: T[] }).rows) as T[];
    },
    async transaction<T>(fn: (tx: ModelDatabase) => Promise<T>): Promise<T> {
      const transaction = executor.transaction as
        ((fn: (tx: typeof executor) => Promise<T>) => Promise<T>) | undefined;
      if (!transaction) return fn(modelDatabase(executor));
      return transaction.call(executor, (tx) => fn(modelDatabase(tx)));
    },
  };
}
export class ModelError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}
const key = z.string().regex(/^[a-z][a-z0-9_]{0,79}$/);
const number = z.number().finite().min(-1000000).max(1000000);
const nonnegative = number.min(0);
const positive = number.positive();
export const modelSchema = z.object({
  version: z.literal(1),
  provenance: z.string().max(1000),
  pillars: z
    .array(
      z.object({
        key,
        name: z.string().trim().min(1).max(120),
        kind: z.enum(["displays", "distribution", "flex", "quality", "custom"]),
        color: z.string().regex(/^#[0-9a-fA-F]{6}$/),
        maxRewardEur: nonnegative,
        payoutMode: z.enum(["highest", "groups"]),
        metrics: z
          .array(
            z.object({
              key,
              label: z.string().trim().min(1).max(160),
              unit: z.enum(["percent", "points", "count"]),
              method: z.enum([
                "manual",
                "answer_sum",
                "availability",
                "sum",
                "difference",
                "average",
                "ratio",
                "steps",
              ]),
              inputs: z.array(key).max(40),
              target: positive.nullable(),
              steps: z.array(z.object({ at: number, value: number })).max(30),
              sources: z
                .array(
                  z.object({
                    questionId: z.string().uuid(),
                    section: z.enum([
                      "standard",
                      "flex",
                      "billa",
                      "kuehler",
                      "mhd",
                      "durcharbeit",
                    ]),
                    scoreKey: z.string().min(1).max(160),
                    factor: z.boolean(),
                    weight: number,
                    label: z.string().max(500),
                    minFrequency: z.number().int().min(0).max(366),
                    chains: z.array(z.string().max(100)).max(50),
                    counting: z.enum(["latest", "once"]),
                  }),
                )
                .max(100),
            }),
          )
          .min(1)
          .max(40),
        tiers: z
          .array(
            z.object({
              key,
              label: z.string().trim().min(1).max(160),
              group: z.string().max(80),
              rewardEur: nonnegative,
              conditions: z
                .array(
                  z.object({
                    metricKey: key,
                    operator: z.enum(["gte", "lte", "eq"]),
                    value: number,
                  }),
                )
                .min(1)
                .max(40),
            }),
          )
          .max(50),
      }),
    )
    .min(1)
    .max(12),
});
export const entrySchema = z.object({
  gmId: z.string().uuid(),
  pillarKey: key,
  metricKey: key,
  value: number.nullable(),
  target: positive.nullable(),
  note: z.string().max(1000),
});
export type WaveInfo = {
  id: string;
  name: string;
  year: number;
  quarter: number;
  status: "draft" | "active" | "archived";
  startDate: string;
  endDate: string;
  updatedAt: string;
};
export type Workspace = {
  legacyTotals?: {
    gmId: string;
    name: string;
    earned: number;
    totalPoints: number;
  }[];
  wave: WaveInfo;
  model: WaveModel | null;
  revision: number;
  entries: MetricEntry[];
  results: GmResult[];
  calculatedAt: string;
  closedAt: string | null;
  history: {
    id: string;
    revision: number;
    type: string;
    actor: string;
    at: string;
    payload: unknown;
  }[];
};
type Setting = {
  model: WaveModel;
  revision: number;
  snapshot: Workspace | null;
  closedAt: string | null;
};
export async function workspaceReady(
  database: ModelDatabase,
): Promise<boolean> {
  const rows = await database.query<{ ready: boolean }>(
    sql`select to_regclass('public.praemien_wave_settings') is not null as ready`,
  );
  return rows[0]?.ready === true;
}
async function waveInfo(
  database: ModelDatabase,
  id: string,
  lock = false,
): Promise<WaveInfo> {
  const [wave] = await database.query<WaveInfo>(
    sql`select id, name, year, quarter, status, start_date::text as "startDate", end_date::text as "endDate", updated_at::text as "updatedAt" from praemien_waves where id=${id} and is_deleted=false ${lock ? sql`for update` : sql``}`,
  );
  if (!wave) throw new ModelError(404, "Prämienwelle nicht gefunden.");
  return wave;
}
async function settings(database: ModelDatabase, id: string) {
  const [row] = await database.query<Setting>(
    sql`select model, revision, closed_snapshot as snapshot, closed_at::text as "closedAt" from praemien_wave_settings where wave_id=${id}`,
  );
  return row;
}
async function observations(
  database: ModelDatabase,
  wave: WaveInfo,
  model: WaveModel,
  gmId?: string,
): Promise<Observation[]> {
  const ids = [
    ...new Set(
      model.pillars.flatMap((p) =>
        p.metrics.flatMap((m) => m.sources.map((s) => s.questionId)),
      ),
    ),
  ];
  if (!ids.length) return [];
  const rows = await database.query<
    Observation & { numeric: number | string | null }
  >(sql`
    select a.id, v.gm_user_id as "gmId", v.market_id as "marketId", a.question_id as "questionId", sec.section::text as section,
      v.submitted_at::text as date, a.value_number as numeric,
      array(select o.option_value from visit_answer_options o where o.visit_answer_id=a.id and o.is_deleted=false) || array[coalesce(a.value_text,'')] as options,
      m.visit_frequency_per_year as frequency, m.db_name as chain
    from visit_answers a join visit_sessions v on v.id=a.visit_session_id
    join visit_session_questions q on q.id=a.visit_session_question_id
    join visit_session_sections sec on sec.id=a.visit_session_section_id
    join markets m on m.id=v.market_id
    where a.question_id in (${sql.join(
      ids.map((id) => sql`${id}::uuid`),
      sql`,`,
    )})
      and v.status='submitted' and v.is_deleted=false and a.is_deleted=false and a.is_valid=true and a.answer_status='answered'
      and sec.is_deleted=false and q.is_deleted=false and q.applies_to_market_chain_snapshot=true
      and (v.submitted_at at time zone 'Europe/Vienna')::date between ${wave.startDate}::date and ${wave.endDate}::date
      ${gmId ? sql`and v.gm_user_id=${gmId}::uuid` : sql``}
  `);
  return rows.map((r) => ({
    ...r,
    date: new Date(r.date).toISOString(),
    numeric: r.numeric === null ? null : Number(r.numeric),
  }));
}
export async function readWorkspace(
  database: ModelDatabase,
  id: string,
  gmId?: string,
): Promise<Workspace> {
  if (!(await workspaceReady(database)))
    throw new ModelError(
      503,
      "Prämien-Erweiterung noch nicht eingerichtet. Die vorbereitete Migration muss zuerst in der jeweiligen Umgebung angewendet werden.",
    );
  const wave = await waveInfo(database, id);
  const config = await settings(database, id);
  if (config?.snapshot) {
    const history = !gmId
      ? await database.query<Workspace["history"][number]>(
          sql`select id,revision,event_type as type,actor_name as actor,created_at::text as at,payload from praemien_wave_events where wave_id=${id} order by created_at desc,id desc limit 100`,
        )
      : [];
    return {
      ...config.snapshot,
      results: gmId
        ? config.snapshot.results.filter((r) => r.gmId === gmId)
        : config.snapshot.results,
      entries: gmId
        ? config.snapshot.entries.filter((e) => e.gmId === gmId)
        : config.snapshot.entries,
      history,
    };
  }
  const roster = await database.query<{
    gmId: string;
    name: string;
    active: boolean;
  }>(
    sql`select id as "gmId", trim(first_name || ' ' || last_name) as name, is_active as active from users where role='gm' ${gmId ? sql`and id=${gmId}::uuid` : sql``} order by first_name,last_name`,
  );
  const entries = config
    ? await database.query<MetricEntry>(
        sql`select gm_user_id as "gmId", pillar_key as "pillarKey", metric_key as "metricKey", value, target, note, actor_name as "actorName", updated_at::text as "updatedAt" from praemien_metric_entries where wave_id=${id} ${gmId ? sql`and gm_user_id=${gmId}::uuid` : sql``}`,
      )
    : [];
  entries.forEach((e) => {
    e.value = e.value === null ? null : Number(e.value);
    e.target = e.target === null ? null : Number(e.target);
  });
  const results = config
    ? evaluateModel(
        config.model,
        roster,
        entries,
        await observations(database, wave, config.model, gmId),
      )
    : [];
  let legacyTotals: Workspace["legacyTotals"];
  if (!config && !gmId) {
    const [exists] = await database.query<{ present: boolean }>(
      sql`select to_regclass('public.praemien_gm_wave_totals') is not null as present`,
    );
    if (exists?.present) {
      const old = await database.query<{
        gmId: string;
        name: string;
        earned: number | string;
        totalPoints: number | string;
      }>(
        sql`select t.gm_user_id as "gmId",trim(u.first_name || ' ' || u.last_name) as name,t.current_reward_eur as earned,t.total_points as "totalPoints" from praemien_gm_wave_totals t join users u on u.id=t.gm_user_id where t.wave_id=${id} order by t.current_reward_eur desc,u.first_name,u.last_name`,
      );
      legacyTotals = old.map((r) => ({
        ...r,
        earned: Number(r.earned),
        totalPoints: Number(r.totalPoints),
      }));
    }
  }
  const history =
    config && !gmId
      ? await database.query<Workspace["history"][number]>(
          sql`select id, revision, event_type as type, actor_name as actor, created_at::text as at, payload from praemien_wave_events where wave_id=${id} order by created_at desc,id desc limit 100`,
        )
      : [];
  return {
    wave,
    model: config?.model ?? null,
    ...(legacyTotals ? {legacyTotals} : {}),
    revision: config?.revision ?? 0,
    entries,
    results,
    calculatedAt: new Date().toISOString(),
    closedAt: null,
    history,
  };
}
async function validateSources(database: ModelDatabase, model: WaveModel) {
  const sources = model.pillars.flatMap((p) =>
    p.metrics.flatMap((m) => m.sources),
  );
  const ids = [...new Set(sources.map((s) => s.questionId))];
  if (!ids.length) return;
  const questions = await database.query<{ id: string; type: string }>(
    sql`select id, question_type as type from question_bank_shared where is_deleted=false and id in (${sql.join(
      ids.map((id) => sql`${id}::uuid`),
      sql`,`,
    )})`,
  );
  if (questions.length !== ids.length)
    throw new ModelError(
      400,
      "Eine zugeordnete Frage wurde gelöscht oder existiert nicht.",
    );
  for (const s of sources) {
    const type = questions.find((q) => q.id === s.questionId)?.type;
    if (s.factor && !["numeric", "slider"].includes(type ?? ""))
      throw new ModelError(
        400,
        `${s.label}: Wert × Faktor benötigt eine Zahlenfrage.`,
      );
  }
}
export async function mutateWorkspace(
  database: ModelDatabase,
  id: string,
  revision: number,
  actor: { id: string; name: string },
  command:
    | { type: "rules"; model: WaveModel }
    | { type: "values"; entries: MetricEntry[] }
    | { type: "activate" | "archive" },
): Promise<Workspace> {
  if (!(await workspaceReady(database)))
    throw new ModelError(503, "Prämien-Migration fehlt.");
  return database.transaction(async (tx) => {
    const wave = await waveInfo(tx, id, true);
    const config = await settings(tx, id);
    if (wave.status === "archived" || config?.snapshot)
      throw new ModelError(
        409,
        "Abgeschlossene Quartale sind eingefroren und können nicht bearbeitet werden.",
      );
    if ((config?.revision ?? 0) !== revision)
      throw new ModelError(
        409,
        "Zwischenzeitlich geändert. Bitte neu laden; deine Eingaben wurden nicht überschrieben.",
      );
    if (command.type === "rules") {
      const model = modelSchema.parse(command.model);
      const errors = validateModel(model);
      if (errors.length) throw new ModelError(400, errors.join(" "));
      await validateSources(tx, model);
      if (config) {
        const oldEntries = await tx.query<MetricEntry>(
          sql`select pillar_key as "pillarKey", metric_key as "metricKey" from praemien_metric_entries where wave_id=${id}`,
        );
        for (const e of oldEntries) {
          const before = config.model.pillars
            .find((p) => p.key === e.pillarKey)
            ?.metrics.find((m) => m.key === e.metricKey);
          const after = model.pillars
            .find((p) => p.key === e.pillarKey)
            ?.metrics.find((m) => m.key === e.metricKey);
          if (!after || (before && before.unit !== after.unit))
            throw new ModelError(
              400,
              "Messgrößen mit erfassten Werten dürfen nicht entfernt oder in eine andere Einheit umgedeutet werden. Zuerst Bewertungen entfernen.",
            );
        }
      }
      await tx.query(
        sql`insert into praemien_wave_settings(wave_id,model,revision) values(${id},${JSON.stringify(model)}::jsonb,${revision + 1}) on conflict(wave_id) do update set model=excluded.model,revision=excluded.revision,updated_at=now()`,
      );
    } else {
      if (!config) throw new ModelError(400, "Zuerst Regeln einrichten.");
      if (command.type === "values") {
        const entries = z.array(entrySchema).max(2000).parse(command.entries);
        const gmIds = [...new Set(entries.map((e) => e.gmId))];
        const known = gmIds.length
          ? await tx.query<{ id: string }>(
              sql`select id from users where role='gm' and id in (${sql.join(
                gmIds.map((g) => sql`${g}::uuid`),
                sql`,`,
              )})`,
            )
          : [];
        if (known.length !== gmIds.length)
          throw new ModelError(400, "Ungültiger GM.");
        if (
          new Set(entries.map((e) => `${e.gmId}/${e.pillarKey}/${e.metricKey}`))
            .size !== entries.length
        )
          throw new ModelError(400, "Messwerte doppelt.");
        for (const e of entries) {
          const metric = config.model.pillars
            .find((p) => p.key === e.pillarKey)
            ?.metrics.find((m) => m.key === e.metricKey);
          if (!metric) throw new ModelError(400, "Unbekannte Messgröße.");
          if (
            e.value !== null &&
            metric.unit === "percent" &&
            (e.value < 0 || e.value > 1000)
          )
            throw new ModelError(
              400,
              "Prozentwert muss zwischen 0 und 1000 liegen.",
            );
          if (e.value === null && e.target === null)
            await tx.query(
              sql`delete from praemien_metric_entries where wave_id=${id} and gm_user_id=${e.gmId} and pillar_key=${e.pillarKey} and metric_key=${e.metricKey}`,
            );
          else
            await tx.query(
              sql`insert into praemien_metric_entries(wave_id,gm_user_id,pillar_key,metric_key,value,target,note,actor_id,actor_name) values(${id},${e.gmId},${e.pillarKey},${e.metricKey},${e.value},${e.target},${e.note},${actor.id},${actor.name}) on conflict(wave_id,gm_user_id,pillar_key,metric_key) do update set value=excluded.value,target=excluded.target,note=excluded.note,actor_id=excluded.actor_id,actor_name=excluded.actor_name,updated_at=now()`,
            );
        }
      } else if (command.type === "activate") {
        if (config.model.pillars.some((p) => !p.tiers.length))
          throw new ModelError(
            400,
            "Jede Säule braucht bestätigte Stufen. Fehlende Qualitäts-/Q3-Regeln zuerst einrichten.",
          );
        const [conflict] = await tx.query(
          sql`select id from praemien_waves where id<>${id} and is_deleted=false and status='active' and start_date<=${wave.endDate}::date and end_date>=${wave.startDate}::date`,
        );
        if (conflict)
          throw new ModelError(
            409,
            "Für diesen Zeitraum läuft bereits eine Welle.",
          );
        await tx.query(
          sql`update praemien_waves set status='active',reward_model='pillar_tiers',updated_at=now() where id=${id}`,
        );
      } else {
        if (wave.status !== "active")
          throw new ModelError(
            400,
            "Nur eine laufende Welle kann abgeschlossen werden.",
          );
        const snapshot = await readWorkspace(tx, id);
        if (!snapshot.results.length || snapshot.results.some((r) => r.pending))
          throw new ModelError(
            400,
            "Abschluss nicht möglich: Es fehlen Bewertungen oder auswertbare Regeln/Sollwerte.",
          );
        const closedAt = new Date().toISOString();
        snapshot.wave.status = "archived";
        snapshot.revision = revision + 1;
        snapshot.closedAt = closedAt;
        await tx.query(
          sql`update praemien_wave_settings set closed_snapshot=${JSON.stringify(snapshot)}::jsonb,closed_at=${closedAt}::timestamptz where wave_id=${id}`,
        );
        await tx.query(
          sql`update praemien_waves set status='archived',updated_at=now() where id=${id}`,
        );
      }
      await tx.query(
        sql`update praemien_wave_settings set revision=${revision + 1},updated_at=now() where wave_id=${id}`,
      );
    }
    await tx.query(
      sql`update praemien_waves set updated_at=now() where id=${id}`,
    );
    await tx.query(
      sql`insert into praemien_wave_events(wave_id,revision,event_type,actor_id,actor_name,payload) values(${id},${revision + 1},${command.type},${actor.id},${actor.name},${JSON.stringify(command)}::jsonb)`,
    );
    return readWorkspace(tx, id);
  });
}
export async function simulateWorkspace(
  database: ModelDatabase,
  id: string,
  proposed: WaveModel,
  proposedEntries?: MetricEntry[],
): Promise<Workspace> {
  const model = modelSchema.parse(proposed);
  const errors = validateModel(model);
  if (errors.length) throw new ModelError(400, errors.join(" "));
  await validateSources(database, model);
  const workspace = await readWorkspace(database, id);
  if (workspace.closedAt)
    throw new ModelError(409, "Abgeschlossene Quartale sind eingefroren.");
  workspace.model = model;
  if (proposedEntries) {
    const entries = z.array(entrySchema).max(2000).parse(proposedEntries);
    for (const entry of entries) {
      const metric = model.pillars
        .find((p) => p.key === entry.pillarKey)
        ?.metrics.find((m) => m.key === entry.metricKey);
      if (!metric) throw new ModelError(400, "Unbekannte Messgröße.");
      if (
        metric.unit === "percent" &&
        entry.value !== null &&
        (entry.value < 0 || entry.value > 1000)
      )
        throw new ModelError(
          400,
          "Prozentwert muss zwischen 0 und 1000 liegen.",
        );
      workspace.entries = workspace.entries.filter(
        (e) =>
          !(
            e.gmId === entry.gmId &&
            e.pillarKey === entry.pillarKey &&
            e.metricKey === entry.metricKey
          ),
      );
      if (entry.value !== null || entry.target !== null)
        workspace.entries.push(entry);
    }
  }
  const roster = await database.query<{
    gmId: string;
    name: string;
    active: boolean;
  }>(
    sql`select id as "gmId", trim(first_name || ' ' || last_name) as name, is_active as active from users where role='gm'`,
  );
  workspace.results = evaluateModel(
    model,
    roster,
    workspace.entries,
    await observations(database, workspace.wave, model),
  );
  return workspace;
}
export { modelTemplate };

export async function isManagedWave(
  database: ModelDatabase,
  id: string,
): Promise<boolean> {
  return (await workspaceReady(database)) && !!(await settings(database, id));
}

// Live managed waves must not be read from the old, independently cached totals.
// Drafts are excluded; closed results come exclusively from their immutable snapshot.
export async function managedCumulative(
  database: ModelDatabase,
  gmIds: string[],
) {
  const result = { waveIds: [] as string[], totals: new Map<string, number>() };
  if (!gmIds.length || !(await workspaceReady(database))) return result;
  const waves = await database.query<{ id: string }>(
    sql`select w.id from praemien_waves w join praemien_wave_settings s on s.wave_id=w.id where w.is_deleted=false`,
  );
  result.waveIds = waves.map((w) => w.id);
  for (const wave of waves) {
    const workspace = await readWorkspace(
      database,
      wave.id,
      gmIds.length === 1 ? gmIds[0] : undefined,
    );
    if (workspace.wave.status === "draft") continue;
    for (const gm of workspace.results)
      if (gmIds.includes(gm.gmId))
        result.totals.set(
          gm.gmId,
          Math.round(((result.totals.get(gm.gmId) ?? 0) + gm.earned) * 100) /
            100,
        );
  }
  return result;
}

export async function managedQuarterQuestionIds(
  database: ModelDatabase,
  waveIds: string[],
  questionIds: string[],
) {
  if (!waveIds.length || !(await workspaceReady(database)))
    return [] as string[];
  const rows = await database.query<{ model: WaveModel }>(
    sql`select model from praemien_wave_settings where wave_id in (${sql.join(
      waveIds.map((id) => sql`${id}::uuid`),
      sql`,`,
    )}) and closed_snapshot is null`,
  );
  return [
    ...new Set(
      rows
        .flatMap((row) =>
          row.model.pillars
            .filter((p) => p.kind === "distribution")
            .flatMap((p) =>
              p.metrics.flatMap((m) => m.sources.map((s) => s.questionId)),
            ),
        )
        .filter((id) => questionIds.includes(id)),
    ),
  ];
}
export async function managedGmSummary(
  database: ModelDatabase,
  id: string,
  gmId: string,
) {
  const workspace = await readWorkspace(database, id, gmId);
  const row = workspace.results[0];
  const goals = (row?.pillars ?? []).map((p) => {
    const percentMetric = p.metrics.filter((m) => m.unit === "percent");
    const percent =
      percentMetric.length === 1
        ? (percentMetric[0]!.value ?? 0)
        : p.maximum > 0
          ? (p.earned / p.maximum) * 100
          : 0;
    return {
      pillarId: p.key,
      name: p.name,
      color: p.color,
      points: p.earned,
      maxPoints: p.maximum,
      percent,
      targetPoints: null,
      rewardEur: p.maximum,
      achieved: p.earned > 0,
      isManual: p.metrics.some((m) => m.origin === "manual"),
      isPending: p.pending,
      earnedRewardEur: p.earned,
      maxRewardEur: p.maximum,
      metricValues: Object.fromEntries(
        p.metrics
          .filter((m) => m.value !== null)
          .map((m) => [m.key, m.value as number]),
      ),
      metricDetails: p.metrics,
      achievedTierLabels: p.achieved,
      nextTierLabel: p.next,
    };
  });
  return {
    hasActiveWave: true,
    waveId: id,
    waveName: workspace.wave.name,
    year: workspace.wave.year,
    quarter: workspace.wave.quarter,
    startDate: workspace.wave.startDate,
    endDate: workspace.wave.endDate,
    rewardModel: "pillar_tiers" as const,
    totalPoints: row?.earned ?? 0,
    totalMaxPoints: row?.maximum ?? 0,
    currentRewardEur: row?.earned ?? 0,
    fullRewardEur: row?.maximum ?? 0,
    goals,
    thresholds: [],
    revision: workspace.revision,
    calculatedAt: workspace.calculatedAt,
    pending: row?.pending ?? true,
    managedModel: true,
  };
}
