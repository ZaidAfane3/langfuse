import { sql, type Expression, type SqlBool } from "kysely";

import {
  eventsTableTraceNameAggregationSql,
  eventsTableTraceNameSelectSql,
  eventsTableTraceNameSql,
} from "../../eventsTable";
import type { ObservationType } from "../../domain";
import { env } from "../../env";
import { convertDateToClickhouseDateTime } from "../clickhouse/client";
import { OBSERVATIONS_TO_TRACE_INTERVAL } from "../repositories/constants";
import type { RenderingProps } from "../utils/rendering";
import { DEFAULT_RENDERING_PROPS } from "../utils/rendering";
import type { ExecutionContext } from "./executionContext";
import {
  compileClickhouseQuery,
  type CompiledClickhouseQuery,
} from "./kysely/compile";
import { getClickhouseKysely } from "./kysely/dialect";
import { limitBy } from "./kysely/extensions";

const METADATA_MAP_SQL =
  "mapFromArrays(arrayReverse(e.metadata_names), arrayReverse(e.metadata_values))";

/** `sql` fragments are compile-only; Kysely's operand types reject RawBuilder. */
function lit<T>(fragment: ReturnType<typeof sql>): Expression<T> {
  return fragment as unknown as Expression<T>;
}

/** ClickHouse DateTime64(3) binds stay strings; the column type is Date. */
function dateTimeParam(value: string): Date {
  return value as unknown as Date;
}

const OBSERVATION_BY_ID_SELECTS = [
  sql`e.span_id`.as("id"),
  sql`e.trace_id`.as("trace_id"),
  sql`e.project_id`.as("project_id"),
  sql`e.environment`.as("environment"),
  sql`e.type`.as("type"),
  sql`e.parent_span_id`.as("parent_observation_id"),
  sql`e.start_time`.as("start_time"),
  sql`e.end_time`.as("end_time"),
  sql`e.name`.as("name"),
  sql.raw(METADATA_MAP_SQL).as("metadata"),
  sql`e.level`.as("level"),
  sql`e.status_message`.as("status_message"),
  sql`e.version`.as("version"),
  sql`e.release`.as("release"),
  sql`e.user_id`.as("user_id"),
  sql`e.session_id`.as("session_id"),
  sql.raw(eventsTableTraceNameSelectSql).as("trace_name"),
  sql`e.tags`.as("tags"),
  sql`e.bookmarked`.as("bookmarked"),
  sql`e.public`.as("public"),
  sql`e.tool_definitions`.as("tool_definitions"),
  sql`e.tool_calls`.as("tool_calls"),
  sql`e.tool_call_names`.as("tool_call_names"),
  sql`e.provided_model_name`.as("provided_model_name"),
  sql`e.model_id`.as("internal_model_id"),
  sql`e.model_parameters`.as("model_parameters"),
  sql`e.provided_usage_details`.as("provided_usage_details"),
  sql`e.usage_details`.as("usage_details"),
  sql`e.provided_cost_details`.as("provided_cost_details"),
  sql`e.cost_details`.as("cost_details"),
  sql`e.total_cost`.as("total_cost"),
  sql`e.usage_pricing_tier_id`.as("usage_pricing_tier_id"),
  sql`e.usage_pricing_tier_name`.as("usage_pricing_tier_name"),
  sql`e.completion_start_time`.as("completion_start_time"),
  sql`e.prompt_id`.as("prompt_id"),
  sql`e.prompt_name`.as("prompt_name"),
  sql`e.prompt_version`.as("prompt_version"),
  sql`e.created_at`.as("created_at"),
  sql`e.updated_at`.as("updated_at"),
  sql`e.event_ts`,
] as const;

const TRACE_AGGREGATION_SELECTS = [
  sql`trace_id`.as("id"),
  sql`project_id`,
  sql.raw(eventsTableTraceNameAggregationSql).as("name"),
  sql`min(start_time)`.as("timestamp"),
  sql`argMaxIf(environment, event_ts, environment <> '')`.as("environment"),
  sql`argMaxIf(version, event_ts, version <> '')`.as("version"),
  sql`argMaxIf(session_id, event_ts, session_id <> '')`.as("session_id"),
  sql`argMaxIf(user_id, event_ts, user_id <> '')`.as("user_id"),
  sql`argMaxIf(input, event_ts, parent_span_id = '')`.as("input"),
  sql`argMaxIf(output, event_ts, parent_span_id = '')`.as("output"),
  sql
    .raw(`argMaxIf(${METADATA_MAP_SQL}, event_ts, parent_span_id = '')`)
    .as("metadata"),
  sql`min(created_at)`.as("created_at"),
  sql`max(updated_at)`.as("updated_at"),
  sql`sum(total_cost)`.as("total_cost"),
  sql`date_diff('millisecond', min(start_time), greatest(max(start_time), max(end_time)))`.as(
    "latency_milliseconds",
  ),
  sql`groupUniqArrayIf(span_id, span_id <> '')`.as("observation_ids"),
  sql`length(groupUniqArrayIf(span_id, span_id <> '' AND span_id <> concat('t-', trace_id)))`.as(
    "observation_count",
  ),
  sql`argMaxIf(bookmarked, event_ts, parent_span_id = '')`.as("bookmarked"),
  sql`max(public)`.as("public"),
  sql`argMaxIf(experiment_item_id, event_ts, experiment_item_id <> '')`.as(
    "experiment_item_id",
  ),
  sql`sumMap(usage_details)`.as("usage_details"),
  sql`sumMap(cost_details)`.as("cost_details"),
  sql`multiIf(arrayExists(x -> x = 'ERROR', groupArray(level)), 'ERROR', arrayExists(x -> x = 'WARNING', groupArray(level)), 'WARNING', arrayExists(x -> x = 'DEFAULT', groupArray(level)), 'DEFAULT', 'DEBUG')`.as(
    "aggregated_level",
  ),
  sql`countIf(level = 'WARNING')`.as("warning_count"),
  sql`countIf(level = 'ERROR')`.as("error_count"),
  sql`countIf(level = 'DEFAULT')`.as("default_count"),
  sql`countIf(level = 'DEBUG')`.as("debug_count"),
  sql`argMaxIf(tags, event_ts, notEmpty(tags))`.as("tags"),
  sql`argMaxIf(release, event_ts, release <> '')`.as("release"),
  sql`argMaxIf(evaluator_id, event_ts, evaluator_id <> '')`.as("evaluator_id"),
  sql`argMaxIf(evaluation_rule_id, event_ts, evaluation_rule_id <> '')`.as(
    "evaluation_rule_id",
  ),
  sql`any(experiment_id)`.as("experiment_id"),
] as const;

export type HasAnyEventsKind = "trace" | "user" | "session";

export function compileHasAnyFromEventsTable(opts: {
  projectId: string;
  kind: HasAnyEventsKind;
}): CompiledClickhouseQuery {
  const db = getClickhouseKysely();
  const ctx: ExecutionContext = { projectId: opts.projectId };

  const query = db
    .selectFrom("events_core")
    .select(sql`1` as never)
    .$if(opts.kind === "user", (qb) =>
      qb
        .where(lit<SqlBool>(sql`user_id IS NOT NULL`))
        .where(lit<SqlBool>(sql`user_id != ''`)),
    )
    .$if(opts.kind === "session", (qb) =>
      qb
        .where(lit<SqlBool>(sql`session_id IS NOT NULL`))
        .where(lit<SqlBool>(sql`session_id != ''`)),
    )
    .where(lit<SqlBool>(sql`is_deleted = 0`))
    .limit(lit<number>(sql`1`));

  return compileClickhouseQuery(query, ctx);
}

export function compileLastTraceTimestampsByProjectsFromEventsTable(opts: {
  projectIds: readonly string[];
}): CompiledClickhouseQuery {
  const db = getClickhouseKysely();
  const ctx: ExecutionContext = { projectIds: opts.projectIds };

  const query = db
    .selectFrom("events_core")
    .select(["project_id", sql`max(start_time)`.as("last_trace_at")])
    .where(lit<SqlBool>(sql`start_time >= now() - INTERVAL 30 DAY`))
    .where(lit<SqlBool>(sql`is_deleted = 0`))
    .groupBy("project_id");

  return compileClickhouseQuery(query, ctx);
}

export function compileAgentGraphDataFromEventsTable(opts: {
  projectId: string;
  traceId: string;
  chMinStartTime: string;
  chMaxStartTime: string;
}): CompiledClickhouseQuery {
  const db = getClickhouseKysely();
  const ctx: ExecutionContext = { projectId: opts.projectId };

  const query = db
    .selectFrom("events_core as e")
    .select([
      sql`e.span_id`.as("id"),
      sql`e.parent_span_id`.as("parent_observation_id"),
      sql`e.type`.as("type"),
      sql`e.name`.as("name"),
      sql`e.start_time`.as("start_time"),
      sql`e.end_time`.as("end_time"),
      sql.raw(`${METADATA_MAP_SQL}['langgraph_node']`).as("node"),
      sql.raw(`${METADATA_MAP_SQL}['langgraph_step']`).as("step"),
    ])
    .where("e.trace_id", "=", opts.traceId)
    .where("e.start_time", ">=", dateTimeParam(opts.chMinStartTime))
    .where("e.start_time", "<=", dateTimeParam(opts.chMaxStartTime));

  return compileClickhouseQuery(query, ctx);
}

export function compileObservationsTraceIdsFromEventsTable(opts: {
  projectId: string;
  observationIds: string[];
}): CompiledClickhouseQuery {
  const db = getClickhouseKysely();
  const ctx: ExecutionContext = { projectId: opts.projectId };

  const query = db
    .selectFrom("events_core as e")
    .select([sql`e.trace_id`.as("trace_id"), sql`e.span_id`.as("span_id")])
    .where("e.span_id", "in", opts.observationIds);

  return compileClickhouseQuery(query, ctx);
}

export function compileTraceMetadataByIdsFromEvents(opts: {
  projectId: string;
  traceIds: string[];
}): CompiledClickhouseQuery {
  const db = getClickhouseKysely();
  const ctx: ExecutionContext = { projectId: opts.projectId };

  const query = db
    .selectFrom("events_core as e")
    .select([
      sql`e.trace_id`.as("id"),
      sql.raw(eventsTableTraceNameSql).as("name"),
      sql`e.user_id`.as("user_id"),
      sql`e.tags`.as("tags"),
    ])
    .where(lit<SqlBool>(sql`${sql.raw(eventsTableTraceNameSql)} IS NOT NULL`))
    .where(lit<SqlBool>(sql`e.is_deleted = 0`))
    .where("e.trace_id", "in", opts.traceIds)
    .$call(limitBy({ count: 1, columns: ["e.trace_id"] }));

  return compileClickhouseQuery(query, ctx);
}

export function compileObservationByIdFromEventsTable(opts: {
  id: string;
  projectId: string;
  fetchWithInputOutput?: boolean;
  startTime?: Date;
  startTimeLowerBound?: Date;
  type?: ObservationType;
  traceId?: string;
  renderingProps?: RenderingProps;
}): CompiledClickhouseQuery {
  const {
    id,
    projectId,
    fetchWithInputOutput = false,
    startTime,
    startTimeLowerBound,
    type,
    traceId,
    renderingProps = DEFAULT_RENDERING_PROPS,
  } = opts;
  const ctx: ExecutionContext = { projectId };
  const db = getClickhouseKysely();
  const ioSelects = observationIoSelects(
    fetchWithInputOutput,
    renderingProps.truncated,
  );
  const needsFullTable =
    fetchWithInputOutput && renderingProps.truncated !== true;

  const query = (
    needsFullTable
      ? db.selectFrom("events_full as e")
      : db.selectFrom("events_core as e")
  )
    .select([...OBSERVATION_BY_ID_SELECTS, ...ioSelects] as never)
    .where("span_id", "=", id)
    .$if(startTime != null, (qb) =>
      qb.where((eb) =>
        eb(
          eb.fn("toStartOfMinute", ["start_time"]),
          "=",
          eb.fn("toStartOfMinute", [
            eb.val(convertDateToClickhouseDateTime(startTime!)),
          ]),
        ),
      ),
    )
    .$if(startTimeLowerBound != null, (qb) =>
      qb.where(
        "start_time",
        ">=",
        lit<Date>(
          sql`${convertDateToClickhouseDateTime(startTimeLowerBound!)} - ${sql.raw(OBSERVATIONS_TO_TRACE_INTERVAL)}`,
        ),
      ),
    )
    .$if(type != null, (qb) => qb.where("type", "=", type!))
    .$if(traceId != null, (qb) => qb.where("trace_id", "=", traceId!))
    .orderBy("start_time", "desc")
    .orderBy("event_ts", "desc")
    .limit(1);

  return compileClickhouseQuery(query, ctx);
}

export function compileTraceByIdFromEventsTable(opts: {
  traceId: string;
  projectId: string;
  timestamp?: Date;
  fromTimestamp?: Date;
  renderingProps?: RenderingProps;
  excludeInputOutput?: boolean;
  excludeMetadata?: boolean;
}): CompiledClickhouseQuery {
  const {
    traceId,
    projectId,
    timestamp,
    fromTimestamp,
    renderingProps = DEFAULT_RENDERING_PROPS,
    excludeInputOutput = false,
    excludeMetadata = false,
  } = opts;
  const ctx: ExecutionContext = { projectId };
  const db = getClickhouseKysely();
  const truncated = renderingProps.truncated === true;
  const startTimeFrom = fromTimestamp
    ? convertDateToClickhouseDateTime(fromTimestamp)
    : null;
  const ioCharLimit = env.LANGFUSE_SERVER_SIDE_IO_CHAR_LIMIT;

  const tracesCte = (qb: ReturnType<typeof getClickhouseKysely>) => {
    const from = truncated
      ? qb.selectFrom("events_core as e")
      : qb.selectFrom("events_full as e");
    return from
      .select([...TRACE_AGGREGATION_SELECTS] as never)
      .where("trace_id", "in", [traceId])
      .$if(startTimeFrom != null, (inner) =>
        inner.where(
          "start_time",
          ">=",
          lit<Date>(
            sql`${startTimeFrom} - ${sql.raw(OBSERVATIONS_TO_TRACE_INTERVAL)}`,
          ),
        ),
      )
      .groupBy(["trace_id", "project_id"])
      .orderBy(lit<Date>(sql`timestamp`), "desc");
  };

  const metadataSelect = excludeMetadata
    ? sql`map()`.as("metadata")
    : sql`t.metadata`;
  const inputSelect = excludeInputOutput
    ? sql`''`.as("input")
    : truncated
      ? sql.raw(`leftUTF8(t.input, ${ioCharLimit})`).as("input")
      : sql`t.input`;
  const outputSelect = excludeInputOutput
    ? sql`''`.as("output")
    : truncated
      ? sql.raw(`leftUTF8(t.output, ${ioCharLimit})`).as("output")
      : sql`t.output`;

  const query = db
    .with("traces", (qb) =>
      tracesCte(qb as ReturnType<typeof getClickhouseKysely>),
    )
    .selectFrom("traces as t")
    .select([
      sql`t.id`,
      sql`t.name`,
      sql`t.user_id`,
      sql`t.release`,
      sql`t.version`,
      sql`t.project_id`,
      sql`t.environment`,
      sql`t.public`,
      sql`t.bookmarked`,
      sql`t.tags`,
      sql`t.session_id`,
      sql`t.timestamp`,
      sql`t.created_at`,
      sql`t.updated_at`,
      metadataSelect,
      sql`0`.as("is_deleted"),
      inputSelect,
      outputSelect,
    ])
    .$if(timestamp != null, (qb) =>
      qb.where((eb) =>
        eb(
          eb.fn("toDate", [eb.ref("t.timestamp")]),
          "=",
          eb.fn("toDate", [
            eb.val(convertDateToClickhouseDateTime(timestamp!)),
          ]),
        ),
      ),
    )
    .orderBy("t.timestamp", "desc")
    .limit(1);

  return compileClickhouseQuery(query, ctx);
}

function observationIoSelects(
  fetchWithInputOutput: boolean,
  truncated: boolean | undefined,
) {
  if (!fetchWithInputOutput) return [];
  if (truncated) {
    const charLimit = env.LANGFUSE_SERVER_SIDE_IO_CHAR_LIMIT;
    return [
      sql.raw(`leftUTF8(input, ${charLimit})`).as("input"),
      sql.raw(`leftUTF8(output, ${charLimit})`).as("output"),
    ];
  }
  return [sql`input`, sql`output`];
}
