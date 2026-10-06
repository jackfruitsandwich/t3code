/**
 * Personal fork: what the v1 → v2 migration carries beyond upstream's shell
 * and message import.
 *
 * - Native session refs. Every v1 thread with a resume cursor gets a provider
 *   thread whose `nativeThreadRef` is that Claude session, Codex thread or
 *   wuwei chat, so its next turn resumes the same native session instead of
 *   starting fresh behind a summary handoff.
 * - Fork families (`projection_threads.fork_family_id`).
 * - Tool history: threads with v1 activity rows get their tool calls, plans,
 *   errors and notices as runless timeline items next to their messages.
 *   wuwei threads instead take their whole history from wuwei's own session
 *   file, which also holds the thinking v1 never stored.
 *
 * Everything here is pure; LegacyV1ThreadImporter decides when to use it.
 */
import {
  PlanId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  ThreadTokenUsageSnapshot,
  TurnItemId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { deriveProviderThread } from "../IdAllocator.ts";
import { wuweiToolItemFields } from "../../provider/wuwei/wuweiItems.ts";
import type { WuweiTranscript } from "../../provider/wuwei/wuweiEmbedApi.ts";

const ITEM_PREFIX = "migration:v1:item";
const MAX_CARRIED_OUTPUT_CHARS = 100_000;
const MAX_FAILURE_MESSAGE_CHARS = 4_096;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * Shell status for a thread with no v2 run. An imported thread keeps its native
 * provider thread so it can resume, and upstream reads "provider thread, no run"
 * as an open session ("Waiting"). Until its first v2 turn it is a finished
 * conversation, so present it as completed.
 */
export function importedShellStatus<Status extends string>(
  status: Status,
  thread: Pick<OrchestrationV2AppThread, "historyOrigin">,
): Status | "completed" {
  return status === "idle" && thread.historyOrigin === "v1_import" ? "completed" : status;
}

export interface LegacyRuntimeColumns {
  readonly runtime_provider_name: string | null;
  readonly runtime_provider_instance_id: string | null;
  readonly runtime_resume_cursor_json: string | null;
}

export interface LegacyActivityRow {
  readonly activity_id: string;
  readonly kind: string;
  readonly tone: string;
  readonly summary: string;
  readonly payload_json: string;
  readonly created_at: string;
  readonly sequence: number | null;
}

/** A message the importer turns into message + user/assistant item events itself. */
export interface ForkTimelineMessage {
  readonly kind: "message";
  readonly message_id: string;
  readonly thread_id: string;
  readonly role: "user" | "assistant";
  readonly text: string;
  readonly attachments_json: string | null;
  readonly context_json: string | null;
  readonly is_streaming: number;
  readonly created_at: string;
  readonly updated_at: string;
}

export interface ForkItemBase {
  readonly id: TurnItemId;
  readonly threadId: ThreadId;
  readonly runId: null;
  readonly nodeId: null;
  readonly providerThreadId: null;
  readonly providerTurnId: null;
  readonly nativeItemRef: null;
  readonly parentItemId: null;
  readonly ordinal: number;
  readonly title: string | null;
  readonly startedAt: DateTime.Utc | null;
  readonly completedAt: DateTime.Utc | null;
  readonly updatedAt: DateTime.Utc;
}

export interface ForkTimelineItem {
  readonly kind: "item";
  readonly id: TurnItemId;
  readonly at: string;
  readonly build: (ordinal: number) => OrchestrationV2TurnItem;
}

export type ForkTimelineEntry = ForkTimelineMessage | ForkTimelineItem;

type Json = Record<string, unknown>;

function parseJson(json: string | null): unknown {
  if (json === null) return undefined;
  try {
    return JSON.parse(json);
  } catch {
    return undefined;
  }
}

const record = (value: unknown): Json | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Json)
    : undefined;

const text = (value: unknown): string | undefined =>
  typeof value === "string" && value.trim() !== "" ? value : undefined;

const clip = (value: string, max: number) =>
  value.length > max ? `${value.slice(0, max)}…` : value;

const epochIso = (ms: number) => DateTime.formatIso(DateTime.makeUnsafe(ms));

// ---------------------------------------------------------------------------
// Native session refs

/** The native session a v1 resume cursor points at, or null when it can't be resumed. */
export function nativeThreadIdFromCursor(driver: string, cursorJson: string | null): string | null {
  const cursor = record(parseJson(cursorJson));
  if (cursor === undefined) return null;
  switch (driver) {
    case "claudeAgent": {
      const id = text(cursor.resume) ?? text(cursor.sessionId);
      if (id === undefined || !UUID.test(id)) return null;
      return typeof cursor.turnCount === "number" && cursor.turnCount <= 0 ? null : id;
    }
    case "codex":
      return text(cursor.threadId) ?? null;
    case "wuwei":
      return text(cursor.sessionId) ?? null;
    default:
      return null;
  }
}

/**
 * The provider thread a migrated thread resumes, when its v1 runtime row
 * belongs to the thread's own provider instance. Its timestamps are the
 * thread's, so migration doesn't reorder the sidebar.
 */
export function legacyProviderThread(input: {
  readonly thread: OrchestrationV2AppThread;
  readonly runtime: LegacyRuntimeColumns;
  readonly contextUsage: ThreadTokenUsageSnapshot | null;
}): OrchestrationV2ProviderThread | null {
  const { thread, runtime } = input;
  const driver = runtime.runtime_provider_name;
  if (driver === null) return null;
  const instanceId = runtime.runtime_provider_instance_id ?? driver;
  if (instanceId !== thread.providerInstanceId) return null;
  const nativeId = nativeThreadIdFromCursor(driver, runtime.runtime_resume_cursor_json);
  if (nativeId === null) return null;
  const driverKind = ProviderDriverKind.make(driver);
  return {
    id: deriveProviderThread({ driver: driverKind, nativeThreadId: nativeId }),
    driver: driverKind,
    providerInstanceId: ProviderInstanceId.make(instanceId),
    providerSessionId: null,
    appThreadId: thread.id,
    ownerNodeId: null,
    nativeThreadRef: { driver: driverKind, nativeId, strength: "strong" },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: null,
    lastRunOrdinal: null,
    handoffIds: [],
    forkedFrom: null,
    pendingBackgroundTasks: [],
    contextUsage: input.contextUsage,
    createdAt: thread.createdAt,
    updatedAt: thread.updatedAt,
  };
}

const decodeTokenUsage = Schema.decodeUnknownOption(ThreadTokenUsageSnapshot);

/** The thread's last v1 context-window reading, for the context meter. */
export function contextUsageFromActivities(
  rows: ReadonlyArray<LegacyActivityRow>,
): ThreadTokenUsageSnapshot | null {
  for (let index = rows.length - 1; index >= 0; index -= 1) {
    const row = rows[index]!;
    if (row.kind !== "context-window.updated") continue;
    const decoded = decodeTokenUsage(parseJson(row.payload_json));
    if (Option.isSome(decoded)) return decoded.value;
  }
  return null;
}

// ---------------------------------------------------------------------------
// v1 activities → timeline items

function itemBase(
  threadId: ThreadId,
  id: TurnItemId,
  ordinal: number,
  title: string | null,
  startedAt: string,
  completedAt: string | null,
): ForkItemBase {
  const started = DateTime.makeUnsafe(startedAt);
  const completed = completedAt === null ? null : DateTime.makeUnsafe(completedAt);
  return {
    id,
    threadId,
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal,
    title,
    startedAt: started,
    completedAt: completed,
    updatedAt: completed ?? started,
  };
}

function contentText(value: unknown): string | undefined {
  if (typeof value === "string") return value;
  if (!Array.isArray(value)) return undefined;
  const parts = value.flatMap((part) => {
    const block = record(part);
    if (block === undefined) return [];
    if (typeof block.text === "string") return [block.text];
    const nested = record(block.content);
    if (nested !== undefined && typeof nested.text === "string") return [nested.text];
    return block.type === "image" ? ["[image]"] : [];
  });
  return parts.length === 0 ? undefined : parts.join("\n");
}

function toolOutput(data: Json | undefined): string | undefined {
  if (data === undefined) return undefined;
  const result = record(data.result);
  const item = record(data.item);
  const output =
    (result === undefined ? undefined : contentText(result.content)) ??
    contentText(data.content) ??
    text(item?.aggregatedOutput) ??
    (item?.result === undefined ? undefined : contentText(record(item.result)?.content));
  return output === undefined ? undefined : clip(output, MAX_CARRIED_OUTPUT_CHARS);
}

/** "Bash: {...}" / "Read: {...}" details carry the tool name before the colon. */
const detailToolName = (detail: string | undefined) =>
  detail === undefined ? undefined : /^([A-Za-z0-9_.-]+):\s/.exec(detail)?.[1];

function toolItem(
  threadId: ThreadId,
  rows: ReadonlyArray<{ readonly row: LegacyActivityRow; readonly payload: Json }>,
): ForkTimelineItem | null {
  const first = rows[0]!;
  const completed = [...rows].reverse().find((entry) => entry.row.kind === "tool.completed");
  const last = completed ?? rows[rows.length - 1]!;
  const payload = last.payload;
  const data =
    record(payload.data) ?? rows.map((entry) => record(entry.payload.data)).find(Boolean);
  const input =
    record(data?.input) ?? record(data?.rawInput) ?? record(record(data?.item)?.arguments);
  const item = record(data?.item);
  const detail = text(payload.detail);
  const toolName = text(data?.toolName) ?? text(item?.tool) ?? detailToolName(detail);
  const output = toolOutput(data);
  // v1 marks a failure on different rows per provider: Claude on a
  // tool.updated row or as result.is_error, Codex on the item itself.
  const failed =
    rows.some((entry) => entry.payload.status === "failed") ||
    record(data?.result)?.is_error === true ||
    item?.status === "failed";
  const status: OrchestrationV2TurnItem["status"] =
    completed === undefined ? "interrupted" : failed ? "failed" : "completed";
  const title = text(payload.title) ?? text(last.row.summary) ?? null;
  const id = TurnItemId.make(`${ITEM_PREFIX}:${first.row.activity_id}`);
  const startedAt = first.row.created_at;
  const completedAt = completed?.row.created_at ?? null;
  const base = (ordinal: number) => itemBase(threadId, id, ordinal, title, startedAt, completedAt);
  const stripName = (value: string) =>
    toolName !== undefined && value.startsWith(`${toolName}: `)
      ? value.slice(toolName.length + 2)
      : value;

  const dynamicTool = (ordinal: number): OrchestrationV2TurnItem => ({
    ...base(ordinal),
    status,
    type: "dynamic_tool",
    toolName:
      toolName ?? text(payload.title) ?? text(data?.kind) ?? text(payload.itemType) ?? "tool",
    input: input ?? (detail === undefined ? null : stripName(detail)),
    ...(output === undefined ? {} : { output }),
  });

  switch (payload.itemType) {
    case "command_execution": {
      const command =
        text(input?.command) ??
        text(data?.command) ??
        text(item?.command) ??
        (detail === undefined ? undefined : stripName(detail));
      if (command === undefined) return { kind: "item", id, at: startedAt, build: dynamicTool };
      const exitCode = typeof item?.exitCode === "number" ? Math.trunc(item.exitCode) : undefined;
      return {
        kind: "item",
        id,
        at: startedAt,
        build: (ordinal) => ({
          ...base(ordinal),
          status,
          type: "command_execution",
          input: command,
          ...(output === undefined ? {} : { output }),
          ...(exitCode === undefined ? {} : { exitCode }),
        }),
      };
    }
    case "file_change": {
      const changes = Array.isArray(item?.changes) ? item.changes.map(record) : [];
      const fileName =
        text(input?.file_path) ??
        text(input?.path) ??
        (Array.isArray(data?.locations) ? text(record(data.locations[0])?.path) : undefined) ??
        text(changes[0]?.path);
      if (fileName === undefined || toolName === "TodoWrite") {
        return { kind: "item", id, at: startedAt, build: dynamicTool };
      }
      const oldStr = text(input?.old_string) ?? text(input?.oldText);
      const newStr = text(input?.new_string) ?? text(input?.newText) ?? text(input?.content);
      return {
        kind: "item",
        id,
        at: startedAt,
        build: (ordinal) => ({
          ...base(ordinal),
          status,
          type: "file_change",
          fileName,
          ...(oldStr === undefined ? {} : { oldStr }),
          ...(newStr === undefined ? {} : { newStr }),
        }),
      };
    }
    case "web_search": {
      const query =
        text(input?.query) ??
        text(item?.query) ??
        (detail === undefined ? undefined : stripName(detail));
      if (query === undefined) return { kind: "item", id, at: startedAt, build: dynamicTool };
      return {
        kind: "item",
        id,
        at: startedAt,
        build: (ordinal) => ({ ...base(ordinal), status, type: "web_search", patterns: [query] }),
      };
    }
    default:
      return { kind: "item", id, at: startedAt, build: dynamicTool };
  }
}

function failureItem(
  threadId: ThreadId,
  row: LegacyActivityRow,
  payload: Json,
): ForkTimelineItem | null {
  const message = text(payload.message) ?? text(payload.detail) ?? text(row.summary);
  if (message === undefined) return null;
  const id = TurnItemId.make(`${ITEM_PREFIX}:${row.activity_id}`);
  return {
    kind: "item",
    id,
    at: row.created_at,
    build: (ordinal) => ({
      ...itemBase(threadId, id, ordinal, text(row.summary) ?? null, row.created_at, row.created_at),
      status: "failed",
      type: "error",
      failure: {
        class: "provider_error",
        message: clip(message.trim(), MAX_FAILURE_MESSAGE_CHARS),
        code: null,
        retryable: null,
      },
    }),
  };
}

function noticeItem(threadId: ThreadId, row: LegacyActivityRow, message: string): ForkTimelineItem {
  const id = TurnItemId.make(`${ITEM_PREFIX}:${row.activity_id}`);
  return {
    kind: "item",
    id,
    at: row.created_at,
    build: (ordinal) => ({
      ...itemBase(threadId, id, ordinal, null, row.created_at, row.created_at),
      status: "completed",
      type: "system_notice",
      message,
    }),
  };
}

function reasoningItem(
  threadId: ThreadId,
  row: LegacyActivityRow,
  reasoning: string,
): ForkTimelineItem {
  const id = TurnItemId.make(`${ITEM_PREFIX}:${row.activity_id}`);
  return {
    kind: "item",
    id,
    at: row.created_at,
    build: (ordinal) => ({
      ...itemBase(threadId, id, ordinal, "Thinking", row.created_at, row.created_at),
      status: "completed",
      type: "reasoning",
      text: reasoning,
      streaming: false,
    }),
  };
}

function planItem(
  threadId: ThreadId,
  row: LegacyActivityRow,
  payload: Json,
): ForkTimelineItem | null {
  const plan = Array.isArray(payload.plan) ? payload.plan.map(record) : [];
  const steps = plan.flatMap((step, index) => {
    const stepText = text(step?.step);
    if (stepText === undefined) return [];
    const status =
      step?.status === "completed"
        ? "completed"
        : step?.status === "inProgress"
          ? "running"
          : "pending";
    return [{ id: `${index + 1}`, text: stepText.trim(), status } as const];
  });
  if (steps.length === 0) return null;
  const id = TurnItemId.make(`${ITEM_PREFIX}:${row.activity_id}`);
  return {
    kind: "item",
    id,
    at: row.created_at,
    build: (ordinal) => ({
      ...itemBase(threadId, id, ordinal, "Plan", row.created_at, row.created_at),
      status: "completed",
      type: "todo_list",
      planId: PlanId.make(`migration:v1:plan:${row.activity_id}`),
      steps,
    }),
  };
}

/** A background task (Claude subagent or shell task) as one row, like v1's work log. */
function taskItem(
  threadId: ThreadId,
  rows: ReadonlyArray<{ readonly row: LegacyActivityRow; readonly payload: Json }>,
): ForkTimelineItem {
  const first = rows[0]!;
  const completed = [...rows].reverse().find((entry) => entry.row.kind === "task.completed");
  const last = completed ?? rows[rows.length - 1]!;
  const titled = rows.map((entry) => text(entry.payload.title)).find(Boolean);
  const title = titled ?? text(first.payload.detail) ?? text(first.row.summary) ?? "Task";
  const id = TurnItemId.make(`${ITEM_PREFIX}:${first.row.activity_id}`);
  const status: OrchestrationV2TurnItem["status"] =
    completed === undefined
      ? "interrupted"
      : last.payload.status === "failed"
        ? "failed"
        : "completed";
  const output = completed === undefined ? undefined : text(completed.payload.detail);
  return {
    kind: "item",
    id,
    at: first.row.created_at,
    build: (ordinal) => ({
      ...itemBase(
        threadId,
        id,
        ordinal,
        clip(title, 200),
        first.row.created_at,
        completed?.row.created_at ?? null,
      ),
      status,
      type: "dynamic_tool",
      toolName: "Task",
      input: { title, taskType: text(first.payload.taskType) ?? null },
      ...(output === undefined ? {} : { output }),
    }),
  };
}

/**
 * v1 activities as timeline items: one item per tool call (its last row),
 * per background task, plan, error and notice. Codex reasoning updates and
 * wuwei's "dream" rows (its thinking, in v1) become reasoning items.
 */
export function activityTimeline(
  threadId: ThreadId,
  rows: ReadonlyArray<LegacyActivityRow>,
): ReadonlyArray<ForkTimelineItem> {
  const parsed = rows.map((row) => ({ row, payload: record(parseJson(row.payload_json)) ?? {} }));
  const items: ForkTimelineItem[] = [];
  const toolGroups = new Map<string, Array<(typeof parsed)[number]>>();
  const taskGroups = new Map<string, Array<(typeof parsed)[number]>>();
  for (const entry of parsed) {
    const { row, payload } = entry;
    if (row.kind.startsWith("tool.")) {
      const resultId = text(record(record(payload.data)?.result)?.tool_use_id);
      const key = text(payload.toolCallId) ?? resultId;
      if (key !== undefined) {
        const group = toolGroups.get(key) ?? [];
        group.push(entry);
        toolGroups.set(key, group);
      } else if (row.kind === "tool.completed") {
        const item = toolItem(threadId, [entry]);
        if (item !== null) items.push(item);
      }
      continue;
    }
    if (row.kind.startsWith("task.")) {
      const isThinking = payload.taskType === "dream" || row.summary === "Reasoning update";
      const thought = text(payload.detail) ?? text(payload.summary);
      if (row.kind === "task.progress" && isThinking) {
        if (thought !== undefined) items.push(reasoningItem(threadId, row, thought));
        continue;
      }
      const taskId = text(payload.taskId);
      // Codex's turn-level "default" task only repeats the turn's own reply.
      if (taskId === undefined || payload.taskType === "dream" || payload.taskType === "default") {
        continue;
      }
      const group = taskGroups.get(taskId) ?? [];
      group.push(entry);
      taskGroups.set(taskId, group);
      continue;
    }
    switch (row.kind) {
      case "runtime.error":
      case "provider.turn.start.failed": {
        const item = failureItem(threadId, row, payload);
        if (item !== null) items.push(item);
        break;
      }
      case "runtime.warning": {
        const message = text(payload.message) ?? text(row.summary);
        if (message !== undefined) items.push(noticeItem(threadId, row, message));
        break;
      }
      case "turn.plan.updated": {
        const item = planItem(threadId, row, payload);
        if (item !== null) items.push(item);
        break;
      }
      default:
        // context-window.updated feeds the provider thread; checkpoints don't carry over.
        break;
    }
  }
  for (const group of toolGroups.values()) {
    const item = toolItem(threadId, group);
    if (item !== null) items.push(item);
  }
  for (const group of taskGroups.values()) items.push(taskItem(threadId, group));
  return items;
}

// ---------------------------------------------------------------------------
// wuwei session file → timeline

/** A wuwei chat's whole history, from its own session file. */
export function wuweiTimeline(
  threadId: ThreadId,
  transcript: WuweiTranscript,
): ReadonlyArray<ForkTimelineEntry> {
  const entries: ForkTimelineEntry[] = [];
  const prefix = `wuwei:${transcript.id}`;
  const message = (
    id: string,
    role: "user" | "assistant",
    body: string,
    at: number,
  ): ForkTimelineMessage => ({
    kind: "message",
    message_id: `${prefix}:${id}`,
    thread_id: threadId,
    role,
    text: body,
    attachments_json: null,
    context_json: null,
    is_streaming: 0,
    created_at: epochIso(at),
    updated_at: epochIso(at),
  });
  for (const turn of transcript.turns) {
    const images =
      turn.userImageCount > 0
        ? `\n\n[${turn.userImageCount} image${turn.userImageCount === 1 ? "" : "s"}]`
        : "";
    // A chat that opens on an assistant message (a duet or a /fork) has no user turn start.
    if (turn.userText.trim() !== "" || turn.userImageCount > 0) {
      entries.push(message(turn.userItemId, "user", `${turn.userText}${images}`, turn.at));
    }
    turn.entries.forEach((entry, index) => {
      const key = `${entry.id}:${index}`;
      switch (entry.kind) {
        case "text":
          if (entry.text.trim() !== "")
            entries.push(message(key, "assistant", entry.text, entry.at));
          return;
        case "steer":
          entries.push(message(key, "user", entry.text, entry.at));
          return;
        case "reasoning": {
          if (entry.text.trim() === "") return;
          const id = TurnItemId.make(`${ITEM_PREFIX}:${prefix}:${key}`);
          const at = epochIso(entry.at);
          entries.push({
            kind: "item",
            id,
            at,
            build: (ordinal) => ({
              ...itemBase(threadId, id, ordinal, "Thinking", at, at),
              status: "completed",
              type: "reasoning",
              text: entry.text,
              streaming: false,
            }),
          });
          return;
        }
        case "tool": {
          const id = TurnItemId.make(`${ITEM_PREFIX}:${prefix}:${key}`);
          const startedAt = epochIso(entry.startedAt);
          const completedAt = entry.completedAt === null ? null : epochIso(entry.completedAt);
          const status: OrchestrationV2TurnItem["status"] =
            entry.status === "completed"
              ? "completed"
              : entry.status === "failed"
                ? "failed"
                : entry.status === "cancelled"
                  ? "cancelled"
                  : "interrupted";
          const fields = wuweiToolItemFields(
            entry.name,
            entry.input,
            entry.output,
            entry.status === "failed",
          );
          entries.push({
            kind: "item",
            id,
            at: startedAt,
            build: (ordinal) =>
              ({
                ...itemBase(threadId, id, ordinal, entry.title, startedAt, completedAt),
                status,
                ...fields,
              }) as OrchestrationV2TurnItem,
          });
          return;
        }
      }
    });
  }
  return entries;
}

// ---------------------------------------------------------------------------
// Ordering

const entryAt = (entry: ForkTimelineEntry) =>
  entry.kind === "message" ? entry.created_at : entry.at;
const entryRank = (entry: ForkTimelineEntry) =>
  entry.kind === "item" ? 1 : entry.role === "user" ? 0 : 2;

/**
 * One timeline, ordered by time. At equal times a user message comes first
 * and an assistant message last, so tools sit inside the turn they ran in.
 * The sort is stable, so a source's own order breaks remaining ties.
 */
export function orderTimeline(
  entries: ReadonlyArray<ForkTimelineEntry>,
): ReadonlyArray<ForkTimelineEntry> {
  return entries
    .map((entry, index) => ({ entry, index, at: Date.parse(entryAt(entry)) }))
    .sort(
      (left, right) =>
        (Number.isNaN(left.at) || Number.isNaN(right.at) ? 0 : left.at - right.at) ||
        entryRank(left.entry) - entryRank(right.entry) ||
        left.index - right.index,
    )
    .map(({ entry }) => entry);
}
