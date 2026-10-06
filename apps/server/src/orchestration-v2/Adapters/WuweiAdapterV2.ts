/**
 * WuweiAdapterV2 — orchestrator-v2 adapter for wuwei, Jack's own agent
 * harness, running natively inside this server process.
 *
 * wuwei is not a child process here: its harness is imported once from
 * wuwei's own bundle (provider/wuwei/WuweiEmbed.ts) and every turn runs in
 * this process. Chats stay wuwei's own session files (`~/.wuwei/sessions`),
 * and the wuwei chat id is the durable `nativeThreadRef`, so a chat started in
 * T3 opens in the wuwei terminal app and vice versa. wuwei holds a chat's lock
 * only while a turn runs.
 *
 * wuwei runs its tools unattended (it has no approvals) and has no plan mode,
 * rollback or native fork. A message sent while a turn runs is queued as the
 * next turn.
 */
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import {
  defaultInstanceIdForDriver,
  ProviderDriverKind,
  WuweiSettings,
  type ChatAttachment,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderFailure,
  type OrchestrationV2ProviderRef,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type ProviderInstanceId,
  type ThreadTokenUsageSnapshot,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import * as ServerConfig from "../../config.ts";
import { loadWuweiHost } from "../../provider/wuwei/WuweiEmbed.ts";
import type {
  WuweiHost,
  WuweiPromptResult,
  WuweiTurnEvent,
  WuweiUserContent,
} from "../../provider/wuwei/wuweiEmbedApi.ts";
import { wuweiToolItemFields } from "../../provider/wuwei/wuweiItems.ts";
import * as IdAllocator from "../IdAllocator.ts";
import * as ProviderAdapter from "../ProviderAdapter.ts";
import {
  ProviderAdapterDriverCreateError,
  type ProviderAdapterDriver,
  type ProviderAdapterDriverCreateInput,
} from "../ProviderAdapterDriver.ts";
import { makeProviderFailure } from "../ProviderFailure.ts";
import { turnScopedSelectionTransition } from "../ProviderSelectionTransition.ts";

export const WUWEI_PROVIDER = ProviderDriverKind.make("wuwei");
const STREAM_FLUSH_MS = 50;
const DEFAULT_WUWEI_SETTINGS = Schema.decodeSync(WuweiSettings)({});

const WuweiProviderCapabilitiesV2 = {
  runtimePolicy: { enforcement: "client-boundary" },
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: true,
    supportsProviderSwitchingViaHandoff: true,
    supportsRuntimeModeSwitchInSession: true,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: true,
    canRollbackThread: false,
    canForkThread: false,
    canForkFromTurn: false,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: false,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: false,
    supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: true,
    terminalStatusQuality: "strong",
  },
  streaming: {
    streamsAssistantText: true,
    streamsReasoning: true,
    streamsToolOutput: false,
    streamsPlanText: false,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: true,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: true,
    supportsMcpTools: false,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: false,
    supportsFileReadApproval: false,
    supportsFileChangeApproval: false,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: false,
    approvalCallbacksAreLiveOnly: true,
    approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: false,
    supportsStructuredQuestions: false,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: false,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: false,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: true,
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: true,
    supportsDeltaHandoff: true,
    supportsFullThreadHandoff: true,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: false,
    providerRollbackReturnsSnapshot: false,
    providerCanReadConversationSnapshot: true,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "weak",
    nativeItemIds: "strong",
    nativeRequestIds: "none",
  },
} satisfies OrchestrationV2ProviderCapabilities;

export interface WuweiAdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly settings: WuweiSettings;
  readonly fileSystem: FileSystem.FileSystem;
  readonly idAllocator: IdAllocator.IdAllocatorV2["Service"];
  readonly serverConfig: ServerConfig.ServerConfig["Service"];
}

const providerRef = (
  nativeId: string,
  strength: "strong" | "weak" = "strong",
): OrchestrationV2ProviderRef => ({ driver: WUWEI_PROVIDER, nativeId, strength });

/** A wuwei prompt's events, then its result, as one async sequence. */
type PromptStreamItem =
  | { readonly kind: "event"; readonly event: WuweiTurnEvent }
  | { readonly kind: "done"; readonly result: WuweiPromptResult }
  | { readonly kind: "failed"; readonly error: unknown };

function promptStream(
  start: (onEvent: (event: WuweiTurnEvent) => void) => Promise<WuweiPromptResult>,
): AsyncIterable<PromptStreamItem> {
  const buffer: Array<PromptStreamItem> = [];
  let wake: (() => void) | null = null;
  const push = (item: PromptStreamItem) => {
    buffer.push(item);
    wake?.();
    wake = null;
  };
  start((event) => push({ kind: "event", event })).then(
    (result) => push({ kind: "done", result }),
    (error: unknown) => push({ kind: "failed", error }),
  );
  return {
    async *[Symbol.asyncIterator]() {
      while (true) {
        const item = buffer.shift();
        if (item === undefined) {
          await new Promise<void>((resolve) => (wake = resolve));
          continue;
        }
        yield item;
        if (item.kind !== "event") return;
      }
    },
  };
}

interface StreamItemState {
  readonly nativeItemId: string;
  readonly kind: "assistant_message" | "reasoning";
  text: string;
  completed: boolean;
  flushScheduled: boolean;
  readonly startedAt: DateTime.Utc;
}

interface ToolState {
  readonly name: string;
  readonly title: string;
  readonly input: Readonly<Record<string, unknown>>;
  readonly startedAt: DateTime.Utc;
}

interface ActiveWuweiTurn {
  readonly turnInput: ProviderAdapter.ProviderAdapterV2TurnInput;
  readonly providerTurn: OrchestrationV2ProviderTurn;
  readonly abort: AbortController;
  readonly itemOrdinals: Map<string, number>;
  nextItemOrdinal: number;
  readonly streamItems: Map<string, StreamItemState>;
  readonly tools: Map<string, ToolState>;
  /** One assistant item collects a slash command's output. */
  commandOutput: StreamItemState | null;
  interrupted: boolean;
  failure: OrchestrationV2ProviderFailure | null;
}

interface WuweiThreadState {
  providerThread: OrchestrationV2ProviderThread;
  activeTurn: ActiveWuweiTurn | null;
}

export function makeWuweiAdapterV2(
  options: WuweiAdapterV2Options,
): ProviderAdapter.ProviderAdapterV2Shape {
  const { idAllocator } = options;

  const protocolError = (detail: string, cause?: unknown) =>
    new ProviderAdapter.ProviderAdapterProtocolError({
      driver: WUWEI_PROVIDER,
      detail,
      ...(cause === undefined ? {} : { cause }),
    });

  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver: WUWEI_PROVIDER,
    getCapabilities: () => Effect.succeed(WuweiProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed(turnScopedSelectionTransition()),
    openSession: Effect.fn("WuweiAdapterV2.openSession")(function* (
      input: ProviderAdapter.ProviderAdapterV2OpenSessionInput,
    ) {
      const scope = yield* Effect.scope;
      const cwd = input.runtimePolicy.cwd ?? options.serverConfig.cwd;
      const host: WuweiHost = yield* loadWuweiHost(options.settings.embedPath).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapter.ProviderAdapterOpenSessionError({
              driver: WUWEI_PROVIDER,
              providerSessionId: input.providerSessionId,
              cause,
            }),
        ),
      );
      const contextWindows = new Map(
        (yield* Effect.promise(() => host.catalog())).models.map(
          (model) => [model.slug, model.contextWindow] as const,
        ),
      );

      const now = yield* DateTime.now;
      let sessionEntity: OrchestrationV2ProviderSession = {
        id: input.providerSessionId,
        driver: WUWEI_PROVIDER,
        providerInstanceId: options.instanceId,
        status: "ready",
        cwd,
        model: input.modelSelection.model,
        capabilities: WuweiProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const events = yield* Queue.unbounded<
        ProviderAdapter.ProviderAdapterV2Event,
        ProviderAdapter.ProviderAdapterV2Error | Cause.Done
      >();
      // A turn's own events and its terminalization never interleave with a
      // concurrent Stop or session close.
      const sessionEventPermit = yield* Semaphore.make(1);
      let threadState: WuweiThreadState | null = null;

      const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
        Queue.offer(events, event).pipe(Effect.asVoid);

      const updateProviderSession = (
        status: OrchestrationV2ProviderSession["status"],
        lastError: string | null = sessionEntity.lastError,
      ) =>
        Effect.gen(function* () {
          const updatedAt = yield* DateTime.now;
          sessionEntity = { ...sessionEntity, status, lastError, updatedAt };
          yield* emit({
            type: "provider_session.updated",
            driver: WUWEI_PROVIDER,
            providerSession: sessionEntity,
          });
        });

      const updateProviderThread = (
        state: WuweiThreadState,
        patch: Partial<OrchestrationV2ProviderThread>,
      ) =>
        Effect.gen(function* () {
          const updatedAt = yield* DateTime.now;
          state.providerThread = { ...state.providerThread, ...patch, updatedAt };
          yield* emit({
            type: "provider_thread.updated",
            driver: WUWEI_PROVIDER,
            providerThread: state.providerThread,
          });
        });

      const itemOrdinal = (turn: ActiveWuweiTurn, nativeItemId: string): number => {
        const existing = turn.itemOrdinals.get(nativeItemId);
        if (existing !== undefined) return existing;
        const ordinal = turn.nextItemOrdinal++;
        turn.itemOrdinals.set(nativeItemId, ordinal);
        return ordinal;
      };

      const baseItemFields = (
        turn: ActiveWuweiTurn,
        nativeItemId: string,
        startedAt: DateTime.Utc,
        updatedAt: DateTime.Utc,
      ) => ({
        id: idAllocator.derive.turnItemFromProviderItem({ driver: WUWEI_PROVIDER, nativeItemId }),
        threadId: turn.turnInput.threadId,
        runId: turn.turnInput.runId,
        nodeId: idAllocator.derive.nodeFromProviderItem({ driver: WUWEI_PROVIDER, nativeItemId }),
        providerThreadId: turn.turnInput.providerThread.id,
        providerTurnId: turn.providerTurn.id,
        nativeItemRef: providerRef(nativeItemId),
        parentItemId: null,
        ordinal: itemOrdinal(turn, nativeItemId),
        startedAt,
        updatedAt,
      });

      const emitItemNode = (
        turn: ActiveWuweiTurn,
        nativeItemId: string,
        kind: OrchestrationV2ExecutionNode["kind"],
        status: OrchestrationV2ExecutionNode["status"],
        startedAt: DateTime.Utc,
        completedAt: DateTime.Utc | null,
      ) =>
        emit({
          type: "node.updated",
          driver: WUWEI_PROVIDER,
          node: {
            id: idAllocator.derive.nodeFromProviderItem({ driver: WUWEI_PROVIDER, nativeItemId }),
            threadId: turn.turnInput.threadId,
            runId: turn.turnInput.runId,
            parentNodeId: turn.turnInput.rootNodeId,
            rootNodeId: turn.turnInput.rootNodeId,
            kind,
            status,
            countsForRun: false,
            providerThreadId: turn.turnInput.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            nativeItemRef: providerRef(nativeItemId),
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt,
            completedAt,
          },
        });

      // ── streaming text / reasoning ────────────────────────

      const emitStreamItem = (turn: ActiveWuweiTurn, item: StreamItemState, streaming: boolean) =>
        Effect.gen(function* () {
          const emittedAt = yield* DateTime.now;
          const base = baseItemFields(turn, item.nativeItemId, item.startedAt, emittedAt);
          yield* emitItemNode(
            turn,
            item.nativeItemId,
            item.kind,
            streaming ? "running" : "completed",
            item.startedAt,
            streaming ? null : emittedAt,
          );
          if (item.kind === "assistant_message") {
            const messageId = idAllocator.derive.messageFromProviderItem({
              driver: WUWEI_PROVIDER,
              nativeItemId: item.nativeItemId,
            });
            yield* emit({
              type: "turn_item.updated",
              driver: WUWEI_PROVIDER,
              turnItem: {
                ...base,
                status: streaming ? "running" : "completed",
                title: null,
                completedAt: streaming ? null : emittedAt,
                type: "assistant_message",
                messageId,
                text: item.text,
                streaming,
              },
            });
            yield* emit({
              type: "message.updated",
              driver: WUWEI_PROVIDER,
              message: {
                id: messageId,
                threadId: turn.turnInput.threadId,
                runId: turn.turnInput.runId,
                nodeId: base.nodeId,
                role: "assistant",
                text: item.text,
                attachments: [],
                streaming,
                createdBy: "agent",
                creationSource: "provider",
                createdAt: item.startedAt,
                updatedAt: emittedAt,
              },
            });
            return;
          }
          yield* emit({
            type: "turn_item.updated",
            driver: WUWEI_PROVIDER,
            turnItem: {
              ...base,
              status: streaming ? "running" : "completed",
              title: "Thinking",
              completedAt: streaming ? null : emittedAt,
              type: "reasoning",
              text: item.text,
              streaming,
            },
          });
        });

      const scheduleStreamFlush = (turn: ActiveWuweiTurn, item: StreamItemState) =>
        Effect.gen(function* () {
          if (item.flushScheduled || item.completed) return;
          item.flushScheduled = true;
          yield* Effect.sleep(Duration.millis(STREAM_FLUSH_MS)).pipe(
            Effect.andThen(
              Effect.suspend(() => {
                item.flushScheduled = false;
                return item.completed ? Effect.void : emitStreamItem(turn, item, true);
              }),
            ),
            sessionEventPermit.withPermits(1),
            Effect.forkIn(scope),
          );
        });

      const streamItemFor = Effect.fnUntraced(function* (
        turn: ActiveWuweiTurn,
        kind: StreamItemState["kind"],
        nativeItemId: string,
      ) {
        const existing = turn.streamItems.get(nativeItemId);
        if (existing !== undefined) return existing;
        const item: StreamItemState = {
          nativeItemId,
          kind,
          text: "",
          completed: false,
          flushScheduled: false,
          startedAt: yield* DateTime.now,
        };
        turn.streamItems.set(nativeItemId, item);
        // Reserve the ordinal on the first delta so items keep stream order.
        itemOrdinal(turn, nativeItemId);
        return item;
      });

      const completeStreamItem = (turn: ActiveWuweiTurn, item: StreamItemState, text?: string) =>
        Effect.suspend(() => {
          if (item.completed) return Effect.void;
          item.completed = true;
          if (text !== undefined && text.length > 0) item.text = text;
          return item.text.length === 0 ? Effect.void : emitStreamItem(turn, item, false);
        });

      const blockId = (turn: ActiveWuweiTurn, step: number, index: number) =>
        `${turn.providerTurn.id}:s${step}:c${index}`;

      // ── tools ─────────────────────────────────────────────

      const emitToolItem = Effect.fnUntraced(function* (
        turn: ActiveWuweiTurn,
        callId: string,
        tool: ToolState,
        result: { readonly isError: boolean; readonly output: string } | null,
      ) {
        const emittedAt = yield* DateTime.now;
        const status =
          result === null
            ? "running"
            : result.isError
              ? turn.interrupted
                ? "interrupted"
                : "failed"
              : "completed";
        const completedAt = result === null ? null : emittedAt;
        yield* emitItemNode(turn, callId, "tool_call", status, tool.startedAt, completedAt);
        yield* emit({
          type: "turn_item.updated",
          driver: WUWEI_PROVIDER,
          turnItem: {
            ...baseItemFields(turn, callId, tool.startedAt, emittedAt),
            status,
            title: tool.title,
            completedAt,
            ...wuweiToolItemFields(
              tool.name,
              tool.input,
              result?.output ?? null,
              result?.isError ?? false,
            ),
          },
        });
      });

      // ── usage ─────────────────────────────────────────────

      /**
       * Each model request reports the context it used. The provider thread
       * carries it as `contextUsage` (with the last request's cached share),
       * which drives the context meter and handoff budgets.
       */
      const reportUsage = (
        state: WuweiThreadState,
        usage: Extract<WuweiTurnEvent, { readonly type: "usage" }>,
      ) => {
        if (usage.contextWindow <= 0) return Effect.void;
        const inputTokens = usage.input + usage.cacheRead + usage.cacheWrite;
        const contextUsage: ThreadTokenUsageSnapshot = {
          usedTokens: usage.contextTokens,
          maxTokens: usage.contextWindow,
          inputTokens,
          cachedInputTokens: usage.cacheRead,
          outputTokens: usage.output,
          ...(usage.reasoning === undefined ? {} : { reasoningOutputTokens: usage.reasoning }),
          lastUsedTokens: usage.contextTokens,
          lastInputTokens: inputTokens,
          lastCachedInputTokens: usage.cacheRead,
          lastOutputTokens: usage.output,
          ...(usage.reasoning === undefined ? {} : { lastReasoningOutputTokens: usage.reasoning }),
        };
        contextWindows.set(usage.model, usage.contextWindow);
        return updateProviderThread(state, { contextUsage });
      };

      // ── turn events ───────────────────────────────────────

      const handleTurnEvent = Effect.fnUntraced(function* (
        state: WuweiThreadState,
        turn: ActiveWuweiTurn,
        event: WuweiTurnEvent,
      ) {
        switch (event.type) {
          case "step_start":
            return;
          case "thinking_delta":
          case "text_delta": {
            const item = yield* streamItemFor(
              turn,
              event.type === "thinking_delta" ? "reasoning" : "assistant_message",
              blockId(turn, event.step, event.index),
            );
            item.text += event.delta;
            yield* scheduleStreamFlush(turn, item);
            return;
          }
          case "thinking_end":
          case "text_end": {
            const item = yield* streamItemFor(
              turn,
              event.type === "thinking_end" ? "reasoning" : "assistant_message",
              blockId(turn, event.step, event.index),
            );
            yield* completeStreamItem(turn, item, event.text);
            return;
          }
          case "tool_start": {
            const tool: ToolState = {
              name: event.name,
              title: event.title,
              input: event.input,
              startedAt: yield* DateTime.now,
            };
            turn.tools.set(event.callId, tool);
            // Reserve the ordinal so the tool sits after the text that called it.
            itemOrdinal(turn, event.callId);
            yield* emitToolItem(turn, event.callId, tool, null);
            return;
          }
          case "tool_end": {
            const tool = turn.tools.get(event.callId) ?? {
              name: event.name,
              title: event.name,
              input: {},
              startedAt: yield* DateTime.now,
            };
            yield* emitToolItem(turn, event.callId, tool, {
              isError: event.isError,
              output: event.output,
            });
            return;
          }
          case "usage":
            yield* reportUsage(state, event);
            return;
          case "steer_committed":
            return;
          case "command_output": {
            const item =
              turn.commandOutput ??
              (yield* streamItemFor(turn, "assistant_message", `${turn.providerTurn.id}:command`));
            turn.commandOutput = item;
            item.text += event.text;
            yield* scheduleStreamFlush(turn, item);
            return;
          }
          case "error":
            turn.failure = makeProviderFailure({ message: event.message, class: "provider_error" });
            return;
        }
      });

      const finalizeTurn = Effect.fnUntraced(function* (state: WuweiThreadState) {
        const turn = state.activeTurn;
        if (turn === null) return;
        state.activeTurn = null;
        const completedAt = yield* DateTime.now;
        yield* Effect.forEach(
          Array.from(turn.streamItems.values()),
          (item) => completeStreamItem(turn, item),
          { discard: true },
        );
        const failure = turn.interrupted ? null : turn.failure;
        yield* emit({
          type: "provider_turn.updated",
          driver: WUWEI_PROVIDER,
          threadId: turn.turnInput.threadId,
          providerTurn: {
            ...turn.providerTurn,
            status: turn.interrupted ? "interrupted" : failure !== null ? "failed" : "completed",
            completedAt,
          },
        });
        yield* updateProviderThread(state, { status: "idle" });
        yield* updateProviderSession(
          failure !== null ? "error" : "ready",
          failure?.message ?? null,
        );
        if (failure !== null) {
          const failureItemId = `terminal-failure:${turn.providerTurn.id}`;
          yield* emit({
            type: "turn_item.updated",
            driver: WUWEI_PROVIDER,
            turnItem: {
              ...baseItemFields(turn, failureItemId, completedAt, completedAt),
              status: "failed",
              title: null,
              completedAt,
              type: "error",
              failure,
            },
          });
          yield* emit({
            type: "turn.terminal",
            driver: WUWEI_PROVIDER,
            providerThreadId: state.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            runOrdinal: turn.turnInput.runOrdinal,
            failureItemOrdinal: itemOrdinal(turn, failureItemId),
            status: "failed",
            failure,
            threadDisposition: "reusable",
          });
          return;
        }
        yield* emit({
          type: "turn.terminal",
          driver: WUWEI_PROVIDER,
          providerThreadId: state.providerThread.id,
          providerTurnId: turn.providerTurn.id,
          runOrdinal: turn.turnInput.runOrdinal,
          status: turn.interrupted ? "interrupted" : "completed",
          failure: null,
          threadDisposition: "reusable",
        });
      });

      /** Runs one wuwei prompt to completion, mapping its events as they arrive. */
      const runPrompt = (
        state: WuweiThreadState,
        turn: ActiveWuweiTurn,
        prompt: {
          readonly threadId: string;
          readonly content: WuweiUserContent;
          readonly model: string;
          readonly effort: string | undefined;
        },
      ) =>
        Stream.fromAsyncIterable(
          promptStream((onEvent) =>
            host.prompt({
              threadId: prompt.threadId,
              cwd,
              content: prompt.content,
              model: prompt.model,
              ...(prompt.effort === undefined ? {} : { effort: prompt.effort }),
              signal: turn.abort.signal,
              onEvent,
            }),
          ),
          (cause) => protocolError("wuwei prompt stream failed", cause),
        ).pipe(
          Stream.runForEach((item) =>
            sessionEventPermit.withPermits(1)(
              Effect.gen(function* () {
                if (state.activeTurn !== turn) return;
                if (item.kind === "event") {
                  yield* handleTurnEvent(state, turn, item.event);
                  return;
                }
                if (item.kind === "failed") {
                  turn.failure ??= makeProviderFailure({
                    cause: item.error,
                    class: "provider_error",
                  });
                } else if (item.result.stopReason === "cancelled") {
                  turn.interrupted = true;
                } else if (item.result.stopReason === "error") {
                  turn.failure ??= makeProviderFailure({
                    message: item.result.errorMessage,
                    class: "provider_error",
                  });
                }
                yield* finalizeTurn(state);
              }),
            ),
          ),
          Effect.catchCause((cause) =>
            sessionEventPermit.withPermits(1)(
              Effect.gen(function* () {
                if (state.activeTurn !== turn) return;
                turn.failure ??= makeProviderFailure({ cause, class: "unknown" });
                yield* finalizeTurn(state);
              }),
            ),
          ),
        );

      const promptContent = Effect.fnUntraced(function* (
        text: string,
        attachments: ReadonlyArray<ChatAttachment>,
      ) {
        const images: Array<{
          readonly type: "image";
          readonly data: string;
          readonly mimeType: string;
        }> = [];
        const notes: Array<string> = [];
        for (const attachment of attachments) {
          const path = resolveAttachmentPath({
            attachmentsDir: options.serverConfig.attachmentsDir,
            attachment,
          });
          if (path === null) continue;
          if (attachment.mimeType.startsWith("image/")) {
            const bytes = yield* options.fileSystem.readFile(path);
            images.push({
              type: "image",
              data: Buffer.from(bytes).toString("base64"),
              mimeType: attachment.mimeType,
            });
          } else {
            notes.push(`[Attachment saved at ${path}]`);
          }
        }
        const body = notes.length === 0 ? text : `${text}\n\n${notes.join("\n")}`;
        const content: WuweiUserContent =
          images.length === 0 ? body : [{ type: "text", text: body }, ...images];
        return content;
      });

      // ── session runtime ───────────────────────────────────

      const registerThread = Effect.fnUntraced(function* (
        threadInput: ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
      ) {
        if (threadState !== null && threadState.activeTurn !== null) {
          return yield* protocolError("Cannot register a wuwei chat while a turn is active");
        }
        const existing = threadInput.existingProviderThread;
        const resumeId = existing?.nativeThreadRef?.nativeId ?? null;
        if (resumeId !== null && !host.hasThread(resumeId)) {
          return yield* protocolError(`wuwei chat ${resumeId} no longer exists`);
        }
        const nativeId = resumeId ?? host.createThread(cwd);
        const updatedAt = yield* DateTime.now;
        const providerThread: OrchestrationV2ProviderThread =
          existing !== undefined
            ? {
                ...existing,
                providerSessionId: input.providerSessionId,
                nativeThreadRef: providerRef(nativeId),
                status: "idle",
                updatedAt,
              }
            : {
                id: idAllocator.derive.providerThread({
                  driver: WUWEI_PROVIDER,
                  nativeThreadId: nativeId,
                }),
                driver: WUWEI_PROVIDER,
                providerInstanceId: options.instanceId,
                providerSessionId: input.providerSessionId,
                appThreadId: threadInput.threadId,
                ownerNodeId: null,
                nativeThreadRef: providerRef(nativeId),
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                pendingBackgroundTasks: [],
                createdAt: updatedAt,
                updatedAt,
              };
        threadState = { providerThread, activeTurn: null };
        yield* emit({ type: "provider_thread.updated", driver: WUWEI_PROVIDER, providerThread });
        return providerThread;
      });

      // Closing the session stops a running turn; nothing of wuwei outlives it.
      yield* Effect.addFinalizer(() =>
        sessionEventPermit.withPermits(1)(
          Effect.gen(function* () {
            const state = threadState;
            if (state?.activeTurn != null) {
              state.activeTurn.interrupted = true;
              state.activeTurn.abort.abort();
              yield* finalizeTurn(state);
            }
            yield* updateProviderSession("stopped", null);
            yield* Queue.end(events);
          }),
        ),
      );

      const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        instanceId: options.instanceId,
        driver: WUWEI_PROVIDER,
        providerSessionId: input.providerSessionId,
        get providerSession() {
          return sessionEntity;
        },
        events: Stream.fromQueue(events),
        getModelContextWindow: (selection) =>
          selection.instanceId === options.instanceId
            ? contextWindows.get(selection.model)
            : undefined,
        ensureThread: (threadInput) =>
          registerThread(threadInput).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterEnsureThreadError({
                  driver: WUWEI_PROVIDER,
                  threadId: threadInput.threadId,
                  cause,
                }),
            ),
          ),
        resumeThread: (threadInput) =>
          registerThread({
            threadId:
              threadInput.threadId ?? threadInput.providerThread.appThreadId ?? input.threadId,
            modelSelection: threadInput.modelSelection ?? input.modelSelection,
            runtimePolicy: threadInput.runtimePolicy ?? input.runtimePolicy,
            existingProviderThread: threadInput.providerThread,
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterResumeThreadError({
                  driver: WUWEI_PROVIDER,
                  providerSessionId: input.providerSessionId,
                  providerThreadId: threadInput.providerThread.id,
                  cause,
                }),
            ),
          ),
        startTurn: (turnInput) =>
          Effect.gen(function* () {
            const state = threadState;
            if (state === null) {
              return yield* protocolError("wuwei session has no registered chat");
            }
            if (state.activeTurn !== null) {
              return yield* protocolError(
                `wuwei chat ${turnInput.providerThread.id} already has an active turn`,
              );
            }
            const nativeId = turnInput.providerThread.nativeThreadRef?.nativeId;
            if (nativeId == null || state.providerThread.nativeThreadRef?.nativeId !== nativeId) {
              return yield* protocolError("wuwei turn requested for a different chat");
            }
            state.providerThread = turnInput.providerThread;
            const content = yield* promptContent(
              turnInput.message.text,
              turnInput.message.attachments,
            );
            const startedAt = yield* DateTime.now;
            const syntheticNativeTurnId = `${state.providerThread.id}:attempt:${turnInput.attemptId}`;
            const providerTurn: OrchestrationV2ProviderTurn = {
              id: idAllocator.derive.providerTurn({
                driver: WUWEI_PROVIDER,
                nativeTurnId: syntheticNativeTurnId,
              }),
              providerThreadId: turnInput.providerThread.id,
              nodeId: turnInput.rootNodeId,
              runAttemptId: turnInput.attemptId,
              nativeTurnRef: providerRef(syntheticNativeTurnId, "weak"),
              ordinal: turnInput.providerTurnOrdinal,
              status: "running",
              startedAt,
              completedAt: null,
            };
            const turn: ActiveWuweiTurn = {
              turnInput,
              providerTurn,
              abort: new AbortController(),
              itemOrdinals: new Map(),
              nextItemOrdinal: turnInput.providerTurnOrdinal * 100 + 1,
              streamItems: new Map(),
              tools: new Map(),
              commandOutput: null,
              interrupted: false,
              failure: null,
            };
            const effort =
              getModelSelectionStringOptionValue(turnInput.modelSelection, "reasoningEffort") ??
              turnInput.runtimePolicy.reasoningEffort;
            yield* sessionEventPermit.withPermits(1)(
              Effect.gen(function* () {
                state.activeTurn = turn;
                yield* emit({
                  type: "provider_turn.updated",
                  driver: WUWEI_PROVIDER,
                  threadId: turnInput.threadId,
                  providerTurn,
                });
                yield* updateProviderThread(state, {
                  status: "active",
                  firstRunOrdinal: state.providerThread.firstRunOrdinal ?? turnInput.runOrdinal,
                  lastRunOrdinal: turnInput.runOrdinal,
                });
                yield* updateProviderSession("running", null);
              }),
            );
            yield* runPrompt(state, turn, {
              threadId: nativeId,
              content,
              model: turnInput.modelSelection.model,
              effort,
            }).pipe(Effect.forkIn(scope));
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterTurnStartError({
                  driver: WUWEI_PROVIDER,
                  threadId: turnInput.threadId,
                  providerThreadId: turnInput.providerThread.id,
                  runId: turnInput.runId,
                  cause,
                }),
            ),
          ),
        steerTurn: (steerInput) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterSteerRunUnsupportedError({
              driver: WUWEI_PROVIDER,
              providerThreadId: steerInput.providerThread.id,
            }),
          ),
        interruptTurn: (interruptInput) =>
          Effect.gen(function* () {
            const turn = threadState?.activeTurn ?? null;
            // Stop on a settled turn: nothing of it is still running.
            if (turn === null && interruptInput.requestRuntimeRestart === true) return;
            if (turn === null || turn.providerTurn.id !== interruptInput.providerTurnId) {
              return yield* protocolError(
                `wuwei turn ${interruptInput.providerTurnId} is not active`,
              );
            }
            turn.interrupted = true;
            // wuwei aborts its request and tools, then settles the prompt as cancelled.
            turn.abort.abort();
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterInterruptError({
                  driver: WUWEI_PROVIDER,
                  providerThreadId: interruptInput.providerThread.id,
                  providerTurnId: interruptInput.providerTurnId,
                  cause,
                }),
            ),
          ),
        respondToRuntimeRequest: (requestInput) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
              driver: WUWEI_PROVIDER,
              requestId: requestInput.requestId,
              cause: "wuwei raises no runtime requests",
            }),
          ),
        readThreadSnapshot: (snapshotInput) =>
          Effect.gen(function* () {
            const nativeId = snapshotInput.providerThread.nativeThreadRef?.nativeId;
            if (nativeId == null) {
              return yield* protocolError("wuwei snapshot requested without a chat id");
            }
            const transcript = yield* Effect.try({
              try: () => host.transcript(nativeId),
              catch: (cause) => protocolError(`wuwei chat ${nativeId} is unreadable`, cause),
            });
            const threadId = snapshotInput.providerThread.appThreadId ?? input.threadId;
            const message = (id: string, role: "user" | "assistant", text: string, at: number) => {
              const createdAt = DateTime.makeUnsafe(at);
              return {
                id: idAllocator.derive.messageFromProviderItem({
                  driver: WUWEI_PROVIDER,
                  nativeItemId: `${nativeId}:snapshot:${id}`,
                }),
                threadId,
                runId: null,
                nodeId: null,
                role,
                text,
                attachments: [],
                streaming: false,
                createdBy: role === "user" ? ("user" as const) : ("agent" as const),
                creationSource: "provider" as const,
                createdAt,
                updatedAt: createdAt,
              };
            };
            const messages = transcript.turns.flatMap((turn) => [
              ...(turn.userText.trim() === ""
                ? []
                : [message(turn.userItemId, "user", turn.userText, turn.at)]),
              ...turn.entries.flatMap((entry, index) =>
                entry.kind === "text" && entry.text.trim() !== ""
                  ? [message(`${entry.id}:${index}`, "assistant", entry.text, entry.at)]
                  : entry.kind === "steer"
                    ? [message(`${entry.id}:${index}`, "user", entry.text, entry.at)]
                    : [],
              ),
            ]);
            return {
              providerThread: threadState?.providerThread ?? snapshotInput.providerThread,
              providerTurns: [],
              messages,
              runtimeRequests: [],
            };
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterReadThreadSnapshotError({
                  driver: WUWEI_PROVIDER,
                  providerThreadId: snapshotInput.providerThread.id,
                  cause,
                }),
            ),
          ),
        rollbackThread: (rollbackInput) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterRollbackThreadError({
              driver: WUWEI_PROVIDER,
              providerThreadId: rollbackInput.providerThread.id,
              cause: "wuwei does not roll back conversations",
            }),
          ),
        forkThread: (forkInput) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterForkThreadError({
              driver: WUWEI_PROVIDER,
              providerThreadId: forkInput.sourceProviderThread.id,
              cause: "wuwei forks with its own /fork command",
            }),
          ),
      };
      return runtime;
    }),
  });
}

// ── driver ────────────────────────────────────────────────────

export type WuweiAdapterV2DriverEnv =
  | FileSystem.FileSystem
  | IdAllocator.IdAllocatorV2
  | ServerConfig.ServerConfig;

export const WuweiAdapterV2Driver: ProviderAdapterDriver<WuweiSettings, WuweiAdapterV2DriverEnv> = {
  driverKind: WUWEI_PROVIDER,
  configSchema: WuweiSettings,
  defaultConfig: (): WuweiSettings => DEFAULT_WUWEI_SETTINGS,
  create: Effect.fn("WuweiAdapterV2Driver.create")(
    function* (input: ProviderAdapterDriverCreateInput<WuweiSettings>) {
      return makeWuweiAdapterV2({
        instanceId: input.instanceId,
        settings: { ...input.config, enabled: input.enabled },
        fileSystem: yield* FileSystem.FileSystem,
        idAllocator: yield* IdAllocator.IdAllocatorV2,
        serverConfig: yield* ServerConfig.ServerConfig,
      });
    },
    (effect, input) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterDriverCreateError({
              driver: WUWEI_PROVIDER,
              instanceId: input.instanceId,
              detail: "Failed to create wuwei adapter.",
              cause,
            }),
        ),
      ),
  ),
};

export const WUWEI_DEFAULT_INSTANCE_ID = defaultInstanceIdForDriver(WUWEI_PROVIDER);
