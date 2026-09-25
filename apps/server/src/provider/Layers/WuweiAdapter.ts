/**
 * WuweiAdapter — wuwei (Jack's harness) over ACP.
 *
 * One ACP process per thread session. wuwei owns its context and thread log
 * (~/.wuwei/sessions), so the resume cursor is just its thread id. Model and
 * reasoning effort ride on every prompt's `_meta.wuwei`, and T3's runtime
 * instructions are not added: the model sees the user's text and wuwei's own
 * prompt only.
 *
 * @module provider/Layers/WuweiAdapter
 */
import {
  EventId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  ProviderDriverKind,
  ProviderInstanceId,
  type ThreadId,
  type ThreadTokenUsageSnapshot,
  TurnId,
  type WuweiSettings,
} from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import type * as EffectAcpSchema from "effect-acp/schema";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import { mapAcpToAdapterError } from "../acp/AcpAdapterSupport.ts";
import {
  makeAcpAssistantItemEvent,
  makeAcpContentDeltaEvent,
  makeAcpToolCallEvent,
} from "../acp/AcpCoreRuntimeEvents.ts";
import { makeAcpNativeLoggerFactory } from "../acp/AcpNativeLogging.ts";
import type * as AcpSessionRuntime from "../acp/AcpSessionRuntime.ts";
import { makeWuweiAcpRuntime } from "../acp/WuweiAcpSupport.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import type { ProviderAdapterError } from "../Errors.ts";
import type { EventNdjsonLogger } from "./EventNdjsonLogger.ts";

const PROVIDER = ProviderDriverKind.make("wuwei");
const WUWEI_RESUME_VERSION = 1 as const;

export interface WuweiAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly nativeEventLogger?: EventNdjsonLogger;
  readonly instanceId?: ProviderInstanceId;
}

interface TokenTotals {
  input: number;
  cachedInput: number;
  output: number;
  reasoning: number;
}

interface WuweiSessionContext {
  readonly threadId: ThreadId;
  session: ProviderSession;
  readonly scope: Scope.Closeable;
  readonly acp: AcpSessionRuntime.AcpSessionRuntime["Service"];
  notificationFiber: Fiber.Fiber<void, never> | undefined;
  readonly turns: Array<{ id: TurnId; items: Array<unknown> }>;
  activeTurnId: TurnId | undefined;
  readonly totals: TokenTotals;
  stopped: boolean;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseWuweiResume(raw: unknown): { sessionId: string } | undefined {
  if (!isRecord(raw)) return undefined;
  if (raw.schemaVersion !== WUWEI_RESUME_VERSION) return undefined;
  if (typeof raw.sessionId !== "string" || !raw.sessionId.trim()) return undefined;
  return { sessionId: raw.sessionId.trim() };
}

function count(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) && value > 0 ? Math.round(value) : 0;
}

/** Per-step usage wuwei attaches to `usage_update` under `_meta.wuwei`. */
function stepUsageFromPayload(rawPayload: unknown) {
  const update =
    isRecord(rawPayload) && isRecord(rawPayload.update) ? rawPayload.update : undefined;
  const meta =
    update && isRecord(update._meta) && isRecord(update._meta.wuwei) ? update._meta.wuwei : {};
  const cacheRead = count(meta.cacheRead);
  const cacheWrite = count(meta.cacheWrite);
  return {
    input: count(meta.input) + cacheRead + cacheWrite,
    cachedInput: cacheRead,
    output: count(meta.output),
    reasoning: count(meta.reasoning),
  };
}

export function makeWuweiAdapter(wuweiSettings: WuweiSettings, options?: WuweiAdapterLiveOptions) {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("wuwei");
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const serverConfig = yield* Effect.service(ServerConfig);
    const crypto = yield* Crypto.Crypto;
    const adapterScope = yield* Effect.scope;
    const makeAcpNativeLoggers = yield* makeAcpNativeLoggerFactory();

    const sessions = new Map<ThreadId, WuweiSessionContext>();
    const threadLocksRef = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());
    const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const randomUUIDv4 = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "crypto/randomUUIDv4",
            detail: "Failed to generate wuwei runtime identifier.",
            cause,
          }),
      ),
    );
    const makeEventStamp = () =>
      Effect.all({
        eventId: Effect.map(randomUUIDv4, (id) => EventId.make(id)),
        createdAt: nowIso,
      });
    const offerRuntimeEvent = (event: ProviderRuntimeEvent) =>
      PubSub.publish(runtimeEventPubSub, event).pipe(Effect.asVoid);

    const withThreadLock = <A, E, R>(threadId: string, effect: Effect.Effect<A, E, R>) =>
      SynchronizedRef.modifyEffect(threadLocksRef, (current) => {
        const existing = current.get(threadId);
        if (existing) return Effect.succeed([existing, current] as const);
        return Semaphore.make(1).pipe(
          Effect.map(
            (semaphore) => [semaphore, new Map(current).set(threadId, semaphore)] as const,
          ),
        );
      }).pipe(Effect.flatMap((semaphore) => semaphore.withPermit(effect)));

    const requireSession = (
      threadId: ThreadId,
    ): Effect.Effect<WuweiSessionContext, ProviderAdapterSessionNotFoundError> => {
      const ctx = sessions.get(threadId);
      return !ctx || ctx.stopped
        ? Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }))
        : Effect.succeed(ctx);
    };

    const stopSessionInternal = (ctx: WuweiSessionContext) =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        ctx.stopped = true;
        if (ctx.notificationFiber) {
          yield* Fiber.interrupt(ctx.notificationFiber);
        }
        yield* Effect.ignore(Scope.close(ctx.scope, Exit.void));
        sessions.delete(ctx.threadId);
        yield* offerRuntimeEvent({
          type: "session.exited",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: { exitKind: "graceful" },
        });
      });

    const handleRuntimeEvent = (
      ctx: WuweiSessionContext,
      event: AcpSessionRuntime.AcpSessionRuntimeEvent,
    ) =>
      Effect.gen(function* () {
        const stamp = yield* makeEventStamp();
        switch (event._tag) {
          case "EventStreamBarrier":
            yield* Deferred.succeed(event.acknowledge, undefined);
            return;
          case "ConnectionTerminated":
            // The wuwei process is gone. Drop the session; the next turn resumes the thread.
            yield* stopSessionInternal(ctx).pipe(Effect.forkIn(adapterScope));
            return;
          case "AssistantItemStarted":
          case "AssistantItemCompleted":
            yield* offerRuntimeEvent(
              makeAcpAssistantItemEvent({
                stamp,
                provider: PROVIDER,
                threadId: ctx.threadId,
                turnId: ctx.activeTurnId,
                itemId: event.itemId,
                lifecycle:
                  event._tag === "AssistantItemStarted" ? "item.started" : "item.completed",
              }),
            );
            return;
          case "ContentDelta":
          case "ThoughtDelta":
            yield* offerRuntimeEvent(
              makeAcpContentDeltaEvent({
                stamp,
                provider: PROVIDER,
                threadId: ctx.threadId,
                turnId: ctx.activeTurnId,
                ...(event._tag === "ContentDelta" && event.itemId ? { itemId: event.itemId } : {}),
                ...(event._tag === "ThoughtDelta" ? { streamKind: "reasoning_text" as const } : {}),
                text: event.text,
                rawPayload: event.rawPayload,
              }),
            );
            return;
          case "ToolCallUpdated":
            yield* offerRuntimeEvent(
              makeAcpToolCallEvent({
                stamp,
                provider: PROVIDER,
                threadId: ctx.threadId,
                turnId: ctx.activeTurnId,
                toolCall: event.toolCall,
                rawPayload: event.rawPayload,
              }),
            );
            return;
          case "UsageUpdated": {
            const step = stepUsageFromPayload(event.rawPayload);
            ctx.totals.input += step.input;
            ctx.totals.cachedInput += step.cachedInput;
            ctx.totals.output += step.output;
            ctx.totals.reasoning += step.reasoning;
            const usage: ThreadTokenUsageSnapshot = {
              usedTokens: count(event.used),
              ...(count(event.size) > 0 ? { maxTokens: count(event.size) } : {}),
              inputTokens: ctx.totals.input,
              cachedInputTokens: ctx.totals.cachedInput,
              outputTokens: ctx.totals.output,
              reasoningOutputTokens: ctx.totals.reasoning,
              lastUsedTokens: count(event.used),
              lastInputTokens: step.input,
              lastCachedInputTokens: step.cachedInput,
              lastOutputTokens: step.output,
              lastReasoningOutputTokens: step.reasoning,
            };
            yield* offerRuntimeEvent({
              type: "thread.token-usage.updated",
              ...stamp,
              provider: PROVIDER,
              threadId: ctx.threadId,
              turnId: ctx.activeTurnId,
              payload: { usage },
            });
            return;
          }
          default:
            return;
        }
      });

    const startSession: ProviderAdapterShape<ProviderAdapterError>["startSession"] = (input) =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          if (input.provider !== undefined && input.provider !== PROVIDER) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
            });
          }
          if (!input.cwd?.trim()) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "cwd is required and must be non-empty.",
            });
          }
          const cwd = path.resolve(input.cwd.trim());
          const modelSelection =
            input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
          const existing = sessions.get(input.threadId);
          if (existing && !existing.stopped) {
            yield* stopSessionInternal(existing);
          }

          const sessionScope = yield* Scope.make("sequential");
          let sessionScopeTransferred = false;
          yield* Effect.addFinalizer(() =>
            sessionScopeTransferred ? Effect.void : Scope.close(sessionScope, Exit.void),
          );

          const resumeSessionId = parseWuweiResume(input.resumeCursor)?.sessionId;
          const acp = yield* makeWuweiAcpRuntime({
            wuweiSettings,
            ...(options?.environment ? { environment: options.environment } : {}),
            childProcessSpawner,
            cwd,
            ...(resumeSessionId ? { resumeSessionId } : {}),
            clientInfo: { name: "t3-code", version: "0.0.0" },
            ...makeAcpNativeLoggers({
              nativeEventLogger: options?.nativeEventLogger,
              provider: PROVIDER,
              threadId: input.threadId,
            }),
          }).pipe(
            Effect.provideService(Crypto.Crypto, crypto),
            Effect.provideService(Scope.Scope, sessionScope),
            Effect.mapError(
              (cause) =>
                new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  detail: cause.message,
                  cause,
                }),
            ),
          );
          const started = yield* acp
            .start()
            .pipe(
              Effect.mapError((error) =>
                mapAcpToAdapterError(PROVIDER, input.threadId, "session/start", error),
              ),
            );

          const now = yield* nowIso;
          const ctx: WuweiSessionContext = {
            threadId: input.threadId,
            session: {
              provider: PROVIDER,
              providerInstanceId: boundInstanceId,
              status: "ready",
              runtimeMode: input.runtimeMode,
              cwd,
              model: modelSelection?.model,
              threadId: input.threadId,
              resumeCursor: { schemaVersion: WUWEI_RESUME_VERSION, sessionId: started.sessionId },
              createdAt: now,
              updatedAt: now,
            },
            scope: sessionScope,
            acp,
            notificationFiber: undefined,
            turns: [],
            activeTurnId: undefined,
            totals: { input: 0, cachedInput: 0, output: 0, reasoning: 0 },
            stopped: false,
          };

          ctx.notificationFiber = yield* Stream.runDrain(
            Stream.mapEffect(acp.getEvents(), (event) => handleRuntimeEvent(ctx, event)),
          ).pipe(
            Effect.catch((cause) =>
              Effect.logError("Failed to process wuwei runtime notification.", { cause }),
            ),
            // Forked into the session scope so it outlives startSession.
            Effect.forkIn(ctx.scope),
          );
          sessions.set(input.threadId, ctx);
          sessionScopeTransferred = true;

          yield* offerRuntimeEvent({
            type: "session.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { resume: started.initializeResult },
          });
          yield* offerRuntimeEvent({
            type: "session.state.changed",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { state: "ready", reason: "wuwei ACP session ready" },
          });
          yield* offerRuntimeEvent({
            type: "thread.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { providerThreadId: started.sessionId },
          });
          return ctx.session;
        }).pipe(Effect.scoped),
      );

    const sendTurn: ProviderAdapterShape<ProviderAdapterError>["sendTurn"] = (input) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(input.threadId);
        const turnModelSelection =
          input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
        const model = turnModelSelection?.model ?? ctx.session.model;
        const effort = getModelSelectionStringOptionValue(turnModelSelection, "reasoningEffort");

        const promptParts: Array<EffectAcpSchema.ContentBlock> = [];
        const text = input.input?.trim() ?? "";
        if (text) promptParts.push({ type: "text", text });
        for (const attachment of input.attachments ?? []) {
          // Images go to the model; other files reach it through the path line in the text.
          if (attachment.type !== "image") continue;
          const attachmentPath = resolveAttachmentPath({
            attachmentsDir: serverConfig.attachmentsDir,
            attachment,
          });
          if (!attachmentPath) {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "session/prompt",
              detail: `Invalid attachment id '${attachment.id}'.`,
            });
          }
          const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "session/prompt",
                  detail: cause.message,
                  cause,
                }),
            ),
          );
          promptParts.push({
            type: "image",
            data: Buffer.from(bytes).toString("base64"),
            mimeType: attachment.mimeType,
          });
        }
        if (promptParts.length === 0) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: "Turn requires non-empty text or attachments.",
          });
        }

        const turnId = TurnId.make(yield* randomUUIDv4);
        ctx.activeTurnId = turnId;
        ctx.session = { ...ctx.session, activeTurnId: turnId, updatedAt: yield* nowIso };
        yield* offerRuntimeEvent({
          type: "turn.started",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: input.threadId,
          turnId,
          payload: { ...(model ? { model } : {}), ...(effort ? { effort } : {}) },
        });

        const result = yield* ctx.acp
          .prompt({
            prompt: promptParts,
            _meta: { wuwei: { ...(model ? { model } : {}), ...(effort ? { effort } : {}) } },
          })
          .pipe(
            Effect.mapError((error) =>
              mapAcpToAdapterError(PROVIDER, input.threadId, "session/prompt", error),
            ),
            // Close the turn opened above; the reactor records the failure itself.
            Effect.tapError((error) =>
              Effect.gen(function* () {
                yield* ctx.acp.drainEvents;
                yield* offerRuntimeEvent({
                  type: "turn.completed",
                  ...(yield* makeEventStamp()),
                  provider: PROVIDER,
                  threadId: input.threadId,
                  turnId,
                  payload: { state: "failed", errorMessage: error.message || "wuwei turn failed" },
                });
              }),
            ),
          );
        yield* ctx.acp.drainEvents;

        const turnRecord = ctx.turns.find((turn) => turn.id === turnId);
        if (turnRecord) turnRecord.items.push({ prompt: promptParts, result });
        else ctx.turns.push({ id: turnId, items: [{ prompt: promptParts, result }] });
        ctx.session = { ...ctx.session, updatedAt: yield* nowIso, ...(model ? { model } : {}) };

        const usage = result.usage;
        yield* offerRuntimeEvent({
          type: "turn.completed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: input.threadId,
          turnId,
          payload: {
            state: result.stopReason === "cancelled" ? "cancelled" : "completed",
            stopReason: result.stopReason ?? null,
            ...(usage
              ? {
                  tokenUsage: {
                    usageScope: "main_agent" as const,
                    usageStatus: "complete" as const,
                    inputTokens: count(usage.inputTokens),
                    outputTokens: count(usage.outputTokens),
                    cachedInputTokens: count(usage.cachedReadTokens),
                    cacheCreationTokens: count(usage.cachedWriteTokens),
                    reasoningTokens: count(usage.thoughtTokens),
                    hasSubagents: false,
                  },
                }
              : {}),
          },
        });
        return { threadId: input.threadId, turnId, resumeCursor: ctx.session.resumeCursor };
      });

    const interruptTurn: ProviderAdapterShape<ProviderAdapterError>["interruptTurn"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        yield* Effect.ignore(ctx.acp.cancel);
      });

    const unsupported = (method: string, detail: string) =>
      Effect.fail(new ProviderAdapterRequestError({ provider: PROVIDER, method, detail }));

    const stopAllSessions = () =>
      Effect.forEach(Array.from(sessions.values()), stopSessionInternal, { discard: true });

    yield* Effect.addFinalizer(() =>
      stopAllSessions().pipe(
        Effect.catch((cause) => Effect.logError("Failed to stop wuwei sessions.", { cause })),
        Effect.tap(() => PubSub.shutdown(runtimeEventPubSub)),
      ),
    );

    return {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "in-session", supportsConversationRollback: false },
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest: () =>
        unsupported("session/request_permission", "wuwei does not ask for approvals."),
      respondToUserInput: () => unsupported("user-input", "wuwei does not ask for user input."),
      readThread: (threadId) =>
        requireSession(threadId).pipe(Effect.map((ctx) => ({ threadId, turns: ctx.turns }))),
      rollbackThread: () =>
        unsupported("thread/rollback", "wuwei sessions do not support rollback from T3 yet."),
      stopSession: (threadId) =>
        withThreadLock(
          threadId,
          requireSession(threadId).pipe(Effect.flatMap(stopSessionInternal)),
        ),
      listSessions: () =>
        Effect.sync(() => Array.from(sessions.values(), (ctx) => ({ ...ctx.session }))),
      hasSession: (threadId) =>
        Effect.sync(() => {
          const ctx = sessions.get(threadId);
          return ctx !== undefined && !ctx.stopped;
        }),
      stopAll: stopAllSessions,
      streamEvents: Stream.fromPubSub(runtimeEventPubSub),
    } satisfies ProviderAdapterShape<ProviderAdapterError>;
  });
}
