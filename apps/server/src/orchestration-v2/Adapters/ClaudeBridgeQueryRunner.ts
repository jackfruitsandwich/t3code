/**
 * ClaudeBridgeQueryRunner - ClaudeAdapterV2's transport over a Claude Code
 * process that runs the t3bridge mod, instead of a CLI process the Agent SDK
 * owns. The session T3 drives and the one open in a terminal are then the
 * same process: a prompt from either side is one turn in one transcript.
 *
 * Opening a query attaches to the live process of the native session, or
 * launches one in the background (`claude --bg --resume <id>`, which also
 * creates a new session under that id). The transcript file is tailed into
 * SDK `assistant` and tool-result `user` frames; the mod's turn events become
 * `init` and `result` frames; a prompt is an inbox command the mod submits as
 * the user; closing detaches and leaves the process running.
 *
 * A session that has no transcript yet cannot be started in the background
 * under a chosen id (`claude --bg` ignores `--session-id`, and `--bg --resume`
 * of an unknown id dies), so a thread's first turn runs through the SDK
 * transport, which creates the session under T3's id, and closes after that
 * turn's result. Every later turn attaches through the bridge.
 *
 * Homes without the plugin, and `T3CODE_CLAUDE_TRANSPORT=sdk`, keep the SDK
 * transport.
 */
import * as NodeOS from "node:os";

import type { SDKMessage, SDKUserMessage } from "@anthropic-ai/claude-agent-sdk";
import { HostProcessEnvironment, HostProcessWorkingDirectory } from "@t3tools/shared/hostProcess";
import type * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import * as ClaudeBridgeRegistry from "../../claude-bridge/ClaudeBridgeRegistry.ts";
import {
  CLAUDE_BRIDGE_T3_HEARTBEAT_MS,
  isClaudeBridgeSessionLive,
  makeClaudeBridgeFrameTranslator,
  readTranscriptRows,
  type ClaudeBridgeEvent,
} from "../../claude-bridge/ClaudeBridgeProtocol.ts";
import { isRecord } from "../../import/sessionImportTypes.ts";
import * as ProviderEventLoggers from "../../provider/ProviderEventLoggers.ts";
import * as ClaudeAdapterV2 from "./ClaudeAdapterV2.ts";

export const CLAUDE_TRANSPORT_ENV = "T3CODE_CLAUDE_TRANSPORT";
const TAIL_INTERVAL_MS = 300;
/** A turn's result waits this long after the last transcript row, so a late row cannot land after it. */
const COMPLETE_QUIET_MS = 400;

const runnerError = (cause: unknown, method: string) =>
  new ClaudeAdapterV2.ClaudeAgentSdkQueryRunnerError({ method, cause });

/** The text of a prompt as the mod can submit it: text blocks only. */
/**
 * The prompt as the mod can submit it: text only. Claude Code's mods API
 * takes no image blocks from a plugin (`$.prompt.submit` refuses attachments
 * and `$.session.append` refuses image rows), so a message with images rides
 * on the attachment paths the adapter already writes into its text, plus one
 * line that tells the model to open them with the Read tool.
 */
export function claudeBridgePromptText(message: SDKUserMessage): string | null {
  const content = message.message.content;
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return null;
  const parts: Array<string> = [];
  let imageCount = 0;
  for (const block of content) {
    if (!isRecord(block)) continue;
    if (block.type === "text" && typeof block.text === "string") {
      parts.push(block.text);
    } else if (block.type === "image") {
      imageCount += 1;
    }
  }
  if (imageCount > 0) parts.push(claudeBridgeImageNote(imageCount));
  return parts.length === 0 ? null : parts.join("\n\n");
}

export function claudeBridgeImageNote(imageCount: number): string {
  const noun = imageCount === 1 ? "an image" : `${imageCount} images`;
  return `[T3: the user attached ${noun} to this message. This Claude Code process receives only the text, so before answering open each attached image with the Read tool at its "is saved at" path above.]`;
}

export const layer: Layer.Layer<
  ClaudeAdapterV2.ClaudeAgentSdkQueryRunner,
  never,
  | ClaudeBridgeRegistry.ClaudeBridgeRegistry
  | ProviderEventLoggers.ProviderEventLoggers
  | Crypto.Crypto
  | FileSystem.FileSystem
  | Path.Path
> = Layer.effect(
  ClaudeAdapterV2.ClaudeAgentSdkQueryRunner,
  Effect.gen(function* () {
    const registry = yield* ClaudeBridgeRegistry.ClaudeBridgeRegistry;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const crypto = yield* Crypto.Crypto;
    const hostEnvironment = yield* HostProcessEnvironment;
    const hostCwd = yield* HostProcessWorkingDirectory;
    const { native: nativeEventLogger } = yield* ProviderEventLoggers.ProviderEventLoggers;
    const sdkRunner = Context.get(
      yield* Layer.build(ClaudeAdapterV2.layerQueryRunner),
      ClaudeAdapterV2.ClaudeAgentSdkQueryRunner,
    );

    /** Claude stores `<config dir>/projects/<cwd with every non-alphanumeric as "-">/<sid>.jsonl`. */
    const transcriptExists = (configDir: string, cwd: string, sid: string) =>
      fs
        .exists(path.join(configDir, "projects", cwd.replace(/[^A-Za-z0-9]/g, "-"), `${sid}.jsonl`))
        .pipe(Effect.orElseSucceed(() => false));

    /** The SDK transport for one turn: its stream ends with the turn's result and the process closes. */
    const openSdkForFirstTurn = (input: ClaudeAdapterV2.ClaudeAgentSdkQueryOpenInput) =>
      Effect.gen(function* () {
        const sdk = yield* sdkRunner.open(input);
        let closed = false;
        const closeOnce = Effect.suspend(() => {
          if (closed) return Effect.void;
          closed = true;
          return sdk.close;
        });
        return {
          ...sdk,
          messages: sdk.messages.pipe(
            Stream.takeUntil((message) => message.type === "result"),
            Stream.ensuring(closeOnce.pipe(Effect.ignore)),
          ),
          close: closeOnce,
        } satisfies ClaudeAdapterV2.ClaudeAgentSdkQuerySession;
      });

    const transcriptSize = (file: string) =>
      fs.stat(file).pipe(
        Effect.map((info) => Number(info.size)),
        Effect.orElseSucceed(() => 0),
      );

    const readTranscriptBytes = (file: string, offset: number, length: number) =>
      Effect.scoped(
        Effect.gen(function* () {
          const handle = yield* fs.open(file, { flag: "r" });
          yield* handle.seek(BigInt(offset), "start");
          const bytes = yield* handle.readAlloc(length);
          return Option.getOrElse(bytes, () => new Uint8Array(0));
        }),
      ).pipe(Effect.orElseSucceed(() => new Uint8Array(0)));

    const openBridge = Effect.fn("ClaudeBridgeQueryRunner.open")(function* (
      input: ClaudeAdapterV2.ClaudeAgentSdkQueryOpenInput,
    ) {
      const options = input.options;
      const sid = options.sessionId ?? options.resume;
      const cwd = options.cwd ?? hostCwd;
      const binaryPath =
        typeof options.pathToClaudeCodeExecutable === "string" &&
        options.pathToClaudeCodeExecutable.length > 0
          ? options.pathToClaudeCodeExecutable
          : "claude";
      const environment = options.env ?? hostEnvironment;
      const configDir = configDirFor(options.env);
      const protocolLogger = ClaudeAdapterV2.makeClaudeAgentSdkProtocolLogger({
        nativeEventLogger,
        threadId: input.threadId,
        providerSessionId: input.providerSessionId,
      });
      const logProtocolEvent = (event: ClaudeAdapterV2.ClaudeAgentSdkProtocolLogEvent) =>
        protocolLogger === undefined ? Effect.void : protocolLogger(event);

      let entry = yield* registry.findLiveProcess(sid);
      if (entry === null) {
        const terminalHome = yield* registry.findTerminalHome(sid, {
          binaryPath,
          environment,
          cwd,
          configDir,
        });
        if (terminalHome !== null) {
          return yield* runnerError(
            new Error(
              `Session ${sid} is open in a Claude Code terminal under ${terminalHome} without the T3 bridge plugin. Install the plugin in that Claude home and restart the session, or close it.`,
            ),
            "attach",
          );
        }
        entry = yield* registry
          .launch({
            sid,
            cwd,
            configDir,
            binaryPath,
            environment,
            model: options.model,
            permissionMode: options.permissionMode,
            ...(options.extraArgs === undefined ? {} : { extraArgs: options.extraArgs }),
          })
          .pipe(Effect.mapError((cause) => runnerError(cause, "launch")));
      }
      const bridgeProcess = entry;
      yield* logProtocolEvent({
        direction: "outgoing",
        stage: "decoded",
        payload: {
          type: "bridge.attach",
          sid,
          instanceId: bridgeProcess.instanceId,
          cwd: bridgeProcess.session.cwd,
          activeTurn: bridgeProcess.session.activeTurn,
        },
      });

      const sessionScope = yield* Scope.make();
      const messages = yield* Queue.unbounded<
        SDKMessage,
        ClaudeAdapterV2.ClaudeAgentSdkQueryRunnerError | Cause.Done
      >();
      const translator = makeClaudeBridgeFrameTranslator({
        sessionId: sid,
        cwd,
        model: options.model,
        permissionMode: options.permissionMode,
      });
      const state = {
        closed: false,
        transcriptPath: bridgeProcess.session.transcriptPath,
        byteOffset: 0,
        lastRowAt: 0,
        pendingComplete: null as {
          readonly event: Extract<ClaudeBridgeEvent, { type: "turn.complete" }>;
          readonly seenAt: number;
        } | null,
        pendingPrompt: null as { commandId: string; readonly promptUuid: string } | null,
        approvals: new Map<string, AbortController>(),
      };

      const emit = (frames: ReadonlyArray<SDKMessage>) =>
        Effect.forEach(
          frames,
          (frame) =>
            Queue.offer(messages, frame).pipe(
              Effect.andThen(
                logProtocolEvent({ direction: "incoming", stage: "decoded", payload: frame }),
              ),
            ),
          { discard: true },
        );

      const finish = Effect.gen(function* () {
        if (state.closed) return;
        state.closed = true;
        for (const controller of state.approvals.values()) controller.abort();
        state.approvals.clear();
        yield* Queue.end(messages);
        yield* registry.clearHeartbeat(bridgeProcess);
        yield* logProtocolEvent({
          direction: "outgoing",
          stage: "decoded",
          payload: { type: "bridge.detach", sid },
        });
        yield* Scope.close(sessionScope, Exit.void);
      });

      if (bridgeProcess.session.activeTurn !== null) {
        translator.adoptTurn(bridgeProcess.session.activeTurn);
        state.byteOffset = bridgeProcess.session.activeTurn.transcriptOffset;
      } else {
        state.byteOffset = yield* transcriptSize(state.transcriptPath);
      }

      const readNewRows = Effect.gen(function* () {
        if (state.transcriptPath.length === 0) return;
        const size = yield* transcriptSize(state.transcriptPath);
        if (size <= state.byteOffset) return;
        const bytes = yield* readTranscriptBytes(
          state.transcriptPath,
          state.byteOffset,
          size - state.byteOffset,
        );
        const lastNewline = bytes.lastIndexOf(0x0a);
        if (lastNewline === -1) return;
        const text = new TextDecoder().decode(bytes.subarray(0, lastNewline + 1));
        state.byteOffset += lastNewline + 1;
        state.lastRowAt = yield* Clock.currentTimeMillis;
        for (const row of readTranscriptRows(text).rows) {
          yield* emit(translator.onTranscriptRow(row));
        }
      });

      const settlePendingComplete = Effect.gen(function* () {
        const pending = state.pendingComplete;
        if (pending === null) return;
        const now = yield* Clock.currentTimeMillis;
        if (
          state.byteOffset >= pending.event.transcriptOffset &&
          now - state.lastRowAt >= COMPLETE_QUIET_MS &&
          now - pending.seenAt >= COMPLETE_QUIET_MS
        ) {
          state.pendingComplete = null;
          yield* emit(translator.onTurnComplete(pending.event));
        }
      });

      const endSession = (reason: string) =>
        Effect.gen(function* () {
          yield* readNewRows.pipe(Effect.ignore);
          const pending = state.pendingComplete;
          if (pending !== null) {
            state.pendingComplete = null;
            yield* emit(translator.onTurnComplete(pending.event));
          }
          yield* emit(translator.onEnded(reason));
          yield* finish;
        });

      const tail = Effect.gen(function* () {
        let lastHeartbeat = 0;
        while (!state.closed) {
          const now = yield* Clock.currentTimeMillis;
          if (now - lastHeartbeat >= CLAUDE_BRIDGE_T3_HEARTBEAT_MS) {
            yield* registry.heartbeat(bridgeProcess);
            lastHeartbeat = now;
          }
          const session = yield* registry.readSession(bridgeProcess);
          if (session === null || !isClaudeBridgeSessionLive(session, now)) {
            yield* endSession(session?.ended?.reason ?? "process gone");
            return;
          }
          if (state.transcriptPath.length === 0 && session.transcriptPath.length > 0) {
            state.transcriptPath = session.transcriptPath;
            state.byteOffset = yield* transcriptSize(state.transcriptPath);
          }
          if (session.model !== null) translator.setModel(session.model);
          yield* readNewRows.pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("claude-bridge.transcript-read-failed", { sid, cause }),
            ),
          );
          yield* settlePendingComplete;
          yield* Effect.sleep(`${TAIL_INTERVAL_MS} millis`);
        }
      });

      const resolveApproval = (event: Extract<ClaudeBridgeEvent, { type: "approval.request" }>) =>
        Effect.gen(function* () {
          const canUseTool = options.canUseTool;
          if (canUseTool === undefined) {
            yield* registry.send(bridgeProcess, {
              type: "approval",
              toolUseId: event.toolUseId,
              decision: "ask",
            });
            return;
          }
          const controller = new AbortController();
          state.approvals.set(event.toolUseId, controller);
          const result = yield* Effect.result(
            Effect.tryPromise({
              try: () =>
                canUseTool(event.tool, isRecord(event.input) ? event.input : {}, {
                  signal: controller.signal,
                  toolUseID: event.toolUseId,
                  requestId: event.toolUseId,
                  ...(event.reason === undefined ? {} : { decisionReason: event.reason }),
                }),
              catch: (cause) => runnerError(cause, "canUseTool"),
            }),
          );
          state.approvals.delete(event.toolUseId);
          if (state.closed) return;
          if (result._tag === "Failure") {
            yield* registry.send(bridgeProcess, {
              type: "approval",
              toolUseId: event.toolUseId,
              decision: "ask",
            });
            return;
          }
          const decision = result.success;
          if (decision === null) {
            yield* registry.send(bridgeProcess, {
              type: "approval",
              toolUseId: event.toolUseId,
              decision: "ask",
            });
            return;
          }
          yield* registry.send(
            bridgeProcess,
            decision.behavior === "allow"
              ? { type: "approval", toolUseId: event.toolUseId, decision: "allow" }
              : {
                  type: "approval",
                  toolUseId: event.toolUseId,
                  decision: "deny",
                  reason: decision.message,
                },
          );
        });

      const handleEvent = (event: ClaudeBridgeEvent) =>
        Effect.gen(function* () {
          if (state.closed) return;
          yield* logProtocolEvent({
            direction: "incoming",
            stage: "decoded",
            payload: { type: `bridge.${event.type}`, event },
          });
          switch (event.type) {
            case "turn.start": {
              if (
                state.pendingPrompt !== null &&
                event.commandId === state.pendingPrompt.commandId
              ) {
                state.pendingPrompt = null;
              }
              yield* emit(translator.onTurnStart(event));
              return;
            }
            case "turn.complete": {
              state.pendingComplete = { event, seenAt: yield* Clock.currentTimeMillis };
              return;
            }
            case "prompt.dropped": {
              if (state.pendingPrompt?.commandId !== event.commandId) return;
              const promptUuid = state.pendingPrompt.promptUuid;
              state.pendingPrompt = null;
              yield* emit(
                translator.onTurnComplete({
                  seq: event.seq,
                  at: event.at,
                  type: "turn.complete",
                  turnId: `dropped-${event.commandId}`,
                  reason: "error",
                  answer: event.reason,
                  isAborted: false,
                  durationMs: 0,
                  transcriptOffset: 0,
                  promptUuid,
                }),
              );
              return;
            }
            case "approval.request": {
              yield* resolveApproval(event).pipe(Effect.forkIn(sessionScope));
              return;
            }
            case "ended": {
              yield* endSession(event.reason);
              return;
            }
            case "approval.resolved":
              return;
            case "error": {
              yield* Effect.logWarning("claude-bridge.mod-error", { sid, message: event.message });
              return;
            }
          }
        });

      yield* registry
        .subscribe(sid, (event) => handleEvent(event))
        .pipe(Effect.provideService(Scope.Scope, sessionScope));
      yield* tail.pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("claude-bridge.tail-failed", { sid, cause }).pipe(
            Effect.andThen(endSession("tail failed")),
          ),
        ),
        Effect.forkIn(sessionScope),
      );

      const session: ClaudeAdapterV2.ClaudeAgentSdkQuerySession = {
        messages: Stream.fromQueue(messages),
        offer: (message) =>
          Effect.gen(function* () {
            const text = claudeBridgePromptText(message);
            if (text === null) {
              return yield* runnerError(
                new Error("The message has no text to send to the bridged Claude Code session."),
                "offer",
              );
            }
            const promptUuid =
              typeof message.uuid === "string"
                ? message.uuid
                : yield* crypto.randomUUIDv4.pipe(Effect.orDie);
            const pending = { commandId: "", promptUuid };
            state.pendingPrompt = pending;
            pending.commandId = yield* registry.send(bridgeProcess, {
              type: "prompt",
              text,
              promptUuid,
            });
            yield* logProtocolEvent({
              direction: "outgoing",
              stage: "decoded",
              payload: { type: "prompt.offer", message },
            });
          }),
        setModel: (model) => Effect.sync(() => translator.setModel(model)),
        // The process keeps its own mode; a T3 mode change applies to the
        // next process T3 launches.
        setPermissionMode: () => Effect.void,
        interrupt: registry.send(bridgeProcess, { type: "interrupt" }).pipe(Effect.asVoid),
        close: finish,
        hasPendingWake: Effect.gen(function* () {
          return (
            translator.hasOpenTurn() ||
            state.pendingComplete !== null ||
            (yield* Queue.size(messages)) > 0
          );
        }),
      };
      return session;
    });

    const configDirFor = (environment: NodeJS.ProcessEnv | undefined) => {
      const fromEnv =
        environment?.CLAUDE_CONFIG_DIR?.trim() ?? hostEnvironment.CLAUDE_CONFIG_DIR?.trim();
      return fromEnv !== undefined && fromEnv.length > 0
        ? fromEnv
        : path.join(NodeOS.homedir(), ".claude");
    };

    return ClaudeAdapterV2.ClaudeAgentSdkQueryRunner.of({
      ...sdkRunner,
      open: (input) =>
        Effect.gen(function* () {
          const transport = hostEnvironment[CLAUDE_TRANSPORT_ENV]?.trim();
          if (transport === "sdk") return yield* sdkRunner.open(input);
          const configDir = configDirFor(input.options.env);
          if (!(yield* registry.isPluginInstalled(configDir))) {
            yield* Effect.logDebug("claude-bridge.plugin-missing-using-sdk", { configDir });
            return yield* sdkRunner.open(input);
          }
          const sid = input.options.sessionId ?? input.options.resume;
          const cwd = input.options.cwd ?? hostCwd;
          if (
            input.options.sessionId !== undefined &&
            !(yield* transcriptExists(configDir, cwd, sid))
          ) {
            yield* Effect.logInfo("claude-bridge.first-turn-over-sdk", { sid, cwd });
            return yield* openSdkForFirstTurn(input);
          }
          return yield* openBridge(input);
        }),
    });
  }),
);
