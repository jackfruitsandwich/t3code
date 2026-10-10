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
  CLAUDE_BRIDGE_WINDOW_PROTOCOL,
  claudeConfigModelChoice,
  claudeTranscriptCommandText,
  claudeTranscriptCommandOutput,
  claudeTranscriptPermissionMode,
  CLAUDE_BRIDGE_UPKEEP_COMMAND,
  isClaudeBridgeModRow,
  isClaudeTurnEndRow,
  isClaudeTypedMidTurnRow,
  isClaudeBridgeCommandOutputFrame,
  isClaudeBridgeSessionLive,
  makeClaudeBridgeFrameTranslator,
  parseClaudeSlashCommandText,
  readTranscriptRows,
  type ClaudeBridgeCommandBody,
  type ClaudeBridgeEvent,
} from "../../claude-bridge/ClaudeBridgeProtocol.ts";
import { isRecord, parseJsonlLine } from "../../import/sessionImportTypes.ts";
import * as ProviderEventLoggers from "../../provider/ProviderEventLoggers.ts";
import * as ClaudeAdapterV2 from "./ClaudeAdapterV2.ts";

export const CLAUDE_TRANSPORT_ENV = "T3CODE_CLAUDE_TRANSPORT";
const TAIL_INTERVAL_MS = 300;
/** How long a message the terminal drew waits for the record's row before it is shown without one. */
const DRAWN_MESSAGE_WAIT_MS = 2_500;
/** How long a reply row's text is remembered, for a drawn copy that arrives after it. */
const RECORDED_TEXT_MEMORY_MS = 60_000;
const normalizeDrawn = (text: string) => text.replace(/\s+/gu, " ").trim();
/** The text blocks of a reply row of the record. */
const assistantRowText = (row: Record<string, unknown>): string => {
  const content = isRecord(row.message) ? row.message.content : null;
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content
    .flatMap((block) =>
      isRecord(block) && block.type === "text" && typeof block.text === "string"
        ? [block.text]
        : [],
    )
    .join("");
};
/**
 * How far ahead another instance's heartbeat must be before the attachment
 * moves to it: more than two of the mod's own beats, so two processes holding
 * one session do not pull it back and forth.
 */
const RELOADED_MOD_LEAD_MS = 6_500;
/** A turn's result waits this long after the last transcript row, so a late row cannot land after it. */
const COMPLETE_QUIET_MS = 400;
/** How long a turn's closing row is waited for when the next turn has already begun. */
const TURN_END_ROW_POLL_MS = 50;
const TURN_END_ROW_ATTEMPTS = 30;
/**
 * How recently a turn nobody was attached for must have ended to be shown
 * from its start by the next attachment. Shorter than the importer's settle
 * time, so a turn is never both replayed here and brought in as history.
 */
const MISSED_TURN_REPLAY_MS = 15_000;
/**
 * A slash command typed in the terminal that starts no turn (`/model`,
 * `/rename`) leaves only its printed output. After this much quiet the output
 * is shown as a short turn of its own.
 */
const LOCAL_COMMAND_QUIET_MS = 1_500;

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

/**
 * What each bridged session last reported as its own model and permission
 * mode, as T3 has shown it. A T3 setting that differs from the session's is
 * the person's change only if T3 had already shown the session's value; before
 * that it is a stale default (an imported thread is stamped with one) and the
 * session's value wins.
 */
const shownNativeSettings = new Map<string, ClaudeAdapterV2.ClaudeBridgeNativeSettings>();

/** How many trailing bytes of a transcript are read to find the session's current permission mode. */
const PERMISSION_MODE_SCAN_BYTES = 262_144;

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
          singleTurn: true,
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
      configDir: string,
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
            new ClaudeBridgeRegistry.ClaudeBridgeLaunchError({
              sid,
              detail: `Session ${sid} is open in a Claude Code terminal under ${terminalHome} without the T3 bridge plugin. Install the plugin in that Claude home and restart the session, or close it.`,
            }),
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
      // The session's process as the mod reports it. The mod loads a newer
      // version of itself when the session is idle and then reports under a
      // new set of bridge files: the attachment follows it there.
      let bridgeProcess = entry;
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
      // Where the session's calls to T3's own tools are answered: the tool
      // server this thread's SDK sessions are given, with its credential.
      const t3Server: unknown = options.mcpServers?.["t3-code"];
      const toolEndpoint =
        isRecord(t3Server) &&
        t3Server.type === "http" &&
        typeof t3Server.url === "string" &&
        isRecord(t3Server.headers) &&
        typeof t3Server.headers.Authorization === "string"
          ? { endpoint: t3Server.url, authorization: t3Server.headers.Authorization }
          : undefined;
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
        pendingPrompt: null as {
          commandId: string;
          readonly promptUuid: string;
          /** `/name args` when the prompt is a slash command. */
          readonly label: string | null;
          /** What was put in the session's inbox, to put again if the mod is reloaded before taking it. */
          readonly body: ClaudeBridgeCommandBody;
        } | null,
        approvals: new Map<string, AbortController>(),
        /** Requests the session settled without T3: nothing is sent back for them. */
        settledElsewhere: new Set<string>(),
        /** Printed command output read while no turn was open, kept for whatever it belongs to. */
        heldOutput: [] as Array<SDKMessage>,
        heldAt: 0,
        lastCommandText: null as string | null,
        /** Rows that close a turn in the record, read so far; and the count when the open turn began. */
        turnEndsRead: 0,
        turnEndsAtTurnStart: 0,
        /** The mod's own reload command was just read: what it printed is skipped too. */
        upkeepOutputDue: false,
        /** The model and mode this attachment was last asked for. */
        selection: null as { readonly model: string; readonly permissionMode: string } | null,
        /** Messages the terminal drew that no row of the record has shown yet. */
        drawnMessages: [] as Array<{
          readonly messageId: string;
          readonly text: string;
          readonly at: number;
        }>,
        /** Texts of the record's recent replies, to tell a drawn message it already holds. */
        recordedTexts: [] as Array<{ readonly text: string; readonly at: number }>,
        /** A slash command T3 ran that started no turn, waiting for its output rows. */
        pendingLocal: null as {
          readonly commandId: string;
          readonly promptUuid: string;
          readonly label: string;
          readonly ok: boolean;
          readonly text: string;
          readonly seenAt: number;
        } | null,
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

      const missedTurn = yield* registry.peekMissedTurn(sid);
      let replayedTurnId: string | null = null;
      if (bridgeProcess.session.activeTurn !== null) {
        translator.adoptTurn(bridgeProcess.session.activeTurn);
        state.byteOffset = bridgeProcess.session.activeTurn.transcriptOffset;
      } else {
        state.byteOffset = yield* transcriptSize(state.transcriptPath);
        // A short turn can be over before the run offered for it attaches.
        // It is shown from its start, as if this had attached in time. Only a
        // turn that just ended: an older one has reached the thread as history.
        const now = yield* Clock.currentTimeMillis;
        const endedAt = missedTurn?.complete?.at ?? missedTurn?.start.at;
        if (missedTurn !== null && endedAt !== undefined && now - endedAt < MISSED_TURN_REPLAY_MS) {
          translator.adoptTurn({
            turnId: missedTurn.start.turnId,
            startedAt: missedTurn.start.at,
            transcriptOffset: missedTurn.start.transcriptOffset,
            ...(missedTurn.start.commandId === undefined
              ? {}
              : { commandId: missedTurn.start.commandId }),
            ...(missedTurn.start.promptUuid === undefined
              ? {}
              : { promptUuid: missedTurn.start.promptUuid }),
          });
          state.byteOffset = missedTurn.start.transcriptOffset;
          replayedTurnId = missedTurn.start.turnId;
          yield* Effect.logInfo("claude-bridge.missed-turn-replayed", {
            sid,
            turnId: missedTurn.start.turnId,
          });
        }
      }

      // The session's permission mode as its transcript last recorded it, for
      // a process whose mod has not named it yet.
      let transcriptMode: string | null = null;
      if (state.transcriptPath.length > 0) {
        const size = yield* transcriptSize(state.transcriptPath);
        const from = Math.max(0, size - PERMISSION_MODE_SCAN_BYTES);
        const tailBytes = yield* readTranscriptBytes(state.transcriptPath, from, size - from);
        const tailText = new TextDecoder().decode(tailBytes);
        for (const row of readTranscriptRows(tailText.slice(tailText.indexOf("\n") + 1)).rows) {
          transcriptMode = claudeTranscriptPermissionMode(row) ?? transcriptMode;
        }
      }

      const nativeSettings = (
        session: ClaudeBridgeRegistry.ClaudeBridgeProcess["session"],
      ): ClaudeAdapterV2.ClaudeBridgeNativeSettings => ({
        model: session.model,
        permissionMode: transcriptMode ?? session.permissionMode,
      });
      /** Shows the session's own settings on the thread when they differ from what T3 last showed. */
      const showNativeSettings = (
        settings: ClaudeAdapterV2.ClaudeBridgeNativeSettings,
        // A run only takes what arrives while it is open, so each turn says it again.
        again = false,
      ) =>
        Effect.sync(() => {
          const shown = shownNativeSettings.get(sid);
          if (
            !again &&
            shown !== undefined &&
            shown.model === settings.model &&
            shown.permissionMode === settings.permissionMode
          ) {
            return;
          }
          shownNativeSettings.set(sid, settings);
          input.bridgeOnNativeSettings?.(settings);
        });

      // A setting changed in T3 since it last showed the session's value is
      // the person's change: apply it in the session, where the mod confirms
      // it and the next heartbeat shows it. Anything else follows the session.
      const offerSelection = (
        selection: { readonly model: string; readonly permissionMode: string },
        session: ClaudeBridgeRegistry.ClaudeBridgeProcess["session"],
      ) =>
        Effect.gen(function* () {
          state.selection = selection;
          const native = nativeSettings(session);
          const shown = shownNativeSettings.get(sid);
          const canForward =
            shown !== undefined && session.protocol >= CLAUDE_BRIDGE_WINDOW_PROTOCOL;
          // A client keeps its own copy of the thread's model and mode and
          // sends it with every message. The same value it sent before, sent
          // again after the session has moved on, is that copy and would
          // switch the session back; only a value it has not sent is a choice.
          const sentBefore = yield* registry.readClientSelection(sid);
          const isNewModelChoice = sentBefore === null || sentBefore.model !== selection.model;
          const isNewModeChoice =
            sentBefore === null || sentBefore.permissionMode !== selection.permissionMode;
          const wantedModel = claudeConfigModelChoice(selection.model);
          const nativeModel = native.model === null ? null : claudeConfigModelChoice(native.model);
          // An account can resolve a family name to its wide-context variant
          // by itself; only a different family, or an asked-for wide variant
          // the session lacks, is a change to apply.
          const family = (choice: string) => choice.replace(/\[1m\]$/u, "");
          if (
            canForward &&
            isNewModelChoice &&
            shown.model === native.model &&
            wantedModel !== null &&
            nativeModel !== null &&
            (family(wantedModel) !== family(nativeModel) ||
              (wantedModel.endsWith("[1m]") && !nativeModel.endsWith("[1m]")))
          ) {
            yield* registry.send(bridgeProcess, {
              type: "config",
              key: "model",
              value: wantedModel,
            });
            yield* Effect.logInfo("claude-bridge.model-forwarded", { sid, model: wantedModel });
          }
          if (
            canForward &&
            isNewModeChoice &&
            shown.permissionMode === native.permissionMode &&
            selection.permissionMode === "plan" &&
            native.permissionMode !== null &&
            native.permissionMode !== "plan"
          ) {
            const promptUuid = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
            yield* registry.send(bridgeProcess, {
              type: "command",
              name: "plan",
              args: "",
              promptUuid,
            });
            yield* Effect.logInfo("claude-bridge.plan-mode-forwarded", { sid });
          }
          yield* showNativeSettings(native);
        });
      yield* offerSelection(
        { model: options.model, permissionMode: options.permissionMode },
        bridgeProcess.session,
      );

      /**
       * Shows the messages the terminal drew at least `olderThanMs` ago and
       * the record has not shown: as a reply row would be, in the open turn.
       */
      const showDrawnMessages = (olderThanMs: number) =>
        Effect.gen(function* () {
          if (state.drawnMessages.length === 0) return;
          const now = yield* Clock.currentTimeMillis;
          const due = state.drawnMessages.filter((drawn) => now - drawn.at >= olderThanMs);
          if (due.length === 0) return;
          state.drawnMessages = state.drawnMessages.filter((drawn) => !due.includes(drawn));
          if (!translator.hasOpenTurn()) return;
          for (const drawn of due) {
            yield* emit(
              translator.onTranscriptRow({
                type: "assistant",
                uuid: drawn.messageId,
                message: { role: "assistant", content: [{ type: "text", text: drawn.text }] },
              }),
            );
          }
        });

      /**
       * Translates the record's new rows. With `untilTurnEnd` it stops after
       * the row that closes a turn, leaving the next turn's rows unread.
       */
      const readRows = (untilTurnEnd: boolean) =>
        Effect.gen(function* () {
          if (state.transcriptPath.length === 0) return;
          const size = yield* transcriptSize(state.transcriptPath);
          if (size <= state.byteOffset) return;
          const bytes = yield* readTranscriptBytes(
            state.transcriptPath,
            state.byteOffset,
            size - state.byteOffset,
          );
          const decoder = new TextDecoder();
          let lineStart = 0;
          for (;;) {
            const newline = bytes.indexOf(0x0a, lineStart);
            if (newline === -1) break;
            const row = parseJsonlLine(decoder.decode(bytes.subarray(lineStart, newline)));
            state.byteOffset += newline + 1 - lineStart;
            lineStart = newline + 1;
            state.lastRowAt = yield* Clock.currentTimeMillis;
            if (row === undefined) continue;
            // What the person typed into the running turn is in the record
            // only. It is placed in the thread now, before the rows that
            // follow it are shown, so it sits where it was written.
            if (isClaudeTypedMidTurnRow(row)) {
              yield* registry.noteRecordChange(bridgeProcess, "typed-mid-turn");
            }
            transcriptMode = claudeTranscriptPermissionMode(row) ?? transcriptMode;
            const commandText = claudeTranscriptCommandText(row);
            // The mod loading a newer version of itself: nobody asked for it.
            // (The person sending that same command from T3 is another
            // matter, and T3 is then waiting on it by name.)
            const reloadSentFromT3 =
              state.pendingPrompt?.label?.startsWith(CLAUDE_BRIDGE_UPKEEP_COMMAND) === true ||
              state.pendingLocal?.label.startsWith(CLAUDE_BRIDGE_UPKEEP_COMMAND) === true;
            // What the reload printed is a row of its own, with nothing on it
            // to say who ran the command: it is the row after the command's.
            if (state.upkeepOutputDue && claudeTranscriptCommandOutput(row) !== null) {
              state.upkeepOutputDue = false;
              continue;
            }
            if (
              isClaudeBridgeModRow(row) &&
              !reloadSentFromT3 &&
              commandText !== null &&
              commandText.startsWith(CLAUDE_BRIDGE_UPKEEP_COMMAND)
            ) {
              state.upkeepOutputDue = true;
              continue;
            }
            if (commandText !== null) state.lastCommandText = commandText;
            // A reply row settles what the terminal drew before it: the same
            // text is the record's own copy, anything else was written first.
            if (isRecord(row) && row.type === "assistant" && row.isSidechain !== true) {
              const recorded = normalizeDrawn(assistantRowText(row));
              if (recorded.length > 0) {
                state.recordedTexts.push({ text: recorded, at: state.lastRowAt });
                if (state.recordedTexts.length > 24) state.recordedTexts.shift();
                state.drawnMessages = state.drawnMessages.filter(
                  (drawn) => normalizeDrawn(drawn.text) !== recorded,
                );
              } else {
                yield* showDrawnMessages(0);
              }
            }
            for (const frame of translator.onTranscriptRow(row)) {
              if (isClaudeBridgeCommandOutputFrame(frame) && !translator.hasOpenTurn()) {
                state.heldOutput.push(frame);
                state.heldAt = state.lastRowAt;
              } else {
                yield* emit([frame]);
              }
            }
            if (isClaudeTurnEndRow(row)) {
              state.turnEndsRead += 1;
              if (untilTurnEnd) break;
            }
          }
        });
      const readNewRows = readRows(false);

      /** A turn's worth of frames for a slash command that started no model turn. */
      const emitCommandTurn = (input: {
        readonly turnId: string;
        readonly text: string;
        readonly promptUuid: string | null;
        readonly ok: boolean;
        readonly answer: string;
      }) =>
        Effect.gen(function* () {
          const now = yield* Clock.currentTimeMillis;
          yield* emit(
            translator.onTurnStart({
              seq: 0,
              at: now,
              type: "turn.start",
              turnId: input.turnId,
              text: input.text,
              transcriptOffset: 0,
              ...(input.promptUuid === null ? {} : { promptUuid: input.promptUuid }),
            }),
          );
          const output = state.heldOutput.splice(0);
          yield* emit(output);
          yield* emit(
            translator.onTurnComplete({
              seq: 0,
              at: now,
              type: "turn.complete",
              turnId: input.turnId,
              reason: input.ok ? "answer" : "error",
              answer: input.ok && output.length > 0 ? "" : input.answer,
              isAborted: false,
              durationMs: 0,
              transcriptOffset: 0,
              ...(input.promptUuid === null ? {} : { promptUuid: input.promptUuid }),
            }),
          );
        });

      const settleLocalCommands = Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        const local = state.pendingLocal;
        if (local !== null) {
          if (now - local.seenAt < COMPLETE_QUIET_MS || now - state.lastRowAt < COMPLETE_QUIET_MS) {
            return;
          }
          state.pendingLocal = null;
          yield* emitCommandTurn({
            turnId: `command-${local.commandId}`,
            text: local.label,
            promptUuid: local.promptUuid,
            ok: local.ok,
            answer: local.text.length > 0 ? local.text : local.ok ? "(no output)" : local.label,
          });
          return;
        }
        if (
          state.heldOutput.length > 0 &&
          state.pendingPrompt === null &&
          state.pendingComplete === null &&
          !translator.hasOpenTurn() &&
          now - state.heldAt >= LOCAL_COMMAND_QUIET_MS
        ) {
          yield* emitCommandTurn({
            turnId: `local-${now}`,
            text: state.lastCommandText ?? "(slash command)",
            promptUuid: null,
            ok: true,
            answer: "",
          });
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

      /**
       * Moves the attachment to the session's newer mod instance when the
       * one it holds has stopped reporting. A prompt put in the old
       * instance's inbox and never taken up is put in the new one.
       */
      const followReloadedMod = Effect.gen(function* () {
        const newest = yield* registry.findLiveProcess(sid);
        if (newest === null || newest.instanceId === bridgeProcess.instanceId) return false;
        const held = yield* registry.readSession(bridgeProcess);
        if (newest.session.heartbeatAt - (held?.heartbeatAt ?? 0) < RELOADED_MOD_LEAD_MS) {
          return false;
        }
        const previous = bridgeProcess;
        bridgeProcess = newest;
        yield* registry.clearHeartbeat(previous);
        yield* registry.heartbeat(newest, toolEndpoint);
        yield* Effect.logInfo("claude-bridge.followed-reloaded-mod", {
          sid,
          from: previous.instanceId,
          to: newest.instanceId,
          modVersion: newest.session.modVersion,
        });
        const pending = state.pendingPrompt;
        if (pending !== null) pending.commandId = yield* registry.send(newest, pending.body);
        return true;
      });

      const tail = Effect.gen(function* () {
        let lastHeartbeat = 0;
        while (!state.closed) {
          const now = yield* Clock.currentTimeMillis;
          yield* followReloadedMod;
          if (now - lastHeartbeat >= CLAUDE_BRIDGE_T3_HEARTBEAT_MS) {
            yield* registry.heartbeat(bridgeProcess, toolEndpoint);
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
          yield* showNativeSettings(nativeSettings(session));
          yield* readNewRows.pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("claude-bridge.transcript-read-failed", { sid, cause }),
            ),
          );
          // A drawn message no reply row followed (a long tool run comes next).
          yield* showDrawnMessages(DRAWN_MESSAGE_WAIT_MS);
          yield* settlePendingComplete;
          yield* settleLocalCommands;
          yield* Effect.sleep(`${TAIL_INTERVAL_MS} millis`);
        }
      });

      const resolveApproval = (event: Extract<ClaudeBridgeEvent, { type: "approval.request" }>) =>
        Effect.gen(function* () {
          const canUseTool =
            (bridgeProcess.session.protocol < CLAUDE_BRIDGE_WINDOW_PROTOCOL
              ? input.bridgeCanUseToolLegacy
              : undefined) ??
            input.bridgeCanUseTool ??
            options.canUseTool;
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
          // The session settled it without T3 (answered in the terminal, the
          // turn ended): there is no one left to tell.
          if (state.settledElsewhere.delete(event.toolUseId)) return;
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
          if (decision.behavior === "allow") {
            // "Allow for this session" arrives as permission updates; the mod
            // keeps the grant for the life of the process.
            const forSession = (decision.updatedPermissions?.length ?? 0) > 0;
            yield* registry.send(bridgeProcess, {
              type: "approval",
              toolUseId: event.toolUseId,
              decision: "allow",
              ...(forSession ? { scope: "session" as const } : {}),
            });
            return;
          }
          yield* registry.send(bridgeProcess, {
            type: "approval",
            toolUseId: event.toolUseId,
            decision: "deny",
            reason: decision.message,
          });
          // Cancel is more than a refusal: in the terminal it stops the turn.
          if (decision.interrupt === true) {
            yield* registry.send(bridgeProcess, { type: "interrupt" });
          }
        });

      /** A question Claude asked: answered with the person's answers, not with an approval. */
      const resolveQuestion = (event: Extract<ClaudeBridgeEvent, { type: "question.request" }>) =>
        Effect.gen(function* () {
          const canUseTool = input.bridgeCanUseTool ?? options.canUseTool;
          const handBack = registry.send(bridgeProcess, {
            type: "answer",
            toolUseId: event.toolUseId,
            decision: "ask",
          });
          if (canUseTool === undefined) {
            yield* handBack;
            return;
          }
          const controller = new AbortController();
          state.approvals.set(event.toolUseId, controller);
          const result = yield* Effect.result(
            Effect.tryPromise({
              try: () =>
                canUseTool(
                  "AskUserQuestion",
                  { questions: event.questions },
                  {
                    signal: controller.signal,
                    toolUseID: event.toolUseId,
                    requestId: event.toolUseId,
                  },
                ),
              catch: (cause) => runnerError(cause, "canUseTool"),
            }),
          );
          state.approvals.delete(event.toolUseId);
          if (state.closed) return;
          if (state.settledElsewhere.delete(event.toolUseId)) return;
          if (result._tag === "Failure" || result.success === null) {
            yield* handBack;
            return;
          }
          const decision = result.success;
          if (decision.behavior !== "allow") {
            yield* registry.send(bridgeProcess, {
              type: "answer",
              toolUseId: event.toolUseId,
              decision: "deny",
              reason: decision.message,
            });
            return;
          }
          const given = isRecord(decision.updatedInput) ? decision.updatedInput.answers : null;
          const answers: Record<string, string> = {};
          if (isRecord(given)) {
            for (const [question, answer] of Object.entries(given)) {
              if (typeof answer === "string") answers[question] = answer;
            }
          }
          yield* registry.send(bridgeProcess, {
            type: "answer",
            toolUseId: event.toolUseId,
            decision: "answer",
            answers,
          });
        });

      const settleElsewhere = (toolUseId: string) =>
        Effect.sync(() => {
          const controller = state.approvals.get(toolUseId);
          if (controller === undefined) return;
          state.settledElsewhere.add(toolUseId);
          controller.abort();
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
              // The turn before this one has ended and its end is still
              // waiting for its last rows (a message queued behind it starts
              // the next at once). It is closed first: otherwise the two run
              // together and the new turn is lost when the old one closes.
              if (
                state.pendingComplete !== null &&
                state.pendingComplete.event.turnId !== event.turnId
              ) {
                // Claude Code reports the next turn before the last rows of
                // this one are on disk: read up to the row that closes it.
                for (
                  let attempt = 0;
                  attempt < TURN_END_ROW_ATTEMPTS &&
                  state.turnEndsRead === state.turnEndsAtTurnStart;
                  attempt += 1
                ) {
                  yield* readRows(true).pipe(Effect.ignore);
                  if (state.turnEndsRead !== state.turnEndsAtTurnStart) break;
                  yield* Effect.sleep(`${TURN_END_ROW_POLL_MS} millis`);
                }
                const ended = state.pendingComplete;
                if (ended !== null) {
                  state.pendingComplete = null;
                  yield* emit(translator.onTurnComplete(ended.event));
                }
              }
              state.turnEndsAtTurnStart = state.turnEndsRead;
              if (
                state.pendingPrompt !== null &&
                event.commandId === state.pendingPrompt.commandId
              ) {
                state.pendingPrompt = null;
              }
              yield* emit(translator.onTurnStart(event));
              // Output a slash command printed just before its turn began.
              yield* emit(state.heldOutput.splice(0));
              const live = yield* registry.readSession(bridgeProcess);
              if (live !== null) yield* showNativeSettings(nativeSettings(live), true);
              return;
            }
            case "turn.complete": {
              // Whatever this turn still asked is over with it.
              for (const toolUseId of state.approvals.keys()) {
                yield* settleElsewhere(toolUseId);
              }
              state.pendingComplete = { event, seenAt: yield* Clock.currentTimeMillis };
              return;
            }
            case "command.result": {
              if (state.pendingPrompt?.commandId !== event.commandId) return;
              const pending = state.pendingPrompt;
              state.pendingPrompt = null;
              if (event.startedTurn) return;
              state.pendingLocal = {
                commandId: event.commandId,
                promptUuid: pending.promptUuid,
                label: pending.label ?? "(slash command)",
                ok: event.ok,
                text: event.text,
                seenAt: yield* Clock.currentTimeMillis,
              };
              return;
            }
            case "question.request": {
              yield* resolveQuestion(event).pipe(Effect.forkIn(sessionScope));
              return;
            }
            case "question.resolved": {
              if (event.by !== "t3") yield* settleElsewhere(event.toolUseId);
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
            case "approval.resolved": {
              if (event.by !== "t3") yield* settleElsewhere(event.toolUseId);
              return;
            }
            case "message.display": {
              const drawn = normalizeDrawn(event.text);
              const now = yield* Clock.currentTimeMillis;
              // The record's row for it can be read before this arrives.
              const recorded = state.recordedTexts.some(
                (row) => row.text === drawn && now - row.at < RECORDED_TEXT_MEMORY_MS,
              );
              if (drawn.length > 0 && !recorded) {
                state.drawnMessages.push({ messageId: event.messageId, text: event.text, at: now });
              }
              return;
            }
            case "config.result": {
              if (!event.ok) {
                yield* Effect.logWarning("claude-bridge.config-refused", {
                  sid,
                  key: event.key,
                  reason: event.text,
                });
              }
              return;
            }
            case "error": {
              yield* Effect.logWarning("claude-bridge.mod-error", { sid, message: event.message });
              return;
            }
          }
        });

      yield* registry
        .subscribe(sid, (event) => handleEvent(event))
        .pipe(Effect.provideService(Scope.Scope, sessionScope));
      // From here the session's events come to this attachment. The end of a
      // replayed turn that arrived before this point is taken up now.
      const missedBeforeSubscribing = yield* registry.takeMissedTurn(sid);
      if (
        replayedTurnId !== null &&
        missedBeforeSubscribing?.complete != null &&
        missedBeforeSubscribing.start.turnId === replayedTurnId
      ) {
        yield* handleEvent(missedBeforeSubscribing.complete);
      }
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
            // A message from a client carries the client's copy of the settings.
            if (state.selection !== null)
              yield* registry.writeClientSelection(sid, state.selection);
            yield* followReloadedMod;
            // A slash command runs as a command in the session, exactly as if
            // typed there: Claude Code refuses one sent as prompt text.
            const slash = parseClaudeSlashCommandText(text);
            if (slash !== null) {
              const live = (yield* registry.readSession(bridgeProcess)) ?? bridgeProcess.session;
              if (live.protocol < CLAUDE_BRIDGE_WINDOW_PROTOCOL) {
                return yield* runnerError(
                  new Error(
                    `This Claude Code session still runs an older T3 bridge (${live.modVersion}). Type /reload-plugins in its terminal, or restart the session, and slash commands will run from here.`,
                  ),
                  "offer",
                );
              }
              const commands = yield* registry.readCommands(bridgeProcess);
              if (commands === null || commands.some((command) => command.name === slash.name)) {
                const label =
                  slash.args.length === 0 ? `/${slash.name}` : `/${slash.name} ${slash.args}`;
                const body: ClaudeBridgeCommandBody = {
                  type: "command",
                  name: slash.name,
                  args: slash.args,
                  promptUuid,
                };
                const pending = { commandId: "", promptUuid, label, body };
                state.pendingPrompt = pending;
                pending.commandId = yield* registry.send(bridgeProcess, body);
                yield* logProtocolEvent({
                  direction: "outgoing",
                  stage: "decoded",
                  payload: { type: "bridge.command", name: slash.name, args: slash.args },
                });
                return;
              }
            }
            const body: ClaudeBridgeCommandBody = {
              type: "prompt",
              // Text that only looks like a command (a path, an unknown name)
              // goes to the model as written; the leading space keeps Claude
              // Code from refusing it as a command.
              text: text.startsWith("/") ? ` ${text}` : text,
              promptUuid,
            };
            const pending = { commandId: "", promptUuid, label: null, body };
            state.pendingPrompt = pending;
            pending.commandId = yield* registry.send(bridgeProcess, body);
            yield* logProtocolEvent({
              direction: "outgoing",
              stage: "decoded",
              payload: { type: "prompt.offer", message },
            });
          }),
        applySelection: (selection) =>
          Effect.gen(function* () {
            const live = (yield* registry.readSession(bridgeProcess)) ?? bridgeProcess.session;
            yield* offerSelection(selection, live);
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
          const sid = input.options.sessionId ?? input.options.resume;
          const cwd = input.options.cwd ?? hostCwd;
          const configuredHome = configDirFor(input.options.env);
          // An existing session resumes under the Claude home that ran it, so it
          // keeps that home's account; Claude's own records name the home. A new
          // session takes the provider's home.
          const sessionHome =
            input.options.resume === undefined
              ? null
              : yield* registry.findSessionHome(sid, configuredHome);
          const configDir = sessionHome ?? configuredHome;
          if (sessionHome !== null && sessionHome !== configuredHome) {
            yield* Effect.logInfo("claude-bridge.session-home", { sid, configDir });
          }
          if (!(yield* registry.isPluginInstalled(configDir))) {
            yield* Effect.logDebug("claude-bridge.plugin-missing-using-sdk", { configDir });
            return yield* sdkRunner.open(input);
          }
          if (
            input.options.sessionId !== undefined &&
            !(yield* transcriptExists(configDir, cwd, sid))
          ) {
            yield* Effect.logInfo("claude-bridge.first-turn-over-sdk", { sid, cwd });
            return yield* openSdkForFirstTurn(input);
          }
          return yield* openBridge(input, configDir);
        }),
    });
  }),
);
