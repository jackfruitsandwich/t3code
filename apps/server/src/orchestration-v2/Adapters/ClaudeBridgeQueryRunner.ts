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
import * as NodeUtil from "node:util";

import type {
  CanUseTool,
  PermissionResult,
  SDKMessage,
  SDKUserMessage,
} from "@anthropic-ai/claude-agent-sdk";
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
  claudeBridgeAttachmentPaths,
  claudeBridgeModAtLeast,
  claudeBridgeRequestTurnText,
  claudeSessionContinuedNotice,
  claudeSessionFollowedNotice,
  claudeSessionMovingNotice,
  CLAUDE_BRIDGE_BASH_MOD_VERSION,
  CLAUDE_BRIDGE_DIALOG_REFUSAL_MOD_VERSION,
  claudeConfigModelChoice,
  claudeApprovalMode,
  claudeRefusalNote,
  makeClaudePlainCommandReader,
  claudeTranscriptCommandText,
  claudeCompactingFrame,
  claudeHookNoticeText,
  claudeQueueOperation,
  claudeTypedWaitingFrame,
  claudeSessionNoticeFrame,
  claudeSessionNotificationText,
  claudeTranscriptCommandOutput,
  claudeTranscriptPermissionMode,
  CLAUDE_BRIDGE_SETTLED_ELSEWHERE,
  CLAUDE_BRIDGE_UPKEEP_COMMAND,
  isClaudeBridgeModRow,
  isClaudeHarnessPrompt,
  isClaudeTurnEndRow,
  claudeBridgeReplacesDrawnFrame,
  claudeNarrationAsText,
  isClaudeTypedMidTurnRow,
  isClaudeBridgeCommandOutputFrame,
  isClaudeBridgeSessionLive,
  makeClaudeBridgeFrameTranslator,
  parseClaudeSlashCommandText,
  readTranscriptRows,
  type ClaudeBridgeActiveTurn,
  type ClaudeBridgeCommandBody,
  type ClaudeBridgeEvent,
  type ClaudeBridgeRequestTurnBy,
} from "../../claude-bridge/ClaudeBridgeProtocol.ts";
import { claudeBetweenTurnsNotice } from "../../import/claudeSessionParser.ts";
import { isRecord, parseJsonlLine } from "../../import/sessionImportTypes.ts";
import * as ProviderEventLoggers from "../../provider/ProviderEventLoggers.ts";
import * as ClaudeAdapterV2 from "./ClaudeAdapterV2.ts";

export const CLAUDE_TRANSPORT_ENV = "T3CODE_CLAUDE_TRANSPORT";
/**
 * For tests of a record that is behind its terminal. Claude Code writes what
 * it says in the middle of a turn to its file 0.2 to 5 seconds after its
 * terminal drew it, unevenly; with a number of milliseconds here, every write
 * is read that much later.
 */
export const CLAUDE_RECORD_LAG_ENV = "T3CODE_CLAUDE_RECORD_LAG_MS";
const TAIL_INTERVAL_MS = 300;
/**
 * How many reads the rows after the end of the turn open here wait for the
 * mod's word that the turn is over (see `endReadOfTurn`), about 5 s. That
 * word is handled within 2.2 s of the row live (157 turns, p99 1.2 s) and
 * mostly within 1.8 s on the rig (2966 turns, p90; longer around a stall,
 * when nothing is read either). Counted in reads, so a stall does not use
 * it up. After it the record is read on, as if the turn were still going.
 */
const TURN_END_HOLD_READS = 16;
/**
 * How many polls in a row must find the session unreadable or quiet before it
 * counts as gone. The mod rewrites session.json in place, several times in a
 * second at a turn's start (the turn, then a request it asks): a read that
 * lands mid-write finds it empty or cut, and one such read is no death.
 */
const SESSION_MISSES_TO_END = 3;
/** How long a message the terminal drew waits for the record's row before it is shown without one. */
const DRAWN_MESSAGE_WAIT_MS = 2_500;
/** How long a reply row's text is remembered, for a drawn copy that arrives after it. */
const RECORDED_TEXT_MEMORY_MS = 60_000;
const normalizeDrawn = (text: string) => text.replace(/\s+/gu, " ").trim();
/** What the person typed, as told by the mod and by the record's queue rows: the same words. */
const sameTyped = (left: string, right: string) => left.trim() === right.trim();
/** A uuid's shape (version 4) for the first 16 bytes of a digest. */
const uuidOfDigest = (digest: Uint8Array) => {
  const bytes = Array.from(digest.subarray(0, 16));
  bytes[6] = ((bytes[6] ?? 0) & 0x0f) | 0x40;
  bytes[8] = ((bytes[8] ?? 0) & 0x3f) | 0x80;
  const hex = bytes.map((byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
};
/** The text of a reply row of the record, as the thread shows it. */
const assistantRowText = (row: Record<string, unknown>): string => {
  const content = isRecord(row.message) ? row.message.content : null;
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content
    .map(claudeNarrationAsText)
    .flatMap((block) =>
      isRecord(block) && block.type === "text" && typeof block.text === "string"
        ? [block.text]
        : [],
    )
    .join("");
};
/** Whether a reply row of the record holds thoughts and nothing else. */
const isThoughtRow = (row: Record<string, unknown>): boolean => {
  const content = isRecord(row.message) ? row.message.content : null;
  return (
    Array.isArray(content) &&
    content.length > 0 &&
    content.every(
      (block) =>
        isRecord(block) && (block.type === "thinking" || block.type === "redacted_thinking"),
    )
  );
};
/**
 * How far ahead another instance's heartbeat must be before the attachment
 * moves to it: more than two of the mod's own beats, so two processes holding
 * one session do not pull it back and forth.
 */
const RELOADED_MOD_LEAD_MS = 6_500;
/** A turn's result waits this long after the last transcript row, so a late row cannot land after it. */
const COMPLETE_QUIET_MS = 400;
/**
 * How long the session's own word that a turn ended is waited for once its
 * record says the conversation carried on under another session id.
 */
const CONTINUED_TURN_WAIT_MS = 2_000;
/** How long a turn's closing row is waited for when the next turn has already begun. */
const TURN_END_ROW_POLL_MS = 50;
const TURN_END_ROW_ATTEMPTS = 30;
/**
 * A slash command typed in the terminal that starts no turn leaves only its
 * printed output; the record places it (see `typedCommandOutput`). One held
 * here (a /compact with its compaction, a /goal for the turn it starts) is
 * shown as a short turn of its own after this much quiet.
 */
const LOCAL_COMMAND_QUIET_MS = 1_500;

/**
 * A typed command whose output the turn it starts shows. What `/goal` prints
 * is how T3 learns the goal (the adapter reads it from that turn). What a `!`
 * line printed goes to the model, which answers it in a turn whose prompt is
 * the line (Claude Code 2.1.300): the record would show the line twice.
 */
const startsItsOwnTurn = (command: string | null) =>
  command !== null && (/^\/goal(?:\s|$)/u.test(command) || command.startsWith("! "));

/** The record row a frame was read from, by uuid. */
const frameRowUuid = (frame: SDKMessage): string | null => {
  const uuid = Reflect.get(frame, "uuid");
  return typeof uuid === "string" && uuid.length > 0 ? uuid : null;
};

/**
 * How long what the record says of a turn before it began (a prompt's hook
 * result, a compaction) waits for that turn to open here.
 */
const HELD_FOR_TURN_MS = 5_000;
/**
 * How long a request the session raised waits for a turn of the thread to
 * take it: the turn it belongs to, whose run is still starting, or one T3
 * opens for it. After that it goes back to Claude Code's own dialog.
 */
const REQUEST_TURN_WAIT_MS = 15_000;
const REQUEST_TURN_POLL_MS = 250;
/** How long an agent's request waits for the line of the command that started it. */
const COMMAND_LINE_WAIT_MS = 3_000;

const isCompactBoundaryFrame = (frame: SDKMessage) =>
  frame.type === "system" && frame.subtype === "compact_boundary";

/** A row the terminal shows for a prompt's own hook (UserPromptSubmit). */
const isPromptHookRow = (row: unknown) =>
  isRecord(row) &&
  isRecord(row.attachment) &&
  row.attachment.hookEvent === "UserPromptSubmit" &&
  claudeHookNoticeText(row) !== null;

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

/** What each bridged session last reported of its effort and status line, as last passed on. */
const shownNativeState = new Map<string, ClaudeAdapterV2.ClaudeBridgeNativeState>();

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
    const recordLagMs = Math.max(0, Number(hostEnvironment[CLAUDE_RECORD_LAG_ENV] ?? 0) || 0);
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
        /** Under `CLAUDE_RECORD_LAG_ENV`: the sizes the record grew to, and when each was first seen. */
        lagging: [] as Array<{ readonly size: number; readonly at: number }>,
        lastRowAt: 0,
        /**
         * When the record said its conversation carried on under another
         * session id. Null again once this session writes to the record.
         */
        continuedAt: null as number | null,
        /**
         * The session began compacting with no turn of its own open here. A
         * turn can begin with a compaction, and the session reports the
         * compaction first: the turn that opens before it stops says so.
         */
        compactingUnsaid: false,
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
        /**
         * Messages put in the session's inbox to steer its running turn that
         * it has not been heard to take, by prompt uuid. More than one can
         * wait at once, which `pendingPrompt` cannot say.
         */
        heldSteers: new Map<string, { readonly commandId: string }>(),
        /**
         * What the person typed in the terminal over the running turn, which
         * the session holds until it hands it to the model, oldest first: from
         * the record's queue rows, and shown waiting (under `key`) once the
         * mod has said the person typed it.
         */
        typed: [] as Array<{ readonly key: string; readonly text: string; shown: boolean }>,
        /** What the mod said the person typed over a running turn that no queue row has held yet. */
        typedUnheld: [] as Array<string>,
        approvals: new Map<string, AbortController>(),
        /** Requests the session settled without T3: nothing is sent back for them. */
        settledElsewhere: new Set<string>(),
        /**
         * A slash command sent from T3 that waits in a dialog of its own in
         * the terminal: its run is open here, saying so, until it settles.
         */
        commandWaiting: null as string | null,
        /** Printed command output read while no turn was open, kept for whatever it belongs to. */
        heldOutput: [] as Array<SDKMessage>,
        heldAt: 0,
        /**
         * What typed commands printed that no turn here will show, by row
         * uuid: the record places them (see `leaveToRecord`).
         */
        leftToRecord: [] as Array<string>,
        lastCommandText: null as string | null,
        plainCommand: makeClaudePlainCommandReader(),
        /** Rows that close a turn in the record, read so far; and the count when the open turn began. */
        turnEndsRead: 0,
        turnEndsAtTurnStart: 0,
        /**
         * The turn open here when the row that closes it in the record was
         * read: the rows after that row wait until it is closed here.
         */
        endReadOfTurn: null as string | null,
        /** Reads the rows after that end have waited; and the turn whose wait ran out. */
        endHeldReads: 0,
        endHoldExpired: null as string | null,
        /**
         * The turns the session began after the one shown at attach, oldest
         * first, each with its end if that came first: each opens here once
         * the one before it has closed (see `takeUpSetAside`).
         */
        setAside: [] as Array<{
          readonly turn: ClaudeBridgeActiveTurn;
          end: Extract<ClaudeBridgeEvent, { type: "turn.complete" }> | null;
        }>,
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
        /** Calls T3 is asking the person about right now, by tool call. */
        asking: new Set<string>(),
        /**
         * Those of them a subagent made. A subagent sent to the background
         * can still be waiting for its answer when the turn that started it
         * has ended.
         */
        agentAsks: new Set<string>(),
        /** Those no turn of the thread has taken yet: nothing shows for them so far. */
        unplaced: new Set<string>(),
        /**
         * A turn T3 opened itself, to show a request the session raised while
         * it had no turn of its own. It closes with the last thing asked in it.
         */
        requestTurn: null as { readonly turnId: string; readonly startedAt: number } | null,
        /** Drawn messages already shown: the record's own copy, arriving later, is the same row. */
        shownDrawn: [] as Array<{ readonly messageId: string; readonly text: string }>,
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

      /** A turn of the session's own is open in the thread, not one T3 opened to carry a request. */
      const sessionTurnOpen = () => translator.hasOpenTurn() && state.requestTurn === null;
      /** Whether the record has closed the turn still open here (see `endReadOfTurn`). */
      const turnEndHeld = () =>
        state.endReadOfTurn !== null &&
        sessionTurnOpen() &&
        translator.openTurnId() === state.endReadOfTurn;

      /** Tells the thread what the session did with a message sent to steer its turn. */
      const reportDelivery = (
        promptUuid: string,
        delivery: ClaudeAdapterV2.ClaudeBridgePromptDelivery["delivery"],
      ) => input.bridgeOnPromptDelivery?.({ promptUuid, delivery }) ?? Effect.void;

      const heldSteerOfCommand = (commandId: string | undefined) => {
        if (commandId === undefined) return undefined;
        for (const [promptUuid, held] of state.heldSteers) {
          if (held.commandId === commandId) return promptUuid;
        }
        return undefined;
      };

      /**
       * The session has a prompt: handed to the model inside the turn that
       * was running, or begun as a turn of its own. Said of any prompt with a
       * uuid, also one this attachment did not send (T3 may have sent it
       * before a restart); whoever is told knows which were sent to steer.
       */
      const promptTaken = (taken: {
        readonly promptUuid: string | undefined;
        readonly commandId: string | undefined;
      }) =>
        Effect.gen(function* () {
          const promptUuid = taken.promptUuid ?? heldSteerOfCommand(taken.commandId);
          if (promptUuid === undefined) return;
          state.heldSteers.delete(promptUuid);
          yield* reportDelivery(promptUuid, "taken");
        });

      /**
       * Nothing more will be heard of the messages still held: the session
       * ended, or T3 stops following it here. They are shown as not taken; a
       * later attachment that hears one taken says so.
       */
      const letGoOfHeldSteers = Effect.gen(function* () {
        const held = [...state.heldSteers.keys()];
        state.heldSteers.clear();
        for (const promptUuid of held) yield* reportDelivery(promptUuid, "dropped");
      });

      /** Shows what the person typed over the turn open here as waiting for the session. */
      const showTyped = (entry: (typeof state.typed)[number]) =>
        Effect.gen(function* () {
          if (entry.shown || !sessionTurnOpen()) return;
          entry.shown = true;
          yield* emit([
            claudeTypedWaitingFrame({ uuid: entry.key, sessionId: sid, text: entry.text }),
          ]);
        });

      /**
       * The session handed the model what the person typed, inside the turn
       * that was running. Said also of one not shown here: an earlier
       * attachment may have shown it.
       */
      const typedTaken = (text: string) =>
        Effect.gen(function* () {
          const index = state.typed.findIndex((entry) => sameTyped(entry.text, text));
          if (index === -1) return;
          const [entry] = state.typed.splice(index, 1);
          if (entry !== undefined) yield* reportDelivery(entry.key, "taken");
        });

      /**
       * The session begins a turn with what the person typed over the turn
       * before: the uuid it was shown waiting under, which then heads the
       * turn, or null.
       */
      const takeTypedTurn = (text: string) => {
        const index = state.typed.findIndex((entry) => sameTyped(entry.text, text));
        if (index === -1) return null;
        const [entry] = state.typed.splice(index, 1);
        return entry?.shown === true ? entry.key : null;
      };

      /**
       * What the person typed is held no more, untaken: taken back into the
       * prompt to edit, or nothing more will be heard of it here.
       */
      const letGoOfTyped = Effect.gen(function* () {
        const shown = state.typed.filter((entry) => entry.shown);
        state.typed = [];
        for (const entry of shown) yield* reportDelivery(entry.key, "dropped");
      });

      /** Follows the record's account of the prompts the session holds behind its turn. */
      const followQueue = (queued: NonNullable<ReturnType<typeof claudeQueueOperation>>) =>
        Effect.gen(function* () {
          switch (queued.operation) {
            case "enqueue": {
              const text = queued.content;
              // A task's notice, another agent's message: never the person's.
              if (text === null || text.trim().length === 0 || isClaudeHarnessPrompt(text)) return;
              if (!sessionTurnOpen()) return;
              // Made from the queue row: one attachment's word that the
              // session took it names the message another showed waiting.
              const key = uuidOfDigest(
                yield* crypto
                  .digest("SHA-256", new TextEncoder().encode(`${sid}\n${queued.at}\n${text}`))
                  .pipe(Effect.orDie),
              );
              if (state.typed.some((entry) => entry.key === key)) return;
              const entry = { key, text, shown: false };
              state.typed.push(entry);
              // One the record never said more of (a shape not read here) is
              // not kept for long.
              if (state.typed.length > 32) state.typed.shift();
              const said = state.typedUnheld.findIndex((typed) => sameTyped(typed, text));
              if (said === -1) return;
              state.typedUnheld.splice(said, 1);
              yield* showTyped(entry);
              return;
            }
            case "remove":
              if (queued.content !== null) yield* typedTaken(queued.content);
              return;
            case "popAll":
              yield* letGoOfTyped;
              return;
            case "dequeue":
              // The turn it begins says which it was (see takeTypedTurn).
              return;
          }
        });

      const finish = Effect.gen(function* () {
        if (state.closed) return;
        state.closed = true;
        state.requestTurn = null;
        yield* letGoOfHeldSteers;
        yield* letGoOfTyped;
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

      // The turns the session began while nothing was attached, oldest first.
      const missedTurns = yield* registry.peekMissedTurns(sid);
      const activeTurn = bridgeProcess.session.activeTurn;
      const attachedAt = yield* Clock.currentTimeMillis;
      // Only a turn that just ended, as long as the heal leaves its rows to
      // a run: an older one has reached the thread as history.
      const justOver = (turn: ClaudeBridgeRegistry.ClaudeBridgeMissedTurn) =>
        attachedAt - (turn.complete?.at ?? turn.start.at) <
        ClaudeBridgeRegistry.claudeBridgeMissedTurnReplayMs(turn);
      // The turn a run was offered for can be over, and the person already in
      // the next, before the run attaches. That run shows the turn it was
      // offered for (the oldest one over that a run was offered for), however
      // long ago it ended, and never a later one: each later one shows after
      // it, as its own.
      const offeredTurnOver = missedTurns.find(
        (turn) =>
          turn.offered === true &&
          turn.complete !== null &&
          turn.start.turnId !== activeTurn?.turnId,
      );
      const lastMissed = missedTurns.at(-1);
      const missedTurn =
        offeredTurnOver !== undefined && (activeTurn !== null || offeredTurnOver !== lastMissed)
          ? offeredTurnOver
          : activeTurn === null && lastMissed !== undefined && justOver(lastMissed)
            ? lastMissed
            : undefined;
      // Over too long ago to show from its start (its rows are history by
      // now): its run closes at once.
      const closeAtOnce = missedTurn !== undefined && !justOver(missedTurn);
      const turnOf = (
        start: ClaudeBridgeRegistry.ClaudeBridgeMissedTurn["start"],
      ): ClaudeBridgeActiveTurn =>
        activeTurn?.turnId === start.turnId
          ? activeTurn
          : {
              turnId: start.turnId,
              startedAt: start.at,
              transcriptOffset: start.transcriptOffset,
              ...(start.commandId === undefined ? {} : { commandId: start.commandId }),
              ...(start.promptUuid === undefined ? {} : { promptUuid: start.promptUuid }),
            };
      // The turns shown here from the list: the replayed one and those taken up after it.
      const shownHere = new Set<string>();
      /**
       * A turn the session began after the replayed one, running or just
       * over, opens here after it: the run offered for it shows it then.
       */
      const setAsideLater = (turn: ClaudeBridgeRegistry.ClaudeBridgeMissedTurn) => {
        const turnId = turn.start.turnId;
        if (shownHere.has(turnId) || state.setAside.some((held) => held.turn.turnId === turnId)) {
          return;
        }
        if (turn.complete !== null && !justOver(turn)) return;
        state.setAside.push({ turn: turnOf(turn.start), end: turn.complete });
      };
      let replayedTurnId: string | null = null;
      if (activeTurn !== null && missedTurn === undefined) {
        translator.adoptTurn(activeTurn);
        state.byteOffset = activeTurn.transcriptOffset;
        // The turn of a message sent to steer the one before it, begun while
        // nothing was attached.
        yield* promptTaken({
          promptUuid: activeTurn.promptUuid,
          commandId: undefined,
        });
      } else {
        state.byteOffset = yield* transcriptSize(state.transcriptPath);
        // A short turn can be over before the run offered for it attaches.
        // It is shown from its start, as if this had attached in time.
        if (missedTurn !== undefined) {
          translator.adoptTurn(turnOf(missedTurn.start));
          replayedTurnId = missedTurn.start.turnId;
          shownHere.add(replayedTurnId);
          for (const later of missedTurns.slice(missedTurns.indexOf(missedTurn) + 1)) {
            setAsideLater(later);
          }
          // A turn closed at once leaves the rows after it to the turns after it.
          state.byteOffset = closeAtOnce
            ? (state.setAside[0]?.turn.transcriptOffset ??
              activeTurn?.transcriptOffset ??
              state.byteOffset)
            : missedTurn.start.transcriptOffset;
          yield* promptTaken({ promptUuid: missedTurn.start.promptUuid, commandId: undefined });
          yield* Effect.logInfo("claude-bridge.missed-turn-replayed", {
            sid,
            turnId: missedTurn.start.turnId,
            ...(activeTurn === null ? {} : { before: activeTurn.turnId }),
            ...(state.setAside.length === 0
              ? {}
              : { setAside: state.setAside.map((held) => held.turn.turnId) }),
            ...(closeAtOnce ? { closedAtOnce: true } : {}),
          });
        }
      }
      // Nothing running and nothing to show: a run recovery kept open over a
      // restart, whose turn ended while T3 was away, closes empty now. The
      // record is compared again once it has: that turn's reply is there.
      if (activeTurn === null && missedTurn === undefined) {
        yield* registry.noteRecordChange(bridgeProcess, "turn.complete");
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

      // The mode the mod last reported. Claude Code writes its record's mode
      // row when it next writes its bookkeeping, at times only after the turn
      // has ended (a plan approved mid-turn, measured on 2.1.302), while the
      // mod hears the change from the next hook. A change the mod reports is
      // newer than the record's last row; a row read after it is newer again.
      let modMode: string | null | undefined;
      // A row that changes the record's mode is said again even when T3 shows
      // that mode already. The mod hears a mode set in the terminal between
      // turns (/plan) as the next terminal turn begins, before the run T3
      // opens for that turn, and a run only takes what arrives while it is open.
      let recordChangedMode = false;
      const nativeSettings = (
        session: ClaudeBridgeRegistry.ClaudeBridgeProcess["session"],
      ): ClaudeAdapterV2.ClaudeBridgeNativeSettings => {
        if (
          modMode !== undefined &&
          session.permissionMode !== null &&
          session.permissionMode !== modMode
        ) {
          transcriptMode = null;
        }
        modMode = session.permissionMode;
        return { model: session.model, permissionMode: transcriptMode ?? session.permissionMode };
      };
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

      /**
       * Passes on what the session reports of its effort and its status
       * line's figures when any of it changes. A mod before 0.3.26 says none.
       */
      const showNativeState = (session: ClaudeBridgeRegistry.ClaudeBridgeProcess["session"]) =>
        Effect.sync(() => {
          if (session.sessionEffort === undefined && session.usage === undefined) return;
          const state: ClaudeAdapterV2.ClaudeBridgeNativeState = {
            sessionEffort: session.sessionEffort ?? null,
            lastTurnEffort: session.effort,
            usage: session.usage ?? null,
          };
          if (NodeUtil.isDeepStrictEqual(shownNativeState.get(sid), state)) return;
          shownNativeState.set(sid, state);
          input.bridgeOnNativeState?.(state);
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
      yield* showNativeState(bridgeProcess.session);

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
            state.shownDrawn.push({ messageId: drawn.messageId, text: normalizeDrawn(drawn.text) });
            if (state.shownDrawn.length > 24) state.shownDrawn.shift();
            yield* emit(
              translator.onTranscriptRow({
                type: "assistant",
                uuid: drawn.messageId,
                message: { role: "assistant", content: [{ type: "text", text: drawn.text }] },
              }),
            );
          }
        });

      /** How much of the record was written at least `recordLagMs` ago. */
      const sizeOldEnough = (written: number) =>
        Effect.gen(function* () {
          const now = yield* Clock.currentTimeMillis;
          const latest = state.lagging.at(-1)?.size ?? state.byteOffset;
          // A smaller file is another file: the session moved its record.
          if (written < latest) state.lagging = [];
          if (written !== latest) state.lagging.push({ size: written, at: now });
          let size = state.byteOffset;
          for (
            let oldest = state.lagging[0];
            oldest !== undefined && now - oldest.at >= recordLagMs;
            oldest = state.lagging[0]
          ) {
            size = oldest.size;
            state.lagging.shift();
          }
          return size;
        });

      /**
       * Whether a row's printed output is a command the person typed in the
       * terminal between turns: no turn of the session's is open here, or the
       * record has closed the one still open here; no command T3 sent waits
       * on its rows; and it is not one held for what goes with it (a
       * /compact's compaction, a /goal's turn).
       */
      const typedCommandOutput = (row: unknown) =>
        !isClaudeBridgeModRow(row) &&
        (!sessionTurnOpen() || state.turnEndsRead > state.turnEndsAtTurnStart) &&
        state.commandWaiting === null &&
        state.pendingLocal === null &&
        (state.pendingPrompt === null || state.pendingPrompt.body.type === "prompt") &&
        !state.heldOutput.some(isCompactBoundaryFrame) &&
        !startsItsOwnTurn(state.lastCommandText);

      /**
       * Has the record place what typed commands printed (and their lines),
       * in the order it was written, once no turn of the session's is open
       * here: a turn the record closed before the command may still have
       * rows to show here, and they go before it.
       */
      const leaveToRecord = Effect.gen(function* () {
        if (state.leftToRecord.length === 0 || sessionTurnOpen()) return;
        const rows = state.leftToRecord.splice(0);
        yield* Effect.logInfo("claude-bridge.commands-left-to-record", { sid, rows: rows.length });
        yield* registry.noteRecordChange(bridgeProcess, "commands-left-to-record", rows);
      });

      /**
       * Translates the record's new rows. With `untilTurnEnd` it stops after
       * the row that closes a turn, leaving the next turn's rows unread.
       */
      const readRows = (untilTurnEnd: boolean) =>
        Effect.gen(function* () {
          if (state.transcriptPath.length === 0) return;
          // What follows the end of the turn open here is the next turn's: a
          // server that read it now would show it in this turn's run.
          if (turnEndHeld()) return;
          const written = yield* transcriptSize(state.transcriptPath);
          const size = recordLagMs === 0 ? written : yield* sizeOldEnough(written);
          if (size <= state.byteOffset) return;
          const bytes = yield* readTranscriptBytes(
            state.transcriptPath,
            state.byteOffset,
            size - state.byteOffset,
          );
          const decoder = new TextDecoder();
          let lineStart = 0;
          let olderRows = 0;
          let noticesBetweenTurns = 0;
          for (;;) {
            const newline = bytes.indexOf(0x0a, lineStart);
            if (newline === -1) break;
            const row = parseJsonlLine(decoder.decode(bytes.subarray(lineStart, newline)));
            let replacesDrawn: string | null = null;
            state.byteOffset += newline + 1 - lineStart;
            lineStart = newline + 1;
            state.lastRowAt = yield* Clock.currentTimeMillis;
            if (row === undefined) continue;
            if (isRecord(row)) {
              if (
                row.type === "continued-in" &&
                typeof row.continuedInSessionId === "string" &&
                row.continuedInSessionId.length > 0
              ) {
                state.continuedAt = state.lastRowAt;
              } else if (row.type === "user" || row.type === "assistant") {
                state.continuedAt = null;
              }
            }
            // Another session's conversation, copied into this record after
            // the turn began: not this turn's replies, tools or turn ends.
            if (translator.isFromBeforeTurn(row)) {
              olderRows += 1;
              continue;
            }
            // What the person typed into the running turn is in the record
            // only. It is placed in the thread now, before the rows that
            // follow it are shown, so it sits where it was written.
            if (isClaudeTypedMidTurnRow(row)) {
              yield* registry.noteRecordChange(bridgeProcess, "typed-mid-turn");
            }
            const queued = claudeQueueOperation(row);
            if (queued !== null) yield* followQueue(queued);
            const recordedMode = claudeTranscriptPermissionMode(row);
            if (recordedMode !== null && recordedMode !== transcriptMode) {
              transcriptMode = recordedMode;
              recordChangedMode = true;
            }
            const plain = state.plainCommand(row);
            const commandText =
              claudeTranscriptCommandText(row) ??
              (plain !== null && "command" in plain ? plain.command : null);
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
            // A reply row settles what the terminal drew before it. The same
            // text is the record's own copy. A tool call says the record has
            // none: a message's text is written before its tool calls. A
            // thought says nothing: its message's text is the row after it.
            if (isRecord(row) && row.type === "assistant" && row.isSidechain !== true) {
              const recorded = normalizeDrawn(assistantRowText(row));
              if (recorded.length > 0) {
                // The record can be seconds behind the terminal, and this
                // row's text is already showing from what the terminal drew.
                // The record's row is the reply and takes that copy's place
                // under its own id (what a restarted T3 would read again).
                const shown = state.shownDrawn.findIndex((drawn) => drawn.text === recorded);
                if (shown !== -1) {
                  replacesDrawn = state.shownDrawn[shown]?.messageId ?? null;
                  state.shownDrawn.splice(shown, 1);
                }
                state.recordedTexts.push({ text: recorded, at: state.lastRowAt });
                if (state.recordedTexts.length > 24) state.recordedTexts.shift();
                state.drawnMessages = state.drawnMessages.filter(
                  (drawn) => normalizeDrawn(drawn.text) !== recorded,
                );
              } else if (!isThoughtRow(row)) {
                yield* showDrawnMessages(0);
              }
            }
            const promptHook = isPromptHookRow(row);
            for (const frame of translator.onTranscriptRow(row)) {
              // What a command typed in the terminal printed between turns is
              // never a turn here: one shown here would sit after a message
              // T3 sent meanwhile (its run is made as it is sent), and beside
              // the record's own copy. The record places it, where it was written.
              if (isClaudeBridgeCommandOutputFrame(frame) && typedCommandOutput(row)) {
                const uuid = frameRowUuid(frame);
                if (uuid !== null) {
                  state.leftToRecord.push(uuid);
                  continue;
                }
              }
              // A /compact typed in the terminal writes its boundary and
              // summary before the run that shows the command opens: they
              // wait with its output and show in that run. A prompt's hook
              // result can be read before the turn it ran for is: it waits
              // for that turn.
              if (
                (isClaudeBridgeCommandOutputFrame(frame) ||
                  isCompactBoundaryFrame(frame) ||
                  promptHook) &&
                (!sessionTurnOpen() || state.commandWaiting !== null)
              ) {
                state.heldOutput.push(frame);
                state.heldAt = state.lastRowAt;
              } else {
                yield* emit([
                  replacesDrawn === null
                    ? frame
                    : claudeBridgeReplacesDrawnFrame(frame, replacesDrawn),
                ]);
              }
            }
            if (replacesDrawn !== null) {
              // The thread is compared with the record too: a T3 that could
              // not make the exchange leaves both, and the repair empties one.
              replacesDrawn = null;
              yield* registry.noteRecordChange(bridgeProcess, "reply-recorded");
            }
            // What the harness says between turns (a recap, a notice) no run
            // is open to show: the thread takes it from the record.
            if (!sessionTurnOpen() && claudeBetweenTurnsNotice(row) !== null) {
              noticesBetweenTurns += 1;
            }
            if (isClaudeTurnEndRow(row)) {
              state.turnEndsRead += 1;
              if (untilTurnEnd) break;
              // A read that reaches past the end of the turn open here (a
              // server that could not read for a while) stops at it; the
              // rest is read once the mod's word has closed the turn here.
              if (sessionTurnOpen() && translator.openTurnId() !== state.endHoldExpired) {
                state.endReadOfTurn = translator.openTurnId();
                break;
              }
            }
          }
          yield* leaveToRecord;
          if (olderRows > 0) {
            // They may have landed after the thread's history was last
            // compared with the record: it is compared again.
            yield* Effect.logInfo("claude-bridge.older-rows-left-to-history", {
              sid,
              rows: olderRows,
            });
            yield* registry.noteRecordChange(bridgeProcess, "older-rows");
          } else if (noticesBetweenTurns > 0) {
            // Compared now, so the notice shows while it is news, not at the next sweep.
            yield* registry.noteRecordChange(bridgeProcess, "older-rows");
          }
        });
      const readNewRows = readRows(false);

      const settleElsewhere = (toolUseId: string) =>
        Effect.sync(() => {
          const controller = state.approvals.get(toolUseId);
          // A request whose wait has not begun yet (it and its settling were
          // read together) is remembered, and is then not asked at all.
          if (controller === undefined && !state.asking.has(toolUseId)) return;
          state.settledElsewhere.add(toolUseId);
          controller?.abort(CLAUDE_BRIDGE_SETTLED_ELSEWHERE);
        });

      /**
       * Opens a turn of T3's own for a request the session raised while it has
       * no turn of its own: a subagent sent to the background asks after the
       * turn that started it has ended. The thread shows a request inside a
       * run, and this turn is what gives the request one. Nothing is opened
       * while a turn is open, or a prompt sent from T3 is about to start one.
       */
      const openRequestTurn = (toolUseId: string, text: string, by: ClaudeBridgeRequestTurnBy) =>
        Effect.gen(function* () {
          if (state.closed || translator.hasOpenTurn() || state.pendingPrompt !== null) return;
          const now = yield* Clock.currentTimeMillis;
          const turnId = `request-${toolUseId}-${now}`;
          state.requestTurn = { turnId, startedAt: now };
          yield* emit(
            translator.onTurnStart(
              { seq: 0, at: now, type: "turn.start", turnId, text, transcriptOffset: 0 },
              { request: by },
            ),
          );
          yield* Effect.logInfo("claude-bridge.request-turn-opened", { sid, turnId });
        });

      /**
       * Closes the turn T3 opened for a request. `superseded`: the session has
       * begun something of its own. What is still asked in the request's turn
       * is put away with it; the mod repeats a request that is still open, and
       * it is then asked in what follows.
       */
      const closeRequestTurn = (superseded: boolean) =>
        Effect.gen(function* () {
          const open = state.requestTurn;
          if (open === null) return;
          state.requestTurn = null;
          // Something else already closed whatever was open.
          if (!translator.hasOpenTurn()) return;
          if (superseded) {
            // A request no turn has taken yet shows nothing: it goes on
            // waiting for the turn that follows.
            for (const toolUseId of state.approvals.keys()) {
              if (!state.unplaced.has(toolUseId)) yield* settleElsewhere(toolUseId);
            }
          }
          const now = yield* Clock.currentTimeMillis;
          yield* emit(
            translator.onTurnComplete({
              seq: 0,
              at: now,
              type: "turn.complete",
              turnId: open.turnId,
              reason: "answer",
              answer: "",
              isAborted: false,
              durationMs: now - open.startedAt,
              transcriptOffset: 0,
            }),
          );
        });

      /**
       * Asks T3 about a call until a turn of the thread takes the request.
       * `ask` answers null when no run is there to show it: the turn it
       * belongs to was reported a moment ago and its run is still starting,
       * or the session has no turn of its own and one is opened for it. Null
       * in the end means nobody took it.
       */
      const askUntilTaken = (input: {
        readonly toolUseId: string;
        readonly signal: AbortSignal;
        readonly ask: () => Promise<PermissionResult | null>;
        readonly openTurn: Effect.Effect<void>;
      }) =>
        Effect.gen(function* () {
          const attempt = Effect.result(
            Effect.tryPromise({
              try: input.ask,
              catch: (cause) => runnerError(cause, "canUseTool"),
            }),
          );
          let result = yield* attempt;
          // A session whose mod asks T3 alone takes no second answer.
          if (bridgeProcess.session.protocol < CLAUDE_BRIDGE_WINDOW_PROTOCOL) return result;
          const deadline = (yield* Clock.currentTimeMillis) + REQUEST_TURN_WAIT_MS;
          while (
            result._tag === "Success" &&
            result.success === null &&
            !state.closed &&
            !input.signal.aborted &&
            (yield* Clock.currentTimeMillis) < deadline
          ) {
            yield* input.openTurn;
            state.unplaced.add(input.toolUseId);
            yield* Effect.sleep(`${REQUEST_TURN_POLL_MS} millis`).pipe(
              Effect.ensuring(Effect.sync(() => state.unplaced.delete(input.toolUseId))),
            );
            if (state.closed || input.signal.aborted) break;
            result = yield* attempt;
          }
          return result;
        });

      /**
       * Takes up what the session is waiting to ask when a run was started
       * for exactly that: nothing was attached, the session listed a request
       * in its file, and a run was offered so that it could be asked. The run
       * has no turn of the session's to show; the turn T3 keeps for requests
       * is opened inside it, without the frames that would ask for a run of
       * its own. The session tells the request itself once it sees T3
       * attached; if it never does (it was settled meanwhile), the turn
       * closes again.
       */
      const adoptWaitingRequestTurn = Effect.gen(function* () {
        if (state.closed || translator.hasOpenTurn() || state.pendingPrompt !== null) return false;
        const live = (yield* registry.readSession(bridgeProcess)) ?? bridgeProcess.session;
        const waiting = live.waiting ?? [];
        if (live.activeTurn !== null || waiting.length === 0) return false;
        const now = yield* Clock.currentTimeMillis;
        const turnId = `request-${waiting[0]!.toolUseId}-${now}`;
        state.requestTurn = { turnId, startedAt: now };
        translator.adoptTurn({
          turnId,
          startedAt: now,
          transcriptOffset: yield* transcriptSize(state.transcriptPath),
        });
        yield* Effect.logInfo("claude-bridge.request-turn-adopted", { sid, turnId });
        yield* Effect.sleep(`${REQUEST_TURN_WAIT_MS} millis`).pipe(
          Effect.andThen(
            Effect.suspend(() =>
              state.requestTurn?.turnId === turnId && state.asking.size === 0
                ? closeRequestTurn(false)
                : Effect.void,
            ),
          ),
          Effect.forkIn(sessionScope),
        );
        return true;
      });

      /** An ask is over, however it ended. The turn T3 opened for requests closes with the last of them. */
      const askOver = (toolUseId: string) =>
        Effect.suspend(() => {
          state.asking.delete(toolUseId);
          state.settledElsewhere.delete(toolUseId);
          state.agentAsks.delete(toolUseId);
          state.unplaced.delete(toolUseId);
          return state.asking.size === 0 ? closeRequestTurn(false) : Effect.void;
        });

      /** A background subagent's request is shown in the run that is open. */
      const agentAskPlaced = () =>
        [...state.agentAsks].some((toolUseId) => !state.unplaced.has(toolUseId));

      /**
       * A turn's worth of frames for a slash command that started no model
       * turn. A command can start an agent in the background instead, and
       * what that agent asks may already wait in the run that carries the
       * command: the turn then ends once that is answered, as a turn of the
       * session's own does.
       */
      const emitCommandTurn = (input: {
        readonly turnId: string;
        readonly text: string;
        readonly promptUuid: string | null;
        readonly ok: boolean;
        readonly answer: string;
      }) =>
        Effect.gen(function* () {
          yield* closeRequestTurn(true);
          const now = yield* Clock.currentTimeMillis;
          // A command that waited in a terminal dialog has its turn open already.
          const opened =
            state.commandWaiting !== null && input.turnId === `command-${state.commandWaiting}`;
          if (opened) state.commandWaiting = null;
          if (!opened) {
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
          }
          const output = state.heldOutput.splice(0);
          yield* emit(output);
          const complete = {
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
          } as const;
          if (agentAskPlaced() && state.pendingComplete === null && !state.closed) {
            state.pendingComplete = { event: complete, seenAt: now };
            return;
          }
          yield* emit(translator.onTurnComplete(complete));
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
        // Held for a turn of the session's own that did not come (a hook's
        // word on a prompt it stopped): it goes on as it would have.
        if (
          state.heldOutput.length > 0 &&
          !state.heldOutput.some(isClaudeBridgeCommandOutputFrame) &&
          state.pendingPrompt === null &&
          !sessionTurnOpen() &&
          now - state.heldAt >= HELD_FOR_TURN_MS
        ) {
          yield* emit(state.heldOutput.splice(0));
          return;
        }
        // A compaction or hook result held alone (before a turn of the
        // session's own) waits for that turn; only printed output is a command.
        if (
          state.heldOutput.some(isClaudeBridgeCommandOutputFrame) &&
          state.pendingPrompt === null &&
          state.pendingComplete === null &&
          !sessionTurnOpen() &&
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

      /**
       * Opens here the next turn set aside at attach, once the turn before it
       * has closed: its rows follow, for the run offered for it.
       */
      const takeUpSetAside = Effect.gen(function* () {
        const next = state.setAside[0];
        if (next === undefined || translator.hasOpenTurn()) return;
        state.setAside.shift();
        shownHere.add(next.turn.turnId);
        translator.adoptTurn(next.turn);
        state.byteOffset = Math.max(state.byteOffset, next.turn.transcriptOffset);
        state.turnEndsAtTurnStart = state.turnEndsRead;
        yield* promptTaken({ promptUuid: next.turn.promptUuid, commandId: undefined });
        if (next.end !== null) {
          state.pendingComplete = { event: next.end, seenAt: yield* Clock.currentTimeMillis };
        }
        yield* Effect.logInfo("claude-bridge.set-aside-turn-taken-up", {
          sid,
          turnId: next.turn.turnId,
        });
      });

      const settlePendingComplete = Effect.gen(function* () {
        const pending = state.pendingComplete;
        if (pending === null) return;
        const now = yield* Clock.currentTimeMillis;
        if (
          // The run stays open while a background subagent's request waits in
          // it: the request can only be answered from a run that is open.
          !agentAskPlaced() &&
          // Claude Code's file is level with its terminal when a turn ends:
          // a record held back for a test is read to its end first. Or to
          // the row that closes the turn, which has every row of it before it.
          (turnEndHeld() ||
            (state.byteOffset >= pending.event.transcriptOffset && state.lagging.length === 0)) &&
          now - state.lastRowAt >= COMPLETE_QUIET_MS &&
          now - pending.seenAt >= COMPLETE_QUIET_MS
        ) {
          state.pendingComplete = null;
          yield* emit(translator.onTurnComplete(pending.event));
          yield* takeUpSetAside;
        }
      });

      /**
       * Closes the turn this process was running when its conversation
       * carried on under another session id. Sent to the background while a
       * tool ran, the process says nothing more of the turn: the rest of it
       * is the new session's, and the thread follows once this run is over.
       */
      /** A mod that says nothing of a turn's end does not stop the record being read for long. */
      const expireTurnEndHold = Effect.gen(function* () {
        if (!turnEndHeld()) {
          state.endHeldReads = 0;
          return;
        }
        state.endHeldReads += 1;
        if (state.endHeldReads < TURN_END_HOLD_READS) return;
        yield* Effect.logInfo("claude-bridge.turn-end-hold-expired", {
          sid,
          turnId: state.endReadOfTurn,
        });
        state.endHoldExpired = state.endReadOfTurn;
        state.endReadOfTurn = null;
        state.endHeldReads = 0;
      });

      const closeTurnLeftBehind = Effect.gen(function* () {
        if (state.continuedAt === null || !sessionTurnOpen() || state.pendingComplete !== null) {
          return;
        }
        const now = yield* Clock.currentTimeMillis;
        if (now - state.continuedAt < CONTINUED_TURN_WAIT_MS) return;
        yield* Effect.logInfo("claude-bridge.turn-left-behind", { sid });
        for (const toolUseId of state.approvals.keys()) {
          if (!state.agentAsks.has(toolUseId)) yield* settleElsewhere(toolUseId);
        }
        yield* emit(translator.onContinuedElsewhere(now));
      });

      const endSession = (reason: string) =>
        Effect.gen(function* () {
          yield* readNewRows.pipe(Effect.ignore);
          // A command sent from T3 that ended the session (/exit, /background)
          // did what it was sent to do: it is shown as done, not as a turn
          // that failed.
          const sent = state.pendingPrompt;
          if (sent !== null && sent.label !== null && sent.body.type === "command") {
            state.pendingPrompt = null;
            yield* emitCommandTurn({
              turnId: `command-${sent.commandId}`,
              text: sent.label,
              promptUuid: sent.promptUuid,
              ok: true,
              answer: "The Claude Code session ended.",
            });
          }
          const pending = state.pendingComplete;
          if (pending !== null) {
            state.pendingComplete = null;
            yield* emit(translator.onTurnComplete(pending.event));
          }
          yield* letGoOfHeldSteers;
          yield* letGoOfTyped;
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
        if (pending === null) return true;
        if (pending.body.type === "bash") {
          // A shell command may already have run in the instance that went
          // away: it is not run a second time on a guess.
          state.pendingPrompt = null;
          const at = yield* Clock.currentTimeMillis;
          yield* emit(
            translator.onTurnComplete({
              seq: 0,
              at,
              type: "turn.complete",
              turnId: `dropped-${pending.commandId}`,
              reason: "error",
              answer:
                "The session reloaded its T3 mod while this command was being handed over. It may or may not have run; it was not sent again.",
              isAborted: false,
              durationMs: 0,
              transcriptOffset: 0,
              promptUuid: pending.promptUuid,
            }),
          );
          return true;
        }
        pending.commandId = yield* registry.send(newest, pending.body);
        return true;
      });

      const tail = Effect.gen(function* () {
        let lastHeartbeat = 0;
        let sessionMisses = 0;
        while (!state.closed) {
          const now = yield* Clock.currentTimeMillis;
          yield* followReloadedMod;
          if (now - lastHeartbeat >= CLAUDE_BRIDGE_T3_HEARTBEAT_MS) {
            yield* registry.heartbeat(bridgeProcess, toolEndpoint);
            lastHeartbeat = now;
          }
          const session = yield* registry.readSession(bridgeProcess);
          if (session === null || !isClaudeBridgeSessionLive(session, now)) {
            // An end the session itself wrote is final; a file that would not
            // read, or a heartbeat that has gone quiet, is looked at again.
            sessionMisses += 1;
            if (session?.ended == null && sessionMisses < SESSION_MISSES_TO_END) {
              yield* Effect.sleep(`${TAIL_INTERVAL_MS} millis`);
              continue;
            }
            yield* endSession(session?.ended?.reason ?? "process gone");
            return;
          }
          sessionMisses = 0;
          // The session names its record's file, and the file can move while
          // the session runs: `/cd <dir>` takes it, as it is, to that
          // directory's project folder. What was read of it stays read.
          if (
            session.transcriptPath.length > 0 &&
            session.transcriptPath !== state.transcriptPath
          ) {
            const moved = state.transcriptPath.length > 0;
            state.transcriptPath = session.transcriptPath;
            const size = yield* transcriptSize(state.transcriptPath);
            state.byteOffset = moved ? Math.min(state.byteOffset, size) : size;
          }
          if (session.model !== null) translator.setModel(session.model);
          yield* showNativeSettings(nativeSettings(session), recordChangedMode);
          recordChangedMode = false;
          yield* showNativeState(session);
          yield* expireTurnEndHold;
          yield* readNewRows.pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("claude-bridge.transcript-read-failed", { sid, cause }),
            ),
          );
          // A drawn message no reply row followed (a long tool run comes next).
          yield* showDrawnMessages(DRAWN_MESSAGE_WAIT_MS);
          yield* settlePendingComplete;
          yield* closeTurnLeftBehind;
          yield* leaveToRecord;
          yield* settleLocalCommands;
          yield* Effect.sleep(`${TAIL_INTERVAL_MS} millis`);
        }
      });

      /**
       * A command sent from T3 can start an agent that asks before the
       * command has reported what it printed. The terminal prints the
       * command's line and then asks; the request waits for that line here.
       */
      const commandLineShown = Effect.gen(function* () {
        const deadline = (yield* Clock.currentTimeMillis) + COMMAND_LINE_WAIT_MS;
        while (
          !state.closed &&
          (state.pendingLocal !== null || state.pendingPrompt?.body.type === "command") &&
          (yield* Clock.currentTimeMillis) < deadline
        ) {
          yield* Effect.sleep(`${REQUEST_TURN_POLL_MS} millis`);
        }
      });

      const resolveApproval = (event: Extract<ClaudeBridgeEvent, { type: "approval.request" }>) =>
        Effect.gen(function* () {
          if (event.agentType !== undefined) yield* commandLineShown;
          // Settled before T3 came to ask: there is nothing to ask.
          if (state.settledElsewhere.has(event.toolUseId)) return;
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
          const result = yield* askUntilTaken({
            toolUseId: event.toolUseId,
            signal: controller.signal,
            ask: () =>
              canUseTool(event.tool, isRecord(event.input) ? event.input : {}, {
                signal: controller.signal,
                toolUseID: event.toolUseId,
                requestId: event.toolUseId,
                ...(event.reason === undefined ? {} : { askedBecause: event.reason }),
                ...(event.alsoInTerminal === true ? { alsoInTerminal: true } : {}),
                ...(event.suggestions === undefined ? {} : { suggestions: event.suggestions }),
                ...(event.agentType === undefined ? {} : { askedByAgent: event.agentType }),
                ...(event.approveWithMode === undefined
                  ? {}
                  : { approveWithMode: event.approveWithMode }),
              } as Parameters<CanUseTool>[2]),
            openTurn: openRequestTurn(
              event.toolUseId,
              claudeBridgeRequestTurnText({ agentType: event.agentType }),
              event.agentType === undefined ? "session" : "subagent",
            ),
          });
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
            // "Allow for this session" arrives as permission updates. Where
            // Claude Code suggested rules for the call (its dialog's "don't
            // ask again" option), those go back and Claude Code applies them
            // itself; otherwise the mod keeps the grant for the same call.
            // An approval that also switches the session's permission mode
            // carries that change alone, and it goes to the session as it is.
            const withMode = claudeApprovalMode(decision) !== undefined;
            const forSession = !withMode && (decision.updatedPermissions?.length ?? 0) > 0;
            const ownRules = forSession && event.suggestions !== undefined;
            yield* registry.send(bridgeProcess, {
              type: "approval",
              toolUseId: event.toolUseId,
              decision: "allow",
              ...(forSession ? { scope: "session" as const } : {}),
              ...(ownRules || withMode
                ? { updatedPermissions: decision.updatedPermissions ?? [] }
                : {}),
            });
            return;
          }
          // Where Claude Code's own dialog is open, a refusal from T3 is that
          // dialog's "No": the call is refused and the turn ends, whichever
          // of T3's two refusals it was. The session then holds the rows the
          // terminal's answer leaves.
          const live = (yield* registry.readSession(bridgeProcess)) ?? bridgeProcess.session;
          // (A subagent's request is refused as before: what the dialog's
          // "No" stops there, the subagent or the turn, is not measured.)
          // Refused with the person's words, the turn goes on with them, as
          // the dialog's "No, and tell Claude what to do differently" has it.
          const throughDialog =
            claudeRefusalNote(decision) === undefined &&
            event.alsoInTerminal === true &&
            event.agentType === undefined &&
            claudeBridgeModAtLeast(live.modVersion, CLAUDE_BRIDGE_DIALOG_REFUSAL_MOD_VERSION);
          yield* registry.send(bridgeProcess, {
            type: "approval",
            toolUseId: event.toolUseId,
            decision: "deny",
            reason: decision.message,
            ...(throughDialog ? { stopTurn: true } : {}),
          });
          // With no dialog to answer, cancel is a refusal and then a stop.
          if (!throughDialog && decision.interrupt === true) {
            yield* registry.send(bridgeProcess, { type: "interrupt" });
          }
        });

      /** A question Claude asked: answered with the person's answers, not with an approval. */
      const resolveQuestion = (event: Extract<ClaudeBridgeEvent, { type: "question.request" }>) =>
        Effect.gen(function* () {
          if (state.settledElsewhere.has(event.toolUseId)) return;
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
          const result = yield* askUntilTaken({
            toolUseId: event.toolUseId,
            signal: controller.signal,
            ask: () =>
              canUseTool(
                "AskUserQuestion",
                { questions: event.questions },
                {
                  signal: controller.signal,
                  toolUseID: event.toolUseId,
                  requestId: event.toolUseId,
                },
              ),
            openTurn: openRequestTurn(
              event.toolUseId,
              claudeBridgeRequestTurnText({ question: true }),
              "session",
            ),
          });
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
              // A turn T3 opened for a request gives way to the session's own.
              yield* closeRequestTurn(true);
              // The turn before this one has ended and its end is still
              // waiting for its last rows (a message queued behind it starts
              // the next at once). It is closed first: otherwise the two run
              // together and the new turn is lost when the old one closes.
              if (
                state.pendingComplete !== null &&
                state.pendingComplete.event.turnId !== event.turnId
              ) {
                // A subagent's request still waiting in the turn that ended
                // closes with it; the mod asks again, into the new turn.
                for (const toolUseId of state.agentAsks) {
                  yield* settleElsewhere(toolUseId);
                }
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
              // The turns set aside at attach are over by now; this one is shown.
              state.setAside = [];
              state.turnEndsAtTurnStart = state.turnEndsRead;
              if (
                state.pendingPrompt !== null &&
                event.commandId === state.pendingPrompt.commandId
              ) {
                state.pendingPrompt = null;
              }
              // A turn begins: whatever a waiting command had open gives way to it.
              state.commandWaiting = null;
              // A message sent to steer the turn before this one met no tool
              // call there: this turn is the session taking it.
              yield* promptTaken({ promptUuid: event.promptUuid, commandId: event.commandId });
              // What the person typed over the turn before, which this turn
              // is the session taking: it heads the turn already.
              const typedTurn =
                event.promptUuid === undefined && event.commandId === undefined
                  ? takeTypedTurn(event.text)
                  : null;
              yield* emit(
                translator.onTurnStart(
                  event,
                  typedTurn === null ? undefined : { waiting: typedTurn },
                ),
              );
              if (state.compactingUnsaid) {
                state.compactingUnsaid = false;
                const uuid = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
                yield* emit([claudeCompactingFrame({ uuid, sessionId: sid, compacting: true })]);
              }
              // Output a slash command printed just before its turn began.
              yield* emit(state.heldOutput.splice(0));
              const live = yield* registry.readSession(bridgeProcess);
              if (live !== null) yield* showNativeSettings(nativeSettings(live), true);
              return;
            }
            case "turn.complete": {
              // The session's late word of a turn already closed here, when
              // its conversation carried on elsewhere: it closes nothing.
              if (state.continuedAt !== null && !translator.hasOpenTurn()) return;
              // Whatever this turn's own loop still asked is over with it.
              for (const toolUseId of state.approvals.keys()) {
                if (!state.agentAsks.has(toolUseId)) yield* settleElsewhere(toolUseId);
              }
              // The end of a turn set aside at attach waits until it opens
              // here: the turn before it closes with its own end.
              const held = state.setAside.find((entry) => entry.turn.turnId === event.turnId);
              if (held !== undefined) {
                held.end = event;
                return;
              }
              state.pendingComplete = { event, seenAt: yield* Clock.currentTimeMillis };
              return;
            }
            case "command.result": {
              if (state.pendingPrompt?.commandId !== event.commandId) return;
              if (event.interim) {
                // It waits in a dialog of its own in the terminal. Its run
                // opens now and says so; it settles when the dialog closes,
                // with what it printed there.
                if (state.commandWaiting === event.commandId) return;
                const waiting = state.pendingPrompt;
                state.commandWaiting = event.commandId;
                yield* emit(
                  translator.onTurnStart({
                    seq: 0,
                    at: event.at,
                    type: "turn.start",
                    turnId: `command-${event.commandId}`,
                    text: waiting.label ?? "(slash command)",
                    transcriptOffset: 0,
                    promptUuid: waiting.promptUuid,
                  }),
                );
                const uuid = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
                yield* emit([
                  claudeSessionNoticeFrame({
                    uuid,
                    sessionId: sid,
                    subtype: "notification",
                    content: event.text,
                  }),
                ]);
                return;
              }
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
              if (state.asking.has(event.toolUseId)) return;
              state.asking.add(event.toolUseId);
              yield* resolveQuestion(event).pipe(
                Effect.ensuring(askOver(event.toolUseId)),
                Effect.forkIn(sessionScope),
              );
              return;
            }
            case "question.resolved": {
              if (event.by !== "t3" && event.by !== "detached") {
                yield* settleElsewhere(event.toolUseId);
              }
              return;
            }
            case "prompt.typed": {
              // The record's queue row says the session holds it: whichever
              // is read first waits for the other.
              const held = state.typed.find(
                (entry) => !entry.shown && sameTyped(entry.text, event.text),
              );
              if (held !== undefined) {
                yield* showTyped(held);
                return;
              }
              state.typedUnheld.push(event.text);
              if (state.typedUnheld.length > 8) state.typedUnheld.shift();
              return;
            }
            case "prompt.delivered": {
              // The model has it, inside the turn that was running: no turn
              // of its own will start for it.
              yield* Effect.logInfo("claude-bridge.prompt-delivered-mid-turn", {
                sid,
                commandId: event.commandId,
                turnId: event.turnId,
              });
              yield* promptTaken({ promptUuid: event.promptUuid, commandId: event.commandId });
              if (state.pendingPrompt?.commandId === event.commandId) state.pendingPrompt = null;
              return;
            }
            case "prompt.dropped": {
              const droppedSteer = heldSteerOfCommand(event.commandId);
              if (droppedSteer !== undefined) {
                state.heldSteers.delete(droppedSteer);
                yield* reportDelivery(droppedSteer, "dropped");
              }
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
              // Asked again for a call T3 is already asking about (the mod
              // lost sight of T3 for a moment and found it again): the
              // request the person sees stands, and its answer still goes.
              if (state.asking.has(event.toolUseId)) return;
              state.asking.add(event.toolUseId);
              if (event.agentType !== undefined) state.agentAsks.add(event.toolUseId);
              yield* resolveApproval(event).pipe(
                Effect.ensuring(askOver(event.toolUseId)),
                Effect.forkIn(sessionScope),
              );
              return;
            }
            case "ended": {
              yield* endSession(event.reason);
              return;
            }
            case "approval.resolved": {
              // "detached" is the mod no longer seeing T3's heartbeat. This
              // process is here to read that, so it was a lapse: the request
              // stays, and the mod asks again when it sees T3.
              if (event.by !== "t3" && event.by !== "detached") {
                yield* settleElsewhere(event.toolUseId);
              }
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
            case "notification": {
              // What it calls about is on a card here already, or no turn of
              // the session's is open to show it in.
              const shown = state.asking.size === 0 && sessionTurnOpen();
              yield* Effect.logInfo("claude-bridge.notification", {
                sid,
                kind: event.kind,
                message: event.message,
                dialog: event.dialog,
                shown,
              });
              if (!shown) return;
              const uuid = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
              yield* emit([
                claudeSessionNoticeFrame({
                  uuid,
                  sessionId: sid,
                  subtype: "notification",
                  content: claudeSessionNotificationText(event),
                }),
              ]);
              return;
            }
            case "compact.started":
            case "compact.ended": {
              // A state of the turn that is running: with none open here
              // there is nothing to put it on, unless one opens while it lasts.
              const compacting = event.type === "compact.started";
              const shown = sessionTurnOpen();
              state.compactingUnsaid = compacting && !shown;
              yield* Effect.logInfo("claude-bridge.compacting", {
                sid,
                compacting,
                ...(event.type === "compact.started" ? { trigger: event.trigger } : {}),
                shown,
              });
              if (!shown) return;
              const uuid = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
              yield* emit([claudeCompactingFrame({ uuid, sessionId: sid, compacting })]);
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

      // The end of the replayed turn, known already, is taken up before
      // anything the session says from here: the next turn's start, coming
      // right after, then closes it first.
      if (missedTurn?.complete != null) {
        if (closeAtOnce) {
          // Without words of its own: a result's words would show as the
          // turn's reply, and the record brings that reply in as history.
          yield* emit(translator.onTurnComplete({ ...missedTurn.complete, answer: "" }));
          yield* takeUpSetAside;
        } else {
          yield* handleEvent(missedTurn.complete);
        }
      }
      yield* registry
        .subscribe(sid, (event) => handleEvent(event))
        .pipe(Effect.provideService(Scope.Scope, sessionScope));
      // From here the session's events come to this attachment. What reached
      // the list before this point is taken up now: the end of a turn open
      // here, and the end or start of a turn after the replayed one, unless
      // a turn the session began since then has opened here.
      const missedBeforeSubscribing = yield* registry.takeMissedTurns(sid);
      const replayedAt = missedBeforeSubscribing.findIndex(
        (turn) => turn.start.turnId === replayedTurnId,
      );
      for (const turn of replayedAt === -1 ? [] : missedBeforeSubscribing.slice(replayedAt)) {
        const turnId = turn.start.turnId;
        const held = state.setAside.find((entry) => entry.turn.turnId === turnId);
        const open = translator.openTurnId();
        if (held !== undefined) {
          held.end ??= turn.complete;
        } else if (open === turnId) {
          if (turn.complete !== null && state.pendingComplete?.event.turnId !== turnId) {
            yield* handleEvent(turn.complete);
          }
        } else if (open === null || shownHere.has(open)) {
          setAsideLater(turn);
        }
      }
      // At once when the turn before it is closed here already.
      yield* takeUpSetAside;
      yield* tail.pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("claude-bridge.tail-failed", { sid, cause }).pipe(
            Effect.andThen(endSession("tail failed")),
          ),
        ),
        Effect.forkIn(sessionScope),
      );
      // The heartbeat is what tells the session T3 is here, and a prompt
      // waiting for T3 is handed back when it stops. It beats on its own, so
      // slow work in the loop above (a large record, a repair) cannot stop it.
      yield* Effect.gen(function* () {
        while (!state.closed) {
          yield* Effect.sleep(`${CLAUDE_BRIDGE_T3_HEARTBEAT_MS} millis`);
          if (!state.closed) yield* registry.heartbeat(bridgeProcess, toolEndpoint);
        }
      }).pipe(Effect.forkIn(sessionScope));

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
            // The conversation moved on under another session id and this
            // process was left behind: nothing is sent into it. The thread
            // goes with the conversation the next time it opens the session;
            // only a move in the moment before this prompt lands here.
            const moved = yield* registry.continuationOf(sid, configDir);
            if (moved !== null) {
              yield* Effect.logInfo("claude-bridge.not-sent-session-continued", {
                sid,
                continuedIn: moved.sid,
                heldElsewhere: moved.heldElsewhere,
              });
              yield* emitCommandTurn({
                turnId: `not-sent-${promptUuid}`,
                text,
                promptUuid,
                ok: true,
                answer: moved.heldElsewhere
                  ? claudeSessionContinuedNotice(moved.sid)
                  : claudeSessionFollowedNotice(moved.sid),
              });
              return;
            }
            // A message from a client carries the client's copy of the settings.
            if (state.selection !== null)
              yield* registry.writeClientSelection(sid, state.selection);
            yield* followReloadedMod;
            // "!" runs a shell command in the session without asking the
            // model, as it does typed in the terminal. A session whose mod
            // cannot do that gets the text as written, as before.
            const shell = /^\s*!\s*(\S[\s\S]*)$/u.exec(text)?.[1]?.trim();
            if (shell !== undefined) {
              const live = (yield* registry.readSession(bridgeProcess)) ?? bridgeProcess.session;
              if (claudeBridgeModAtLeast(live.modVersion, CLAUDE_BRIDGE_BASH_MOD_VERSION)) {
                const body: ClaudeBridgeCommandBody = { type: "bash", command: shell, promptUuid };
                const pending = { commandId: "", promptUuid, label: `! ${shell}`, body };
                state.pendingPrompt = pending;
                pending.commandId = yield* registry.send(bridgeProcess, body);
                return;
              }
            }
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
                const moving = claudeSessionMovingNotice(slash.name);
                if (moving !== null) {
                  yield* emitCommandTurn({
                    turnId: `not-run-${promptUuid}`,
                    text: label,
                    promptUuid,
                    ok: true,
                    answer: moving,
                  });
                  return;
                }
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
            // What the person attached was saved by T3 and the prompt says
            // where: the session may read those files without asking.
            const savedForPrompt = claudeBridgeAttachmentPaths(text);
            const body: ClaudeBridgeCommandBody = {
              type: "prompt",
              // Text that only looks like a command (a path, an unknown name)
              // goes to the model as written; the leading space keeps Claude
              // Code from refusing it as a command.
              text: text.startsWith("/") ? ` ${text}` : text,
              promptUuid,
              ...(savedForPrompt.length === 0 ? {} : { attachments: savedForPrompt }),
              // Sent to steer the turn that is running: the mod hands it to
              // the model at that turn's next tool boundary.
              ...(message.priority === "now" ? { midTurn: true as const } : {}),
            };
            const pending = { commandId: "", promptUuid, label: null, body };
            state.pendingPrompt = pending;
            // Held from before it is put in the inbox, so the session taking
            // it at once is never heard before it is known to be waiting.
            if (body.midTurn === true) state.heldSteers.set(promptUuid, pending);
            pending.commandId = yield* registry
              .send(bridgeProcess, body)
              .pipe(Effect.tapError(() => Effect.sync(() => state.heldSteers.delete(promptUuid))));
            if (state.heldSteers.has(promptUuid)) yield* reportDelivery(promptUuid, "waiting");
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
        continuedElsewhere: registry
          .continuationOf(sid, configDir)
          .pipe(Effect.map((moved) => moved !== null && !moved.heldElsewhere)),
        hasPendingWake: Effect.gen(function* () {
          if (
            translator.hasOpenTurn() ||
            state.pendingComplete !== null ||
            state.setAside.length > 0 ||
            (yield* Queue.size(messages)) > 0
          ) {
            return true;
          }
          // Asked of a run that found nothing to show: one offered because
          // the session has something to ask is not an empty one.
          return yield* adoptWaitingRequestTurn;
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
          // The session was sent to the background in its terminal and the
          // conversation carries on under another session id: the thread goes
          // with the conversation, as Claude Code's own list of sessions does.
          const moved = yield* registry.continuationOf(sid, configDir);
          if (moved !== null && !moved.heldElsewhere) {
            yield* Effect.logInfo("claude-bridge.following-continuation", {
              sid,
              continuedIn: moved.sid,
            });
            const { sessionId: _named, ...options } = input.options;
            return yield* openBridge(
              { ...input, options: { ...options, resume: moved.sid } },
              configDir,
            );
          }
          return yield* openBridge(input, configDir);
        }),
    });
  }),
);
