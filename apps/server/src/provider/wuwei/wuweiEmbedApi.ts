/**
 * wuwei's in-process embed API (version 1), as T3 consumes it.
 *
 * wuwei (its own repo, default ~/CODEEEEEEEE/wuwei) builds `dist/embed.mjs`,
 * a Node ESM bundle of its harness. The wuwei driver imports it into this
 * server process; wuwei chats run here, not in a child process. This file is
 * the whole contract: plain JSON-able data, so T3 never type-checks wuwei's
 * source. Keep it in sync with wuwei's src/embed/index.ts.
 */
export const EMBED_API_VERSION = 1;

/** Module shape of dist/embed.mjs. */
export interface WuweiEmbedModule {
  readonly EMBED_API_VERSION: 1;
  /**
   * Idempotent. The first call installs wuwei's payload observer (it wraps
   * globalThis.fetch for the whole process; must happen before any provider
   * module loads), loads EXA_API_KEY from wuwei's own .env when the process
   * lacks it, and starts loading the model catalog in the background.
   */
  loadWuwei(options?: WuweiHostOptions): Promise<WuweiHost>;
}

export interface WuweiHostOptions {
  /** Defaults to wuwei's defaultThreadDir() (today ~/.wuwei/sessions), same as the TUI. */
  readonly threadDir?: string;
  /** Defaults to ~/.wuwei/t3 (system-prompt.md, memory.md, requests/). */
  readonly t3Home?: string;
}

export interface WuweiModelInfo {
  readonly slug: string; // e.g. "gpt-6-sol", "claude-opus-5-5"
  readonly name: string;
  readonly backend: string; // "codex-direct" | "anthropic-direct"
  readonly backendLabel: string;
  readonly reasoningEfforts: readonly string[]; // "low" | "medium" | ... in wuwei order
  readonly contextWindow: number;
}

export interface WuweiCatalog {
  readonly models: readonly WuweiModelInfo[];
  readonly warnings: readonly string[];
}

export interface WuweiCommandInfo {
  readonly name: string; // without the leading "/"
  readonly description: string;
  readonly hint?: string;
}

export interface WuweiAuthStatus {
  readonly codex: string; // human sentence, same text /login status prints
  readonly anthropic: string;
  readonly codexReady: boolean;
  readonly anthropicReady: boolean;
}

/** User content for one prompt. Images are base64. */
export type WuweiUserContent =
  | string
  | ReadonlyArray<
      | { readonly type: "text"; readonly text: string }
      | { readonly type: "image"; readonly data: string; readonly mimeType: string }
    >;

export interface WuweiPromptInput {
  readonly threadId: string;
  /** Absolute workspace directory for this chat (T3's thread cwd). */
  readonly cwd: string;
  readonly content: WuweiUserContent;
  /** Model slug. Omit to keep the chat's stored model. Required for a chat's first prompt. */
  readonly model?: string;
  /** One of the model's reasoningEfforts; invalid/absent → "medium" or the first allowed. */
  readonly effort?: string;
  readonly signal: AbortSignal;
  /** Steering: called at tool-loop boundaries; return queued user texts to commit now. */
  readonly takeQueuedUsers?: () => readonly string[];
  /** Called synchronously, in order, for every event of the turn. */
  readonly onEvent: (event: WuweiTurnEvent) => void;
}

/**
 * Turn events. `step` counts model requests within the turn from 0. Content
 * blocks are keyed by (step, index): index is pi-ai's contentIndex in that step.
 */
export type WuweiTurnEvent =
  | { readonly type: "step_start"; readonly step: number }
  | {
      readonly type: "thinking_delta";
      readonly step: number;
      readonly index: number;
      readonly delta: string;
    }
  | {
      readonly type: "thinking_end";
      readonly step: number;
      readonly index: number;
      readonly text: string;
    }
  | {
      readonly type: "text_delta";
      readonly step: number;
      readonly index: number;
      readonly delta: string;
    }
  | {
      readonly type: "text_end";
      readonly step: number;
      readonly index: number;
      readonly text: string;
    }
  | {
      readonly type: "tool_start";
      readonly callId: string;
      readonly name: string; // bash | read | write | edit | exa | look | calendar | ...
      readonly title: string; // same as the ACP toolTitle()
      /** Arguments with every string clipped to 4,000 chars (ACP displayArguments). */
      readonly input: Readonly<Record<string, unknown>>;
    }
  | {
      readonly type: "tool_end";
      readonly callId: string;
      readonly name: string;
      readonly isError: boolean;
      /** Text parts joined with "\n", images as "[image]", clipped to 20,000 chars. */
      readonly output: string;
    }
  | {
      /** After each model step (pi-ai "done"). Token counts are that one request's. */
      readonly type: "usage";
      readonly step: number;
      readonly model: string;
      readonly contextWindow: number;
      readonly input: number; // uncached input
      readonly cacheRead: number;
      readonly cacheWrite: number;
      readonly output: number;
      readonly reasoning?: number;
      /** usage.totalTokens || input+cacheRead+cacheWrite+output — the context size now in use. */
      readonly contextTokens: number;
    }
  | { readonly type: "steer_committed"; readonly text: string }
  /** Output of a wuwei slash command; the model never sees the command. */
  | { readonly type: "command_output"; readonly text: string }
  /** Provider error message, emitted right before prompt() settles with stopReason "error". */
  | { readonly type: "error"; readonly message: string };

export interface WuweiTurnTotals {
  readonly input: number;
  readonly cacheRead: number;
  readonly cacheWrite: number;
  readonly output: number;
  readonly reasoning: number;
}

export interface WuweiPromptResult {
  /** "command" = the prompt was a wuwei slash command, no model call. */
  readonly stopReason: "end_turn" | "cancelled" | "error" | "command";
  readonly errorMessage?: string;
  readonly totals: WuweiTurnTotals;
  readonly model?: string;
}

/** Display transcript rebuilt from a session file (for chat history in T3). */
export interface WuweiTranscript {
  readonly id: string;
  readonly label: string;
  readonly createdAt: number; // epoch ms
  readonly updatedAt: number; // file mtime, epoch ms
  readonly workspaceRoot: string | null;
  readonly model: string | null; // stored backend model slug
  readonly turns: readonly WuweiTranscriptTurn[];
}

export interface WuweiTranscriptTurn {
  /** Context item id of the user message ("u12"). */
  readonly userItemId: string;
  readonly userText: string;
  readonly userImageCount: number;
  readonly at: number; // epoch ms (message timestamp; falls back to the file's createdAt)
  readonly entries: readonly WuweiTranscriptEntry[];
}

export type WuweiTranscriptEntry =
  | { readonly kind: "reasoning"; readonly id: string; readonly text: string; readonly at: number }
  | { readonly kind: "text"; readonly id: string; readonly text: string; readonly at: number }
  | {
      readonly kind: "tool";
      readonly id: string; // tool call id
      readonly name: string;
      readonly title: string;
      readonly input: Readonly<Record<string, unknown>>; // clipped like tool_start
      readonly output: string | null; // clipped like tool_end; null when no result was recorded
      readonly status: "completed" | "failed" | "cancelled" | "unknown" | "missing";
      readonly startedAt: number;
      readonly completedAt: number | null;
    }
  /** A user message committed mid-turn (steer). */
  | { readonly kind: "steer"; readonly id: string; readonly text: string; readonly at: number };

export interface WuweiThreadInfo {
  readonly id: string;
  readonly mtime: number;
  readonly workspaceRoot: string | null;
  readonly label: string;
  readonly messages: number;
}

export interface WuweiHost {
  readonly apiVersion: 1;
  readonly threadDir: string;
  /** Cached; `refresh` refetches. Never throws: failures land in warnings. */
  catalog(options?: { readonly refresh?: boolean }): Promise<WuweiCatalog>;
  commands(): readonly WuweiCommandInfo[];
  authStatus(): Promise<WuweiAuthStatus>;
  /**
   * Allocate a new chat for `cwd` and return its id. Nothing is written until
   * the chat's first event (wuwei semantics); the host keeps the unwritten
   * Thread in memory until then.
   */
  createThread(cwd: string): string;
  /** True when the session file exists or the host holds the unwritten chat. */
  hasThread(threadId: string): boolean;
  /**
   * Run one prompt. Loads (locks) the chat, runs a slash command or a model
   * turn, then releases the lock, so the wuwei TUI can open the chat between
   * turns. Rejects only for setup failures (unknown chat, chat open in the
   * TUI, workspace mismatch, unknown model, backend switch); turn failures
   * resolve with stopReason "error" or "cancelled".
   */
  prompt(input: WuweiPromptInput): Promise<WuweiPromptResult>;
  /** Read-only replay (no lock). Throws if the session file is missing. */
  transcript(threadId: string): WuweiTranscript;
  listThreads(): readonly WuweiThreadInfo[];
}
