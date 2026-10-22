import * as Schema from "effect/Schema";

import { ThreadId } from "./baseSchemas.ts";

/**
 * The unsent draft in the prompt box of the provider session behind a thread:
 * the Claude Code terminal's own box, shared with the thread's message box.
 */
export const SessionDraftSubscribeInput = Schema.Struct({
  threadId: ThreadId,
});
export type SessionDraftSubscribeInput = typeof SessionDraftSubscribeInput.Type;

/**
 * Null while no prompt box of the session is there to share (no terminal on
 * it, or a provider without one). Sent first, then after every change of the
 * box; `rev` moves with each change, whoever made it.
 */
export const SessionDraftStreamEvent = Schema.NullOr(
  Schema.Struct({
    text: Schema.String,
    rev: Schema.Number,
  }),
);
export type SessionDraftStreamEvent = typeof SessionDraftStreamEvent.Type;

/**
 * The thread's message box changed. The session's box takes `text` only while
 * it still holds `base`, the draft both boxes last held; a draft typed there
 * since stays.
 */
export const SessionDraftOfferInput = Schema.Struct({
  threadId: ThreadId,
  text: Schema.String,
  base: Schema.String,
});
export type SessionDraftOfferInput = typeof SessionDraftOfferInput.Type;

export const SessionDraftOfferResult = Schema.Struct({
  /** False where no prompt box of the session is there to take it. */
  sent: Schema.Boolean,
});
export type SessionDraftOfferResult = typeof SessionDraftOfferResult.Type;
