import { WS_METHODS } from "@t3tools/contracts";
import { Atom } from "effect/reactivity";

import type { EnvironmentRegistry } from "../connection/registry.ts";
import {
  createEnvironmentRpcCommand,
  createEnvironmentRpcSubscriptionAtomFamily,
} from "./runtime.ts";

export function createSessionDraftEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
) {
  return {
    /**
     * The unsent draft in the prompt box of the Claude Code session behind a
     * thread; null while it has none to share. Short TTL so a closed thread
     * stops watching.
     */
    draft: createEnvironmentRpcSubscriptionAtomFamily(runtime, {
      label: "environment-data:session-draft",
      tag: WS_METHODS.subscribeSessionDraft,
      idleTtlMs: 5_000,
    }),
    offer: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:session-draft:offer",
      tag: WS_METHODS.sessionDraftOffer,
    }),
  };
}

/**
 * What one message box knows of the session's draft: the last text both held
 * (null before they first agree), the texts it offered since, so their echo
 * back is not taken for the session's own change, and the text it held when it
 * met a different draft there (null once they agree).
 */
export interface SessionDraftShare {
  readonly shared: string | null;
  readonly offered: ReadonlyArray<string>;
  readonly found: string | null;
}

export const SESSION_DRAFT_SHARE_START: SessionDraftShare = {
  shared: null,
  offered: [],
  found: null,
};

const OFFERED_KEPT = 8;

/** The share after `text` went to the session. */
export function sessionDraftOffered(share: SessionDraftShare, text: string): SessionDraftShare {
  return { ...share, offered: [...share.offered, text].slice(-OFFERED_KEPT) };
}

/**
 * One step of keeping a message box and the session's prompt box on the same
 * draft. The session's box is the authority: the message box takes its draft
 * only where the message box still holds the last shared text, and offers its
 * own only where the session still holds it (the session checks that again).
 * Where both changed, neither is overwritten and `differs` is set. A draft the
 * message box held before it met the session's is never offered: only once it
 * is typed past, and only into an empty box there.
 *
 * `local` is the message box's text; `session` the session's draft, null when
 * there is none to share; `plain` whether the message box holds only text it
 * can offer (no attachments).
 */
export function reconcileSessionDraft(input: {
  readonly share: SessionDraftShare;
  readonly local: string;
  readonly session: string | null;
  readonly plain: boolean;
}): {
  readonly share: SessionDraftShare;
  readonly take: string | null;
  readonly offer: { readonly text: string; readonly base: string } | null;
  readonly differs: boolean;
} {
  const { local, session, plain } = input;
  const settled = (share: SessionDraftShare) => ({ share, take: null, offer: null, differs: false });
  if (session === null) return settled(SESSION_DRAFT_SHARE_START);
  const agreed: SessionDraftShare = { shared: session, offered: [], found: null };
  if (session === local) return settled(agreed);
  let share = input.share;
  const echoed = share.offered.lastIndexOf(session);
  if (echoed >= 0) share = { ...share, shared: session, offered: share.offered.slice(echoed + 1) };
  if (share.shared === null) {
    if (local === "") return { share: agreed, take: session, offer: null, differs: false };
    const found = share.found ?? local;
    if (session !== "" || local === found) {
      return { share: { ...share, found }, take: null, offer: null, differs: true };
    }
    share = { shared: "", offered: share.offered, found: null };
  }
  if (local === share.shared) return { share: agreed, take: session, offer: null, differs: false };
  if (session === share.shared) {
    const offer = plain && !share.offered.includes(local) ? { text: local, base: session } : null;
    return { share, take: null, offer, differs: false };
  }
  return { share, take: null, offer: null, differs: true };
}

const NOTICE_PREVIEW_CHARS = 160;

/** What a message box says of the session's draft where the two differ. */
export function sessionDraftNotice(draft: string): {
  readonly title: string;
  readonly preview: string | null;
  readonly takeLabel: string;
} {
  if (draft === "") {
    return { title: "The terminal's prompt box is empty", preview: null, takeLabel: "Clear mine" };
  }
  return {
    title: "The terminal has a different draft",
    preview:
      draft.length > NOTICE_PREVIEW_CHARS ? `${draft.slice(0, NOTICE_PREVIEW_CHARS)}…` : draft,
    takeLabel: "Use it",
  };
}
