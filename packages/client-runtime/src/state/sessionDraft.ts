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
 * (null before they first agree), and the texts it offered since, so their
 * echo back is not taken for the session's own change.
 */
export interface SessionDraftShare {
  readonly shared: string | null;
  readonly offered: ReadonlyArray<string>;
}

export const SESSION_DRAFT_SHARE_START: SessionDraftShare = { shared: null, offered: [] };

const OFFERED_KEPT = 8;

/** The share after `text` went to the session. */
export function sessionDraftOffered(share: SessionDraftShare, text: string): SessionDraftShare {
  return { shared: share.shared, offered: [...share.offered, text].slice(-OFFERED_KEPT) };
}

/**
 * One step of keeping a message box and the session's prompt box on the same
 * draft. The session's box is the authority: the message box takes its draft
 * only where the message box still holds the last shared text, and offers its
 * own only where the session still holds it (the session checks that again).
 * Where both changed, neither is overwritten and `differs` is set.
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
  if (session === local) return settled({ shared: session, offered: [] });
  let share = input.share;
  const echoed = share.offered.lastIndexOf(session);
  if (echoed >= 0) share = { shared: session, offered: share.offered.slice(echoed + 1) };
  if (share.shared === null) {
    if (local === "") return { share: { shared: session, offered: [] }, take: session, offer: null, differs: false };
    if (session !== "") return { share, take: null, offer: null, differs: true };
    share = { shared: "", offered: share.offered };
  }
  if (local === share.shared) {
    return { share: { shared: session, offered: [] }, take: session, offer: null, differs: false };
  }
  if (session === share.shared) {
    const offer = plain && !share.offered.includes(local) ? { text: local, base: session } : null;
    return { share, take: null, offer, differs: false };
  }
  return { share, take: null, offer: null, differs: true };
}
