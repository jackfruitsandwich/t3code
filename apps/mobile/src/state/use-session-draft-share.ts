import {
  createSessionDraftEnvironmentAtoms,
  reconcileSessionDraft,
  SESSION_DRAFT_SHARE_START,
  sessionDraftOffered,
} from "@t3tools/client-runtime/state/sessionDraft";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { useCallback, useEffect, useRef, useState } from "react";

import { connectionAtomRuntime } from "../connection/runtime";
import { useEnvironmentQuery } from "./query";
import { useAtomCommand } from "./use-atom-command";

export const sessionDraftEnvironment = createSessionDraftEnvironmentAtoms(connectionAtomRuntime);

// Typing settles this long before the composer's draft goes to the session.
const OFFER_DEBOUNCE_MS = 300;

/**
 * Keeps the thread composer on the same unsent draft as the prompt box of the
 * Claude Code session behind the thread (`threadId` null: none). The session's
 * box is the authority and neither box is overwritten while both changed;
 * then this returns the session's draft, to show with a way to take it.
 * `plain` is false while the draft holds what the session's box cannot.
 */
export function useSessionDraftShare(input: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId | null;
  readonly prompt: string;
  readonly plain: boolean;
  readonly setPrompt: (prompt: string) => void;
}): { readonly draft: string; readonly take: () => void; readonly dismiss: () => void } | null {
  const { environmentId, threadId, prompt, plain, setPrompt } = input;
  const draftQuery = useEnvironmentQuery(
    threadId === null
      ? null
      : sessionDraftEnvironment.draft({ environmentId, input: { threadId } }),
  );
  const session = draftQuery.data?.text ?? null;
  const sendOffer = useAtomCommand(sessionDraftEnvironment.offer, { reportFailure: false });
  const shareRef = useRef(SESSION_DRAFT_SHARE_START);
  const [differing, setDiffering] = useState<string | null>(null);
  const [dismissed, setDismissed] = useState<string | null>(null);

  useEffect(() => {
    shareRef.current = SESSION_DRAFT_SHARE_START;
    setDiffering(null);
    setDismissed(null);
  }, [environmentId, threadId]);

  useEffect(() => {
    if (threadId === null) return;
    const step = reconcileSessionDraft({ share: shareRef.current, local: prompt, session, plain });
    shareRef.current = step.share;
    setDiffering(step.differs ? session : null);
    if (step.take !== null) {
      setPrompt(step.take);
      return;
    }
    const offer = step.offer;
    if (offer === null) return;
    const timer = setTimeout(() => {
      shareRef.current = sessionDraftOffered(shareRef.current, offer.text);
      void sendOffer({ environmentId, input: { threadId, text: offer.text, base: offer.base } });
    }, OFFER_DEBOUNCE_MS);
    return () => clearTimeout(timer);
  }, [environmentId, threadId, prompt, session, plain, setPrompt, sendOffer]);

  const take = useCallback(() => {
    if (differing !== null) setPrompt(differing);
  }, [differing, setPrompt]);
  const dismiss = useCallback(() => setDismissed(differing), [differing]);
  if (differing === null || differing === dismissed) return null;
  return { draft: differing, take, dismiss };
}
