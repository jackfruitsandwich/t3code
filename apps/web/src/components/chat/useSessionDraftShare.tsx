import {
  reconcileSessionDraft,
  SESSION_DRAFT_SHARE_START,
  sessionDraftOffered,
} from "@t3tools/client-runtime/state/sessionDraft";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { TerminalIcon } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";

import { useEnvironmentQuery } from "../../state/query";
import { sessionDraftEnvironment } from "../../state/sessionDraft";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../ui/button";
import type { ComposerBannerStackItem } from "./ComposerBannerStack";

// Typing settles this long before the message box's draft goes to the session.
const OFFER_DEBOUNCE_MS = 300;
const PREVIEW_CHARS = 160;

/**
 * Keeps the message box on the same unsent draft as the prompt box of the
 * Claude Code session behind the thread (`threadId` null: none). The session's
 * box is the authority and neither box is overwritten while both changed;
 * then the returned banner shows the session's draft and offers to take it.
 * `plain` is false while the draft holds what the session's box cannot.
 */
export function useSessionDraftShare(input: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId | null;
  readonly prompt: string;
  readonly plain: boolean;
  readonly setPrompt: (prompt: string) => void;
}): ComposerBannerStackItem | null {
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

  return useMemo(() => {
    if (differing === null || differing === dismissed) return null;
    const emptied = differing === "";
    return {
      id: "session-draft",
      variant: "info",
      priority: "notice",
      icon: <TerminalIcon />,
      title: emptied
        ? "The terminal's prompt box was emptied"
        : "The terminal has a different draft",
      ...(emptied
        ? {}
        : {
            description:
              differing.length > PREVIEW_CHARS
                ? `${differing.slice(0, PREVIEW_CHARS)}…`
                : differing,
          }),
      actions: (
        <Button size="xs" variant="ghost" onClick={() => setPrompt(differing)}>
          {emptied ? "Clear mine" : "Use it"}
        </Button>
      ),
      dismissLabel: "Keep my draft",
      onDismiss: () => setDismissed(differing),
    };
  }, [differing, dismissed, setPrompt]);
}
