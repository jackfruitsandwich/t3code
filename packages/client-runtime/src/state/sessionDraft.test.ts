import { describe, expect, it } from "vite-plus/test";

import {
  reconcileSessionDraft,
  SESSION_DRAFT_SHARE_START,
  sessionDraftOffered,
  type SessionDraftShare,
} from "./sessionDraft.ts";

const step = (share: SessionDraftShare, local: string, session: string | null, plain = true) =>
  reconcileSessionDraft({ share, local, session, plain });

describe("reconcileSessionDraft", () => {
  it("takes the terminal's draft while the message box holds what both last had", () => {
    const first = step(SESSION_DRAFT_SHARE_START, "", "fix the");
    expect(first.take).toBe("fix the");
    const next = step(first.share, "fix the", "fix the bug");
    expect(next).toMatchObject({ take: "fix the bug", offer: null, differs: false });
    // The terminal's send empties its box, and the message box with it.
    expect(step(next.share, "fix the bug", "").take).toBe("");
  });

  it("offers the message box's draft against the text the terminal holds, and knows its echo", () => {
    const agreed = step(SESSION_DRAFT_SHARE_START, "fix", "fix").share;
    const typed = step(agreed, "fix it", "fix");
    expect(typed.offer).toEqual({ text: "fix it", base: "fix" });
    let share = sessionDraftOffered(typed.share, "fix it");
    // Typing on before the echo: offered again against the same base.
    const more = step(share, "fix it now", "fix");
    expect(more.offer).toEqual({ text: "fix it now", base: "fix" });
    share = sessionDraftOffered(more.share, "fix it now");
    // The first offer's echo arrives: not a change of the terminal's.
    const echo = step(share, "fix it now", "fix it");
    expect(echo).toMatchObject({ take: null, differs: false });
    expect(echo.share).toEqual({ shared: "fix it", offered: ["fix it now"] });
    // The same text is not offered twice.
    expect(step(echo.share, "fix it now", "fix it").offer).toBeNull();
    expect(step(echo.share, "fix it now", "fix it now").share).toEqual({
      shared: "fix it now",
      offered: [],
    });
  });

  it("overwrites neither box where both changed", () => {
    const agreed = step(SESSION_DRAFT_SHARE_START, "fix", "fix").share;
    const both = step(agreed, "fix it", "fix that");
    expect(both).toMatchObject({ take: null, offer: null, differs: true });
    // Both still differ when the boxes first meet holding different drafts.
    expect(step(SESSION_DRAFT_SHARE_START, "mine", "yours").differs).toBe(true);
    // Once one box takes the other's, they agree again.
    expect(step(both.share, "fix that", "fix that")).toMatchObject({ differs: false });
  });

  it("offers into an empty terminal box when they first meet, and sends the empty box after a send", () => {
    expect(step(SESSION_DRAFT_SHARE_START, "left over", "").offer).toEqual({
      text: "left over",
      base: "",
    });
    const agreed = step(SESSION_DRAFT_SHARE_START, "fix it", "fix it").share;
    expect(step(agreed, "", "fix it").offer).toEqual({ text: "", base: "fix it" });
  });

  it("offers nothing that carries attachments, and forgets the share without a terminal box", () => {
    const agreed = step(SESSION_DRAFT_SHARE_START, "fix", "fix").share;
    expect(step(agreed, "fix this image", "fix", false)).toMatchObject({
      offer: null,
      differs: false,
    });
    expect(step(agreed, "fix", null)).toEqual({
      share: SESSION_DRAFT_SHARE_START,
      take: null,
      offer: null,
      differs: false,
    });
  });
});
