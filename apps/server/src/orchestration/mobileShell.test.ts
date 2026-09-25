import type { OrchestrationShellSnapshot, OrchestrationThreadShell } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { limitShellForMobile } from "./mobileShell.ts";

const thread = (
  id: string,
  instanceId: string,
  updatedAt: string,
  latestUserMessageAt: string | null = null,
): OrchestrationThreadShell =>
  ({
    id,
    modelSelection: { instanceId, model: "m" },
    updatedAt,
    latestUserMessageAt,
  }) as unknown as OrchestrationThreadShell;

const snapshot = (threads: ReadonlyArray<OrchestrationThreadShell>): OrchestrationShellSnapshot =>
  ({
    snapshotSequence: 1,
    projects: [],
    threads,
    updatedAt: "2026-09-25T00:00:00.000Z",
  }) as unknown as OrchestrationShellSnapshot;

describe("limitShellForMobile", () => {
  it("keeps the N most recent threads of each provider, in snapshot order", () => {
    const threads = [
      thread("c1", "codex", "2026-09-01T00:00:00Z"),
      thread("w1", "wuwei", "2026-09-02T00:00:00Z"),
      thread("c2", "codex", "2026-09-03T00:00:00Z"),
      thread("a1", "claudeAgent", "2026-09-04T00:00:00Z"),
      thread("c3", "codex", "2026-09-05T00:00:00Z"),
    ];
    const limited = limitShellForMobile(snapshot(threads), 2);
    expect(limited.threads.map((t) => t.id)).toEqual(["w1", "c2", "a1", "c3"]);
  });

  it("ranks by the last user message before updatedAt", () => {
    const threads = [
      thread("old-touched", "codex", "2026-09-09T00:00:00Z", "2026-08-01T00:00:00Z"),
      thread("recent-chat", "codex", "2026-09-01T00:00:00Z", "2026-09-08T00:00:00Z"),
    ];
    expect(limitShellForMobile(snapshot(threads), 1).threads.map((t) => t.id)).toEqual([
      "recent-chat",
    ]);
  });

  it("returns the same snapshot when nothing is cut", () => {
    const input = snapshot([thread("c1", "codex", "2026-09-01T00:00:00Z")]);
    expect(limitShellForMobile(input)).toBe(input);
  });
});
