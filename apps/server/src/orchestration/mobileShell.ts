/**
 * The phone gets a short thread list. This T3 holds ~14.5k threads, almost all
 * CLI imports; the desktop renders that fine, the phone app does not. So the
 * phone sees the 20 most recent chats per provider (Codex, Claude, wuwei) and
 * the desktop and box keep the full list.
 *
 * @module orchestration/mobileShell
 */
import type { OrchestrationShellSnapshot, OrchestrationThreadShell } from "@t3tools/contracts";

export const MOBILE_RECENT_THREADS_PER_PROVIDER = 20;

const recency = (thread: OrchestrationThreadShell): string =>
  thread.latestUserMessageAt ?? thread.updatedAt;

export function limitShellForMobile(
  snapshot: OrchestrationShellSnapshot,
  perProvider = MOBILE_RECENT_THREADS_PER_PROVIDER,
): OrchestrationShellSnapshot {
  const byProvider = new Map<string, Array<OrchestrationThreadShell>>();
  for (const thread of snapshot.threads) {
    const key = thread.modelSelection.instanceId;
    const group = byProvider.get(key);
    if (group) {
      group.push(thread);
    } else {
      byProvider.set(key, [thread]);
    }
  }
  const kept = new Set<string>();
  for (const group of byProvider.values()) {
    group
      .sort((a, b) => (recency(a) < recency(b) ? 1 : recency(a) > recency(b) ? -1 : 0))
      .slice(0, perProvider)
      .forEach((thread) => kept.add(thread.id));
  }
  if (kept.size === snapshot.threads.length) {
    return snapshot;
  }
  // Keep the snapshot's own order; only membership changes.
  return { ...snapshot, threads: snapshot.threads.filter((thread) => kept.has(thread.id)) };
}
