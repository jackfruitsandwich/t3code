/**
 * threadForkFamily - Pure folding of fork families in thread lists.
 *
 * Threads stamped with the same `forkFamilyId` are branches of one
 * conversation. In a list they collapse into a single row — the first member
 * in the list's own sort order — with the remaining branches revealed in
 * place, directly under the head, when the family is expanded. Families with
 * a single member in the given list render as plain rows. Shared by the web
 * sidebar and the mobile thread list so both surfaces fold identically.
 *
 * @module state/threadForkFamily
 */

export interface ForkFamilyRowMeta {
  readonly familyId: string;
  /** Members of this family present in the folded list. */
  readonly memberCount: number;
  readonly role: "head" | "branch";
  /** Whether the family's branches are currently revealed (head rows only). */
  readonly expanded: boolean;
}

export interface FoldedForkFamilies<T> {
  readonly rows: ReadonlyArray<T>;
  /** Meta per rendered row's thread id; absent for plain rows. */
  readonly metaByThreadId: ReadonlyMap<string, ForkFamilyRowMeta>;
}

interface ForkFamilyThread {
  readonly id: string;
  readonly environmentId?: string | undefined;
  readonly forkFamilyId?: string | null | undefined;
}

/** Thread IDs and imported family IDs are local to their environment. */
export const forkFamilyThreadKey = (thread: ForkFamilyThread): string =>
  thread.environmentId === undefined ? thread.id : `${thread.environmentId}:${thread.id}`;

const familyKey = (thread: ForkFamilyThread): string | null =>
  thread.forkFamilyId == null
    ? null
    : thread.environmentId === undefined
      ? thread.forkFamilyId
      : `${thread.environmentId}:${thread.forkFamilyId}`;

/**
 * Fold `threads` (already in display order) so each multi-member fork family
 * occupies one row, expanding in place to head-then-branches when its family
 * id is in `expandedFamilyIds`.
 */
export function foldForkFamilies<T extends ForkFamilyThread>(
  threads: ReadonlyArray<T>,
  expandedFamilyIds: ReadonlySet<string>,
  selectedThreadKey?: string | null,
): FoldedForkFamilies<T> {
  const membersByFamily = new Map<string, T[]>();
  for (const thread of threads) {
    const familyId = familyKey(thread);
    if (familyId === undefined || familyId === null) {
      continue;
    }
    const members = membersByFamily.get(familyId);
    if (members === undefined) membersByFamily.set(familyId, [thread]);
    else members.push(thread);
  }

  const rows: T[] = [];
  const metaByThreadId = new Map<string, ForkFamilyRowMeta>();
  const foldedFamilies = new Set<string>();
  for (const thread of threads) {
    const familyId = familyKey(thread);
    const members =
      familyId === undefined || familyId === null ? undefined : membersByFamily.get(familyId);
    if (
      familyId === undefined ||
      familyId === null ||
      members === undefined ||
      members.length < 2
    ) {
      rows.push(thread);
      continue;
    }
    if (foldedFamilies.has(familyId)) {
      continue;
    }
    foldedFamilies.add(familyId);
    const expanded =
      expandedFamilyIds.has(familyId) ||
      members.slice(1).some((member) => forkFamilyThreadKey(member) === selectedThreadKey);
    rows.push(thread);
    metaByThreadId.set(forkFamilyThreadKey(thread), {
      familyId,
      memberCount: members.length,
      role: "head",
      expanded,
    });
    if (expanded) {
      for (const member of members) {
        if (forkFamilyThreadKey(member) === forkFamilyThreadKey(thread)) {
          continue;
        }
        rows.push(member);
        metaByThreadId.set(forkFamilyThreadKey(member), {
          familyId,
          memberCount: members.length,
          role: "branch",
          expanded: true,
        });
      }
    }
  }
  return { rows, metaByThreadId };
}

/** Page by conversations, keeping an expanded family and the selected family intact. */
export function paginateForkFamilies<T extends ForkFamilyThread>(
  folded: FoldedForkFamilies<T>,
  limit: number,
  selectedThreadKey?: string | null,
): T[] {
  const visibleKeys = new Set<string>();
  const visibleFamilies = new Set<string>();
  let roots = 0;
  for (const thread of folded.rows) {
    const key = forkFamilyThreadKey(thread);
    const meta = folded.metaByThreadId.get(key);
    const root = meta?.role !== "branch";
    if ((root && roots < limit) || key === selectedThreadKey) {
      visibleKeys.add(key);
      if (meta) visibleFamilies.add(meta.familyId);
    }
    if (root) roots += 1;
  }
  return folded.rows.filter((thread) => {
    const key = forkFamilyThreadKey(thread);
    const meta = folded.metaByThreadId.get(key);
    return visibleKeys.has(key) || (meta !== undefined && visibleFamilies.has(meta.familyId));
  });
}
