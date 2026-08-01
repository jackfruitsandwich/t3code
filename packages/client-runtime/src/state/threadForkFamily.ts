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
  readonly forkFamilyId?: string | null | undefined;
}

/**
 * Fold `threads` (already in display order) so each multi-member fork family
 * occupies one row, expanding in place to head-then-branches when its family
 * id is in `expandedFamilyIds`.
 */
export function foldForkFamilies<T extends ForkFamilyThread>(
  threads: ReadonlyArray<T>,
  expandedFamilyIds: ReadonlySet<string>,
): FoldedForkFamilies<T> {
  const membersByFamily = new Map<string, T[]>();
  for (const thread of threads) {
    const familyId = thread.forkFamilyId;
    if (familyId === undefined || familyId === null) {
      continue;
    }
    membersByFamily.set(familyId, [...(membersByFamily.get(familyId) ?? []), thread]);
  }

  const rows: T[] = [];
  const metaByThreadId = new Map<string, ForkFamilyRowMeta>();
  const foldedFamilies = new Set<string>();
  for (const thread of threads) {
    const familyId = thread.forkFamilyId;
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
    const expanded = expandedFamilyIds.has(familyId);
    rows.push(thread);
    metaByThreadId.set(thread.id, {
      familyId,
      memberCount: members.length,
      role: "head",
      expanded,
    });
    if (expanded) {
      for (const member of members) {
        if (member.id === thread.id) {
          continue;
        }
        rows.push(member);
        metaByThreadId.set(member.id, {
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
